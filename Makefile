.PHONY: build docker-build test test-gateway test-protobuf test-cli vet gen generate test-e2e up down poc-up poc-down poc-smoke

.DEFAULT_GOAL := build

GOTESTSUM_VERSION := v1.13.0
GOTESTSUM := $(CURDIR)/.tools/bin/gotestsum-$(GOTESTSUM_VERSION)
REPORTS := $(CURDIR)/.reports

$(GOTESTSUM):
	mkdir -p .tools/bin
	GOBIN="$(CURDIR)/.tools/bin" GOWORK=off go install gotest.tools/gotestsum@$(GOTESTSUM_VERSION)
	mv .tools/bin/gotestsum "$@"

build:
	go build -o bin/oracache ./cmd/oracache
	go build -o bin/gateway ./cmd/gateway
	go build -o bin/cp-api ./cmd/cp-api
	go build -o bin/storage-node ./cmd/storage-node
	go build -o bin/cache-gateway ./cmd/cache-gateway

# Clear every expected report before even installing tools: an aborted run must
# never publish an older module's results. Individual targets do the same.
test:
	mkdir -p $(REPORTS)
	rm -f $(REPORTS)/gateway.xml $(REPORTS)/protobuf.xml $(REPORTS)/cli.xml
	@status=0; for module in gateway protobuf cli; do \
		$(MAKE) test-$$module || status=1; \
	done; exit $$status

define unit-test
test-$(1):
	mkdir -p $(REPORTS)
	rm -f $(REPORTS)/$(1).xml
	$$(MAKE) $$(GOTESTSUM)
	cd $(2) && GOWORK=off $$(GOTESTSUM) --format testname --junitfile $$(REPORTS)/$(1).xml -- -race ./...
endef

$(eval $(call unit-test,gateway,.))
$(eval $(call unit-test,protobuf,gen))
$(eval $(call unit-test,cli,cmd/oracache))

vet:
	@set -e; for module in . gen cmd/oracache; do \
		(cd $$module && GOWORK=off go vet ./...); \
	done

# Reproducible generators, installed only under .tools/. The same protoc
# version is used on Linux CI and local macOS checkouts.
PROTOC_VERSION := 27.2
UNAME_S := $(shell uname -s)
UNAME_M := $(shell uname -m)
ifeq ($(UNAME_S),Darwin)
  ifeq ($(UNAME_M),arm64)
    PROTOC_PLATFORM := osx-aarch_64
    PROTOC_SHA256 := 877de17b5d2662b96e68a6e208cb1851437ab3e2b419c2ef5b7b873ffac5357d
  else
    PROTOC_PLATFORM := osx-x86_64
    PROTOC_SHA256 := abc25a236571612d45eb4b6b6e6abe3ac9aecc34b195f76f248786844f5619c7
  endif
else
  PROTOC_PLATFORM := linux-x86_64
  PROTOC_SHA256 := 4a95e0ea2e51720af86a92f48d4997c8756923a9d0c58fd8a850657cd7479caf
endif
PROTOC_ARCHIVE := .tools/protoc-$(PROTOC_VERSION)-$(PROTOC_PLATFORM).zip

$(PROTOC_ARCHIVE):
	mkdir -p .tools
	curl -fL --retry 2 https://github.com/protocolbuffers/protobuf/releases/download/v$(PROTOC_VERSION)/protoc-$(PROTOC_VERSION)-$(PROTOC_PLATFORM).zip -o $@

gen: generate

generate: $(PROTOC_ARCHIVE)
	printf '%s  %s\n' '$(PROTOC_SHA256)' '$(PROTOC_ARCHIVE)' | sha256sum -c -
	unzip -oq $(PROTOC_ARCHIVE) -d .tools/protoc
	mkdir -p .tools/bin
	GOTOOLCHAIN=go1.27.3 GOBIN="$(CURDIR)/.tools/bin" go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.34.2
	GOTOOLCHAIN=go1.27.3 GOBIN="$(CURDIR)/.tools/bin" go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.4.0
	PATH="$(CURDIR)/.tools/bin:$$PATH" .tools/protoc/bin/protoc -I proto -I .tools/protoc/include \
		--go_out=. --go_opt=module=github.com/OraCache/oracache \
		--go-grpc_out=. --go-grpc_opt=module=github.com/OraCache/oracache \
		proto/cache/gateway.proto proto/controlplane/v1/controlplane.proto

test-e2e:
	mkdir -p $(REPORTS)
	rm -f $(REPORTS)/e2e.xml
	$(MAKE) $(GOTESTSUM)
	cd integration/e2e && GOWORK=off $(GOTESTSUM) --format testname --junitfile $(REPORTS)/e2e.xml -- -tags=e2e -count=1 -timeout=10m ./...

docker-build:
	docker compose build gateway

up:
	docker compose up -d --build

down:
	docker compose down

# Single-region, single-shard PoC (Compose project oracache-poc, ports 28080+).
poc-up:
	docker compose -f compose.poc.yaml up -d --build

poc-down:
	docker compose -f compose.poc.yaml down

# CLI smoke plus kill-the-elected-leader failover; needs poc-up first.
poc-smoke:
	go build -o bin/oracache ./cmd/oracache
	bash scripts/poc-smoke.sh
