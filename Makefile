.PHONY: build docker-build test test-gateway test-protobuf test-cli vet gen generate test-e2e up down test-report test-report-tools

.DEFAULT_GOAL := build

GOTESTSUM_VERSION := v1.12.0
GOTESTSUM := $(CURDIR)/.tools/bin/gotestsum-$(GOTESTSUM_VERSION)
REPORTS := $(CURDIR)/.reports

$(GOTESTSUM):
	mkdir -p .tools/bin
	GOBIN="$(CURDIR)/.tools/bin" GOWORK=off go install gotest.tools/gotestsum@$(GOTESTSUM_VERSION)
	mv .tools/bin/gotestsum "$@"

build:
	go build -o bin/oracache ./cmd/oracache
	go build -o bin/gateway ./cmd/gateway

# Clear every expected report before even installing tools: an aborted run must
# never publish an older module's results. Individual targets do the same.
test:
	mkdir -p $(REPORTS)
	rm -f $(REPORTS)/gateway.xml $(REPORTS)/protobuf.xml $(REPORTS)/cli.xml
	$(MAKE) test-report-tools
	$(MAKE) test-gateway
	$(MAKE) test-protobuf
	$(MAKE) test-cli

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

test-report-tools:
	python3 -m unittest discover -s tools -v

# Reporting never substitutes for the test targets or changes their exit codes.
test-report:
	python3 tools/testreport.py

vet:
	@set -e; for module in . gen cmd/oracache; do \
		(cd $$module && GOWORK=off go vet ./...); \
	done

# Reproducible Linux x86_64 generators, installed only under .tools/.
PROTOC_VERSION := 27.2
PROTOC_ARCHIVE := .tools/protoc-$(PROTOC_VERSION)-linux-x86_64.zip
PROTOC_SHA256 := 4a95e0ea2e51720af86a92f48d4997c8756923a9d0c58fd8a850657cd7479caf

.tools/protoc-$(PROTOC_VERSION)-linux-x86_64.zip:
	mkdir -p .tools
	curl -fL --retry 2 https://github.com/protocolbuffers/protobuf/releases/download/v$(PROTOC_VERSION)/protoc-$(PROTOC_VERSION)-linux-x86_64.zip -o $@

gen: generate

generate: $(PROTOC_ARCHIVE)
	printf '%s  %s\n' '$(PROTOC_SHA256)' '$(PROTOC_ARCHIVE)' | sha256sum -c -
	unzip -oq $(PROTOC_ARCHIVE) -d .tools/protoc
	mkdir -p .tools/bin
	GOBIN="$(CURDIR)/.tools/bin" go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.34.2
	GOBIN="$(CURDIR)/.tools/bin" go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.4.0
	PATH="$(CURDIR)/.tools/bin:$$PATH" .tools/protoc/bin/protoc -I proto -I .tools/protoc/include \
		--go_out=. --go_opt=module=github.com/OraCache/oracache \
		--go-grpc_out=. --go-grpc_opt=module=github.com/OraCache/oracache proto/cache/gateway.proto

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
