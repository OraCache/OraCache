.PHONY: build docker-build test test-gateway test-protobuf test-cli vet gen generate test-e2e up down

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
	go build -o bin/storage-node ./cmd/storage-node
	go build -o bin/cp-api ./cmd/cp-api
	go build -o bin/oracache-node ./cmd/oracache-node
	go build -o bin/api-gateway ./cmd/api-gateway

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

# Воспроизводимые генераторы, устанавливаются только в .tools/. Одна и та же
# версия protoc используется в Linux CI и локальных macOS-проверках.
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
		proto/cache/gateway.proto proto/cache/membership.proto proto/controlplane/v1/controlplane.proto proto/replication/v1/replication.proto

test-e2e:
	mkdir -p $(REPORTS)
	rm -f $(REPORTS)/e2e.xml $(REPORTS)/e2e-cp.xml $(REPORTS)/e2e-replication.xml
	$(MAKE) $(GOTESTSUM)
	# Separate Go invocations keep the Compose projects in separate Ryuk sessions.
	cd integration/e2e && GOWORK=off $(GOTESTSUM) --format testname --junitfile $(REPORTS)/e2e.xml -- -tags=e2e -count=1 -timeout=10m -skip '^(TestControlPlaneGatewayFailover|TestRegionReplication)' ./...
	cd integration/e2e && GOWORK=off $(GOTESTSUM) --format testname --junitfile $(REPORTS)/e2e-cp.xml -- -tags=e2e -count=1 -timeout=10m -run '^TestControlPlaneGatewayFailover$$' .
	cd integration/e2e && GOWORK=off $(GOTESTSUM) --format testname --junitfile $(REPORTS)/e2e-replication.xml -- -tags=e2e -count=1 -timeout=10m -run '^TestRegionReplication' .

docker-build:
	docker compose build gateway

up:
	docker compose up -d --build

down:
	docker compose down

.PHONY: demo-build demo-up demo-up-a demo-up-b demo-up-learner demo-stop-node demo-down
DEMO := docker compose -f compose.replication.yaml

demo-build:
	$(DEMO) build cp-api

demo-up: demo-build
	$(DEMO) up -d --no-build --no-recreate
	$(call demo-wait-route,api-gateway,write,.write_routes["0"].NodeID // "" | length > 0)
	$(call demo-wait-route,api-gateway-b,local read,.read_routes["0"].Region == "region-b" and (.read_routes["0"].NodeID // "" | length > 0))

define demo-wait-route
	@command -v curl >/dev/null && command -v jq >/dev/null || { echo '$@ requires curl and jq' >&2; exit 1; }
	@addr=$$($(DEMO) port $(1) 9466) && test -n "$$addr" || exit 1; \
	url="http://$$addr/routes"; \
	echo "Waiting for $(1) $(2) route at $$url"; \
	for attempt in $$(seq 30); do \
		if curl -fs --max-time 1 "$$url" | jq -e '$(3)' >/dev/null 2>&1; then \
			echo '$(1) $(2) route is ready'; exit 0; \
		fi; \
		sleep 1; \
	done; \
	echo "No $(1) $(2) route at $$url; check: $(DEMO) logs --tail=50 $(1) cp-api" >&2; exit 1
endef

demo-up-a: demo-build
	$(DEMO) up -d --no-build --no-recreate api-gateway a-1 a-2 a-3 prometheus grafana
	$(call demo-wait-route,api-gateway,write,.write_routes["0"].NodeID // "" | length > 0)

demo-up-b:
	$(DEMO) up -d --no-build --no-recreate b-1 b-2 b-3 api-gateway-b
	$(call demo-wait-route,api-gateway-b,local read,.read_routes["0"].Region == "region-b" and (.read_routes["0"].NodeID // "" | length > 0))

demo-up-learner:
	$(DEMO) --profile learner up -d --no-build --no-recreate a-4

demo-stop-node:
	$(if $(and $(filter 1,$(words $(ID))),$(filter a-1 a-2 a-3 a-4 b-1 b-2 b-3,$(ID))),,$(error ID must be a storage node ID))
	$(DEMO) --profile learner stop $(ID)

demo-down:
	$(DEMO) --profile learner down -v
