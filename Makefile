.PHONY: build test vet gen generate test-e2e up down

build:
	go build -o bin/oracache ./cmd/oracache
	go build -o bin/gateway ./cmd/gateway

test:
	go test -race ./...

vet:
	go vet ./...

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
	go test -tags=e2e -count=1 -timeout=10m -v ./integration/e2e

up:
	docker compose up -d --build

down:
	docker compose down
