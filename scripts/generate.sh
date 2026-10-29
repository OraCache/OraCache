#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
# Reproducible Linux x86_64 toolchain, kept entirely in the repository.
mkdir -p .tools/bin
archive=.tools/protoc-27.2-linux-x86_64.zip
if [[ ! -f "$archive" ]]; then
  curl -fL --retry 2 https://github.com/protocolbuffers/protobuf/releases/download/v27.2/protoc-27.2-linux-x86_64.zip -o "$archive"
fi
printf '%s  %s\n' 4a95e0ea2e51720af86a92f48d4997c8756923a9d0c58fd8a850657cd7479caf "$archive" | sha256sum -c -
unzip -oq "$archive" -d .tools/protoc
GOBIN="$PWD/.tools/bin" go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.34.2
GOBIN="$PWD/.tools/bin" go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.4.0
PATH="$PWD/.tools/bin:$PATH" .tools/protoc/bin/protoc -I proto -I .tools/protoc/include \
  --go_out=. --go_opt=module=github.com/OraCache/oracache \
  --go-grpc_out=. --go-grpc_opt=module=github.com/OraCache/oracache \
  proto/cache/gateway.proto
