# syntax=docker/dockerfile:1
FROM golang:1.27.3-alpine AS build
WORKDIR /src
ENV GOWORK=off CGO_ENABLED=0
COPY go.mod go.sum ./
COPY gen/go.mod gen/go.sum ./gen/
RUN go mod download
COPY gen ./gen
COPY internal ./internal
COPY shardraft ./shardraft
COPY cmd ./cmd
RUN --mount=type=cache,target=/root/.cache/go-build \
    go build -trimpath -o /out/ ./cmd/cp-api ./cmd/storage-node ./cmd/api-gateway && mkdir /state

FROM scratch
COPY --from=build /out/ /
COPY --chown=65532:65532 --from=build /state/ /state/
COPY configs/cp-raft-compose.json /cp-config.json
USER 65532:65532
