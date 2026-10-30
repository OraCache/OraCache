# syntax=docker/dockerfile:1
FROM golang:1.27.3-alpine AS build
ARG TARGETPLATFORM
WORKDIR /src
ENV GOWORK=off
COPY go.mod go.sum ./
COPY gen/go.mod gen/go.sum ./gen/
# Keep modules in a layer: CI exports layers, not mutable cache mounts.
RUN go mod download
COPY cmd/gateway ./cmd/gateway
COPY internal/cache ./internal/cache
COPY gen ./gen
RUN --mount=type=cache,id=oracache-go1.27.3-${TARGETPLATFORM},target=/root/.cache/go-build,sharing=locked \
    CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /gateway ./cmd/gateway

FROM scratch
COPY --from=build /gateway /gateway
USER 65532:65532
EXPOSE 50051
ENTRYPOINT ["/gateway"]
CMD ["--listen", "0.0.0.0:50051"]
