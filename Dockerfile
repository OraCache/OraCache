FROM golang:1.22.2-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY cmd/gateway ./cmd/gateway
COPY internal/cache ./internal/cache
COPY gen ./gen
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /gateway ./cmd/gateway

FROM scratch
COPY --from=build /gateway /gateway
USER 65532:65532
EXPOSE 50051
ENTRYPOINT ["/gateway"]
CMD ["--listen", "0.0.0.0:50051"]
