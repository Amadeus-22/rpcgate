# syntax=docker/dockerfile:1

FROM golang:1.24-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/rpcgate ./cmd/rpcgate

# distroless/static: no shell, no package manager, CA certificates for HTTPS
# upstreams, and a non-root user (uid 65532).
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/rpcgate /usr/local/bin/rpcgate
USER nonroot:nonroot
EXPOSE 8545
ENV RPCGATE_LISTEN_ADDR=:8545 \
    RPCGATE_CONFIG=/etc/rpcgate/config.yaml
ENTRYPOINT ["/usr/local/bin/rpcgate"]
