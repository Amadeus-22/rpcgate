APP      := rpcgate
BIN      := bin/$(APP)
IMAGE    ?= $(APP):dev
GO       ?= go

.PHONY: build test test-integration lint check run docker clean

build:
	CGO_ENABLED=0 $(GO) build -trimpath -ldflags="-s -w" -o $(BIN) ./cmd/$(APP)

test:
	$(GO) test -race ./...

# Integration tests run against the docker compose stack (three anvil nodes).
test-integration:
	$(GO) test -race -tags integration ./...

lint:
	$(GO) vet ./...
	@out=$$(gofmt -l .); if [ -n "$$out" ]; then echo "gofmt needed:"; echo "$$out"; exit 1; fi

check: lint test

run: build
	@test -f .env || { echo ".env missing: cp .env.example .env"; exit 1; }
	set -a && . ./.env && set +a && ./$(BIN)

docker:
	docker build -f deploy/Dockerfile -t $(IMAGE) .

clean:
	rm -rf bin
