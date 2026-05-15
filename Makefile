VERSION   ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")
BUILD_TIME ?= $(shell date -u +%Y-%m-%dT%H:%M:%SZ)
IMAGE     ?= ghcr.io/echomessenger/tinode-router
PLATFORM  ?= linux/amd64

.PHONY: deps tidy test coverage build lint docker-build docker-push

# ---- Go ----

deps:
	go mod download

tidy:
	go mod tidy

test:
	go test ./...

coverage:
	go test ./... -coverprofile=coverage.out
	@echo "Coverage report generated: coverage.out"

build:
	mkdir -p bin
	go build \
		-trimpath \
		-ldflags="-s -w -X main.version=$(VERSION) -X main.buildTime=$(BUILD_TIME)" \
		-o bin/router \
		./main.go
	@echo "built bin/router  version=$(VERSION)"

lint:
	@which golangci-lint > /dev/null || (echo "install golangci-lint: https://golangci-lint.run/usage/install/" && exit 1)
	golangci-lint run ./...

# ---- Docker ----

docker-build:
	docker build \
		--platform $(PLATFORM) \
		--build-arg VERSION=$(VERSION) \
		--build-arg BUILD_TIME=$(BUILD_TIME) \
		-t $(IMAGE):$(VERSION) \
		-t $(IMAGE):latest \
		.
	@echo "built $(IMAGE):$(VERSION)"

docker-push:
	docker push $(IMAGE):$(VERSION)
	docker push $(IMAGE):latest
