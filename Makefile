BIN_DIR := ./bin

.PHONY: build test docker-build demo bench lint clean build-broker build-client build-api-gateway build-auth-cli build-dlq-replay

build: build-broker build-client build-api-gateway build-auth-cli build-dlq-replay

build-broker:
	@mkdir -p $(BIN_DIR)
	go build -o $(BIN_DIR)/broker ./cmd/broker

build-client:
	@mkdir -p $(BIN_DIR)
	go build -o $(BIN_DIR)/client ./cmd/client

build-api-gateway:
	@mkdir -p $(BIN_DIR)
	go build -o $(BIN_DIR)/api-gateway ./cmd/api-gateway

build-auth-cli:
	@mkdir -p $(BIN_DIR)
	go build -o $(BIN_DIR)/auth-cli ./cmd/auth-cli

build-dlq-replay:
	@mkdir -p $(BIN_DIR)
	go build -o $(BIN_DIR)/dlq-replay ./cmd/dlq-replay

test:
	go test ./... -race -cover

docker-build:
	docker build -t kafkalite:latest .

demo:
ifeq ($(OS),Windows_NT)
	powershell -ExecutionPolicy Bypass -File .\demo.ps1
else
	@chmod +x ./demo.sh
	./demo.sh
endif

bench:
	@echo "Running benchmarking script..."
	@mkdir -p scripts
	@chmod +x scripts/benchmark.sh
	./scripts/benchmark.sh

lint:
	golangci-lint run ./...

clean:
	rm -rf ./bin ./data *.log *.index *.lock *.json
