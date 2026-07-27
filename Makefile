.PHONY: build test docker-build demo bench lint clean

build:
	@mkdir -p bin
	go build -o ./bin/broker ./cmd/broker
	go build -o ./bin/client ./cmd/client

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
	@echo "Benchmark requires a running cluster. Starting temp cluster..."
	docker-compose up -d --build
	@sleep 5
	go run cmd/client/main.go --benchmark produce --messages 100000
	go run cmd/client/main.go --benchmark consume
	@docker-compose down -v

lint:
	golangci-lint run ./...

clean:
	rm -rf ./bin ./data *.log *.index *.lock *.json
