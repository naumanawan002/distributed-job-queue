.PHONY: all build test run docker-up docker-down clean

BIN_DIR := ./bin

all: build test

build:
	@mkdir -p $(BIN_DIR)
	go build -o $(BIN_DIR)/queue-server ./cmd/server
	@echo "Build complete. Artifact written to $(BIN_DIR)/"

test:
	go test -v -race ./pkg/...

run:
	go run ./cmd/server -port :8090 -workers 4

docker-up:
	docker compose up --build

docker-down:
	docker compose down

clean:
	rm -rf $(BIN_DIR)
