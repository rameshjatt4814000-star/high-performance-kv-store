.PHONY: build test run-cluster clean lint

build:
	@echo "Building server binary..."
	go build -o bin/server cmd/server/main.go

test:
	@echo "Running unit tests with race detector..."
	go test -v -race ./...

run-cluster:
	@echo "Spinning up 3-node distributed cluster..."
	docker-compose -f deploy/docker-compose.yml up --build

clean:
	@echo "Cleaning up build artifacts, data folders, and logs..."
	rm -rf bin/
	rm -rf data/ node1_data/ node2_data/ node3_data/
	rm -f *.log *.wal *.bin

lint:
	@echo "Running go vet..."
	go vet ./...
