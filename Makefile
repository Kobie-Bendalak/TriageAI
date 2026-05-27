BINARY := triage
BUILD_DIR := dist
VERSION := $(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")
LDFLAGS := -ldflags "-X main.Version=$(VERSION)"

.PHONY: build install clean test

build:
	go build $(LDFLAGS) -o $(BUILD_DIR)/$(BINARY) ./cmd/triage

install:
	go install $(LDFLAGS) ./cmd/triage

clean:
	rm -rf $(BUILD_DIR)

test:
	go test ./...

release:
	GOOS=darwin GOARCH=amd64  go build $(LDFLAGS) -o $(BUILD_DIR)/$(BINARY)-darwin-amd64  ./cmd/triage
	GOOS=darwin GOARCH=arm64  go build $(LDFLAGS) -o $(BUILD_DIR)/$(BINARY)-darwin-arm64  ./cmd/triage
	GOOS=linux  GOARCH=amd64  go build $(LDFLAGS) -o $(BUILD_DIR)/$(BINARY)-linux-amd64   ./cmd/triage
