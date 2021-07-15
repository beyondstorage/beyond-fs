SHELL := /bin/bash

.PHONY: all check format vet build test generate tidy

help:
	@echo "Please use \`make <target>\` where <target> is one of"
	@echo "  check               to do static check"
	@echo "  build               to create bin directory and build"
	@echo "  test                to run test"

format:
	go fmt ./...

vet:
	go vet ./...

generate:
	go generate ./...

build: tidy generate format vet
	go build -o bin/beyondfs ./cmd/beyondfs

test:
	go test -race -coverprofile=coverage.txt -covermode=atomic -v ./...
	go tool cover -html="coverage.txt" -o "coverage.html"

tidy:
	go mod tidy
	go mod verify

clean:
	find . -type f -name '*gen*.go' -delete
