APP_NAME := sylos-api

.PHONY: build run test tidy fmt

build:
	go build ./...

run:
	go run ./cmd/server

test:
	go test ./...

tidy:
	go mod tidy

fmt:
	go fmt ./...


