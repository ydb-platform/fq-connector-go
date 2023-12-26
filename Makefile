build:
	go build -o fq-connector-go ./app

run: build
	./fq-connector-go server -c ./example.conf	

lint:
	golangci-lint run ./app/... ./tests/...

unit_test:
	go test -v ./app/...

PROJECT_PATH = $(shell pwd)
integration_test:
	go test -c -o fq-connector-go-tests ./tests
	./fq-connector-go-tests -projectPath=$(PROJECT_PATH)

build_image_base: 
	docker build -t ghcr.io/ydb-platform/fq-connector-go:base -f ./Dockerfile.base .
