build:
	go build -o fq-connector-go ./app

run: build
	./fq-connector-go server -c ./example.conf	

lint:
	golangci-lint run ./app/...

unit_test:
	go test -v ./app/...

build_image_base: 
	docker build -t ghcr.io/ydb-platform/fq-connector-go:base -f ./Dockerfile.base .
