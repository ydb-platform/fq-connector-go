build:
	go build -o fq-connector-go ./app

unit_test:
	go test -v ./app/...

build_image_base: 
	docker build -t ghcr.io/ydb-platform/fq-connector-go:base -f ./Dockerfile.base .
