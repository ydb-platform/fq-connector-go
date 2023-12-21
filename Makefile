build:
	go build -o fq-connector-go ./app

build_image_base: 
	docker build -t ghcr.io/ydb-platform/fq-connector-go:base -f ./Dockerfile.base .

build_image: build
	docker build -t ghcr.io/ydb-platform/fq-connector-go:latest -f ./Dockerfile.release .
