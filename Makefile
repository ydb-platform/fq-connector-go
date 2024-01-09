PROJECT_PATH = $(shell pwd)

build:
	go build -o fq-connector-go ./app

run: build
	./fq-connector-go server -c ./example.conf	

lint:
	golangci-lint run ./app/... ./tests/...

unit_test:
	go test -v ./app/...

integration_test: integration_test_env
	go test -c -o fq-connector-go-tests ./tests
	./fq-connector-go-tests -projectPath=$(PROJECT_PATH)

integration_test_env:
	docker-compose -f ./tests/infra/datasource/docker-compose.yaml stop
	docker-compose -f ./tests/infra/datasource/docker-compose.yaml rm -f -v 
	docker-compose -f ./tests/infra/datasource/docker-compose.yaml up -d

test_coverage: integration_test_env
	go test -coverpkg=./... -coverprofile=coverage_unit_tests.out -covermode=atomic ./app/...
	go test -c -o fq-connector-go-tests -coverpkg=./... -covermode=atomic ./tests
	./fq-connector-go-tests -projectPath=$(PROJECT_PATH) -test.coverprofile=coverage_integration_tests.out 
	cat coverage_unit_tests.out | grep -v 'pb.go\|mock.go\|library' > coverage.out
	cat coverage_integration_tests.out | grep -v 'atomic\|pb.go\|mock.go\|library' >> coverage.out
	go tool cover -func=coverage.out
	
build_image_base: 
	docker build -t ghcr.io/ydb-platform/fq-connector-go:base -f ./Dockerfile.base .
