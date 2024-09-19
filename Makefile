PROJECT_PATH = $(shell pwd)

build:
	go build -o fq-connector-go ./app

run: build
	./fq-connector-go server -c ./app/server/config/config.debug.yaml	

lint:
	golangci-lint run --fix ./app/... ./common/... ./tests/... ./tools/...

unit_test:
	go test ./app/... ./common/... ./tests/utils/...

integration_test: integration_test_build
	./fq-connector-go-tests -projectPath=$(PROJECT_PATH) -test.failfast

integration_test_build: 
	go test -c -o fq-connector-go-tests ./tests

integration_test_env_clean:
	docker compose -f ./tests/infra/datasource/docker-compose.yaml stop
	docker compose -f ./tests/infra/datasource/docker-compose.yaml rm -f -v

integration_test_env_run: integration_test_env_clean
	docker compose -f ./tests/infra/datasource/docker-compose.yaml up -d --build

test_coverage: integration_test_env_run
	go test -coverpkg=./... -coverprofile=coverage_unit_tests.out -covermode=atomic ./app/... ./common/... ./tests/utils/...
	sleep 10
	go test -c -o fq-connector-go-tests -coverpkg=./... -covermode=atomic ./tests
	./fq-connector-go-tests -projectPath=$(PROJECT_PATH) -test.coverprofile=coverage_integration_tests.out 
	cat coverage_unit_tests.out | grep -v 'pb.go\|mock.go\|library' > coverage.out
	cat coverage_integration_tests.out | grep -v 'atomic\|pb.go\|mock.go\|library' >> coverage.out
	go tool cover -func=coverage.out
	
build_image_base: 
	docker build -t ghcr.io/ydb-platform/fq-connector-go:base -f ./Dockerfile.base .

build_image_release: 
	docker build --network=host -t ghcr.io/ydb-platform/fq-connector-go:latest -f ./Dockerfile.release .

cloc:
	cloc ./app ./common ./tests --git 

docker_compose_update:
	go run ./tools/docker_compose_update -path="$(path)"

generate_docs:
	python3 ./docs/generate.py ./docs
