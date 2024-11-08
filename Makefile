projectPath = $(shell pwd)

build:
	go build -o fq-connector-go ./app

run: build
	./fq-connector-go server -c ./app/server/config/config.debug.yaml	

lint:
	golangci-lint run --fix ./app/... ./common/... ./tests/... ./tools/...

unit_test:
	go test ./app/... ./common/... ./tests/utils/...

integration_test: integration_test_build
	./fq-connector-go-tests -projectPath="$(projectPath)" -suiteName="$(suiteName)" -test.failfast

integration_test_build: 
	go test -c -o fq-connector-go-tests ./tests

integration_test_env_clean:
	docker compose -f ./tests/infra/datasource/docker-compose.yaml stop
	docker compose -f ./tests/infra/datasource/docker-compose.yaml rm -f -v

integration_test_env_run: integration_test_env_clean
	docker compose -f ./tests/infra/datasource/docker-compose.yaml up -d --build --pull=always

test_coverage: integration_test_env_run
	go test -coverpkg=./... -coverprofile=coverage_unit_tests.out -covermode=atomic ./app/... ./common/... ./tests/utils/...
	sleep 10
	go test -c -o fq-connector-go-tests -coverpkg=./... -covermode=atomic ./tests
	./fq-connector-go-tests -projectPath="$(projectPath)" -test.coverprofile=coverage_integration_tests.out 
	cat coverage_unit_tests.out | grep -v 'pb.go\|mock.go\|library' > coverage.out
	cat coverage_integration_tests.out | grep -v 'atomic\|pb.go\|mock.go\|library' >> coverage.out
	go tool cover -func=sudo apt install cloc 

docker_compose_update:
	go run ./tools/docker_compose_update -path="$(path)"

generate_docs:
	python3 ./docs/generate.py ./docs

count_lines:
	cloc --vcs=git . --exclude-dir=library,api --exclude-ext=pb.go