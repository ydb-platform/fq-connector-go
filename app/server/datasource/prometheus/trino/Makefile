build-env:
	docker build -t go_env:latest . -f env.dockerfile

up:
	docker compose up -d

trino-result:
	docker exec -it trino sh -c 'cd bin && ./trino -f /etc/trino/info.trino'

down:
	@docker compose down

rm: down
	@docker image rm go_env:latest

init-mod:
	go mod init promtrino
	go mod edit -require github.com/slok/go-http-metrics@v0.12.0
	go mod edit -require github.com/prometheus/prometheus@v0.300.0
	go mod tidy -go=1.22.5

.PHONY: up-all trino-result down rm init-mod