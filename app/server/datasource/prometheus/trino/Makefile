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

.PHONY: up-all trino-result down rm