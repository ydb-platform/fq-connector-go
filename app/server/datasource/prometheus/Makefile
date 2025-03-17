bench:
	go run bench/client.go

init-mod:
	go mod init promtrino
	go mod edit -require github.com/prometheus/prometheus@v0.300.0
	go mod tidy -go=1.22.5

.PHONY: bench init-mod