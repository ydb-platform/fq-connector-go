FROM golang:1.20.0

WORKDIR /app

COPY . .

RUN export GOPATH="/app"

RUN go version

RUN go mod init promtrino && go mod edit -require github.com/slok/go-http-metrics@v0.12.0 && go mod tidy