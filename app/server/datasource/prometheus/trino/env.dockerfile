FROM golang:1.22.5

COPY go.* .

RUN go mod download

COPY . .