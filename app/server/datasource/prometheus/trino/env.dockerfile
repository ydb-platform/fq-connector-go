FROM golang:latest

COPY go.* .

RUN go mod download

COPY . .