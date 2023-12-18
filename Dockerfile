FROM alpine

LABEL org.opencontainers.image.source=https://github.com/ydb-platform/fq-connector-go

RUN apk add libc6-compat

COPY example.conf /usr/local/etc/fq-connector-go.conf
COPY fq-connector-go /usr/local/bin/fq-connector-go

CMD ["/usr/local/bin/fq-connector-go", "server", "-c", "/usr/local/etc/fq-connector-go.conf"]
