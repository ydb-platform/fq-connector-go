FROM opensearchproject/opensearch:latest

USER root

RUN mkdir -p /usr/share/opensearch/docker-entrypoint-initdb.d && \
    chown opensearch:opensearch /usr/share/opensearch/docker-entrypoint-initdb.d

COPY opensearch-init.sh /usr/share/opensearch/docker-entrypoint-initdb.d/
COPY entrypoint.sh /usr/share/opensearch/

RUN chmod +x /usr/share/opensearch/entrypoint.sh && \
    chmod +x /usr/share/opensearch/docker-entrypoint-initdb.d/opensearch-init.sh

USER opensearch

ENTRYPOINT ["/usr/share/opensearch/entrypoint.sh"]