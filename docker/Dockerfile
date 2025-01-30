FROM ubuntu:24.04

RUN apt-get update && apt-get install -y \
  ca-certificates jq \
  && rm -rf /var/lib/apt/lists/*

COPY out/ozmadb/ /opt/ozmadb
COPY docker/entrypoint.sh /usr/local/bin/docker-entrypoint.sh

EXPOSE 5000/tcp
CMD ["/usr/local/bin/docker-entrypoint.sh"]
