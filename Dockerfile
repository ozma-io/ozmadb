FROM ubuntu:24.04

COPY out/ozmadb/ /opt/ozmadb
CMD ["/opt/ozmadb/OzmaDB", "/etc/ozmadb/config.json"]
