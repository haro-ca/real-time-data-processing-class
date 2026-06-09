FROM postgres:16

# wal2json — the readable-JSON logical-decoding output plugin we build the CDC
# consumer against. It is NOT in the base image; it is pulled from the PGDG apt
# repo that the official postgres image already configures. (pgoutput, the binary
# plugin Debezium uses, is built in — we read JSON instead so the lesson stays
# about CDC, not byte-offset parsing.)
RUN apt-get update \
 && apt-get install -y --no-install-recommends postgresql-16-wal2json \
 && rm -rf /var/lib/apt/lists/*
