FROM redpandadata/redpanda:v24.2.7

USER root

RUN apt-get update && apt-get install -y \
    python3 python3-pip curl ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && pip3 install uv

# ClickHouse single statically-linked binary; non-interactive in Docker (no TTY)
WORKDIR /tmp
RUN curl -fsSL https://clickhouse.com/ | sh \
    && mv ./clickhouse /usr/local/bin/clickhouse

RUN mkdir -p /var/lib/redpanda /var/lib/clickhouse /var/log/clickhouse-server

WORKDIR /app
COPY uv.lock pyproject.toml ./
RUN uv sync --frozen
COPY app ./app
COPY clickhouse/init.sql /init.sql
COPY start.sh /start.sh
RUN chmod +x /start.sh

EXPOSE 8000
CMD ["/start.sh"]
