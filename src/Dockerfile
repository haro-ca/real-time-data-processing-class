FROM redpandadata/redpanda:v24.2.7 AS redpanda

FROM python:3.13-slim

# Redpanda: /usr/bin/redpanda is a wrapper that calls /opt/redpanda/libexec/redpanda
COPY --from=redpanda /usr/bin/redpanda /usr/bin/redpanda
COPY --from=redpanda /usr/bin/rpk /usr/bin/rpk
COPY --from=redpanda /opt/redpanda /opt/redpanda

# Runtime libs Redpanda needs that slim doesn't ship
RUN apt-get update && apt-get install -y \
    curl ca-certificates libnuma1 libatomic1 \
    && rm -rf /var/lib/apt/lists/*

# ClickHouse single statically-linked binary; non-interactive without a TTY
WORKDIR /tmp
RUN curl -fsSL https://clickhouse.com/ | sh \
    && mv ./clickhouse /usr/local/bin/clickhouse

RUN mkdir -p /var/lib/redpanda /var/lib/clickhouse /var/log/clickhouse-server

RUN pip install uv

WORKDIR /app
COPY uv.lock pyproject.toml ./
RUN uv sync --frozen
COPY app ./app
COPY clickhouse/init.sql /init.sql
COPY start.sh /start.sh
RUN chmod +x /start.sh

EXPOSE 8000
CMD ["/start.sh"]
