FROM python:3.13-slim

RUN pip install --no-cache-dir uv

# Put the venv outside /workspace so the bind mount doesn't clobber it.
ENV UV_LINK_MODE=copy
RUN uv venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH" \
    VIRTUAL_ENV="/opt/venv"

RUN uv pip install --python /opt/venv/bin/python \
    "duckdb>=1.5" \
    "psycopg[binary]>=3.3" \
    "asyncpg>=0.31"

# Pre-warm the DuckDB postgres extension so first benchmark run isn't slowed
# by the extension download.
RUN python -c "import duckdb; c=duckdb.connect(); c.execute('INSTALL postgres'); c.execute('LOAD postgres')"

WORKDIR /workspace
CMD ["sleep", "infinity"]
