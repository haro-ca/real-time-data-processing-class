FROM python:3.13-slim

RUN pip install --no-cache-dir uv

# Keep the venv outside /workspace so the bind mount doesn't clobber it.
ENV UV_LINK_MODE=copy
RUN uv venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH" \
    VIRTUAL_ENV="/opt/venv" \
    PYTHONPATH="/workspace/src"

RUN uv pip install --python /opt/venv/bin/python \
    "duckdb>=1.1.0" \
    "psycopg[binary]>=3.2.0"

# Pre-warm the DuckDB postgres extension so the first seed/snapshot isn't slowed
# by the extension download (seed_data.py, snapshot.py, poll_sync.py use it).
RUN python -c "import duckdb; c=duckdb.connect(); c.execute('INSTALL postgres'); c.execute('LOAD postgres')"

WORKDIR /workspace
CMD ["sleep", "infinity"]
