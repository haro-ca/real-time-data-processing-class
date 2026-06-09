"""Shared connection config + DuckDB target helpers for the Lesson 5 CDC demo.

Every script imports from here so the source/target wiring lives in one place.

Environment overrides (all optional, sane localhost defaults):
    PG_HOST       Postgres host          (default: localhost)
    PG_PORT       Postgres port          (default: 5432)
    DUCKDB_PATH   analytical mirror file (default: <repo>/data/cdc.duckdb)
    CDC_SLOT      replication slot name  (default: orders_slot)
"""

import hashlib
import os
from pathlib import Path

import duckdb

# ── Paths ────────────────────────────────────────────────────────────────────
DATA_DIR = Path(__file__).parent.parent / "data"
DATA_DIR.mkdir(exist_ok=True)
DUCKDB_PATH = Path(os.environ.get("DUCKDB_PATH", DATA_DIR / "cdc.duckdb"))

# ── Postgres OLTP source ───────────────────────────────────────────────────────
PG_HOST = os.environ.get("PG_HOST", "localhost")
PG_PORT = os.environ.get("PG_PORT", "5432")
# psycopg / libpq URI form
PG_DSN = f"postgresql://bench:bench@{PG_HOST}:{PG_PORT}/bench"
# DuckDB postgres-extension ATTACH form (key=value)
PG_CONN_STR = f"host={PG_HOST} port={PG_PORT} user=bench password=bench dbname=bench"

# ── CDC wiring (created by setup_cdc.py) ───────────────────────────────────────
SLOT = os.environ.get("CDC_SLOT", "orders_slot")
PUBLICATION = "orders_pub"
PLUGIN = "wal2json"


def connect_target() -> duckdb.DuckDBPyConnection:
    """Open the analytical DuckDB mirror, ensuring its schema exists.

    DuckDB is single-writer: only one process may hold a write handle at a time.
    Run the consumer, poll_sync, and snapshot one at a time.
    """
    con = duckdb.connect(str(DUCKDB_PATH))
    ensure_target_schema(con)
    return con


def ensure_target_schema(con: duckdb.DuckDBPyConnection) -> None:
    """Create the analytical mirror tables if they don't exist (idempotent DDL)."""
    # The CDC mirror of public.orders. _cdc_* columns are replication METADATA —
    # they describe the pipeline, not the source row. Best practice: keep them
    # separate from source columns so you never confuse "when the row was created"
    # with "when CDC last touched it".
    con.execute("""
        CREATE TABLE IF NOT EXISTS orders (
            id              BIGINT PRIMARY KEY,
            customer_id     INTEGER,
            amount          DECIMAL(10,2),
            status          VARCHAR,
            created_at      TIMESTAMPTZ,
            _cdc_lsn        VARCHAR,
            _cdc_updated_at TIMESTAMP DEFAULT now()
        )
    """)


def mirror_checksum(con: duckdb.DuckDBPyConnection) -> tuple[int, str]:
    """Return (row_count, content checksum) for the mirror, ignoring CDC metadata.

    Used to prove that crash + replay leaves the mirror byte-identical: the apply
    logic is idempotent, so re-seeing events can't change the answer.
    """
    rows = con.execute(
        "SELECT id, customer_id, amount, status FROM orders ORDER BY id"
    ).fetchall()
    digest = hashlib.md5(repr(rows).encode()).hexdigest()[:12]
    return len(rows), digest
