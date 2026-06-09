-- Lesson 5: OLTP SOURCE schema (Postgres) for the Change Data Capture workshop.
--
-- Same orders/customers shape as Lesson 4, with ONE addition: orders gets an
-- `updated_at` column. That column is the (false) hope polling-based CDC is built
-- on — poll_sync.py reads `WHERE updated_at > last_sync` and gets caught lying the
-- moment a write forgets to bump it, or a row is DELETEd (no trace to poll).
--
-- The logical-replication wiring (REPLICA IDENTITY FULL, publication, slot) is
-- deliberately NOT here — setup_cdc.py does it, because wiring the slot by hand
-- is the lesson. wal_level=logical is set in docker-compose.

CREATE TABLE IF NOT EXISTS customers (
    id          INT PRIMARY KEY,
    name        TEXT        NOT NULL,
    city        TEXT        NOT NULL,
    region      TEXT        NOT NULL,
    signup_date DATE        NOT NULL,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS orders (
    id          BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    customer_id INT           NOT NULL,
    amount      NUMERIC(10,2) NOT NULL,
    status      TEXT          NOT NULL DEFAULT 'pending',
    created_at  TIMESTAMPTZ   NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ   NOT NULL DEFAULT now()   -- what polling (wrongly) trusts
);

-- Polling filters on updated_at; index it so the naive poll isn't a seq scan.
CREATE INDEX IF NOT EXISTS idx_orders_updated_at ON orders (updated_at);
