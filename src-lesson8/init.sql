CREATE TABLE IF NOT EXISTS enriched_transactions (
    transaction_id VARCHAR(64) PRIMARY KEY,
    customer_id VARCHAR(64) NOT NULL,
    amount NUMERIC(12, 2) NOT NULL,
    currency VARCHAR(3) NOT NULL,
    transaction_time TIMESTAMPTZ NOT NULL,
    customer_name VARCHAR(256),
    customer_tier VARCHAR(32),
    customer_region VARCHAR(64),
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Phase 5: per-customer per-minute rollup, fed by streaming_aggregate.py.
-- The composite primary key is what makes the aggregate upsert idempotent.
CREATE TABLE IF NOT EXISTS customer_activity (
    customer_id VARCHAR(64) NOT NULL,
    window_start TIMESTAMPTZ NOT NULL,
    window_end TIMESTAMPTZ NOT NULL,
    txn_count BIGINT NOT NULL,
    total_amount NUMERIC(14, 2) NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (customer_id, window_start)
);
