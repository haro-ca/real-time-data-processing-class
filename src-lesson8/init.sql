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
