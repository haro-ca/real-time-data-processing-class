-- Extensions for instrumentation
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;
CREATE EXTENSION IF NOT EXISTS pg_buffercache;

-- Main table for benchmarking
CREATE TABLE orders (
    id         BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    customer_id INT NOT NULL,
    amount     NUMERIC(10, 2),
    status     TEXT DEFAULT 'pending',
    created_at TIMESTAMPTZ DEFAULT now()
);

-- Index for customer lookups (used in some experiments)
CREATE INDEX idx_orders_customer ON orders (customer_id);
