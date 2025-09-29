--Constellation schema
-- Fact table(aggregated) - one record per DAG run
CREATE TABLE best_price_quotes (
    run_id SERIAL PRIMARY KEY,
    completed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    order_volume NUMERIC(14,8) NOT NULL,
    order_cost_usd NUMERIC (20,8) NOT NULL,
    price_per_btc NUMERIC (16,8) GENERATED ALWAYS AS (order_cost_usd/order_volume) STORED,
    CONSTRAINT order_volume_is_not_null CHECK (order_volume > 0)
);

-- Dimension table - information about exchanges
CREATE TABLE exchanges (
    exchange_id SERIAL PRIMARY KEY,
    name VARCHAR(30) UNIQUE NOT NULL
);

-- Fact table (detailed) - best available price across exchanges at a given time
CREATE TABLE exchange_quotes_snapshots (
    quote_id SERIAL PRIMARY KEY,
    run_id INT REFERENCES best_price_quotes(run_id) ON DELETE CASCADE, 
    exchange_id INT REFERENCES exchanges(exchange_id),
    snapshot_time TIMESTAMPTZ not NULL,
    quote_volume NUMERIC(14,8) NOT NULL,
    quote_cost NUMERIC (20,8) NOT NULL
    CONSTRAINT prevent_accidental_duplicate_snapshots UNIQUE (run_id, exchange_id, snapshot_time)
);


