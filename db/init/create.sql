CREATE TABLE IF NOT EXISTS scoring_results (
    id SERIAL PRIMARY KEY,
    transaction_id VARCHAR(128) NOT NULL,
    score DOUBLE PRECISION NOT NULL,
    fraud_flag INTEGER NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW()
);