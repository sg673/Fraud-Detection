DROP TABLE IF EXISTS fraud_velocity_windows;
DROP TABLE IF EXISTS user_statistics;

-- Main fraud detection table
CREATE TABLE fraud_velocity_windows (
    user_id INTEGER NOT NULL,
    window_start TIMESTAMP NOT NULL,
    window_end TIMESTAMP NOT NULL,
    tx_count BIGINT NOT NULL,
    sum_amount DOUBLE PRECISION NOT NULL,
    avg_amount DOUBLE PRECISION NOT NULL,
    velocity_flag BOOLEAN NOT NULL,
    amount_flag BOOLEAN NOT NULL,
    amount_deviation_ratio DOUBLE PRECISION,
    deviation_flag BOOLEAN,
    risk_score DOUBLE PRECISION NOT NULL,
    risk_level VARCHAR(10) NOT NULL,
    PRIMARY KEY (user_id, window_start)
);

-- User statistics table
CREATE TABLE user_statistics (
    user_id INTEGER PRIMARY KEY,
    total_transactions BIGINT NOT NULL,
    total_amount DOUBLE PRECISION NOT NULL,
    avg_transaction_amount DOUBLE PRECISION NOT NULL,
    max_transaction_amount DOUBLE PRECISION NOT NULL,
    distinct_locations BIGINT NOT NULL,
    last_updated TIMESTAMP NOT NULL
);

-- Indexes for performance
CREATE INDEX idx_fraud_window_end ON fraud_velocity_windows(window_end);
CREATE INDEX idx_fraud_risk_level ON fraud_velocity_windows(risk_level);
CREATE INDEX idx_fraud_risk_score ON fraud_velocity_windows(risk_score DESC);
CREATE INDEX idx_user_stats_updated ON user_statistics(last_updated);
