-- Heartbeat Records Table with Unique Constraint for Idempotent Upserts
CREATE TABLE IF NOT EXISTS heartbeat_records (
    id SERIAL PRIMARY KEY,
    customer_id VARCHAR(20) NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL,
    heart_rate INTEGER NOT NULL CHECK (heart_rate > 0 AND heart_rate < 300),
    is_anomaly BOOLEAN DEFAULT FALSE,
    anomaly_type VARCHAR(50),
    severity INTEGER DEFAULT 0,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    
    -- Unique constraint prevents duplicate readings for same customer at same time
    -- This enables idempotent writes: re-processing a message won't create duplicates
    CONSTRAINT unique_customer_timestamp UNIQUE (customer_id, timestamp)
);

-- Index for time-range queries (most common access pattern)
CREATE INDEX idx_heartbeat_timestamp ON heartbeat_records(timestamp DESC);

-- Index for per-customer lookups
CREATE INDEX idx_heartbeat_customer ON heartbeat_records(customer_id);

-- Composite index for customer history queries
CREATE INDEX idx_heartbeat_customer_time ON heartbeat_records(customer_id, timestamp DESC);

-- Index for anomaly filtering (partial index - only indexes anomalies, smaller and faster)
CREATE INDEX idx_heartbeat_anomaly ON heartbeat_records(is_anomaly) WHERE is_anomaly = TRUE;

-- Index for anomaly type analysis
CREATE INDEX idx_heartbeat_anomaly_type ON heartbeat_records(anomaly_type) WHERE anomaly_type IS NOT NULL;

-- Dead Letter Queue table for failed messages
CREATE TABLE IF NOT EXISTS dlq_messages (
    id SERIAL PRIMARY KEY,
    original_topic VARCHAR(100) NOT NULL,
    original_partition INTEGER,
    original_offset BIGINT,
    original_key VARCHAR(100),
    original_value JSONB NOT NULL,
    error_type VARCHAR(100) NOT NULL,
    error_message TEXT NOT NULL,
    error_stage VARCHAR(50) NOT NULL,
    retry_count INTEGER DEFAULT 0,
    status VARCHAR(20) DEFAULT 'pending',
    created_at TIMESTAMPTZ DEFAULT NOW(),
    processed_at TIMESTAMPTZ
);

CREATE INDEX idx_dlq_status ON dlq_messages(status) WHERE status = 'pending';
CREATE INDEX idx_dlq_created ON dlq_messages(created_at DESC);

-- Grant permissions
GRANT ALL PRIVILEGES ON TABLE heartbeat_records TO postgres;
GRANT USAGE, SELECT ON SEQUENCE heartbeat_records_id_seq TO postgres;
GRANT ALL PRIVILEGES ON TABLE dlq_messages TO postgres;
GRANT USAGE, SELECT ON SEQUENCE dlq_messages_id_seq TO postgres;

-- Add helpful comments
COMMENT ON TABLE heartbeat_records IS 'Stores heart rate readings with deduplication via unique constraint on (customer_id, timestamp)';
COMMENT ON CONSTRAINT unique_customer_timestamp ON heartbeat_records IS 'Enables idempotent upserts - duplicate messages are safely ignored';
COMMENT ON TABLE dlq_messages IS 'Dead letter queue for messages that failed processing';
