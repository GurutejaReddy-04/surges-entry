CREATE TABLE processed_events (
    id            BIGSERIAL PRIMARY KEY,
    user_id       TEXT           NOT NULL,
    event_type    TEXT           NOT NULL,
    event_value   DECIMAL(10,2)  NOT NULL,
    rolling_avg   DECIMAL(10,2),              -- NULL when Redis was unavailable at processing time
    is_anomaly    BOOLEAN        DEFAULT FALSE,
    trace_id      TEXT,                       -- joins this row back to the OTel trace in Jaeger
    ingested_at   TIMESTAMP      DEFAULT NOW()
);

CREATE INDEX idx_user_id     ON processed_events(user_id);
CREATE INDEX idx_ingested_at ON processed_events(ingested_at);
CREATE INDEX idx_trace_id    ON processed_events(trace_id);
