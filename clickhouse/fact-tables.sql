CREATE TABLE IF NOT EXISTS transformed.behavioral_events
(
    `timestamp` DateTime64(3, 'UTC'),
    event_date Date MATERIALIZED toDate(`timestamp`),

    user_id String,
    event_type LowCardinality(String),
    device LowCardinality(String),
    session_id String,

    event_properties JSON,

    ingested_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(event_date)
ORDER BY (event_type, event_date, user_id, session_id, `timestamp`);
