CREATE MATERIALIZED VIEW IF NOT EXISTS transformed.behavioral_events_mv
TO transformed.behavioral_events
AS
SELECT
    parseDateTime64BestEffort(JSONExtractString(raw, 'timestamp'), 3, 'UTC') AS `timestamp`,
    JSONExtractString(raw, 'user_id') AS user_id,
    JSONExtractString(raw, 'event_type') AS event_type,
    JSONExtractString(raw, 'device') AS device,
    JSONExtractString(raw, 'session_id') AS session_id,

    CAST(
        JSON_REMOVE(
            raw,
            '$.timestamp',
            '$.user_id',
            '$.event_type',
            '$.device',
            '$.session_id'
        ),
        'JSON'
    ) AS event_properties,

    now64(3) AS ingested_at
FROM transformed.behavioral_events_raw;
