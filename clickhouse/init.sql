CREATE DATABASE IF NOT EXISTS otel;

CREATE TABLE IF NOT EXISTS otel.logs
(
    Timestamp     DateTime64(3) CODEC(Delta, ZSTD),
    TraceID       String CODEC(ZSTD),
    SpanID        String CODEC(ZSTD),
    ServiceName   LowCardinality(String),
    LogLevel      LowCardinality(String),
    Message       String CODEC(ZSTD(3)),
    Attributes    Map(LowCardinality(String), String) CODEC(ZSTD),
    IngestedAt    DateTime DEFAULT now()
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(Timestamp)
ORDER BY (ServiceName, LogLevel, Timestamp)
--TTL toDateTime(Timestamp) + INTERVAL 30 DAY
TTL toDateTime(IngestedAt) + INTERVAL 5 MINUTE
SETTINGS index_granularity = 8192;
