# OTel-native Ingestion Pipeline (Go + Redpanda + ClickHouse)

Pipeline di ingestione telemetria ad alto throughput, disaccoppiata via Redpanda
(Kafka-API compatible) e ottimizzata per storage analitico su ClickHouse.

## Struttura

```
otel-pipeline/
├── docker-compose.yml
├── clickhouse/init.sql      # schema con LowCardinality + MergeTree partizionato
├── generator/                # simula migliaia di microservizi
├── ingestor/                 # HTTP -> worker pool -> Redpanda producer
└── worker/                   # Redpanda consumer -> batch double-trigger -> ClickHouse
```

## Avvio

```bash
docker compose up --build
```

Servizi esposti:
- ClickHouse HTTP: http://localhost:8123 (play UI su /play)
- ClickHouse native: localhost:9000
- Redpanda Kafka API: localhost:9092
- Ingestor: http://localhost:8080/ingest

## Verifica dati

```sql
SELECT ServiceName, LogLevel, count()
FROM otel.logs
GROUP BY ServiceName, LogLevel
ORDER BY count() DESC;
```

## Note di scalabilità

- `worker` gira con `replicas: 2` nel compose: aumenta insieme al numero di
  partizioni del topic `otel-logs` per parallelizzare i consumer nello stesso
  consumer group.
- `WORKER_POOL_SIZE` e `CHANNEL_BUFFER` in ingestor regolano la backpressure
  interna: se il canale si riempie, l'HTTP risponde 503 invece di bloccare
  indefinitamente o esaurire la RAM.
- `BATCH_SIZE` / `FLUSH_INTERVAL_MS` nel worker implementano il double-trigger:
  50k righe o 2s, il primo che si verifica.

## Cosa manca per portarlo in produzione

- Creazione automatica del topic Redpanda con N partizioni (oggi si affida
  all'auto-create di default).
- TLS/mTLS tra i servizi.
- Metriche Prometheus su ingestor/worker (queue depth, batch latency, errori).
- Retry/backoff con dead-letter topic per i messaggi malformati nel worker.
- Dato che il progetto non ha accesso a Internet in questo ambiente, `go.sum`
  non è stato generato: al primo build servirà `go mod tidy` con accesso alla
  rete per scaricare `clickhouse-go/v2` e `kafka-go`.
