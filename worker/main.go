package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/segmentio/kafka-go"
)

type LogRecord struct {
	Timestamp   time.Time         `json:"timestamp"`
	TraceID     string            `json:"trace_id"`
	SpanID      string            `json:"span_id"`
	ServiceName string            `json:"service_name"`
	LogLevel    string            `json:"log_level"`
	Message     string            `json:"message"`
	Attributes  map[string]string `json:"attributes"`
}

func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func main() {
	brokers := getenv("KAFKA_BROKERS", "localhost:9092")
	topic := getenv("KAFKA_TOPIC", "otel-logs")
	group := getenv("KAFKA_GROUP", "ch-writers")
	chAddr := getenv("CLICKHOUSE_ADDR", "localhost:9000")
	chDB := getenv("CLICKHOUSE_DB", "otel")
	chUser := getenv("CLICKHOUSE_USERNAME", "otel")
	chPassword := getenv("CLICKHOUSE_PASSWORD", "otel-pw")
	batchSize, _ := strconv.Atoi(getenv("BATCH_SIZE", "50000"))
	flushMs, _ := strconv.Atoi(getenv("FLUSH_INTERVAL_MS", "2000"))

	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{chAddr},
		Auth: clickhouse.Auth{
			Username: chUser,
			Password: chPassword,
			Database: chDB,
		},
		Settings: clickhouse.Settings{
			"async_insert": 0, // il batching lo gestiamo noi lato applicativo
		},
		Compression: &clickhouse.Compression{Method: clickhouse.CompressionLZ4},
		DialTimeout: 10 * time.Second,
	})
	if err != nil {
		log.Fatalf("connessione ClickHouse fallita: %v", err)
	}
	if err := conn.Ping(context.Background()); err != nil {
		log.Fatalf("ping ClickHouse fallito: %v", err)
	}

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{brokers},
		Topic:    topic,
		GroupID:  group,
		MinBytes: 1e3,
		MaxBytes: 10e6,
		MaxWait:  500 * time.Millisecond,
	})
	defer reader.Close()

	buffer := make([]LogRecord, 0, batchSize)
	var mu sync.Mutex

	flush := func() {
		mu.Lock()
		if len(buffer) == 0 {
			mu.Unlock()
			return
		}
		toWrite := make([]LogRecord, len(buffer))
		copy(toWrite, buffer)
		buffer = buffer[:0]
		mu.Unlock()

		if err := writeBatch(conn, chDB, toWrite); err != nil {
			log.Printf("errore scrittura batch (%d righe): %v", len(toWrite), err)
			return
		}
		log.Printf("flush: %d righe scritte su ClickHouse", len(toWrite))
	}

	// Trigger "per tempo": ogni flushMs, indipendentemente dalla dimensione.
	ticker := time.NewTicker(time.Duration(flushMs) * time.Millisecond)
	defer ticker.Stop()
	go func() {
		for range ticker.C {
			flush()
		}
	}()

	log.Printf("worker: consumo da %s topic=%s group=%s -> clickhouse=%s db=%s (batch=%d, flush=%dms)",
		brokers, topic, group, chAddr, chDB, batchSize, flushMs)

	ctx := context.Background()
	for {
		m, err := reader.FetchMessage(ctx)
		if err != nil {
			log.Printf("errore fetch da kafka: %v", err)
			continue
		}

		var rec LogRecord
		if err := json.Unmarshal(m.Value, &rec); err != nil {
			// messaggio malformato: si logga e si commit comunque per non bloccare la coda
			log.Printf("json malformato, scartato: %v", err)
			reader.CommitMessages(ctx, m)
			continue
		}

		mu.Lock()
		buffer = append(buffer, rec)
		full := len(buffer) >= batchSize
		mu.Unlock()

		// Trigger "per dimensione": il primo che si verifica tra i due vince.
		if full {
			flush()
		}

		reader.CommitMessages(ctx, m)
	}
}

// writeBatch usa PrepareBatch + Append (protocollo nativo, porta 9000)
// per l'inserimento massivo, come richiesto dal design colonnare di ClickHouse.
func writeBatch(conn clickhouse.Conn, db string, records []LogRecord) error {
	ctx := context.Background()
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO "+db+`.logs (
		Timestamp,
		TraceID,
		SpanID,
		ServiceName,
		LogLevel,
		Message,
		Attributes
	)`)
	if err != nil {
		return err
	}

	for _, r := range records {
		if err := batch.Append(
			r.Timestamp,
			r.TraceID,
			r.SpanID,
			r.ServiceName,
			r.LogLevel,
			r.Message,
			r.Attributes,
		); err != nil {
			return err
		}
	}

	return batch.Send()
}
