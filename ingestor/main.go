package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"

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

// bufPool ricicla i buffer usati per leggere il body HTTP, azzerando le
// allocazioni continue sotto carico (alloc-free hot path).
var bufPool = &BufferPool{}

type BufferPool struct{}

func (p *BufferPool) Get() *bytes.Buffer {
	v := pool.Get()
	buf := v.(*bytes.Buffer)
	buf.Reset()
	return buf
}

func (p *BufferPool) Put(b *bytes.Buffer) {
	pool.Put(b)
}

var pool = newSyncPool()

type Server struct {
	jobs   chan []byte // canale bufferizzato: qui si applica la backpressure
	writer *kafka.Writer
}

func main() {
	brokers := getenv("KAFKA_BROKERS", "localhost:9092")
	topic := getenv("KAFKA_TOPIC", "otel-logs")
	httpPort := getenv("HTTP_PORT", "8080")
	poolSize, _ := strconv.Atoi(getenv("WORKER_POOL_SIZE", "64"))
	chanBuf, _ := strconv.Atoi(getenv("CHANNEL_BUFFER", "100000"))

	w := &kafka.Writer{
		Addr:         kafka.TCP(brokers),
		Topic:        topic,
		Balancer:     &kafka.LeastBytes{},
		BatchSize:    1000,                   // il writer stesso fa batching lato client
		BatchTimeout: 50 * time.Millisecond,
		Async:        true,                   // fire-and-forget per massimizzare throughput
		Compression:  kafka.Snappy,
		RequiredAcks: kafka.RequireOne,
	}
	defer w.Close()

	srv := &Server{
		jobs:   make(chan []byte, chanBuf),
		writer: w,
	}

	// Worker pool: N goroutine consumano dal canale e pubblicano su Redpanda.
	for i := 0; i < poolSize; i++ {
		go srv.publishLoop()
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/ingest", srv.handleIngest)
	mux.HandleFunc("/healthz", func(rw http.ResponseWriter, r *http.Request) {
		rw.WriteHeader(http.StatusOK)
	})

	log.Printf("ingestor: in ascolto su :%s, pool=%d, buffer=%d, kafka=%s topic=%s",
		httpPort, poolSize, chanBuf, brokers, topic)

	server := &http.Server{
		Addr:         ":" + httpPort,
		Handler:      mux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 5 * time.Second,
	}
	log.Fatal(server.ListenAndServe())
}

// handleIngest valida rapidamente lo shape del JSON e lo mette in coda.
// Non si scrive mai direttamente su ClickHouse da qui.
func (s *Server) handleIngest(rw http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(rw, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	buf := bufPool.Get()
	defer bufPool.Put(buf)

	if _, err := io.Copy(buf, io.LimitReader(r.Body, 1<<20)); err != nil {
		http.Error(rw, "read error", http.StatusBadRequest)
		return
	}

	var rec LogRecord
	if err := json.Unmarshal(buf.Bytes(), &rec); err != nil {
		http.Error(rw, "invalid json", http.StatusBadRequest)
		return
	}
	if rec.ServiceName == "" || rec.Timestamp.IsZero() {
		http.Error(rw, "missing required fields", http.StatusBadRequest)
		return
	}

	payload := make([]byte, buf.Len())
	copy(payload, buf.Bytes())

	select {
	case s.jobs <- payload:
		rw.WriteHeader(http.StatusAccepted)
	default:
		// canale pieno -> backpressure esplicita verso il client
		http.Error(rw, "queue full, retry later", http.StatusServiceUnavailable)
	}
}

func (s *Server) publishLoop() {
	ctx := context.Background()
	for payload := range s.jobs {
		msg := kafka.Message{Value: payload}
		if err := s.writer.WriteMessages(ctx, msg); err != nil {
			log.Printf("kafka write error: %v", err)
		}
	}
}
