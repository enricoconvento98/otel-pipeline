package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"
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

var (
	services = []string{"checkout-svc", "payments-svc", "auth-svc", "cart-svc", "inventory-svc", "search-svc", "notification-svc"}
	levels   = []string{"DEBUG", "INFO", "WARN", "ERROR"}
	messages = []string{
		"request handled successfully",
		"upstream timeout",
		"cache miss, fetching from db",
		"retrying operation",
		"connection pool exhausted",
		"validation failed for payload",
	}
)

func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func randHex(n int) string {
	const hex = "0123456789abcdef"
	b := make([]byte, n)
	for i := range b {
		b[i] = hex[rand.Intn(len(hex))]
	}
	return string(b)
}

func buildLog() LogRecord {
	return LogRecord{
		Timestamp:   time.Now().UTC(),
		TraceID:     randHex(32),
		SpanID:      randHex(16),
		ServiceName: services[rand.Intn(len(services))],
		LogLevel:    levels[rand.Intn(len(levels))],
		Message:     messages[rand.Intn(len(messages))],
		Attributes: map[string]string{
			"http.status_code": strconv.Itoa([]int{200, 200, 200, 404, 500}[rand.Intn(5)]),
			"region":           []string{"eu-west-1", "us-east-1", "ap-south-1"}[rand.Intn(3)],
		},
	}
}

// worker spara log verso l'ingestor a un ritmo controllato (rate limiting locale).
func worker(id int, targetURL string, ratePerSec int, client *http.Client, wg *sync.WaitGroup) {
	defer wg.Done()

	interval := time.Second / time.Duration(ratePerSec)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	buf := new(bytes.Buffer)

	for range ticker.C {
		rec := buildLog()
		buf.Reset()
		if err := json.NewEncoder(buf).Encode(rec); err != nil {
			continue
		}
		resp, err := client.Post(targetURL, "application/json", buf)
		if err != nil {
			continue
		}
		resp.Body.Close()
	}
}

func main() {
	targetURL := getenv("TARGET_URL", "http://localhost:8080/ingest")
	concurrency, _ := strconv.Atoi(getenv("CONCURRENCY", "50"))
	ratePerWorker, _ := strconv.Atoi(getenv("RATE_PER_WORKER", "1000"))

	fmt.Printf("generator: %d goroutine x %d log/sec verso %s (~%d log/sec totali)\n",
		concurrency, ratePerWorker, targetURL, concurrency*ratePerWorker)

	client := &http.Client{
		Timeout: 2 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:        concurrency,
			MaxIdleConnsPerHost: concurrency,
			IdleConnTimeout:     30 * time.Second,
		},
	}

	var wg sync.WaitGroup
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go worker(i, targetURL, ratePerWorker, client, &wg)
	}
	wg.Wait()
}
