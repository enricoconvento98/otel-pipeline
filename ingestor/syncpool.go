package main

import (
	"bytes"
	"sync"
)

// newSyncPool crea il sync.Pool che alimenta il BufferPool in main.go.
// Isolato in un file separato per chiarezza.
func newSyncPool() *sync.Pool {
	return &sync.Pool{
		New: func() interface{} {
			return new(bytes.Buffer)
		},
	}
}
