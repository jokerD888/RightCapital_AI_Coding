// Package mockvendor is a local demo, not an external integration dependency.
package mockvendor

import (
	"crypto/sha256"
	"encoding/json"
	"io"
	"net/http"
	"sync"
)

type delivery struct {
	Hash      [32]byte
	Calls     int
	Succeeded bool
}

func New(failFirst int) http.Handler {
	var mu sync.Mutex
	seen := make(map[string]delivery)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(200) })
	endpoint := func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 1<<20))
		if err != nil {
			http.Error(w, "invalid or oversized body", 413)
			return
		}
		if !json.Valid(body) {
			http.Error(w, "expected JSON", 400)
			return
		}
		key := r.Header.Get("Idempotency-Key")
		if key == "" {
			http.Error(w, "Idempotency-Key required", 400)
			return
		}
		key = r.URL.Path + ":" + key
		hash := sha256.Sum256(body)
		mu.Lock()
		item, exists := seen[key]
		if exists && item.Hash != hash {
			mu.Unlock()
			http.Error(w, "same key with different payload", 409)
			return
		}
		item.Hash = hash
		item.Calls++
		duplicate := item.Succeeded
		fail := !duplicate && item.Calls <= failFirst
		if !fail {
			item.Succeeded = true
		}
		seen[key] = item
		mu.Unlock()
		if fail {
			http.Error(w, "simulated transient failure", 503)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"status": "ok", "duplicate": duplicate, "attempt": item.Calls})
	}
	mux.HandleFunc("POST /ads/conversions", endpoint)
	mux.HandleFunc("PATCH /crm/contacts", endpoint)
	mux.HandleFunc("POST /inventory/adjustments", endpoint)
	return mux
}
