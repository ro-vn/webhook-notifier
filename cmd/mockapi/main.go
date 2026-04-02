package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"sync/atomic"
	"time"
)

var (
	totalReceived atomic.Int64
	totalFailed   atomic.Int64
)

func main() {
	failRate := flag.Float64("failRate", 0.01, "Fraction of requests that return HTTP 500 (0.0–1.0)")
	port := flag.Int("port", 8090, "Port to listen on")
	flag.Parse()

	if *failRate < 0 || *failRate > 1 {
		log.Fatal("-failRate must be between 0.0 and 1.0")
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/webhook", makeWebhookHandler(*failRate))
	mux.HandleFunc("/stats", statsHandler)
	mux.HandleFunc("/reset", resetHandler)

	addr := fmt.Sprintf(":%d", *port)
	log.Printf("Mock API  addr=%s  fail_rate=%.1f%%", addr, *failRate*100)

	go func() {
		t := time.NewTicker(5 * time.Second)
		defer t.Stop()
		for range t.C {
			log.Printf("[mockapi] received=%d  failed=%d", totalReceived.Load(), totalFailed.Load())
		}
	}()

	log.Fatal(http.ListenAndServe(addr, mux))
}

func makeWebhookHandler(failRate float64) http.HandlerFunc {
	// Could be improved throughput by using per-goroutine source
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		// Consume and discard the body so the connection can be reused.
		_, _ = io.Copy(io.Discard, r.Body)
		_ = r.Body.Close()

		totalReceived.Add(1)

		if rand.Float64() < failRate {
			totalFailed.Add(1)
			http.Error(w, "simulated upstream failure", http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	}
}

func statsHandler(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]int64{ //nolint:errcheck
		"received": totalReceived.Load(),
		"failed":   totalFailed.Load(),
	})
}

func resetHandler(w http.ResponseWriter, _ *http.Request) {
	totalReceived.Store(0)
	totalFailed.Store(0)
	w.WriteHeader(http.StatusNoContent)
}
