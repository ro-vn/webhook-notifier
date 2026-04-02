package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	_ "github.com/lib/pq"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/ro-vn/webhook-notifier/internal/dlq"
	"github.com/ro-vn/webhook-notifier/internal/event"
)

type metrics struct {
	processed  atomic.Int64
	successes  atomic.Int64
	failures   atomic.Int64
	retries    atomic.Int64
	totalBytes atomic.Int64
}

var m metrics

func main() {
	brokerStr := flag.String("brokers", "127.0.0.1:19092,127.0.0.1:29092,127.0.0.1:39092", "Comma-separated Kafka brokers")
	dsn := flag.String("dsn", "postgres://postgres:postgres@localhost:5432/webhooks?sslmode=disable", "PostgreSQL DSN")
	workers := flag.Int("workers", 100, "Parallel HTTP dispatch goroutines per consumer group")
	maxRetries := flag.Int("maxRetries", 3, "Max delivery attempts per webhook URL")
	groupBase := flag.String("group", "webhook-notifier", "Base consumer group name")
	groups := flag.Int("groups", 0, "Number of consumer groups to run in parallel (default = broker count)")
	totalMessages := flag.Int("totalMessages", 10_000_000, "Exit after processing N messages across all groups (0 = run until SIGINT)")
	mockAPIAddr := flag.String("mockAPIAddr", "http://localhost:8090", "Mock API base URL for final stats")
	flag.Parse()

	brokers := splitBrokers(*brokerStr)

	numGroups := *groups
	if numGroups <= 0 {
		numGroups = len(brokers)
	}

	db, err := sql.Open("postgres", *dsn)
	if err != nil {
		log.Fatalf("db open: %v", err)
	}
	db.SetMaxOpenConns(5)
	db.SetMaxIdleConns(5)
	defer db.Close()

	log.Println("Loading webhook subscription cache …")
	cache, err := loadCache(db)
	if err != nil {
		log.Fatalf("load cache: %v", err)
	}
	log.Printf("Cache loaded: %d (account, event_type) entries", len(cache))

	// pool sized to workers×groups so every goroutine can get a connection.
	totalWorkers := *workers * numGroups
	httpClient := &http.Client{
		Timeout: 10 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:        totalWorkers,
			MaxIdleConnsPerHost: totalWorkers,
			MaxConnsPerHost:     totalWorkers,
			IdleConnTimeout:     30 * time.Second,
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// ── DLQ writer ───────────────────────────────────────────────────────
	dlqWriter := dlq.New(ctx, db)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		log.Println("Signal received, draining …")
		cancel()
	}()

	start := time.Now()
	go func() {
		t := time.NewTicker(5 * time.Second)
		defer t.Stop()
		var lastProc int64
		for {
			select {
			case <-t.C:
				proc := m.processed.Load()
				rate := float64(proc-lastProc) / 5.0
				lastProc = proc
				log.Printf("[consumer] groups=%d  processed=%s  success=%d  fail=%d  retry=%d  rate=%.0f msg/sec",
					numGroups,
					formatInt(int(proc)),
					m.successes.Load(),
					m.failures.Load(),
					m.retries.Load(),
					rate,
				)
			case <-ctx.Done():
				return
			}
		}
	}()

	log.Printf("Starting %d consumer group(s)  brokers=%d  workers-per-group=%d  maxRetries=%d  topic=%q",
		numGroups, len(brokers), *workers, *maxRetries, event.TopicName)

	// ── fan out one goroutine per consumer group ────────────────────────
	var wg sync.WaitGroup
	for i := 0; i < numGroups; i++ {
		groupName := fmt.Sprintf("%s-g%d", *groupBase, i%3)
		wg.Add(1)
		go func(gName string) {
			defer wg.Done()
			runGroup(ctx, gName, brokers, cache, httpClient, dlqWriter, *workers, *maxRetries, *totalMessages, cancel)
		}(groupName)
	}

	wg.Wait()

	duration := time.Since(start)
	proc := m.processed.Load()
	bytesTotal := m.totalBytes.Load()

	dlqWritten, dlqDropped := dlqWriter.Stats()
	lines := []string{
		fmt.Sprintf("%-24s %d", "Consumer groups:", numGroups),
		fmt.Sprintf("%-24s %d", "Brokers:", len(brokers)),
		fmt.Sprintf("%-24s %s", "Duration:", duration.Round(time.Millisecond)),
		fmt.Sprintf("%-24s %s", "Messages processed:", formatInt(int(proc))),
		fmt.Sprintf("%-24s %d", "Successes:", m.successes.Load()),
		fmt.Sprintf("%-24s %d", "Failures (exhausted):", m.failures.Load()),
		fmt.Sprintf("%-24s %d", "Retry attempts:", m.retries.Load()),
		fmt.Sprintf("%-24s %.0f msg/sec", "Throughput:", float64(proc)/duration.Seconds()),
		fmt.Sprintf("%-24s %.2f MB/sec", "Data throughput:", float64(bytesTotal)/duration.Seconds()/1024/1024),
		fmt.Sprintf("%-24s %d", "DLQ written:", dlqWritten),
		fmt.Sprintf("%-24s %d", "DLQ dropped (full):", dlqDropped),
	}

	if stats, err := fetchMockStats(*mockAPIAddr); err == nil {
		lines = append(lines,
			fmt.Sprintf("%-24s %d", "MockAPI received:", stats["received"]),
			fmt.Sprintf("%-24s %d", "MockAPI simulated fails:", stats["failed"]),
		)
	}

	printBox("CONSUMER BENCHMARK RESULTS", lines)
}

// runGroup creates a dedicated Kafka client joined to groupName and runs the
// poll→dispatch loop until ctx is cancelled or totalMessages is reached.
func runGroup(
	ctx context.Context,
	groupName string,
	brokers []string,
	cache map[string][]string,
	httpClient *http.Client,
	dlqWriter *dlq.Writer,
	workers int,
	maxRetries int,
	totalMessages int,
	cancel context.CancelFunc,
) {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup(groupName),
		kgo.ConsumeTopics(event.TopicName),
		kgo.DisableAutoCommit(),
		kgo.FetchMaxBytes(50<<20),          // 50MB
		kgo.FetchMaxPartitionBytes(10<<20), // 20MB
	)
	if err != nil {
		log.Printf("[%s] kafka client init error: %v", groupName, err)
		return
	}
	defer client.Close()

	log.Printf("[%s] started", groupName)

	// Per-group semaphore so each group gets its own worker budget.
	sem := make(chan struct{}, workers)

	for {
		if ctx.Err() != nil {
			break
		}
		fetches := client.PollFetches(ctx)
		if fetches.IsClientClosed() {
			break
		}
		fetches.EachError(func(t string, p int32, err error) {
			log.Printf("[%s] fetch error topic=%s partition=%d: %v", groupName, t, p, err)
		})

		var batchWg sync.WaitGroup

		fetches.EachRecord(func(r *kgo.Record) {
			// Check total-message exit condition before acquiring sem.
			if totalMessages > 0 && m.processed.Load() >= int64(totalMessages) {
				return
			}

			sem <- struct{}{}
			batchWg.Add(1)
			go func(rec *kgo.Record) {
				defer func() {
					<-sem
					batchWg.Done()
				}()
				dispatch(rec, cache, httpClient, dlqWriter, maxRetries)
			}(r)
		})

		batchWg.Wait()

		// Commit after every batch – at-least-once semantics.
		if err := client.CommitUncommittedOffsets(ctx); err != nil && ctx.Err() == nil {
			log.Printf("[%s] commit error: %v", groupName, err)
		}

		// When total threshold is reached, cancel all sibling groups.
		if totalMessages > 0 && m.processed.Load() >= int64(totalMessages) {
			log.Printf("[%s] message threshold reached, signalling shutdown", groupName)
			cancel()
			break
		}
	}

	log.Printf("[%s] stopped", groupName)
}

// dispatch deserialises the Kafka record, looks up the registered webhook URLs
// and sends an HTTP POST to each with retry/backoff.
// Permanently-failed deliveries are recorded in the DLQ.
func dispatch(
	rec *kgo.Record,
	cache map[string][]string,
	client *http.Client,
	dlqWriter *dlq.Writer,
	maxRetries int,
) {
	var ev event.Event
	if err := json.Unmarshal(rec.Value, &ev); err != nil {
		log.Printf("unmarshal error offset=%d: %v", rec.Offset, err)
		m.processed.Add(1)
		m.failures.Add(1)
		return
	}

	cacheKey := ev.AccountID + ":" + ev.EventType
	urls, ok := cache[cacheKey]
	if !ok || len(urls) == 0 {
		m.processed.Add(1)
		return
	}

	body, _ := json.Marshal(ev)

	for _, url := range urls {
		if err := sendWithRetry(client, url, body, maxRetries); err != nil {
			m.failures.Add(1)
			dlqWriter.Record(dlq.Entry{
				EventID:      ev.EventID,
				AccountID:    ev.AccountID,
				WebhookURL:   url,
				Status:       "exhausted",
				Attempts:     maxRetries + 1,
				ErrorMessage: err.Error(),
			})
		} else {
			m.successes.Add(1)
			m.totalBytes.Add(int64(len(body)))
		}
	}
	m.processed.Add(1)
}

// sendWithRetry sends the payload to url, retrying up to maxRetries times
// on non-2xx responses or transport errors, using exponential backoff with
// full jitter to avoid thundering herd.
func sendWithRetry(client *http.Client, url string, body []byte, maxRetries int) error {
	backoff := 10 * time.Millisecond

	for attempt := 0; attempt <= maxRetries; attempt++ {
		if attempt > 0 {
			jitter := time.Duration(rand.Int63n(int64(backoff))) //nolint:gosec
			time.Sleep(jitter)
			m.retries.Add(1)
			backoff *= 2
		}

		resp, err := client.Post(url, "application/json", bytes.NewReader(body)) //nolint:noctx
		if err != nil {
			continue
		}
		io.Copy(io.Discard, resp.Body) //nolint:errcheck
		resp.Body.Close()

		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			return nil
		}
		// Non-2xx (e.g. 500 from mock API) – retry.
	}
	return fmt.Errorf("delivery failed after %d attempts", maxRetries+1)
}

// loadCache reads all active webhook subscriptions into memory.
// Key format: "<account_id>:<event_type>" → []url
func loadCache(db *sql.DB) (map[string][]string, error) {
	rows, err := db.Query(
		"SELECT account_id, url, event_type FROM webhook_subscriptions WHERE active = TRUE",
	)
	if err != nil {
		return nil, err
	}
	defer func(rows *sql.Rows) {
		_ = rows.Close()
	}(rows)

	cache := make(map[string][]string)
	for rows.Next() {
		var accountID, url, eventType string
		if err := rows.Scan(&accountID, &url, &eventType); err != nil {
			return nil, err
		}
		key := accountID + ":" + eventType
		cache[key] = append(cache[key], url)
	}
	return cache, rows.Err()
}

func fetchMockStats(baseURL string) (map[string]int64, error) {
	resp, err := http.Get(baseURL + "/stats") //nolint:noctx
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	var out map[string]int64
	return out, json.NewDecoder(resp.Body).Decode(&out)
}

func splitBrokers(s string) []string {
	var out []string
	for _, b := range strings.Split(s, ",") {
		if b = strings.TrimSpace(b); b != "" {
			out = append(out, b)
		}
	}
	return out
}

func formatInt(n int) string {
	s := fmt.Sprintf("%d", n)
	var b strings.Builder
	for i, ch := range s {
		if i > 0 && (len(s)-i)%3 == 0 {
			b.WriteByte(',')
		}
		b.WriteRune(ch)
	}
	return b.String()
}

func printBox(title string, lines []string) {
	width := len(title) + 4
	for _, l := range lines {
		if len(l)+4 > width {
			width = len(l) + 4
		}
	}
	border := strings.Repeat("═", width)
	fmt.Printf("\n╔%s╗\n", border)
	pad := strings.Repeat(" ", (width-len(title))/2)
	fmt.Printf("║%s%s%s║\n", pad, title, strings.Repeat(" ", width-len(pad)-len(title)))
	fmt.Printf("╠%s╣\n", border)
	for _, l := range lines {
		fmt.Printf("║  %-*s  ║\n", width-4, l)
	}
	fmt.Printf("╚%s╝\n\n", border)
}
