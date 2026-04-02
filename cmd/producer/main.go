package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"github.com/ro-vn/webhook-notifier/internal/event"
	"log"
	"math/rand"
	"strings"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	_ "github.com/lib/pq"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	normalAccountCount = 990
	whaleAccountCount  = 10
)

func main() {
	numMessages := flag.Int("numberOfMessages", 1_000_000, "Total events to produce")
	whaleEnabled := flag.Bool("whaleAccountEnabled", false, "Let 10 whale accounts generate 90% of events")
	brokerStr := flag.String("brokers", "127.0.0.1:19092,127.0.0.1:29092,127.0.0.1:39092", "Comma-separated Kafka brokers")
	dsn := flag.String("dsn", "postgres://postgres:postgres@localhost:5432/webhooks?sslmode=disable", "PostgreSQL DSN")
	webhookURL := flag.String("webhookURL", "http://localhost:8090/webhook", "URL seeded into webhook_subscriptions")
	partitions := flag.Int("partitions", 12, "Kafka topic partition count")
	flag.Parse()

	brokers := splitBrokers(*brokerStr)

	whaleAccounts := make([]string, whaleAccountCount)
	normalAccounts := make([]string, normalAccountCount)
	for i := range whaleAccounts {
		whaleAccounts[i] = fmt.Sprintf("whale-%04d", i)
	}
	for i := range normalAccounts {
		normalAccounts[i] = fmt.Sprintf("account-%04d", i)
	}
	allAccounts := append(whaleAccounts, normalAccounts...)

	db, err := sql.Open("postgres", *dsn)
	if err != nil {
		log.Fatalf("db open: %v", err)
	}
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(10)
	defer func(db *sql.DB) {
		_ = db.Close()
	}(db)

	log.Printf("Seeding %d webhook subscriptions → %s", len(allAccounts), *webhookURL)
	if err := seedWebhooks(db, allAccounts, *webhookURL, "subscriber.created"); err != nil {
		log.Fatalf("seed: %v", err)
	}

	ctx := context.Background()
	ensureTopic(ctx, brokers, int32(*partitions))

	// Kafka producer
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.DefaultProduceTopic(event.TopicName),
		// Route by account key so all events for an account land on the same
		// partition, preserving per-account ordering.
		kgo.RecordPartitioner(kgo.StickyKeyPartitioner(nil)),
		kgo.ProducerBatchMaxBytes(1<<20),       // 1 MB batch
		kgo.MaxBufferedRecords(500_000),        // allow large in-flight buffer
		kgo.ProducerLinger(5*time.Millisecond), // small linger for batching
	)
	if err != nil {
		log.Fatalf("kafka client: %v", err)
	}
	defer client.Close()

	var (
		acked      atomic.Int64
		errored    atomic.Int64
		totalBytes atomic.Int64
	)

	log.Printf("Producing %s events  whale=%v …", formatInt(*numMessages), *whaleEnabled)
	start := time.Now()

	for i := 0; i < *numMessages; i++ {
		accountID := pickAccount(*whaleEnabled, whaleAccounts, normalAccounts)

		payload, _ := json.Marshal(event.SubscriberPayload{
			Email:     fmt.Sprintf("user%d@example.com", rand.Intn(10_000_000)), //nolint:gosec
			FirstName: "Jane",
			LastName:  "Doe",
		})

		ev := event.Event{
			EventID:   uuid.New().String(),
			AccountID: accountID,
			EventType: "subscriber.created",
			Payload:   json.RawMessage(payload),
			CreatedAt: time.Now(),
		}

		data, _ := json.Marshal(ev)
		totalBytes.Add(int64(len(data)))

		client.Produce(ctx, &kgo.Record{
			Key:   []byte(accountID),
			Value: data,
		}, func(_ *kgo.Record, err error) {
			if err != nil {
				errored.Add(1)
			} else {
				acked.Add(1)
			}
		})
	}

	log.Println("Flushing buffered records …")
	if err := client.Flush(ctx); err != nil {
		log.Printf("flush: %v", err)
	}

	duration := time.Since(start)
	msgs := acked.Load()
	bytes := totalBytes.Load()

	printBox("PRODUCER BENCHMARK RESULTS", []string{
		fmt.Sprintf("%-22s %s", "Duration:", duration.Round(time.Millisecond)),
		fmt.Sprintf("%-22s %s", "Messages sent:", formatInt(int(msgs))),
		fmt.Sprintf("%-22s %s", "Errors:", formatInt(int(errored.Load()))),
		fmt.Sprintf("%-22s %.0f msg/sec", "Throughput:", float64(msgs)/duration.Seconds()),
		fmt.Sprintf("%-22s %.2f MB/sec", "Data throughput:", float64(bytes)/duration.Seconds()/1024/1024),
		fmt.Sprintf("%-22s %.0f bytes", "Avg message size:", func() float64 {
			if msgs == 0 {
				return 0
			}
			return float64(bytes) / float64(msgs)
		}()),
		fmt.Sprintf("%-22s %v", "Whale accounts:", *whaleEnabled),
		fmt.Sprintf("%-22s %d normal / %d whale", "Account mix:", normalAccountCount, whaleAccountCount),
	})
}

func pickAccount(whaleEnabled bool, whales, normals []string) string {
	if whaleEnabled && rand.Float64() < 0.9 {
		return whales[rand.Intn(len(whales))]
	}
	return normals[rand.Intn(len(normals))]
}

func ensureTopic(ctx context.Context, brokers []string, partitions int32) {
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		log.Fatalf("admin kgo client: %v", err)
	}
	defer cl.Close()

	admin := kadm.NewClient(cl)
	defer admin.Close()

	resp, err := admin.CreateTopics(ctx, partitions, 1, nil, event.TopicName)
	if err != nil {
		log.Fatalf("create topics request: %v", err)
	}
	for _, r := range resp {
		if r.Err != nil && !errors.Is(r.Err, kerr.TopicAlreadyExists) {
			log.Fatalf("create topic %s: %v", r.Topic, r.Err)
		}
	}
	log.Printf("Topic %q ready  partitions=%d", event.TopicName, partitions)
}

func seedWebhooks(db *sql.DB, accounts []string, url, eventType string) error {
	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("begin: %w", err)
	}
	defer tx.Rollback() //nolint:errcheck

	if _, err := tx.Exec("TRUNCATE webhook_subscriptions"); err != nil {
		return fmt.Errorf("truncate: %w", err)
	}

	stmt, err := tx.Prepare(
		"INSERT INTO webhook_subscriptions (account_id, url, event_type) VALUES ($1, $2, $3)",
	)
	if err != nil {
		return fmt.Errorf("prepare: %w", err)
	}
	defer stmt.Close()

	for _, acc := range accounts {
		if _, err := stmt.Exec(acc, url, eventType); err != nil {
			return fmt.Errorf("insert %s: %w", acc, err)
		}
	}
	return tx.Commit()
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
