// Package dlq A non-blocking Dead Letter Queue writer backed by
// PostgreSQL. Failed webhook deliveries are inserted into the
// dead_letter_queue table defined in init.sql.
//
// A background goroutine flushes rows every FlushInterval or whenever the batch reaches
// BatchSize.
package dlq

import (
	"context"
	"database/sql"
	"log"
	"sync/atomic"
	"time"
)

type Entry struct {
	EventID      string
	AccountID    string
	WebhookURL   string
	Status       string // e.g. "exhausted"
	Attempts     int
	ErrorMessage string
}

// Writer is a goroutine-safe, non-blocking DLQ writer.
// Create one with [New] and stop it by cancelling the context passed to New.
type Writer struct {
	db      *sql.DB
	ch      chan Entry
	dropped atomic.Int64
	written atomic.Int64

	batchSize     int
	flushInterval time.Duration
}

const (
	defaultChannelSize   = 8192
	defaultBatchSize     = 256
	defaultFlushInterval = 2 * time.Second
)

// New creates a Writer and starts its background flush goroutine.
func New(ctx context.Context, db *sql.DB) *Writer {
	w := &Writer{
		db:            db,
		ch:            make(chan Entry, defaultChannelSize),
		batchSize:     defaultBatchSize,
		flushInterval: defaultFlushInterval,
	}
	go w.run(ctx)
	return w
}

// Record enqueues a failed delivery for async persistence.
// If the internal channel is full the entry is dropped and the drop counter is incremented.
func (w *Writer) Record(e Entry) {
	select {
	case w.ch <- e:
	default:
		w.dropped.Add(1)
	}
}

// Stats returns (written, dropped) counters.
func (w *Writer) Stats() (written, dropped int64) {
	return w.written.Load(), w.dropped.Load()
}

func (w *Writer) run(ctx context.Context) {
	ticker := time.NewTicker(w.flushInterval)
	defer ticker.Stop()

	batch := make([]Entry, 0, w.batchSize)

	flush := func() {
		if len(batch) == 0 {
			return
		}
		if err := w.insertBatch(ctx, batch); err != nil {
			log.Printf("[dlq] flush error (batch=%d): %v", len(batch), err)
		} else {
			w.written.Add(int64(len(batch)))
		}
		batch = batch[:0]
	}

	for {
		select {
		case e := <-w.ch:
			batch = append(batch, e)
			if len(batch) >= w.batchSize {
				flush()
			}
		case <-ticker.C:
			flush()
		case <-ctx.Done():
			// Drain remaining entries before exiting.
			for {
				select {
				case e := <-w.ch:
					batch = append(batch, e)
				default:
					flush()
					return
				}
			}
		}
	}
}

// insertBatch persists a batch of entries in a single transaction.
func (w *Writer) insertBatch(ctx context.Context, entries []Entry) error {
	tx, err := w.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck

	stmt, err := tx.PrepareContext(ctx, `
		INSERT INTO dead_letter_queue
			(event_id, account_id, webhook_url, status, attempts, error_message, delivered_at)
		VALUES ($1, $2, $3, $4, $5, $6, NOW())
	`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for _, e := range entries {
		if _, err := stmt.ExecContext(ctx, e.EventID, e.AccountID, e.WebhookURL, e.Status, e.Attempts, e.ErrorMessage); err != nil {
			return err
		}
	}
	return tx.Commit()
}
