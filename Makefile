BROKERS       ?= 127.0.0.1:19092,127.0.0.1:29092,127.0.0.1:39092
DSN           ?= postgres://postgres:postgres@localhost:5432/webhooks?sslmode=disable
WEBHOOK_URL   ?= http://localhost:8090/webhook
MOCK_API_ADDR ?= http://localhost:8090

NUM_MESSAGES  ?= 1000000
WHALE         ?= false
FAIL_RATE     ?= 0.01
WORKERS       ?= 100
MAX_RETRIES   ?= 3

.PHONY: help up down logs tidy build \
        mockapi consumer consumer-drr producer \
        bench-normal bench-whale bench-small dlq-stats

# ── infra ────────────────────────────────────────────────────────────────────
help:
	@echo ""
	@echo "  make up            Start Kafka (KRaft) + Postgres + Kafka-UI"
	@echo "  make down          Tear down all containers and volumes"
	@echo ""
	@echo "  make build         Compile all binaries into ./bin/"
	@echo "  make tidy          go mod tidy + download"
	@echo ""
	@echo "  make mockapi       Run mock webhook receiver (terminal 1)"
	@echo "  make consumer      Run webhook notifier consumer (terminal 2)"
	@echo "  make consumer-drr  Run DRR-fair consumer        (terminal 2)"
	@echo "  make producer      Run event producer            (terminal 3)"
	@echo ""
	@echo "  make bench-normal  Quick benchmark: 100k msgs, no whales"
	@echo "  make bench-whale   Quick benchmark: 100k msgs, whale accounts ON"
	@echo ""
	@echo "  make dlq-stats     Show DLQ top failures from Postgres"
	@echo ""
	@echo "  Kafka UI → http://localhost:8080"
	@echo ""

up:
	docker compose up -d
	@echo ""
	@echo "Waiting for Kafka and Postgres to be healthy …"
	@docker compose wait kafka postgres 2>/dev/null || true
	@echo "Infrastructure ready."
	@echo "  Kafka UI → http://localhost:8080"

down:
	docker compose down -v

# GO
tidy:
	go mod tidy

build: tidy
	@mkdir -p bin
	go build -o bin/mockapi      ./cmd/mockapi
	go build -o bin/consumer     ./cmd/consumer
	go build -o bin/producer     ./cmd/producer
	@echo "Built: bin/mockapi  bin/consumer  bin/consumer-drr  bin/producer"

# Run components
mockapi:
	./bin/mockapi \
		-failRate   $(FAIL_RATE) \
		-port       8090

consumer:
	./bin/consumer \
		-brokers        $(BROKERS) \
		-dsn            $(DSN) \
		-workers        $(WORKERS) \
		-maxRetries     $(MAX_RETRIES) \
		-totalMessages  $(NUM_MESSAGES) \
		-mockAPIAddr    $(MOCK_API_ADDR)

dlq-stats:
	@echo "=== Dead Letter Queue — top failing accounts ==="
	@psql "$(DSN)" -c "\
		SELECT account_id, status, COUNT(*) AS failures, MAX(delivered_at) AS last_failure \
		FROM dead_letter_queue \
		GROUP BY account_id, status \
		ORDER BY failures DESC \
		LIMIT 20;"
	@echo ""
	@echo "=== Dead Letter Queue — total rows by status ==="
	@psql "$(DSN)" -c "\
		SELECT status, COUNT(*) AS total \
		FROM dead_letter_queue \
		GROUP BY status \
		ORDER BY total DESC;"

producer:
	./bin/producer \
		-brokers              $(BROKERS) \
		-dsn                  $(DSN) \
		-webhookURL           $(WEBHOOK_URL) \
		-numberOfMessages     $(NUM_MESSAGES) \
		-whaleAccountEnabled=$(WHALE)


# bench-normal: 100k messages, no whale accounts
# Runs all three components sequentially in the background; waits for consumer to finish.
bench-normal: build
	@echo "=== BENCHMARK: normal accounts (no whales), $(NUM_MESSAGES) messages ==="
	@$(MAKE) _run-bench WHALE=false

bench-whale: build
	@echo "=== BENCHMARK: whale accounts ENABLED, $(NUM_MESSAGES) messages ==="
	@$(MAKE) _run-bench WHALE=true

_run-bench:
	@# Reset mock API counters if it is already running, else start it.
	curl -sf $(MOCK_API_ADDR)/reset >/dev/null 2>&1 || \
		(./bin/mockapi -failRate $(FAIL_RATE) & echo $$! > /tmp/mockapi.pid && sleep 1)
	@# Start consumer in background.
	./bin/consumer \
		-brokers        $(BROKERS) \
		-dsn            $(DSN) \
		-workers        $(WORKERS) \
		-maxRetries     $(MAX_RETRIES) \
		-totalMessages  $(NUM_MESSAGES) \
		-mockAPIAddr    $(MOCK_API_ADDR) & echo $$! > /tmp/consumer.pid
	@sleep 2
	@# Run producer (blocks until done).
	./bin/producer \
		-brokers              $(BROKERS) \
		-dsn                  $(DSN) \
		-webhookURL           $(WEBHOOK_URL) \
		-numberOfMessages     $(NUM_MESSAGES) \
		-whaleAccountEnabled=$(WHALE)
	@# Wait for consumer to finish.
	@wait $$(cat /tmp/consumer.pid 2>/dev/null) 2>/dev/null || true
	@# Clean up background mock API if we started it.
	@-kill $$(cat /tmp/mockapi.pid 2>/dev/null) 2>/dev/null; rm -f /tmp/mockapi.pid /tmp/consumer.pid
