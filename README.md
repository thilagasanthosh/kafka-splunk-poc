# Kafka → Splunk forwarder (Go proof of concept)

One security event (a click on a malicious URL) travels from a Kafka topic to Splunk's
HTTP Event Collector (HEC). The Kafka offset is committed **only after Splunk returns 200**,
so delivery is at-least-once. It is a proof of concept, not production code.

```
produce ──► Kafka: security-events (3 partitions, key = tenant)
                 │
                 ▼
            forwarder ── POST /services/collector/event ──► Splunk HEC ──► index=main
                 │  200 → commit offset
                 │  429 / 5xx / timeout → retry with backoff (0.5s, 1s, 2s, 4s)
                 │  still failing → exit WITHOUT commit, container restarts, message re-read
                 │  bad JSON or 4xx → security-events-dlq (with error header), then commit
```

| File | What it does |
|---|---|
| `internal/event` | Event struct, `Parse` returns `ErrInvalid` for poison messages |
| `internal/hec` | HEC client: 5s timeout, retry on 429/5xx/network, `ErrRejected` on other 4xx |
| `internal/forwarder` | Fetch → parse → send → commit loop; dead-letter routing |
| `cmd/forwarder` | Wires kafka-go reader, DLQ writer and HEC client from env vars |
| `cmd/produce` | Publishes fictional sample events (`-n` count, `-bad` adds a broken message) |
| `splunk/apps/kafka_poc` | Splunk app with the security events dashboard |

## Run it

```bash
cp .env.example .env      # set a password and a HEC token (any UUID)
```

Read the Splunk licence terms, then set `SPLUNK_START_ARGS=--accept-license` in `.env`
yourself (newer images also want `SPLUNK_GENERAL_TERMS=--accept-sgt-current-at-splunk-com`).

```bash
docker compose up -d --build     # first Splunk start takes a few minutes (amd64 emulation on Apple Silicon)
go run ./cmd/produce -n 40 -bad   # 40 mixed events across two tenants + one broken message
docker compose logs -f forwarder
```

Open http://localhost:8000 (user `admin`, your `SPLUNK_PASSWORD`) and search:

```
index=main sourcetype=poc:security_event | table _time host event.type event.severity event.user event.indicator
```

Dashboard: http://localhost:8000/en-US/app/kafka_poc/security_events
(loaded from `splunk/apps/kafka_poc`, mounted into the Splunk container): event count,
critical count, users affected, events by type, severity by tenant, events over time, latest events.

### See the guarantees

```bash
# 1. Splunk down: forwarder retries, exits, offset is NOT committed (LAG stays 1)
docker compose stop splunk
go run ./cmd/produce
docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group splunk-forwarder

# 2. Splunk back: forwarder restarts, re-reads, delivers, LAG goes to 0
docker compose start splunk

# 3. Dead-letter topic
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic security-events-dlq --from-beginning --property print.headers=true
```

## Tests

```bash
go test ./...
```

Unit tests use `httptest` for HEC and in-memory fakes for Kafka, and check call order
(`send` before `commit`, no `commit` when Splunk is down).

## Not done (production)

- HEC indexer acknowledgement (`useACK`) — 200 means "received", not "indexed"
- TLS to HEC, token from a secret store
- Batching events per HEC request; parallel partitions
- Metrics (lag, retries, DLQ count) and alerting
