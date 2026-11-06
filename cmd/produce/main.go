// Command produce publishes sample attack events (and optionally one bad message) to Kafka.
//
//	go run ./cmd/produce            # one malicious URL click
//	go run ./cmd/produce -n 40      # 40 mixed events across two tenants
//	go run ./cmd/produce -bad       # also a broken message, to see the dead-letter topic
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand/v2"
	"os"
	"time"

	"github.com/segmentio/kafka-go"

	"kafka-splunk-poc/internal/event"
)

var (
	tenants  = []string{"acme-demo", "globex-demo"}
	types    = []string{"malicious_url_click", "phishing_email_delivered", "malware_attachment_opened", "credential_submission"}
	severity = map[string]string{
		"malicious_url_click":       "high",
		"phishing_email_delivered":  "medium",
		"malware_attachment_opened": "critical",
		"credential_submission":     "critical",
	}
	users = []string{"alice", "bob", "carol", "dave"}
)

func main() {
	n := flag.Int("n", 1, "number of events")
	bad := flag.Bool("bad", false, "also send an invalid message")
	flag.Parse()

	brokers := os.Getenv("KAFKA_BROKERS")
	if brokers == "" {
		brokers = "localhost:9094"
	}
	w := &kafka.Writer{Addr: kafka.TCP(brokers), Topic: "security-events", Balancer: &kafka.Hash{}, RequiredAcks: kafka.RequireAll}
	defer w.Close()

	now := time.Now().UTC()
	msgs := make([]kafka.Message, 0, *n+1)
	for i := 0; i < *n; i++ {
		e := sample(i, now)
		value, err := json.Marshal(e)
		if err != nil {
			log.Fatal(err)
		}
		msgs = append(msgs, kafka.Message{Key: []byte(e.Tenant), Value: value})
	}
	if *bad {
		msgs = append(msgs, kafka.Message{Key: []byte("acme-demo"), Value: []byte(`{"oops":`)})
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := w.WriteMessages(ctx, msgs...); err != nil {
		log.Fatalf("produce: %v", err)
	}
	log.Printf("sent %d message(s)", len(msgs))
}

// sample builds fictional event i; the first one is always a malicious URL click.
func sample(i int, now time.Time) event.SecurityEvent {
	t, tenant := types[0], tenants[0]
	if i > 0 {
		t, tenant = types[rand.IntN(len(types))], tenants[rand.IntN(len(tenants))]
	}
	user := users[rand.IntN(len(users))]
	return event.SecurityEvent{
		ID:        fmt.Sprintf("evt-%d-%d", now.UnixNano(), i),
		Tenant:    tenant,
		Type:      t,
		Severity:  severity[t],
		User:      fmt.Sprintf("%s@%s.test", user, tenant),
		Indicator: fmt.Sprintf("http://login-%s.example/verify?c=%d", tenant, i),
		Time:      now.Add(-time.Duration(rand.IntN(3600)) * time.Second),
	}
}
