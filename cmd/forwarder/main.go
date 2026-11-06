// Command forwarder reads security events from Kafka and delivers them to Splunk HEC.
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/segmentio/kafka-go"

	"kafka-splunk-poc/internal/forwarder"
	"kafka-splunk-poc/internal/hec"
)

func main() {
	brokers := strings.Split(env("KAFKA_BROKERS", "localhost:9094"), ",")
	topic := env("KAFKA_TOPIC", "security-events")
	token := os.Getenv("SPLUNK_HEC_TOKEN")
	if token == "" {
		slog.Error("SPLUNK_HEC_TOKEN is not set")
		os.Exit(1)
	}

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     brokers,
		Topic:       topic,
		GroupID:     env("KAFKA_GROUP", "splunk-forwarder"),
		StartOffset: kafka.FirstOffset,
		// CommitInterval 0 = synchronous commits; we commit by hand after Splunk acks.
	})
	defer reader.Close()

	dlq := &kafka.Writer{Addr: kafka.TCP(brokers...), Topic: topic + "-dlq", Balancer: &kafka.Hash{}, RequiredAcks: kafka.RequireAll}
	defer dlq.Close()

	f := &forwarder.Forwarder{
		In:  reader,
		Out: hec.New(env("SPLUNK_HEC_URL", "http://localhost:8088/services/collector/event"), token),
		DLQ: dlq,
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	slog.Info("forwarder started", "brokers", brokers, "topic", topic)
	if err := f.Run(ctx); err != nil {
		slog.Error("forwarder stopped", "err", err)
		os.Exit(1) // container restarts and re-reads the uncommitted message
	}
	slog.Info("forwarder shut down")
}

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
