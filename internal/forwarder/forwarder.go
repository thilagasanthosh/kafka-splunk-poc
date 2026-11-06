// Package forwarder moves events from Kafka to Splunk with at-least-once delivery:
// the Kafka offset is committed only after Splunk accepted the event.
package forwarder

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/segmentio/kafka-go"

	"kafka-splunk-poc/internal/event"
	"kafka-splunk-poc/internal/hec"
)

type Reader interface {
	FetchMessage(ctx context.Context) (kafka.Message, error)
	CommitMessages(ctx context.Context, msgs ...kafka.Message) error
}

type Sender interface {
	Send(ctx context.Context, e event.SecurityEvent) error
}

type DeadLetter interface {
	WriteMessages(ctx context.Context, msgs ...kafka.Message) error
}

type Forwarder struct {
	In  Reader
	Out Sender
	DLQ DeadLetter
}

// Run processes messages until ctx is cancelled or delivery fails in a way that
// needs a restart. It returns that error instead of committing, so the message
// is read again after restart.
func (f *Forwarder) Run(ctx context.Context) error {
	for {
		msg, err := f.In.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("fetch: %w", err)
		}
		if err := f.handle(ctx, msg); err != nil {
			return err
		}
	}
}

func (f *Forwarder) handle(ctx context.Context, msg kafka.Message) error {
	e, err := event.Parse(msg.Value)
	if err == nil {
		err = f.Out.Send(ctx, e)
	}

	switch {
	case err == nil:
		// delivered
	case errors.Is(err, event.ErrInvalid), errors.Is(err, hec.ErrRejected):
		// poison message: park it so it does not block the partition
		if dlqErr := f.toDLQ(ctx, msg, err); dlqErr != nil {
			return dlqErr
		}
	default:
		// Splunk unavailable after retries: do NOT commit
		return fmt.Errorf("deliver offset %d: %w", msg.Offset, err)
	}

	if err := f.In.CommitMessages(ctx, msg); err != nil {
		return fmt.Errorf("commit offset %d: %w", msg.Offset, err)
	}
	slog.Info("committed", "partition", msg.Partition, "offset", msg.Offset)
	return nil
}

func (f *Forwarder) toDLQ(ctx context.Context, msg kafka.Message, cause error) error {
	slog.Warn("dead-letter", "offset", msg.Offset, "err", cause)
	dead := kafka.Message{
		Key:     msg.Key,
		Value:   msg.Value,
		Headers: append(msg.Headers, kafka.Header{Key: "error", Value: []byte(cause.Error())}),
	}
	if err := f.DLQ.WriteMessages(ctx, dead); err != nil {
		return fmt.Errorf("dead-letter offset %d: %w", msg.Offset, err)
	}
	return nil
}
