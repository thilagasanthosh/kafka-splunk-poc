package forwarder

import (
	"context"
	"errors"
	"io"
	"reflect"
	"testing"

	"github.com/segmentio/kafka-go"

	"kafka-splunk-poc/internal/event"
	"kafka-splunk-poc/internal/hec"
)

const good = `{"id":"e1","tenant":"t1","type":"malicious_url_click","time":"2026-11-06T10:00:00Z"}`

// fakes record every call in one shared log so tests can check the order.
type world struct {
	queue   []kafka.Message
	sendErr error
	log     []string
}

func (w *world) FetchMessage(ctx context.Context) (kafka.Message, error) {
	if len(w.queue) == 0 {
		return kafka.Message{}, io.EOF
	}
	m := w.queue[0]
	w.queue = w.queue[1:]
	return m, nil
}
func (w *world) CommitMessages(ctx context.Context, msgs ...kafka.Message) error {
	w.log = append(w.log, "commit")
	return nil
}
func (w *world) Send(ctx context.Context, e event.SecurityEvent) error {
	w.log = append(w.log, "send")
	return w.sendErr
}
func (w *world) WriteMessages(ctx context.Context, msgs ...kafka.Message) error {
	w.log = append(w.log, "dlq")
	return nil
}

func run(w *world) error {
	f := &Forwarder{In: w, Out: w, DLQ: w}
	return f.Run(context.Background())
}

func TestCommitsOnlyAfterSplunkAccepts(t *testing.T) {
	w := &world{queue: []kafka.Message{{Value: []byte(good)}}}
	_ = run(w)
	if want := []string{"send", "commit"}; !reflect.DeepEqual(w.log, want) {
		t.Fatalf("log = %v, want %v", w.log, want)
	}
}

func TestDoesNotCommitWhenSplunkIsDown(t *testing.T) {
	w := &world{queue: []kafka.Message{{Value: []byte(good)}, {Value: []byte(good)}}, sendErr: errors.New("connection refused")}
	err := run(w)
	if err == nil {
		t.Fatal("want error so the process restarts")
	}
	if want := []string{"send"}; !reflect.DeepEqual(w.log, want) {
		t.Fatalf("log = %v, want %v (no commit, no second message)", w.log, want)
	}
}

func TestBrokenMessageGoesToDeadLetterThenCommits(t *testing.T) {
	w := &world{queue: []kafka.Message{{Value: []byte(`{"id":`)}}}
	_ = run(w)
	if want := []string{"dlq", "commit"}; !reflect.DeepEqual(w.log, want) {
		t.Fatalf("log = %v, want %v", w.log, want)
	}
}

func TestRejectedEventGoesToDeadLetterThenCommits(t *testing.T) {
	w := &world{queue: []kafka.Message{{Value: []byte(good)}}, sendErr: hec.ErrRejected}
	_ = run(w)
	if want := []string{"send", "dlq", "commit"}; !reflect.DeepEqual(w.log, want) {
		t.Fatalf("log = %v, want %v", w.log, want)
	}
}
