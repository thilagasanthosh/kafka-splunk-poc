package hec

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"kafka-splunk-poc/internal/event"
)

var sample = event.SecurityEvent{ID: "e1", Tenant: "t1", Type: "malicious_url_click", Time: time.Unix(1700000000, 0)}

func server(t *testing.T, statuses ...int) (*Client, *atomic.Int32) {
	t.Helper()
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := int(calls.Add(1))
		if got := r.Header.Get("Authorization"); got != "Splunk test-token" {
			t.Errorf("auth header = %q", got)
		}
		w.WriteHeader(statuses[min(n, len(statuses))-1])
	}))
	t.Cleanup(srv.Close)
	c := New(srv.URL, "test-token")
	c.Backoff = time.Millisecond
	c.MaxAttempts = 3
	return c, &calls
}

func TestSendSucceedsOn200(t *testing.T) {
	c, calls := server(t, 200)
	if err := c.Send(context.Background(), sample); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 1 {
		t.Fatalf("calls = %d", calls.Load())
	}
}

func TestSendRetries503ThenSucceeds(t *testing.T) {
	c, calls := server(t, 503, 429, 200)
	if err := c.Send(context.Background(), sample); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 3 {
		t.Fatalf("calls = %d", calls.Load())
	}
}

func TestSendDoesNotRetry400(t *testing.T) {
	c, calls := server(t, 400)
	err := c.Send(context.Background(), sample)
	if !errors.Is(err, ErrRejected) {
		t.Fatalf("want ErrRejected, got %v", err)
	}
	if calls.Load() != 1 {
		t.Fatalf("calls = %d", calls.Load())
	}
}

func TestSendGivesUpAfterMaxAttemptsAsTransientError(t *testing.T) {
	c, calls := server(t, 500)
	err := c.Send(context.Background(), sample)
	if err == nil || errors.Is(err, ErrRejected) {
		t.Fatalf("want transient error, got %v", err)
	}
	if calls.Load() != 3 {
		t.Fatalf("calls = %d", calls.Load())
	}
}
