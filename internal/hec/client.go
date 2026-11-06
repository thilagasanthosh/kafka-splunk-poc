// Package hec sends events to the Splunk HTTP Event Collector.
package hec

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"time"

	"kafka-splunk-poc/internal/event"
)

// ErrRejected means Splunk refused the event (4xx other than 429). Retrying will not help.
var ErrRejected = errors.New("rejected by splunk")

// Client posts events to HEC with timeouts and retry with backoff on 429, 5xx and network errors.
type Client struct {
	URL         string // e.g. http://splunk:8088/services/collector/event
	Token       string
	HTTP        *http.Client
	MaxAttempts int
	Backoff     time.Duration // first wait; doubles each retry
}

func New(url, token string) *Client {
	return &Client{
		URL:         url,
		Token:       token,
		HTTP:        &http.Client{Timeout: 5 * time.Second},
		MaxAttempts: 5,
		Backoff:     500 * time.Millisecond,
	}
}

type payload struct {
	Time       float64             `json:"time"`
	Host       string              `json:"host"`
	Source     string              `json:"source"`
	Sourcetype string              `json:"sourcetype"`
	Event      event.SecurityEvent `json:"event"`
}

// Send returns nil only when Splunk acknowledged the event with HTTP 200.
func (c *Client) Send(ctx context.Context, e event.SecurityEvent) error {
	body, err := json.Marshal(payload{
		Time:       float64(e.Time.UnixMilli()) / 1000,
		Host:       e.Tenant,
		Source:     "kafka-forwarder",
		Sourcetype: "poc:security_event",
		Event:      e,
	})
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}

	wait := c.Backoff
	var lastErr error
	for attempt := 1; attempt <= c.MaxAttempts; attempt++ {
		retry, err := c.post(ctx, body)
		if err == nil {
			slog.Info("hec accepted", "event", e.ID, "attempt", attempt)
			return nil
		}
		if !retry {
			slog.Warn("hec rejected", "event", e.ID, "err", err)
			return err
		}
		lastErr = err
		if attempt == c.MaxAttempts {
			break
		}
		slog.Warn("hec retry", "event", e.ID, "attempt", attempt, "wait", wait, "err", err)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(wait):
		}
		wait *= 2
	}
	return fmt.Errorf("gave up after %d attempts: %w", c.MaxAttempts, lastErr)
}

// post makes one request and says whether a failure is worth retrying.
func (c *Client) post(ctx context.Context, body []byte) (retry bool, err error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.URL, bytes.NewReader(body))
	if err != nil {
		return false, err
	}
	req.Header.Set("Authorization", "Splunk "+c.Token)
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.HTTP.Do(req)
	if err != nil {
		return true, err // timeout, connection refused, Splunk still starting
	}
	defer resp.Body.Close()
	msg, _ := io.ReadAll(io.LimitReader(resp.Body, 512))

	switch {
	case resp.StatusCode == http.StatusOK:
		return false, nil
	case resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500:
		return true, fmt.Errorf("status %d: %s", resp.StatusCode, msg)
	default:
		return false, fmt.Errorf("%w: status %d: %s", ErrRejected, resp.StatusCode, msg)
	}
}
