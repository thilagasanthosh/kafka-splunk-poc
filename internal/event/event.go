// Package event defines the security event that travels from Kafka to Splunk.
package event

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// SecurityEvent is one classic attack event, for example a click on a malicious URL.
type SecurityEvent struct {
	ID        string    `json:"id"`
	Tenant    string    `json:"tenant"`
	Type      string    `json:"type"`     // e.g. "malicious_url_click"
	Severity  string    `json:"severity"` // low | medium | high | critical
	User      string    `json:"user"`
	Indicator string    `json:"indicator"` // URL, hash or sender address
	Time      time.Time `json:"time"`
}

// ErrInvalid marks a message that can never be delivered (bad JSON or missing fields).
var ErrInvalid = errors.New("invalid event")

// Parse decodes and validates a Kafka message value.
func Parse(raw []byte) (SecurityEvent, error) {
	var e SecurityEvent
	if err := json.Unmarshal(raw, &e); err != nil {
		return SecurityEvent{}, fmt.Errorf("%w: %v", ErrInvalid, err)
	}
	for name, v := range map[string]string{"id": e.ID, "tenant": e.Tenant, "type": e.Type} {
		if v == "" {
			return SecurityEvent{}, fmt.Errorf("%w: missing %s", ErrInvalid, name)
		}
	}
	if e.Time.IsZero() {
		return SecurityEvent{}, fmt.Errorf("%w: missing time", ErrInvalid)
	}
	return e, nil
}
