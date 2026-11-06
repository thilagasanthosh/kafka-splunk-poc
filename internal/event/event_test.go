package event

import (
	"errors"
	"testing"
)

func TestParseAcceptsCompleteEvent(t *testing.T) {
	e, err := Parse([]byte(`{"id":"e1","tenant":"t1","type":"malicious_url_click","time":"2026-11-06T10:00:00Z"}`))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if e.ID != "e1" || e.Tenant != "t1" {
		t.Fatalf("got %+v", e)
	}
}

func TestParseRejectsBrokenJSON(t *testing.T) {
	if _, err := Parse([]byte(`{"id":`)); !errors.Is(err, ErrInvalid) {
		t.Fatalf("want ErrInvalid, got %v", err)
	}
}

func TestParseRejectsMissingTenant(t *testing.T) {
	_, err := Parse([]byte(`{"id":"e1","type":"x","time":"2026-11-06T10:00:00Z"}`))
	if !errors.Is(err, ErrInvalid) {
		t.Fatalf("want ErrInvalid, got %v", err)
	}
}
