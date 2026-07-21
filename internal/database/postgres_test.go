package database

import (
	"context"
	"strings"
	"testing"
)

func TestConnectRejectsInvalidDatabaseURL(t *testing.T) {
	pool, err := Connect(context.Background(), "://invalid-url")
	if err == nil {
		t.Fatal("expected invalid database URL to return an error")
	}

	if pool != nil {
		t.Fatal("expected pool to be nil when connection fails")
	}

	if !strings.Contains(err.Error(), "parse database configuration") {
		t.Fatalf("expected parse database configuration error, got %q", err.Error())
	}
}
