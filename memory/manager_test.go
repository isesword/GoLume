package memory

import (
	"context"
	"errors"
	"testing"
)

func TestBudgetManager(t *testing.T) {
	m := NewBudgetManager(100)
	r, err := m.Reserve(context.Background(), 60)
	if err != nil || m.Used() != 60 {
		t.Fatalf("reserve: used=%d err=%v", m.Used(), err)
	}
	if _, err := m.Reserve(context.Background(), 50); !errors.Is(err, ErrBudgetExceeded) {
		t.Fatalf("expected budget error, got %v", err)
	}
	r.Release()
	if m.Used() != 0 {
		t.Fatalf("release: used=%d", m.Used())
	}
}
