package engine_test

import (
	"context"
	"errors"
	"gocolumn/engine"
	"testing"
)

func TestEnginePrepareValidatesExecutionPolicy(t *testing.T) {
	query := nullableQuery("direct-fused", 1)

	invalid := engine.DefaultOptions()
	invalid.ExecutionPolicy = engine.ExecutionPolicy(255)
	invalidEngine := engine.New(invalid)
	defer invalidEngine.Close()
	if _, err := invalidEngine.Prepare(context.Background(), nullableI32Source(t), query, engine.PrepareOptions{}); err == nil {
		t.Fatal("invalid execution policy was accepted")
	}

	unlimited := engine.DefaultOptions()
	unlimited.ExecutionPolicy = engine.ExecutionBounded
	unlimitedEngine := engine.New(unlimited)
	defer unlimitedEngine.Close()
	if _, err := unlimitedEngine.Prepare(context.Background(), nullableI32Source(t), query, engine.PrepareOptions{}); !errors.Is(err, engine.ErrBoundedMemoryLimitRequired) {
		t.Fatalf("bounded unlimited error=%v", err)
	}

	limited := engine.DefaultOptions()
	limited.ExecutionPolicy = engine.ExecutionBounded
	limited.MemoryLimitBytes = 1 << 20
	limitedEngine := engine.New(limited)
	defer limitedEngine.Close()
	_, err := limitedEngine.Prepare(context.Background(), nullableI32Source(t), query, engine.PrepareOptions{})
	var unsupported *engine.UnsupportedBoundedPlanError
	if !errors.As(err, &unsupported) || unsupported.Operator != "Aggregate" || !errors.Is(err, engine.ErrBoundedSpillRequired) {
		t.Fatalf("bounded aggregate error=%T %v", err, err)
	}
}
