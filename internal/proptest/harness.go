package proptest

import (
	"context"
	"fmt"
	"testing"

	"gocolumn/engine"
	"gocolumn/kernel"
	"gocolumn/lazy"
)

// Backend is one named execution configuration under test. Run must execute
// the query built by build over table and return its fully materialized
// result. Backends are expected to be deterministic: the same inputs must
// always produce the same relation.
type Backend struct {
	Name string
	Run  func(ctx context.Context, table *Table, build func(lazy.LazyFrame) lazy.LazyFrame) (*Relation, error)
}

// EngineBackend returns a backend that executes each query on a fresh engine
// configured with the given kernel mode. A new engine, session, and source
// are created per run so no execution state leaks between queries.
func EngineBackend(name string, mode kernel.Mode) Backend {
	return Backend{
		Name: name,
		Run: func(ctx context.Context, table *Table, build func(lazy.LazyFrame) lazy.LazyFrame) (*Relation, error) {
			src, err := table.Source()
			if err != nil {
				return nil, err
			}
			eng := engine.New(engine.Options{KernelMode: mode})
			defer func() { _ = eng.Close() }()
			session := lazy.NewSessionWithEngine(eng)
			frame, err := build(session.FromSource(src)).Collect(ctx)
			if err != nil {
				return nil, err
			}
			defer func() { _ = frame.Close() }()
			return RelationFromFrame(frame)
		},
	}
}

// DefaultBackends returns the standard kernel-mode matrix: forced scalar,
// gate-approved auto, and forced SIMD. Under toolchains without SIMD support
// the forced-SIMD backend legally degenerates to scalar. GOEXPERIMENT/GODEBUG
// SIMD variants are exercised by running the test binary under those
// environment settings; they need no separate registration here.
func DefaultBackends() []Backend {
	return []Backend{
		EngineBackend(kernel.ModeScalar.String(), kernel.ModeScalar),
		EngineBackend(kernel.ModeAuto.String(), kernel.ModeAuto),
		EngineBackend(kernel.ModeSIMD.String(), kernel.ModeSIMD),
	}
}

// Query is one backend-consistency test case: a named logical query built
// over a shared input table.
type Query struct {
	// Name identifies the query in diagnostics.
	Name string
	// Table is the shared input scanned by every backend.
	Table *Table
	// Build appends logical operations to the scanned table. It must be
	// deterministic and side-effect free; each backend calls it once.
	Build func(lazy.LazyFrame) lazy.LazyFrame
}

// Harness runs one query on every registered backend and requires all
// results to be row-by-row, column-by-column identical, including NULL
// bitmaps and float bit patterns.
type Harness struct {
	backends []Backend
}

// NewHarness constructs a harness over at least two distinct named backends.
func NewHarness(backends ...Backend) (*Harness, error) {
	if len(backends) < 2 {
		return nil, fmt.Errorf("proptest: harness requires at least 2 backends, got %d", len(backends))
	}
	seen := make(map[string]struct{}, len(backends))
	for i, b := range backends {
		if b.Name == "" {
			return nil, fmt.Errorf("proptest: backend %d has an empty name", i)
		}
		if b.Run == nil {
			return nil, fmt.Errorf("proptest: backend %q has a nil Run function", b.Name)
		}
		if _, dup := seen[b.Name]; dup {
			return nil, fmt.Errorf("proptest: duplicate backend name %q", b.Name)
		}
		seen[b.Name] = struct{}{}
	}
	return &Harness{backends: append([]Backend(nil), backends...)}, nil
}

// Backends returns the registered backend names in registration order.
func (h *Harness) Backends() []string {
	names := make([]string, len(h.backends))
	for i, b := range h.backends {
		names[i] = b.Name
	}
	return names
}

// Check executes q on every backend and compares each result against the
// first (reference) backend. A divergence error names the query, both
// backends, and the first differing row, column, and cell values.
func (h *Harness) Check(ctx context.Context, q Query) error {
	if ctx == nil {
		return fmt.Errorf("proptest: harness context is nil")
	}
	if q.Name == "" {
		return fmt.Errorf("proptest: query has an empty name")
	}
	if q.Table == nil {
		return fmt.Errorf("proptest: query %q has a nil table", q.Name)
	}
	if q.Build == nil {
		return fmt.Errorf("proptest: query %q has a nil Build function", q.Name)
	}
	reference := h.backends[0]
	want, err := reference.Run(ctx, q.Table, q.Build)
	if err != nil {
		return fmt.Errorf("proptest: query %q: reference backend %q failed: %w", q.Name, reference.Name, err)
	}
	for _, backend := range h.backends[1:] {
		got, err := backend.Run(ctx, q.Table, q.Build)
		if err != nil {
			return fmt.Errorf("proptest: query %q: backend %q failed: %w", q.Name, backend.Name, err)
		}
		if err := CheckEqual(want, got); err != nil {
			return fmt.Errorf("proptest: query %q: backend %q diverges from %q: %w", q.Name, backend.Name, reference.Name, err)
		}
	}
	return nil
}

// Run executes q on every registered backend and fails the test on the first
// divergence. It is the testing.T front end of Check.
func (h *Harness) Run(t *testing.T, q Query) {
	t.Helper()
	if err := h.Check(context.Background(), q); err != nil {
		t.Fatal(err)
	}
}
