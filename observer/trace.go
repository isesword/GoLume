package observer

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"
)

// Phase identifies whether an operator event happened while preparing or
// executing a physical plan.
type Phase string

const (
	// PhasePrepare records work performed while a reusable physical plan is built.
	PhasePrepare Phase = "prepare"
	// PhaseExecute records work performed while a prepared plan processes rows.
	PhaseExecute Phase = "execute"
)

type phaseKey struct{}
type traceKey struct{}

// WithOperatorTrace returns a context that carries trace for engine-level
// operators that cannot be represented as Source wrappers.
func WithOperatorTrace(ctx context.Context, trace *OperatorTrace) context.Context {
	return context.WithValue(ctx, traceKey{}, trace)
}

// TraceFromContext returns the operator trace carried by ctx, if any.
func TraceFromContext(ctx context.Context) *OperatorTrace {
	if ctx == nil {
		return nil
	}
	trace, _ := ctx.Value(traceKey{}).(*OperatorTrace)
	return trace
}

// WithPhase returns a context that labels subsequent traced operator work.
func WithPhase(ctx context.Context, phase Phase) context.Context {
	return context.WithValue(ctx, phaseKey{}, phase)
}

// PhaseFromContext returns the tracing phase carried by ctx. Execute is the
// conservative default because ordinary reader calls process query rows.
func PhaseFromContext(ctx context.Context) Phase {
	if ctx != nil {
		if phase, ok := ctx.Value(phaseKey{}).(Phase); ok && phase != "" {
			return phase
		}
	}
	return PhaseExecute
}

// OperatorNode describes one physical operator in an Analyze trace. Children
// are operator IDs whose inclusive time is subtracted when computing SelfTime.
type OperatorNode struct {
	ID       string
	Kind     string
	Label    string
	Children []string
}

// OperatorStat aggregates reader calls for one operator and one phase.
type OperatorStat struct {
	ID          string
	Kind        string
	Label       string
	Phase       Phase
	Calls       int64
	InputRows   int64
	OutputRows  int64
	OutputBytes int64
	Inclusive   time.Duration
	SelfTime    time.Duration
}

type operatorKey struct {
	id    string
	phase Phase
}

// OperatorTrace stores diagnostic operator timings for one ExplainAnalyze run.
// It is safe for concurrent reader calls.
type OperatorTrace struct {
	mu    sync.Mutex
	nodes map[string]OperatorNode
	stats map[operatorKey]OperatorStat
	order []string
	next  int
}

// NewOperatorTrace constructs an empty physical-operator trace.
func NewOperatorTrace() *OperatorTrace {
	return &OperatorTrace{nodes: make(map[string]OperatorNode), stats: make(map[operatorKey]OperatorStat)}
}

// NewNode allocates and registers a stable operator ID for one Analyze lowering.
func (t *OperatorTrace) NewNode(kind, label string, children ...string) OperatorNode {
	if t == nil {
		return OperatorNode{}
	}
	t.mu.Lock()
	t.next++
	id := fmt.Sprintf("op%02d", t.next)
	node := OperatorNode{ID: id, Kind: kind, Label: label, Children: append([]string(nil), children...)}
	if _, exists := t.nodes[id]; !exists {
		t.order = append(t.order, id)
	}
	t.nodes[id] = node
	t.mu.Unlock()
	return node
}

// Register records operator topology. Re-registering an ID updates metadata but
// preserves the first registration order used by stable explain output.
func (t *OperatorTrace) Register(node OperatorNode) {
	if t == nil || node.ID == "" {
		return
	}
	t.mu.Lock()
	if _, exists := t.nodes[node.ID]; !exists {
		t.order = append(t.order, node.ID)
	}
	node.Children = append([]string(nil), node.Children...)
	t.nodes[node.ID] = node
	t.mu.Unlock()
}

// Record adds one inclusive operator reader measurement.
func (t *OperatorTrace) Record(id string, phase Phase, inputRows, outputRows int64, elapsed time.Duration) {
	t.RecordBytes(id, phase, inputRows, outputRows, 0, elapsed)
}

// RecordBytes adds one inclusive operator measurement with an optional decoded
// output-byte estimate. Bytes describe columnar output memory, not compressed
// source bytes.
func (t *OperatorTrace) RecordBytes(id string, phase Phase, inputRows, outputRows, outputBytes int64, elapsed time.Duration) {
	if t == nil || id == "" {
		return
	}
	t.mu.Lock()
	n := t.nodes[id]
	k := operatorKey{id: id, phase: phase}
	s := t.stats[k]
	s.ID, s.Kind, s.Label, s.Phase = id, n.Kind, n.Label, phase
	s.Calls++
	s.InputRows += inputRows
	s.OutputRows += outputRows
	s.OutputBytes += outputBytes
	s.Inclusive += elapsed
	t.stats[k] = s
	t.mu.Unlock()
}

// Stats returns stable phase-filtered operator statistics. SelfTime is the
// inclusive duration minus direct-child inclusive durations in the same phase;
// it is clamped at zero because concurrent child execution can overlap.
func (t *OperatorTrace) Stats(phase Phase) []OperatorStat {
	if t == nil {
		return nil
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	out := make([]OperatorStat, 0, len(t.order))
	for _, id := range t.order {
		k := operatorKey{id: id, phase: phase}
		s, ok := t.stats[k]
		if !ok {
			continue
		}
		self := s.Inclusive
		for _, child := range t.nodes[id].Children {
			self -= t.stats[operatorKey{id: child, phase: phase}].Inclusive
		}
		if self < 0 {
			self = 0
		}
		s.SelfTime = self
		out = append(out, s)
	}
	return out
}

// Nodes returns a stable snapshot of registered operator topology.
func (t *OperatorTrace) Nodes() []OperatorNode {
	if t == nil {
		return nil
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	out := make([]OperatorNode, 0, len(t.nodes))
	for _, n := range t.nodes {
		n.Children = append([]string(nil), n.Children...)
		out = append(out, n)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}
