package engine

import "sync"

// RuntimeFeedback is a smoothed observation from successful executions of one
// stable lazy-plan signature. Explicit user hints always take precedence over
// feedback when a plan is prepared.
type RuntimeFeedback struct {
	Samples     uint64
	Rows        int64
	Selectivity float64
	Cardinality int
}

type feedbackStore struct {
	mu    sync.RWMutex
	data  map[string]RuntimeFeedback
	order []string
	max   int
}

func newFeedbackStore() *feedbackStore {
	return &feedbackStore{data: make(map[string]RuntimeFeedback), max: 1024}
}

func (s *feedbackStore) get(key string) (RuntimeFeedback, bool) {
	if s == nil || key == "" {
		return RuntimeFeedback{}, false
	}
	s.mu.RLock()
	v, ok := s.data[key]
	s.mu.RUnlock()
	return v, ok
}

func (s *feedbackStore) observe(key string, rows int64, selected, groups int) {
	if s == nil || key == "" || rows <= 0 || selected < 0 || int64(selected) > rows || groups < 0 {
		return
	}
	sel := float64(selected) / float64(rows)
	card := float64(groups)
	s.mu.Lock()
	old, ok := s.data[key]
	if !ok || old.Samples == 0 {
		if s.max > 0 && len(s.data) >= s.max && len(s.order) > 0 {
			evict := s.order[0]
			s.order = s.order[1:]
			delete(s.data, evict)
		}
		s.data[key] = RuntimeFeedback{Samples: 1, Rows: rows, Selectivity: sel, Cardinality: groups}
		s.order = append(s.order, key)
		s.mu.Unlock()
		return
	}
	const alpha = 0.25
	old.Samples++
	old.Rows = rows
	old.Selectivity = old.Selectivity*(1-alpha) + sel*alpha
	old.Cardinality = int(float64(old.Cardinality)*(1-alpha) + card*alpha + 0.5)
	s.data[key] = old
	s.mu.Unlock()
}

// RuntimeFeedback returns the current smoothed observation for key.
func (e *Engine) RuntimeFeedback(key string) (RuntimeFeedback, bool) {
	if e == nil {
		return RuntimeFeedback{}, false
	}
	return e.feedback.get(key)
}

// ResetRuntimeFeedback clears all adaptive planner observations. It is intended
// for tests, benchmarks, and workloads whose data distribution changed abruptly.
func (e *Engine) ResetRuntimeFeedback() {
	if e == nil || e.feedback == nil {
		return
	}
	e.feedback.mu.Lock()
	clear(e.feedback.data)
	e.feedback.order = e.feedback.order[:0]
	e.feedback.mu.Unlock()
}
