package engine

import "testing"

func TestFeedbackStoreIsBoundedAndUsesEWMA(t *testing.T) {
	s := &feedbackStore{data: make(map[string]RuntimeFeedback), max: 2}
	s.observe("a", 100, 50, 20)
	s.observe("a", 100, 100, 4)
	got, ok := s.get("a")
	if !ok {
		t.Fatal("feedback a missing")
	}
	if got.Samples != 2 {
		t.Fatalf("samples=%d, want 2", got.Samples)
	}
	if got.Selectivity < 0.624 || got.Selectivity > 0.626 {
		t.Fatalf("selectivity=%f, want EWMA 0.625", got.Selectivity)
	}
	if got.Cardinality != 16 {
		t.Fatalf("cardinality=%d, want EWMA-rounded 16", got.Cardinality)
	}
	s.observe("b", 10, 5, 2)
	s.observe("c", 10, 5, 2)
	if _, ok := s.get("a"); ok {
		t.Fatal("oldest feedback key a was not evicted")
	}
	if len(s.data) != 2 || len(s.order) != 2 {
		t.Fatalf("bounded store data=%d order=%d, want 2/2", len(s.data), len(s.order))
	}
}
