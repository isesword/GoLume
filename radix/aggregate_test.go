package radix

import (
	"context"
	"gocolumn/hashagg"
	"gocolumn/scheduler"
	"testing"
)

func TestAggregate(t *testing.T) {
	p := scheduler.NewPool(4)
	defer p.Close()
	keys := []uint64{1, 2, 1, 3, 2, 2}
	vals := []int64{10, 20, 5, 7, 2, 3}
	w := NewWorkspace(len(keys), 4, 2)
	s, err := w.Aggregate(context.Background(), p, keys, vals, 4)
	if err != nil {
		t.Fatal(err)
	}
	if s.Groups != 3 || s.Checksum != 47 || s.Rows != 6 {
		t.Fatalf("summary=%+v", s)
	}
	m := map[uint64]Group{}
	for _, g := range w.Materialize(nil) {
		m[g.Key] = g
	}
	if m[2].Sum != 25 || m[2].Count != 3 {
		t.Fatalf("group2=%+v", m[2])
	}
}

func TestPairAggregateNullable(t *testing.T) {
	p := scheduler.NewPool(3)
	defer p.Close()
	keys := []hashagg.Key128{
		{A: 1, B: 10},
		{A: 1, B: 999}, // B is NULL; payload must not affect grouping.
		{A: 1, B: 20},
		{A: 777, B: 888}, // both NULL; payload must not affect grouping.
		{A: 0, B: 0},
	}
	valid := []uint8{3, 1, 3, 0, 0}
	vals := []int64{1, 2, 3, 4, 5}
	w := NewPairWorkspace(len(keys), 4, 2)
	s, err := w.AggregateNullable(context.Background(), p, keys, valid, vals, 3)
	if err != nil {
		t.Fatal(err)
	}
	if s.Groups != 4 || s.Rows != 5 || s.Checksum != 15 {
		t.Fatalf("summary=%+v", s)
	}
	type k struct {
		a, b uint64
		vm   uint8
	}
	got := map[k]PairGroup{}
	for _, g := range w.Materialize(nil) {
		got[k{g.Key.A, g.Key.B, g.ValidMask}] = g
	}
	if g := got[k{1, 0, 1}]; g.Sum != 2 || g.Count != 1 {
		t.Fatalf("nullable-B group=%+v", g)
	}
	if g := got[k{0, 0, 0}]; g.Sum != 9 || g.Count != 2 {
		t.Fatalf("all-NULL group=%+v", g)
	}
}
