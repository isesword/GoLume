package join

import (
	"gocolumn/hashagg"
	"testing"
)

func TestRadixPairJoinMatchesPairTableSemantics(t *testing.T) {
	build := []hashagg.Key128{{A: 1, B: 10}, {A: 1, B: 10}, {A: 2, B: 20}, {A: 9, B: 90}}
	buildValid := []uint8{3, 3, 3, 1}
	probe := []hashagg.Key128{{A: 1, B: 10}, {A: 2, B: 20}, {A: 9, B: 90}, {A: 3, B: 30}}
	probeValid := []uint8{3, 3, 3, 3}

	for _, typ := range []Type{Inner, Left, Semi, Anti} {
		base := NewPairTable(8)
		if err := base.Build(build, buildValid); err != nil {
			t.Fatal(err)
		}
		radix := NewRadixPairTable(2)
		if err := radix.Build(build, buildValid); err != nil {
			t.Fatal(err)
		}
		var want, got []Pair
		ws, err := base.Probe(probe, probeValid, typ, func(p Pair) error { want = append(want, p); return nil })
		if err != nil {
			t.Fatal(err)
		}
		gs, err := radix.Probe(probe, probeValid, typ, func(p Pair) error { got = append(got, p); return nil })
		if err != nil {
			t.Fatal(err)
		}
		if ws != gs {
			t.Fatalf("type=%v summary radix=%+v base=%+v", typ, gs, ws)
		}
		if len(got) != len(want) {
			t.Fatalf("type=%v output=%v want=%v", typ, got, want)
		}
		// Duplicate build rows may be emitted in a different stable bucket-local
		// order, so verify row multisets rather than insertion order.
		counts := map[Pair]int{}
		for _, p := range want {
			counts[p]++
		}
		for _, p := range got {
			counts[p]--
		}
		for p, n := range counts {
			if n != 0 {
				t.Fatalf("type=%v pair=%+v delta=%d got=%v want=%v", typ, p, n, got, want)
			}
		}
	}
	if got := NewRadixPairTable(4).RadixBits(); got != 4 {
		t.Fatalf("bits=%d", got)
	}
}
