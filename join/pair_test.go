package join

import (
	"gocolumn/hashagg"
	"testing"
)

func TestPairJoinDuplicatesNullAndTypes(t *testing.T) {
	build := []hashagg.Key128{{A: 1, B: 10}, {A: 1, B: 10}, {A: 2, B: 20}, {A: 9, B: 90}}
	buildValid := []uint8{3, 3, 3, 1} // final row has NULL B and must not join
	probe := []hashagg.Key128{{A: 1, B: 10}, {A: 2, B: 20}, {A: 9, B: 90}, {A: 3, B: 30}}
	probeValid := []uint8{3, 3, 3, 3}

	tab := NewPairTable(1) // deliberately underestimate build cardinality
	if err := tab.Build(build, buildValid); err != nil {
		t.Fatal(err)
	}

	var pairs []Pair
	s, err := tab.Probe(probe, probeValid, Inner, func(p Pair) error {
		pairs = append(pairs, p)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if s.ProbeRows != 4 || s.MatchedProbeRows != 2 || s.OutputRows != 3 {
		t.Fatalf("inner summary=%+v pairs=%v", s, pairs)
	}

	s, err = tab.Probe(probe, probeValid, Left, nil)
	if err != nil {
		t.Fatal(err)
	}
	if s.OutputRows != 5 { // two duplicates + one key2 + two unmatched
		t.Fatalf("left summary=%+v", s)
	}

	s, err = tab.Probe(probe, probeValid, Semi, nil)
	if err != nil {
		t.Fatal(err)
	}
	if s.OutputRows != 2 {
		t.Fatalf("semi summary=%+v", s)
	}

	s, err = tab.Probe(probe, probeValid, Anti, nil)
	if err != nil {
		t.Fatal(err)
	}
	if s.OutputRows != 2 {
		t.Fatalf("anti summary=%+v", s)
	}
}

func TestPairJoinProbeNullNeverMatches(t *testing.T) {
	tab := NewPairTable(8)
	if err := tab.Build([]hashagg.Key128{{A: 1, B: 2}}, nil); err != nil {
		t.Fatal(err)
	}
	s, err := tab.Probe([]hashagg.Key128{{A: 1, B: 2}}, []uint8{1}, Inner, nil)
	if err != nil {
		t.Fatal(err)
	}
	if s.MatchedProbeRows != 0 || s.OutputRows != 0 {
		t.Fatalf("NULL composite key matched: %+v", s)
	}
}

func TestPairJoinBuildUnderestimatedHint(t *testing.T) {
	const n = 4096
	keys := make([]hashagg.Key128, n)
	for i := range keys {
		keys[i] = hashagg.Key128{A: uint64(i), B: uint64(i * 17)}
	}
	tab := NewPairTable(8)
	if err := tab.Build(keys, nil); err != nil {
		t.Fatal(err)
	}
	s, err := tab.Probe(keys, nil, Semi, nil)
	if err != nil {
		t.Fatal(err)
	}
	if s.MatchedProbeRows != n || s.OutputRows != n {
		t.Fatalf("summary=%+v", s)
	}
}
