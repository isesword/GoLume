package join

import (
	"gocolumn/memory"
	"testing"
)

func TestI32JoinTypesAndDuplicates(t *testing.T) {
	tab := NewI32Table(8)
	tab.Build([]int32{1, 2, 2, 3}, nil)
	probe := []int32{2, 4, 3, 2}
	cases := []struct {
		typ          Type
		out, matched int
	}{{Inner, 5, 3}, {Left, 6, 3}, {Semi, 3, 3}, {Anti, 1, 3}}
	for _, tc := range cases {
		s, err := tab.Probe(probe, nil, tc.typ, nil)
		if err != nil {
			t.Fatal(err)
		}
		if s.OutputRows != tc.out || s.MatchedProbeRows != tc.matched {
			t.Fatalf("%s: %+v", tc.typ, s)
		}
	}
}
func TestI32JoinNullNeverMatches(t *testing.T) {
	buildValid := make(memory.Bitmap, 1)
	memory.Set(buildValid, 0) // build row 1 (key=2) is NULL
	tab := NewI32Table(8)
	tab.Build([]int32{1, 2}, buildValid)
	probeValid := make(memory.Bitmap, 1)
	memory.Set(probeValid, 0) // probe second row NULL
	probe := []int32{1, 1}
	s, err := tab.Probe(probe, probeValid, Left, nil)
	if err != nil {
		t.Fatal(err)
	}
	if s.MatchedProbeRows != 1 || s.OutputRows != 2 {
		t.Fatalf("left null semantics: %+v", s)
	}
}

func TestI32JoinGrowsBeyondCardinalityHint(t *testing.T) {
	const n = 4096
	build := make([]int32, n)
	probe := make([]int32, n)
	for i := 0; i < n; i++ {
		build[i], probe[i] = int32(i), int32(i)
	}
	tab := NewI32Table(8)
	tab.Build(build, nil)
	s, err := tab.Probe(probe, nil, Inner, nil)
	if err != nil {
		t.Fatal(err)
	}
	if s.MatchedProbeRows != n || s.OutputRows != n {
		t.Fatalf("summary=%+v", s)
	}
}
