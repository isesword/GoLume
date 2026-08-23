package join

import (
	"gocolumn/memory"
	"testing"
)

func dict(domain string, values ...string) Dictionary {
	d := Dictionary{Domain: domain, Offsets: []uint32{0}}
	for _, value := range values {
		d.Data = append(d.Data, value...)
		d.Offsets = append(d.Offsets, uint32(len(d.Data)))
	}
	return d
}

func TestDictionaryJoinSameDomainFastPath(t *testing.T) {
	tab := NewDictionaryTable(2)
	buildDict := dict("cities-v1", "ny", "sf", "sg")
	if err := tab.Build([]uint32{0, 1, 1, 2}, nil, buildDict); err != nil {
		t.Fatal(err)
	}
	var pairs []Pair
	s, err := tab.Probe([]uint32{1, 2, 0}, nil, buildDict, Inner, func(p Pair) error {
		pairs = append(pairs, p)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if s.ProbeRows != 3 || s.MatchedProbeRows != 3 || s.OutputRows != 4 || len(pairs) != 4 {
		t.Fatalf("summary=%+v pairs=%v", s, pairs)
	}
}

func TestDictionaryJoinNormalizesDifferentDomains(t *testing.T) {
	tab := NewDictionaryTable(2)
	buildDict := dict("build", "alpha", "beta", "gamma")
	probeDict := dict("probe", "gamma", "alpha", "missing", "beta")
	if err := tab.Build([]uint32{0, 1, 2}, nil, buildDict); err != nil {
		t.Fatal(err)
	}
	var pairs []Pair
	s, err := tab.Probe([]uint32{0, 1, 2, 3}, nil, probeDict, Left, func(p Pair) error {
		pairs = append(pairs, p)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if s.MatchedProbeRows != 3 || s.OutputRows != 4 {
		t.Fatalf("summary=%+v", s)
	}
	if pairs[0].BuildRow != 2 || pairs[1].BuildRow != 0 || pairs[2].BuildRow != -1 || pairs[3].BuildRow != 1 {
		t.Fatalf("pairs=%v", pairs)
	}
}

func TestDictionaryJoinNullNeverMatches(t *testing.T) {
	tab := NewDictionaryTable(2)
	d := dict("d", "a", "b")
	buildValid := make(memory.Bitmap, memory.WordsFor(2))
	memory.Set(buildValid, 0)
	if err := tab.Build([]uint32{0, 1}, buildValid, d); err != nil {
		t.Fatal(err)
	}
	probeValid := make(memory.Bitmap, memory.WordsFor(2))
	memory.Set(probeValid, 0)
	s, err := tab.Probe([]uint32{0, 1}, probeValid, d, Left, nil)
	if err != nil {
		t.Fatal(err)
	}
	if s.MatchedProbeRows != 1 || s.OutputRows != 2 {
		t.Fatalf("summary=%+v", s)
	}
}

func TestDictionaryJoinCanonicalizesDuplicateDictionaryValues(t *testing.T) {
	tab := NewDictionaryTable(2)
	buildDict := dict("", "same", "same", "other")
	probeDict := dict("", "same")
	if err := tab.Build([]uint32{0, 1, 2}, nil, buildDict); err != nil {
		t.Fatal(err)
	}
	s, err := tab.Probe([]uint32{0}, nil, probeDict, Inner, nil)
	if err != nil {
		t.Fatal(err)
	}
	if s.OutputRows != 2 {
		t.Fatalf("output=%d want=2", s.OutputRows)
	}
}
