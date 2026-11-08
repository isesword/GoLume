package join

import (
	"errors"
	"fmt"
	"math"
	"reflect"
	"testing"
)

func byteKeys(values []string, nulls map[int]bool) BytesKeyFunc {
	return func(row int) ([]byte, bool, error) { return []byte(values[row]), !nulls[row], nil }
}

func TestBytesTableCollisionChainsAndModes(t *testing.T) {
	// Force distinct keys into the same initial slot. The table must compare
	// exact bytes rather than treating a hash/slot match as key equality.
	first := "a"
	second := ""
	for i := 0; i < 10000; i++ {
		k := fmt.Sprint(i)
		if hashBytes([]byte(k))&15 == hashBytes([]byte(first))&15 {
			second = k
			break
		}
	}
	if second == "" {
		t.Fatal("no colliding slot found")
	}
	build := []string{first, second, first, "", "null"}
	table, err := BuildBytesTable(len(build), byteKeys(build, map[int]bool{4: true}))
	if err != nil {
		t.Fatal(err)
	}
	probe := []string{first, second, "", "missing", "null"}
	for _, tc := range []struct {
		typ  Type
		want []Pair
	}{
		{Inner, []Pair{{0, 2}, {0, 0}, {1, 1}, {2, 3}}},
		{Left, []Pair{{0, 2}, {0, 0}, {1, 1}, {2, 3}, {3, -1}, {4, -1}}},
		{Right, []Pair{{0, 2}, {0, 0}, {1, 1}, {2, 3}}},
		{Full, []Pair{{0, 2}, {0, 0}, {1, 1}, {2, 3}, {3, -1}, {4, -1}}},
		{Semi, []Pair{{0, 2}, {1, 1}, {2, 3}}},
		{Anti, []Pair{{3, -1}, {4, -1}}},
	} {
		t.Run(tc.typ.String(), func(t *testing.T) {
			var got []Pair
			summary, err := table.Probe(len(probe), byteKeys(probe, nil), tc.typ, func(p Pair) error { got = append(got, p); return nil })
			if err != nil || !reflect.DeepEqual(got, tc.want) || summary.OutputRows != len(got) || summary.MatchedProbeRows != 3 {
				t.Fatalf("got=%v summary=%+v err=%v want=%v", got, summary, err, tc.want)
			}
		})
	}
	if table.MaxMultiplicity() != 2 {
		t.Fatal(table.MaxMultiplicity())
	}
}

func TestBytesTableCopiesBuildKeyAndPropagatesErrors(t *testing.T) {
	buf := []byte("one")
	table, err := BuildBytesTable(1, func(int) ([]byte, bool, error) { return buf, true, nil })
	if err != nil {
		t.Fatal(err)
	}
	copy(buf, "two")
	got, err := table.Probe(1, byteKeys([]string{"one"}, nil), Inner, nil)
	if err != nil || got.OutputRows != 1 {
		t.Fatalf("%+v %v", got, err)
	}
	marker := errors.New("stop")
	_, err = table.Probe(1, byteKeys([]string{"one"}, nil), Inner, func(Pair) error { return marker })
	if !errors.Is(err, marker) {
		t.Fatal(err)
	}
	_, err = BuildBytesTable(1, func(int) ([]byte, bool, error) { return nil, false, marker })
	if !errors.Is(err, marker) {
		t.Fatal(err)
	}
	if _, err = table.Probe(0, byteKeys(nil, nil), Type(255), nil); err == nil {
		t.Fatal("invalid type accepted for empty probe")
	}
}

func TestBytesTableEstimateRejectsOverflow(t *testing.T) {
	for _, rows := range []int{-1, math.MaxInt} {
		if _, err := EstimateBytesTableBytes(rows, 0); err == nil {
			t.Fatalf("accepted rows=%d", rows)
		}
	}
	for _, bytes := range []int64{-1, math.MaxInt64} {
		if _, err := EstimateBytesTableBytes(1, bytes); err == nil {
			t.Fatalf("accepted bytes=%d", bytes)
		}
	}
	if _, err := BuildBytesTable(math.MaxInt, byteKeys(nil, nil)); err == nil {
		t.Fatal("overflow allocated")
	}
}

func BenchmarkBytesTable(b *testing.B) {
	const rows = 4096
	values := make([]string, rows)
	for i := range values {
		values[i] = fmt.Sprintf("customer-%08d", i)
	}
	encoded := make([][]byte, rows)
	for i := range encoded {
		encoded[i] = []byte(values[i])
	}
	// Keep callback conversion allocation out of the table benchmark. The
	// physical operator likewise supplies a reusable encoded byte buffer.
	reader := func(row int) ([]byte, bool, error) { return encoded[row], true, nil }
	table, err := BuildBytesTable(rows, reader)
	if err != nil {
		b.Fatal(err)
	}
	b.Run("Build", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if _, err := BuildBytesTable(rows, reader); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("Probe", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if _, err := table.Probe(rows, reader, Inner, nil); err != nil {
				b.Fatal(err)
			}
		}
	})
}
