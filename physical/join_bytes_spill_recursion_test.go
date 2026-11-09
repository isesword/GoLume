package physical

import (
	"errors"
	"fmt"
	"gocolumn/join"
	"gocolumn/memory"
	"reflect"
	"sort"
	"strings"
	"testing"
)

func TestGenericSpillCollisionRecursionAllModes(t *testing.T) {
	hasher := &JoinSource{keys: []JoinKey{{"s", "s"}}, keyFields: []memory.Field{{Type: memory.TypeString}}}
	b := memory.NewBatch()
	b.Rows = 1
	right := make([]genericJoinRow, 0, 768)
	hashes := map[string]uint64{}
	for i := 0; len(right) < cap(right); i++ {
		v := fmt.Sprintf("prefix-collision-%09d", i)
		b.Strings["s"] = &memory.StringViewVector{Offsets: []uint32{0, uint32(len(v))}, Data: []byte(v)}
		h, err := hasher.bytePartitionHash(b, false, 0)
		if err != nil {
			t.Fatal(err)
		}
		if h&255 == 0 {
			right = append(right, genericJoinRow{s: v, x: strings.Repeat("payload", 42)})
			hashes[v] = h
		}
	}
	sort.Slice(right, func(i, j int) bool { return (hashes[right[i].s] >> 8 & 15) < (hashes[right[j].s] >> 8 & 15) })
	left := append([]genericJoinRow(nil), right[10:22]...)
	left = append(left, genericJoinRow{s: "missing"}, genericJoinRow{s: right[0].s, null: 1})
	for typ := join.Inner; typ <= join.Full; typ++ {
		t.Run(typ.String(), func(t *testing.T) {
			ctx, o, s, q, dir := openByteSpill(t, genericJoinSource(t, left, false, 7), genericJoinSource(t, right, false, 64), typ, []JoinKey{{"s", "s"}}, 512<<10, 64<<20)
			got, err := drainByteSpill(ctx, s)
			if err != nil {
				t.Fatal(err)
			}
			want := genericJoinExpected(left, right, 1, typ)
			sortBytePairs(got)
			sortBytePairs(want)
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("got=%d want=%d", len(got), len(want))
			}
			if s.maxDepth < 3 {
				t.Fatalf("expected depth>=3 got %d", s.maxDepth)
			}
			assertByteSpillClean(t, o, q, dir)
		})
	}
}
func TestGenericSpillSingleSkewRejectsDifferentKeys(t *testing.T) {
	hasher := &JoinSource{keys: []JoinKey{{"s", "s"}}, keyFields: []memory.Field{{Type: memory.TypeString}}}
	b := memory.NewBatch()
	b.Rows = 1
	var right []genericJoinRow
	for i := 0; len(right) < 2; i++ {
		v := fmt.Sprint(i)
		b.Strings["s"] = &memory.StringViewVector{Offsets: []uint32{0, uint32(len(v))}, Data: []byte(v)}
		h, _ := hasher.bytePartitionHash(b, false, 0)
		if h&15 == 0 {
			right = append(right, genericJoinRow{s: v})
		}
	}
	ctx, o, s, q, dir := openByteSpill(t, genericJoinSource(t, right, false, 2), genericJoinSource(t, right, false, 2), join.Full, []JoinKey{{"s", "s"}}, 1<<20, 64<<20)
	if err := s.prepare(ctx); err != nil {
		t.Fatal(err)
	}
	err := s.processByteSkew(ctx, s.leftRuns[0].Values, s.rightRuns[0].Values, 0)
	if !errors.Is(err, ErrJoinSkewExhausted) {
		t.Fatalf("different same-partition keys treated as one: %v", err)
	}
	if len(s.outputRuns.Values) > 0 {
		t.Fatal("incorrect Cartesian output after skew rejection")
	}
	s.Close()
	assertByteSpillClean(t, o, q, dir)
}
