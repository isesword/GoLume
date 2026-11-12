package physical

import (
	"context"
	"gocolumn/join"
	"gocolumn/kernel"
	"gocolumn/memory"
	"reflect"
	"testing"
)

func TestInt64ListTakeAndBudgetPlans(t *testing.T) {
	b := memory.NewBatch()
	b.Reset(4)
	b.Int64Lists["l"] = &memory.Int64ListVector{Offsets: []uint32{0, 2, 2, 2, 3}, Validity: memory.Bitmap{13}, Values: &memory.Int64Vector{Values: []int64{-7, 0, 9}, Validity: memory.Bitmap{5}}}
	rows := []int{3, 1, 2, 0, 3}
	out := memory.NewBatch()
	if err := takeRows(context.Background(), out, b, rows); err != nil {
		t.Fatal(err)
	}
	v := out.Int64Lists["l"]
	if !reflect.DeepEqual(v.Offsets, []uint32{0, 1, 1, 1, 3, 4}) || !reflect.DeepEqual(v.Values.Values, []int64{9, -7, 0, 9}) || v.IsValid(1) || !v.IsValid(2) || v.Values.IsValid(2) {
		t.Fatalf("unexpected take: %+v %+v", v, v.Values)
	}
	if _, _, err := compactLayout(b, memory.NewBatch(), rows, false, true); err != nil {
		t.Fatal(err)
	}
	if _, err := joinGatherInt64ListSize(v, []int{0, 4}, nil); err != nil {
		t.Fatal(err)
	}
	var size int64
	if err := joinGatherInt64ListPayload(&size, b.Int64Lists["l"], 4); err != nil {
		t.Fatal(err)
	}
	if size != 60 {
		t.Fatalf("payload got %d want 60", size)
	}
	empty := memory.NewBatch()
	if err := takeRows(context.Background(), empty, b, nil); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(empty.Int64Lists["l"].Offsets, []uint32{0}) {
		t.Fatal("empty loses offset")
	}
	b.Int64Lists["l"].Offsets[4] = 99
	if _, err := compactInt64ListSize(b.Int64Lists["l"], []int{3}, nil); err == nil {
		t.Fatal("malformed offset accepted")
	}
}

func TestInt64ListCompactAndOuterJoin(t *testing.T) {
	b := memory.NewBatch()
	b.Rows = 3
	b.Int64Lists["l"] = &memory.Int64ListVector{Offsets: []uint32{0, 2, 2, 2}, Validity: memory.Bitmap{5}, Values: &memory.Int64Vector{Values: []int64{4, 0}, Validity: memory.Bitmap{1}}}
	out := memory.NewBatch()
	rows := []int{0, 1, 2}
	if _, _, err := compactLayout(b, out, rows, true, true); err != nil {
		t.Fatal(err)
	}
	if err := compactSelected(context.Background(), out, b, rows, memory.Bitmap{7}, kernel.Set{}); err != nil {
		t.Fatal(err)
	}
	v := out.Int64Lists["l"]
	if v.IsValid(1) || !v.IsValid(2) || v.Values.IsValid(1) {
		t.Fatal("compact validity")
	}
	f := memory.ListInt64Field("l", true, true)
	owner := &JoinSource{schema: memory.NewSchema(f), bindings: []joinOutputBinding{{in: "l", out: "l", field: f}}}
	pairs := []join.Pair{{ProbeRow: 0, BuildRow: -1}, {ProbeRow: -1, BuildRow: -1}, {ProbeRow: 2, BuildRow: -1}}
	dst := memory.NewBatch()
	if err := materializeJoinBatchMode(dst, owner, b, pairs, true); err != nil {
		t.Fatal(err)
	}
	v = dst.Int64Lists["l"]
	if v.IsValid(1) || !v.IsValid(2) || v.Values.IsValid(1) || cap(v.Values.Values) != 2 {
		t.Fatalf("outer join %+v", v)
	}
	if _, err := estimateJoinMaterializationBytes(owner, b, pairs); err != nil {
		t.Fatal(err)
	}
	if _, err := joinGatherAdmissionBytes(owner.schema, b, rows); err != nil {
		t.Fatal(err)
	}
}

func TestInt64ListWindowValidity(t *testing.T) {
	b := memory.NewBatch()
	b.Rows = 2
	b.Int64Lists["l"] = &memory.Int64ListVector{Offsets: []uint32{0, 0, 0}, Validity: memory.Bitmap{1}, Values: &memory.Int64Vector{}}
	schema := memory.NewSchema(memory.ListInt64Field("l", true, true))
	for i := 0; i < 2; i++ {
		valid, err := windowValueValid(b, schema, "l", i)
		if err != nil || valid != (i == 0) {
			t.Fatalf("row %d valid %v err %v", i, valid, err)
		}
	}
}

// NULL child payload must be cleared both on fresh materialization and reuse.
func TestInt64ListCompactClearsNullChildPayload(t *testing.T) {
	b := memory.NewBatch()
	b.Rows = 1
	b.Int64Lists["l"] = &memory.Int64ListVector{Offsets: []uint32{0, 2}, Values: &memory.Int64Vector{Values: []int64{17, 99123}, Validity: memory.Bitmap{1}}}
	rows := []int{0}
	dst := memory.NewBatch()
	if _, _, err := compactLayout(b, dst, rows, true, true); err != nil {
		t.Fatal(err)
	}
	compact := func() {
		t.Helper()
		if err := compactSelected(context.Background(), dst, b, rows, memory.Bitmap{1}, kernel.Set{}); err != nil {
			t.Fatal(err)
		}
	}
	compact()
	v := dst.Int64Lists["l"]
	if v.Values.Values[0] != 17 || v.Values.Values[1] != 0 || v.Values.IsValid(1) {
		t.Fatalf("fresh NULL payload: %+v", v.Values)
	}
	// Populate that same destination slot, then transition it back to NULL.
	b.Int64Lists["l"].Values.Validity = memory.Bitmap{3}
	compact()
	if v.Values.Values[1] != 99123 || !v.Values.IsValid(1) {
		t.Fatal("valid population failed")
	}
	ptr := &v.Values.Values[0]
	b.Int64Lists["l"].Values.Validity = memory.Bitmap{1}
	if _, fits, err := compactLayout(b, dst, rows, false, true); err != nil || !fits {
		t.Fatalf("reuse fits=%v err=%v", fits, err)
	}
	compact()
	if ptr != &v.Values.Values[0] || v.Values.Values[1] != 0 || v.Values.IsValid(1) {
		t.Fatalf("reuse left hidden payload: %+v", v.Values)
	}
	taken := memory.NewBatch()
	if err := takeRows(context.Background(), taken, b, rows); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(v.Values.Values, taken.Int64Lists["l"].Values.Values) {
		t.Fatal("Filter and Take payload differ")
	}
}
