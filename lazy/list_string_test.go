package lazy

import (
	"context"
	"testing"

	"gocolumn/engine"
	"gocolumn/memory"
	"gocolumn/source/inmemory"
)

func TestLazyListStringSurvivesFilterSortLimitAndProjection(t *testing.T) {
	schema := memory.NewSchema(
		memory.Field{Name: "id", Type: memory.TypeInt64},
		memory.ListStringField("tags", true, true),
	)
	batch := memory.NewBatch()
	batch.Int64s["id"] = &memory.Int64Vector{Values: []int64{1, 2, 3, 4}}
	batch.StringLists["tags"] = lazyStringListVector()
	batch.Reset(4)
	src, err := inmemory.New(schema, batch)
	if err != nil {
		t.Fatal(err)
	}
	session := NewSession(engine.DefaultOptions())
	defer session.Close()
	frame, err := session.FromSource(src).
		Filter(Col("id").Gt(Lit(1))).
		Sort(Desc("id")).
		Limit(3).
		SelectExprs(Col("tags").Alias("result")).
		Collect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer frame.Close()
	if frame.Height() != 3 || len(frame.batches()) != 1 || frame.Schema().Fields[0].Type != memory.TypeList {
		t.Fatalf("frame rows=%d batches=%d schema=%+v", frame.Height(), len(frame.batches()), frame.Schema().Fields)
	}
	v, err := frame.batches()[0].StringList("result")
	if err != nil {
		t.Fatal(err)
	}
	if err := v.Validate(3); err != nil {
		t.Fatal(err)
	}
	lazyRequireListRow(t, v, 0, true, nil, nil)
	lazyRequireListRow(t, v, 1, true, []string{"中", ""}, []bool{true, false})
	lazyRequireListRow(t, v, 2, false, nil, nil)
}

func TestLazyLeftJoinMaterializesListStringPayload(t *testing.T) {
	leftBatch := memory.NewBatch()
	leftBatch.Int32s["k"] = &memory.Int32Vector{Values: []int32{1, 2, 3}}
	leftBatch.Reset(3)
	left, err := inmemory.New(memory.NewSchema(memory.Field{Name: "k", Type: memory.TypeInt32}), leftBatch)
	if err != nil {
		t.Fatal(err)
	}
	rightBatch := memory.NewBatch()
	rightBatch.Int32s["rk"] = &memory.Int32Vector{Values: []int32{2, 3}}
	rightBatch.StringLists["tags"] = &memory.StringListVector{
		Offsets:  []uint32{0, 2, 2},
		Values:   &memory.StringViewVector{Offsets: []uint32{0, 1, 4}, Data: []byte("x中"), Encoding: memory.EncodingView},
		Encoding: memory.EncodingView,
	}
	rightBatch.Reset(2)
	right, err := inmemory.New(memory.NewSchema(memory.Field{Name: "rk", Type: memory.TypeInt32}, memory.ListStringField("tags", false, false)), rightBatch)
	if err != nil {
		t.Fatal(err)
	}
	session := NewSession(engine.DefaultOptions())
	defer session.Close()
	frame, err := session.FromSource(left).
		Join(session.FromSource(right), JoinOptions{Type: LeftJoin, LeftOn: []string{"k"}, RightOn: []string{"rk"}}).
		SelectExprs(Col("k").Alias("k"), Col("tags").Alias("tags")).
		Collect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer frame.Close()
	if frame.Height() != 3 || len(frame.batches()) != 1 {
		t.Fatalf("rows=%d batches=%d", frame.Height(), len(frame.batches()))
	}
	if field, ok := frame.Schema().Field("tags"); !ok || !field.Nullable {
		t.Fatalf("left-join list payload must become nullable: %+v", frame.Schema().Fields)
	}
	keys, err := frame.batches()[0].Int32("k")
	if err != nil {
		t.Fatal(err)
	}
	tags, err := frame.batches()[0].StringList("tags")
	if err != nil {
		t.Fatal(err)
	}
	for row, key := range keys.Values {
		switch key {
		case 1:
			lazyRequireListRow(t, tags, row, false, nil, nil)
		case 2:
			lazyRequireListRow(t, tags, row, true, []string{"x", "中"}, []bool{true, true})
		case 3:
			lazyRequireListRow(t, tags, row, true, nil, nil)
		default:
			t.Fatalf("unexpected key %d", key)
		}
	}
}

func lazyStringListVector() *memory.StringListVector {
	parent := memory.EnsureBitmap(nil, 4)
	memory.Set(parent, 0)
	memory.Set(parent, 2)
	memory.Set(parent, 3)
	child := memory.EnsureBitmap(nil, 4)
	memory.Set(child, 0)
	memory.Set(child, 1)
	return &memory.StringListVector{
		Offsets:  []uint32{0, 1, 1, 3, 3},
		Validity: parent,
		Encoding: memory.EncodingView,
		Values: &memory.StringViewVector{
			Offsets:  []uint32{0, 1, 4, 4},
			Data:     []byte("a中"),
			Validity: child,
			Encoding: memory.EncodingView,
		},
	}
}

func lazyRequireListRow(t *testing.T, v *memory.StringListVector, row int, valid bool, values []string, elementValid []bool) {
	t.Helper()
	if v.IsValid(row) != valid {
		t.Fatalf("row %d valid=%v want=%v", row, v.IsValid(row), valid)
	}
	if !valid {
		return
	}
	start, end, ok := v.ValueRange(row)
	if !ok || end-start != len(values) {
		t.Fatalf("row %d range=(%d,%d,%v) want=%d", row, start, end, ok, len(values))
	}
	for i, want := range values {
		child := start + i
		if v.Values.IsValid(child) != elementValid[i] {
			t.Fatalf("row %d child %d validity", row, i)
		}
		if elementValid[i] && string(v.Values.ValueBytes(child)) != want {
			t.Fatalf("row %d child %d=%q want=%q", row, i, v.Values.ValueBytes(child), want)
		}
	}
}
