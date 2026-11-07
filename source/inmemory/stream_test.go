package inmemory

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"gocolumn/kernel"
	"gocolumn/memory"
	"gocolumn/source"
)

func streamOptions(rows int) source.ReadOptions {
	return source.ReadOptions{Kernels: kernel.New(kernel.ModeScalar), BatchRows: rows}
}

func TestStreamPreparedPartitionsAreExactAndBounded(t *testing.T) {
	schema := memory.NewSchema(memory.Field{Name: "x", Type: memory.TypeInt64})
	var batches []*memory.Batch
	for _, values := range [][]int64{{1, 2, 3, 4, 5}, {}, {10, 11, 12}} {
		b := memory.NewBatch()
		b.Int64s["x"] = &memory.Int64Vector{Values: values}
		b.Reset(len(values))
		batches = append(batches, b)
	}
	src, err := New(schema, batches...)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		parts []source.Partition
		want  []int64
		sizes []int
	}{
		{nil, nil, nil},
		{[]source.Partition{}, nil, nil},
		{[]source.Partition{{ID: 2}, {ID: 1}, {ID: 0}, {ID: 2}}, []int64{10, 11, 12, 1, 2, 3, 4, 5, 10, 11, 12}, []int{2, 1, 0, 2, 2, 1, 2, 1}},
	} {
		stream, err := src.NewBatchStreamWithPartitions(context.Background(), streamOptions(2), tc.parts)
		if err != nil {
			t.Fatal(err)
		}
		out := memory.NewBatch()
		var got []int64
		var sizes []int
		for {
			ok, err := stream.Next(context.Background(), out)
			if err != nil {
				t.Fatal(err)
			}
			if !ok {
				break
			}
			sizes = append(sizes, out.Rows)
			got = append(got, out.Int64s["x"].Values...)
		}
		if !reflect.DeepEqual(got, tc.want) || !reflect.DeepEqual(sizes, tc.sizes) {
			t.Fatalf("values=%v sizes=%v want=%v/%v", got, sizes, tc.want, tc.sizes)
		}
		if ok, err := stream.Next(context.Background(), out); ok || err != nil || out.Rows != 0 {
			t.Fatalf("EOF ok=%v err=%v rows=%d", ok, err, out.Rows)
		}
		stream.Close()
		stream.Close()
		if ok, err := stream.Next(context.Background(), out); ok || !errors.Is(err, source.ErrBatchStreamClosed) {
			t.Fatalf("closed ok=%v err=%v", ok, err)
		}
	}
	if _, err := src.NewBatchStreamWithPartitions(context.Background(), streamOptions(2), []source.Partition{{ID: 3}}); err == nil {
		t.Fatal("invalid partition accepted")
	}
}

func TestStreamSlicesZeroCopyAndAccountsOnlyScratch(t *testing.T) {
	schema := memory.NewSchema(memory.Field{Name: "x", Type: memory.TypeInt64, Nullable: true}, memory.Field{Name: "s", Type: memory.TypeString, Nullable: true})
	batch := memory.NewBatch()
	valid := memory.Bitmap{0b1011011}
	values := []int64{1, 2, 3, 4, 5, 6, 7}
	batch.Int64s["x"] = &memory.Int64Vector{Values: values, Validity: valid}
	batch.Strings["s"] = &memory.StringViewVector{Offsets: []uint32{0, 1, 2, 3, 4, 5, 6, 7}, Data: []byte("abcdefg"), Validity: valid}
	batch.Reset(7)
	src, err := New(schema, batch)
	if err != nil {
		t.Fatal(err)
	}
	manager := memory.NewBudgetManager(24)
	opts := streamOptions(3)
	opts.BatchBytes = 24
	opts.MemoryManager = manager
	stream, err := src.NewBatchStream(context.Background(), opts)
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	out := memory.NewBatch()
	offset := 0
	for {
		ok, err := stream.Next(context.Background(), out)
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			break
		}
		if out.Rows > 3 || manager.Used() > 24 {
			t.Fatalf("rows=%d used=%d", out.Rows, manager.Used())
		}
		if &out.Int64s["x"].Values[0] != &values[offset] {
			t.Fatal("numeric source buffer copied")
		}
		for i := 0; i < out.Rows; i++ {
			want := memory.IsSet(valid, offset+i)
			if out.Int64s["x"].IsValid(i) != want || out.Strings["s"].IsValid(i) != want {
				t.Fatalf("row %d validity lost", offset+i)
			}
			if string(out.Strings["s"].ValueBytes(i)) != string(batch.Strings["s"].ValueBytes(offset+i)) {
				t.Fatal("string slice mismatch")
			}
		}
		offset += out.Rows
	}
	if offset != 7 || manager.Used() != 0 {
		t.Fatalf("rows=%d used=%d", offset, manager.Used())
	}
}

func TestStreamProjectionDoesNotLeakNarrowColumns(t *testing.T) {
	batch := memory.NewBatch()
	batch.Int8s["a"] = &memory.Int8Vector{Values: []int8{1, 2, 3}}
	batch.Float32s["b"] = &memory.Float32Vector{Values: []float32{4, 5, 6}}
	batch.Reset(3)
	src, err := New(memory.NewSchema(memory.Field{Name: "a", Type: memory.TypeInt8}, memory.Field{Name: "b", Type: memory.TypeFloat32}), batch)
	if err != nil {
		t.Fatal(err)
	}
	projected, _, err := src.ApplyPushdown(context.Background(), source.PushdownRequest{Projection: []string{"a"}})
	if err != nil {
		t.Fatal(err)
	}
	stream, err := projected.NewBatchStream(context.Background(), streamOptions(2))
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	out := memory.NewBatch()
	for {
		ok, err := stream.Next(context.Background(), out)
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			break
		}
		if len(out.Float32s) != 0 || len(out.Int8s) != 1 {
			t.Fatal("unprojected column leaked")
		}
	}
}

func TestStreamCancellationAndBudgetErrorsAreTerminal(t *testing.T) {
	b := memory.NewBatch()
	b.Int64s["x"] = &memory.Int64Vector{Values: []int64{1, 2, 3}, Validity: memory.Bitmap{7}}
	b.Reset(3)
	src, err := New(memory.NewSchema(memory.Field{Name: "x", Type: memory.TypeInt64, Nullable: true}), b)
	if err != nil {
		t.Fatal(err)
	}
	for _, cancel := range []bool{false, true} {
		opts := streamOptions(1)
		opts.BatchBytes = 1
		stream, err := src.NewBatchStream(context.Background(), opts)
		if err != nil {
			t.Fatal(err)
		}
		out := memory.NewBatch()
		if ok, err := stream.Next(context.Background(), out); err != nil || !ok {
			t.Fatalf("first ok=%v err=%v", ok, err)
		}
		ctx := context.Background()
		want := memory.ErrBudgetExceeded
		if cancel {
			var stop context.CancelFunc
			ctx, stop = context.WithCancel(ctx)
			stop()
			want = context.Canceled
		}
		if ok, err := stream.Next(ctx, out); ok || !errors.Is(err, want) || out.Rows != 0 || len(out.Int64s) != 0 {
			t.Fatalf("failed next ok=%v err=%v output=%+v", ok, err, out)
		}
		if ok, err := stream.Next(context.Background(), out); ok || !errors.Is(err, want) {
			t.Fatalf("terminal next ok=%v err=%v", ok, err)
		}
		stream.Close()
	}
}

func TestStreamNestedListAndStructRanges(t *testing.T) {
	tags := &memory.StringListVector{Offsets: []uint32{0, 2, 2, 3, 5}, Validity: memory.Bitmap{0b1101}, Encoding: memory.EncodingView, Values: &memory.StringViewVector{Offsets: []uint32{0, 1, 2, 3, 4, 5}, Data: []byte("abcde"), Validity: memory.Bitmap{0b10101}, Encoding: memory.EncodingView}}
	fields := []memory.Field{memory.ListStringField("tags", true, true)}
	child := memory.NewBatch()
	child.StringLists["tags"] = tags
	child.Reset(4)
	b := memory.NewBatch()
	b.Structs["nested"] = &memory.StructVector{Rows: 4, Fields: fields, Values: child, Validity: memory.Bitmap{0b1111}, Encoding: memory.EncodingView}
	b.Reset(4)
	field := memory.Field{Name: "nested", Type: memory.TypeStruct, Nullable: true, Children: fields}
	src, err := New(memory.NewSchema(field), b)
	if err != nil {
		t.Fatal(err)
	}
	stream, err := src.NewBatchStream(context.Background(), streamOptions(1))
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	out := memory.NewBatch()
	for row := 0; row < 4; row++ {
		if ok, err := stream.Next(context.Background(), out); !ok || err != nil {
			t.Fatalf("row %d ok=%v err=%v", row, ok, err)
		}
		v := out.Structs["nested"]
		if err := v.Validate(field, 1); err != nil {
			t.Fatal(err)
		}
		list := v.Values.StringLists["tags"]
		lo, hi, valid := tags.ValueRange(row)
		if list.IsValid(0) != valid {
			t.Fatal("list parent null changed")
		}
		if !valid {
			continue
		}
		if list.Values.Len() != hi-lo {
			t.Fatal("list offsets not rebased")
		}
		for i := lo; i < hi; i++ {
			if string(list.Values.ValueBytes(i-lo)) != string(tags.Values.ValueBytes(i)) || list.Values.IsValid(i-lo) != tags.Values.IsValid(i) {
				t.Fatal("list child changed")
			}
		}
	}
}
