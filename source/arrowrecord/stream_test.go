package arrowrecord

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	arrowmem "github.com/apache/arrow-go/v18/arrow/memory"
	"gocolumn/memory"
	"gocolumn/source"
)

func TestPreparedStreamBoundedViewsAndSourceLifetime(t *testing.T) {
	alloc := arrowmem.NewCheckedAllocator(arrowmem.NewGoAllocator())
	rec1 := makeRecord(t, alloc, []int64{1, 2, 3}, []int32{1, 2, 3}, []string{"a", "b", "c"}, []string{"x", "y"}, []uint8{0, 1, 0})
	rec2 := makeRecord(t, alloc, []int64{4, 5}, []int32{4, 5}, []string{"d", "e"}, []string{"y", "x"}, []uint8{0, 1})
	src, err := NewSource(rec1, rec2)
	if err != nil {
		t.Fatal(err)
	}
	rec1.Release()
	rec2.Release()
	opts := testReadOptions()
	opts.BatchRows = 1
	stream, err := src.NewBatchStreamWithPartitions(context.Background(), opts, []source.Partition{{ID: 1}, {ID: 0}, {ID: 1}})
	if err != nil {
		t.Fatal(err)
	}
	// Closing the source prevents new scans but does not invalidate an open one.
	src.Close()
	src.Close()
	if _, err := src.NewBatchStream(context.Background(), opts); err == nil {
		t.Fatal("opened closed source")
	}
	out := memory.NewBatch()
	var values []int64
	for {
		ok, err := stream.Next(context.Background(), out)
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			break
		}
		if out.Rows != 1 {
			t.Fatalf("rows=%d", out.Rows)
		}
		values = append(values, out.Int64s["pred"].Values...)
		a := out.Opaque["list"].Value.(arrow.Array)
		if a.Len() != 1 {
			t.Fatalf("opaque slice rows=%d", a.Len())
		}
		if err := out.Strings["plain"].Validate(1); err != nil {
			t.Fatal(err)
		}
	}
	if !reflect.DeepEqual(values, []int64{4, 5, 1, 2, 3, 4, 5}) {
		t.Fatalf("values=%v", values)
	}
	if ok, err := stream.Next(context.Background(), out); ok || err != nil {
		t.Fatalf("EOF ok=%v err=%v", ok, err)
	}
	stream.Close()
	stream.Close()
	if ok, err := stream.Next(context.Background(), out); ok || !errors.Is(err, source.ErrBatchStreamClosed) {
		t.Fatalf("closed ok=%v err=%v", ok, err)
	}
	alloc.AssertSize(t, 0)
}

func TestPreparedStreamEmptyAndInvalidPartitions(t *testing.T) {
	alloc := arrowmem.NewGoAllocator()
	builder := array.NewInt64Builder(alloc)
	builder.Append(1)
	a := builder.NewArray()
	builder.Release()
	defer a.Release()
	rec := array.NewRecordBatch(arrow.NewSchema([]arrow.Field{{Name: "x", Type: arrow.PrimitiveTypes.Int64}}, nil), []arrow.Array{a}, 1)
	defer rec.Release()
	src, err := NewSource(rec)
	if err != nil {
		t.Fatal(err)
	}
	defer src.Close()
	for _, parts := range [][]source.Partition{nil, {}} {
		stream, err := src.NewBatchStreamWithPartitions(context.Background(), testReadOptions(), parts)
		if err != nil {
			t.Fatal(err)
		}
		out := memory.NewBatch()
		out.Reset(99)
		if ok, err := stream.Next(context.Background(), out); ok || err != nil || out.Rows != 0 {
			t.Fatalf("empty ok=%v err=%v rows=%d", ok, err, out.Rows)
		}
		stream.Close()
	}
	if _, err := src.NewBatchStreamWithPartitions(context.Background(), testReadOptions(), []source.Partition{{ID: -1}}); err == nil {
		t.Fatal("invalid partition accepted")
	}
}

func TestStreamCancellationReleasesRetainedRecords(t *testing.T) {
	alloc := arrowmem.NewCheckedAllocator(arrowmem.NewGoAllocator())
	rec := makeRecord(t, alloc, []int64{1, 2}, []int32{1, 2}, []string{"a", "b"}, []string{"x"}, []uint8{0, 0})
	src, err := NewSource(rec)
	if err != nil {
		t.Fatal(err)
	}
	rec.Release()
	opts := testReadOptions()
	opts.BatchRows = 1
	stream, err := src.NewBatchStream(context.Background(), opts)
	if err != nil {
		t.Fatal(err)
	}
	src.Close()
	out := memory.NewBatch()
	if ok, err := stream.Next(context.Background(), out); !ok || err != nil {
		t.Fatalf("next ok=%v err=%v", ok, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if ok, err := stream.Next(ctx, out); ok || !errors.Is(err, context.Canceled) || out.Rows != 0 {
		t.Fatalf("cancel ok=%v err=%v rows=%d", ok, err, out.Rows)
	}
	if ok, err := stream.Next(context.Background(), out); ok || !errors.Is(err, context.Canceled) {
		t.Fatalf("terminal ok=%v err=%v", ok, err)
	}
	stream.Close()
	alloc.AssertSize(t, 0)
}
