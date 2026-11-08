package physical

import (
	"context"
	"gocolumn/memory"
	"testing"
)

type topKCountingManager struct {
	*memory.BudgetManager
	calls int
}

func (m *topKCountingManager) Reserve(ctx context.Context, bytes int64) (memory.Reservation, error) {
	m.calls++
	return m.BudgetManager.Reserve(ctx, bytes)
}

func TestTopKFixedSnapshotsReuseAllNativeFamiliesAndValidity(t *testing.T) {
	const rows, k = 4096, 7
	input := topKMutableSource(rows, 0)
	delete(input.batch.Strings, "payload")
	input.schema = memory.NewSchema(memory.Field{Name: "key", Type: memory.TypeInt64},
		memory.Field{Name: "i8", Type: memory.TypeInt8, Nullable: true},
		memory.Field{Name: "i16", Type: memory.TypeInt16, Nullable: true},
		memory.Field{Name: "i32", Type: memory.TypeInt32, Nullable: true},
		memory.Field{Name: "i64", Type: memory.TypeInt64, Nullable: true},
		memory.Field{Name: "u8", Type: memory.TypeUint8, Nullable: true},
		memory.Field{Name: "u16", Type: memory.TypeUint16, Nullable: true},
		memory.Field{Name: "u32", Type: memory.TypeUint32, Nullable: true},
		memory.Field{Name: "u64", Type: memory.TypeUint64, Nullable: true},
		memory.Field{Name: "f32", Type: memory.TypeFloat32, Nullable: true},
		memory.Field{Name: "f64", Type: memory.TypeFloat64, Nullable: true},
	)
	input.batch.Int8s["i8"] = &memory.Int8Vector{Values: make([]int8, 1)}
	input.batch.Int16s["i16"] = &memory.Int16Vector{Values: make([]int16, 1)}
	input.batch.Int32s["i32"] = &memory.Int32Vector{Values: make([]int32, 1)}
	input.batch.Int64s["i64"] = &memory.Int64Vector{Values: make([]int64, 1)}
	input.batch.Uint8s["u8"] = &memory.Uint8Vector{Values: make([]uint8, 1)}
	input.batch.Uint16s["u16"] = &memory.Uint16Vector{Values: make([]uint16, 1)}
	input.batch.Uint32s["u32"] = &memory.Uint32Vector{Values: make([]uint32, 1)}
	input.batch.Uint64s["u64"] = &memory.Uint64Vector{Values: make([]uint64, 1)}
	input.batch.Float32s["f32"] = &memory.Float32Vector{Values: make([]float32, 1)}
	input.batch.Float64s["f64"] = &memory.Float64Vector{Values: make([]float64, 1)}
	input.fill = func(i int, b *memory.Batch) {
		b.Int64s["key"].Values[0] = int64(rows - i)
		// First candidates have no validity storage. Replacements alternate
		// explicit NULL bitmaps and all-valid nil bitmaps.
		var validity memory.Bitmap
		if i >= k && i%3 != 0 {
			validity = memory.Bitmap{1}
			if i%3 == 1 {
				validity[0] = 0
			}
		}
		b.Int8s["i8"].Values[0], b.Int8s["i8"].Validity = int8(i), validity
		b.Int16s["i16"].Values[0], b.Int16s["i16"].Validity = int16(i), validity
		b.Int32s["i32"].Values[0], b.Int32s["i32"].Validity = int32(i), validity
		b.Int64s["i64"].Values[0], b.Int64s["i64"].Validity = int64(i), validity
		b.Uint8s["u8"].Values[0], b.Uint8s["u8"].Validity = uint8(i), validity
		b.Uint16s["u16"].Values[0], b.Uint16s["u16"].Validity = uint16(i), validity
		b.Uint32s["u32"].Values[0], b.Uint32s["u32"].Validity = uint32(i), validity
		b.Uint64s["u64"].Values[0], b.Uint64s["u64"].Validity = uint64(i), validity
		b.Float32s["f32"].Values[0], b.Float32s["f32"].Validity = float32(i), validity
		b.Float64s["f64"].Values[0], b.Float64s["f64"].Validity = float64(i), validity
	}
	manager := &topKCountingManager{BudgetManager: memory.NewBudgetManager(128 << 10)}
	top, err := NewTopKSourceWithManager(input, k, []SortKey{{Column: "key"}}, manager)
	if err != nil {
		t.Fatal(err)
	}
	stream, err := top.NewBatchStream(context.Background(), testReadOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	out := memory.NewBatch()
	for output := 0; output < k; output++ {
		if ok, err := stream.Next(context.Background(), out); !ok || err != nil {
			t.Fatal(ok, err)
		}
		i := rows - 1 - output
		valid := i%3 != 1
		if out.Int64s["key"].Values[0] != int64(output+1) {
			t.Fatal("winner order changed")
		}
		if out.Int8s["i8"].Values[0] != int8(i) || out.Int8s["i8"].IsValid(0) != valid {
			t.Fatalf("i8 value/validity changed at %d", i)
		}
		if out.Int16s["i16"].Values[0] != int16(i) || out.Int16s["i16"].IsValid(0) != valid {
			t.Fatalf("i16 value/validity changed at %d", i)
		}
		if out.Int32s["i32"].Values[0] != int32(i) || out.Int32s["i32"].IsValid(0) != valid {
			t.Fatalf("i32 value/validity changed at %d", i)
		}
		if out.Int64s["i64"].Values[0] != int64(i) || out.Int64s["i64"].IsValid(0) != valid {
			t.Fatalf("i64 value/validity changed at %d", i)
		}
		if out.Uint8s["u8"].Values[0] != uint8(i) || out.Uint8s["u8"].IsValid(0) != valid {
			t.Fatalf("u8 value/validity changed at %d", i)
		}
		if out.Uint16s["u16"].Values[0] != uint16(i) || out.Uint16s["u16"].IsValid(0) != valid {
			t.Fatalf("u16 value/validity changed at %d", i)
		}
		if out.Uint32s["u32"].Values[0] != uint32(i) || out.Uint32s["u32"].IsValid(0) != valid {
			t.Fatalf("u32 value/validity changed at %d", i)
		}
		if out.Uint64s["u64"].Values[0] != uint64(i) || out.Uint64s["u64"].IsValid(0) != valid {
			t.Fatalf("u64 value/validity changed at %d", i)
		}
		if out.Float32s["f32"].Values[0] != float32(i) || out.Float32s["f32"].IsValid(0) != valid {
			t.Fatalf("f32 value/validity changed at %d", i)
		}
		if out.Float64s["f64"].Values[0] != float64(i) || out.Float64s["f64"].IsValid(0) != valid {
			t.Fatalf("f64 value/validity changed at %d", i)
		}
	}
	// One reservation per initial candidate plus geometric heap capacity;
	// neither NULL transitions nor replacements reserve/allocate fresh rows.
	if manager.calls > k+4 {
		t.Fatalf("reservations scale with input: %d", manager.calls)
	}
	if err := stream.Close(); err != nil {
		t.Fatal(err)
	}
	if manager.Used() != 0 || input.opens != 1 || input.closes != 1 {
		t.Fatalf("used=%d opens=%d closes=%d", manager.Used(), input.opens, input.closes)
	}
}
