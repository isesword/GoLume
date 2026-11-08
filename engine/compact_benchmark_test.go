package engine

import (
	"context"
	"fmt"
	"gocolumn/expr"
	"gocolumn/kernel"
	"gocolumn/memory"
	"gocolumn/physical"
	"gocolumn/source"
	"gocolumn/source/inmemory"
	"testing"
)

// The public filter boundary materializes selected native-width columns before
// the complete query consumes them. It cannot be replaced by an aggregate's
// fused predicate path, so these fixtures exercise physical compaction.
func nativeCompactQuery(tb testing.TB, typ memory.DataType, rows, batchRows, pct int, nullable bool) (source.Source, expr.Query) {
	tb.Helper()
	schema := memory.NewSchema(memory.Field{Name: "x", Type: memory.TypeInt64}, memory.Field{Name: "v", Type: typ, Nullable: nullable})
	batches := []*memory.Batch{}
	for base := 0; base < rows; base += batchRows {
		n := min(batchRows, rows-base)
		batch := memory.NewBatch()
		batch.Reset(n)
		x := make([]int64, n)
		f32 := make([]float32, n)
		i8 := make([]int8, n)
		u8 := make([]uint8, n)
		i16 := make([]int16, n)
		u16 := make([]uint16, n)
		var valid memory.Bitmap
		if nullable {
			valid = make(memory.Bitmap, memory.WordsFor(n))
		}
		for i := range x {
			row := base + i
			x[i] = int64((row * 37) % 100)
			v := row % 97
			f32[i] = float32(v) + 0.25
			i8[i] = int8(v)
			u8[i] = uint8(v)
			i16[i] = int16(v)
			u16[i] = uint16(v)
			if nullable && row%17 != 0 {
				memory.Set(valid, i)
			}
		}
		batch.Int64s["x"] = &memory.Int64Vector{Values: x}
		switch typ {
		case memory.TypeFloat32:
			batch.Float32s["v"] = &memory.Float32Vector{Values: f32, Validity: valid}
		case memory.TypeInt8:
			batch.Int8s["v"] = &memory.Int8Vector{Values: i8, Validity: valid}
		case memory.TypeUint8:
			batch.Uint8s["v"] = &memory.Uint8Vector{Values: u8, Validity: valid}
		case memory.TypeInt16:
			batch.Int16s["v"] = &memory.Int16Vector{Values: i16, Validity: valid}
		case memory.TypeUint16:
			batch.Uint16s["v"] = &memory.Uint16Vector{Values: u16, Validity: valid}
		}
		batches = append(batches, batch)
	}
	src, err := inmemory.New(schema, batches...)
	if err != nil {
		tb.Fatal(err)
	}
	filtered, err := physical.NewFilterSource(src, expr.LT(expr.Col("x"), expr.I64(int64(pct))))
	if err != nil {
		tb.Fatal(err)
	}
	return filtered, expr.Query{Aggregates: []expr.Aggregate{expr.Sum("v", "sum"), expr.Count("count")}}
}

func BenchmarkNativeCompactQuery(b *testing.B) {
	for _, typ := range []memory.DataType{memory.TypeFloat32, memory.TypeInt8, memory.TypeUint8, memory.TypeInt16, memory.TypeUint16} {
		for _, nullable := range []bool{false, true} {
			for _, pct := range []int{1, 50, 99, 100} {
				b.Run(fmt.Sprintf("%s/null%t/pct%d", typ, nullable, pct), func(b *testing.B) {
					src, q := nativeCompactQuery(b, typ, 65539, 65539, pct, nullable)
					opts := DefaultOptions()
					opts.Workers = 1
					opts.PreparedCacheBytes = 0
					opts.KernelMode = kernel.ModeAuto
					e := New(opts)
					defer e.Close()
					prepared, err := e.Prepare(context.Background(), src, q, PrepareOptions{})
					if err != nil {
						b.Fatal(err)
					}
					defer prepared.Close()
					if _, err := prepared.ExecuteTyped(context.Background(), ExecuteOptions{}); err != nil {
						b.Fatal(err)
					}
					b.ReportAllocs()
					b.ResetTimer()
					for range b.N {
						if _, err := prepared.ExecuteTyped(context.Background(), ExecuteOptions{}); err != nil {
							b.Fatal(err)
						}
					}
				})
			}
		}
	}
}
