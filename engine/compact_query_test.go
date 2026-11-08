package engine

import (
	"context"
	"fmt"
	"gocolumn/kernel"
	"gocolumn/memory"
	"testing"
)

func TestNativeCompactQuery(t *testing.T) {
	for _, typ := range []memory.DataType{memory.TypeFloat32, memory.TypeInt8, memory.TypeUint8, memory.TypeInt16, memory.TypeUint16} {
		for _, nullable := range []bool{false, true} {
			for _, pct := range []int{0, 1, 50, 99, 100} {
				for _, batchRows := range []int{31, 65, 1021} {
					t.Run(fmt.Sprintf("%s/null%t/pct%d/batch%d", typ, nullable, pct, batchRows), func(t *testing.T) {
						const rows = 2051
						src, q := nativeCompactQuery(t, typ, rows, batchRows, pct, nullable)
						count := uint64(0)
						sum := float64(0)
						valid := false
						for row := 0; row < rows; row++ {
							if (row*37)%100 >= pct {
								continue
							}
							count++
							if nullable && row%17 == 0 {
								continue
							}
							valid = true
							sum += float64(row % 97)
							if typ == memory.TypeFloat32 {
								sum += 0.25
							}
						}
						for _, mode := range []kernel.Mode{kernel.ModeScalar, kernel.ModeAuto, kernel.ModeSIMD} {
							opts := DefaultOptions()
							opts.Workers = 1
							opts.PreparedCacheBytes = 0
							opts.KernelMode = mode
							e := New(opts)
							result, err := e.ExecuteTyped(context.Background(), src, q, ExecuteOptions{Materialize: true})
							if err != nil {
								t.Fatal(err)
							}
							if err := e.Close(); err != nil {
								t.Fatal(err)
							}
							if len(result.Rows) != 1 {
								t.Fatalf("rows=%d", len(result.Rows))
							}
							gotCount, ok := result.Rows[0].Aggregates[1].Value.AsUint64()
							if !ok || gotCount != count {
								t.Fatalf("count=%d want=%d", gotCount, count)
							}
							v := result.Rows[0].Aggregates[0].Value
							got := float64(0)
							hasValue := false
							switch typ {
							case memory.TypeFloat32:
								got, hasValue = v.AsFloat64()
							default:
								i, ok := v.AsInt64()
								got = float64(i)
								hasValue = ok
							}
							if hasValue != valid || valid && got != sum {
								t.Fatalf("mode=%v sum=%v valid=%v want=%v valid=%v", mode, got, hasValue, sum, valid)
							}
						}
					})
				}
			}
		}
	}
}
