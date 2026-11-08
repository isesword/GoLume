package engine

import (
	"context"
	"fmt"
	"gocolumn/kernel"
	"math"
	"testing"
)

func TestWideningQueryResults(t *testing.T) {
	for _, rows := range []int{1, 65, 1025} {
		for _, mixed := range []bool{false, true} {
			for _, nullable := range []bool{false, true} {
				for _, grouped := range []bool{false, true} {
					t.Run(fmt.Sprintf("rows=%d/mixed=%t/null=%t/grouped=%t", rows, mixed, nullable, grouped), func(t *testing.T) {
						src, q := wideningQuery(t, rows, 31, mixed, nullable, grouped)
						sums, counts := map[int64]float64{}, map[int64]uint64{}
						if !grouped {
							sums[0] = 0
							counts[0] = 0
						}
						for row := 0; row < rows; row++ {
							if nullable && row%17 == 0 {
								continue
							}
							a, b := float64(float32(row%251)+0.25), float64(float32(row%31)+0.75)
							if mixed {
								b = float64(int32(row%31) + 1)
							}
							difference := a - b
							product := difference * a
							value := product / b
							if !(value > 1200) {
								continue
							}
							var key int64
							if grouped {
								key = int64(row % 64)
							}
							sums[key] += a
							counts[key]++
						}
						for _, mode := range []kernel.Mode{kernel.ModeScalar, kernel.ModeAuto, kernel.ModeSIMD} {
							opts := DefaultOptions()
							opts.Workers = 1
							opts.KernelMode = mode
							opts.PreparedCacheBytes = 0
							e := New(opts)
							result, err := e.ExecuteTyped(context.Background(), src, q, ExecuteOptions{Materialize: true})
							if err != nil {
								t.Fatal(err)
							}
							if err := e.Close(); err != nil {
								t.Fatal(err)
							}
							if len(result.Rows) != len(counts) {
								t.Fatalf("mode=%s groups=%d want %d", mode, len(result.Rows), len(counts))
							}
							for _, r := range result.Rows {
								var key int64
								if grouped {
									key, _ = r.Keys[0].Value.AsInt64()
								}
								count, ok := r.Aggregates[1].Value.AsUint64()
								if !ok || count != counts[key] {
									t.Fatalf("mode=%s key=%d count=%v want=%d", mode, key, r.Aggregates[1], counts[key])
								}
								sum, valid := r.Aggregates[0].Value.AsFloat64()
								if valid != (counts[key] > 0) || valid && math.Float64bits(sum) != math.Float64bits(sums[key]) {
									t.Fatalf("mode=%s key=%d sum=%v want=%v", mode, key, r.Aggregates[0], sums[key])
								}
							}
						}
					})
				}
			}
		}
	}
}
