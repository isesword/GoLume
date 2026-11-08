package engine

import (
	"context"
	"gocolumn/kernel"
	"gocolumn/memory"
	"testing"
)

func TestIndependentCheckedQueryChargeEquality(t *testing.T) {
	for _, typ := range []memory.DataType{memory.TypeInt64, memory.TypeUint64} {
		for _, op := range []kernel.ArithmeticOp{kernel.ArithmeticAdd, kernel.ArithmeticSub, kernel.ArithmeticMul} {
			for _, nullPct := range []int{0, 50, 100} {
				for _, selected := range []bool{false, true} {
					for _, batch := range []int{65, 1021} {
						var wantPeak int64
						for mi, mode := range []kernel.Mode{kernel.ModeScalar, kernel.ModeAuto, kernel.ModeSIMD} {
							src, q, want := checkedIntegerQuery(t, typ, op, 2051, batch, nullPct, selected, true)
							opts := DefaultOptions()
							opts.Workers = 1
							opts.PreparedCacheBytes = 0
							opts.KernelMode = mode
							opts.MemoryLimitBytes = 4 << 20
							opts.ExecutionPolicy = ExecutionBounded
							e := New(opts)
							budget, err := e.BeginQueryMemory()
							if err != nil {
								t.Fatal(err)
							}
							result, err := e.ExecuteTyped(memory.WithQueryBudget(context.Background(), budget), src, q, ExecuteOptions{Materialize: true})
							budget.Close()
							closeErr := e.Close()
							if err != nil || closeErr != nil {
								t.Fatalf("%s %s %s errors %v %v", typ, op, mode, err, closeErr)
							}
							got, ok := result.Rows[0].Aggregates[0].Value.AsUint64()
							if !ok || got != want {
								t.Fatalf("result%d want%d", got, want)
							}
							snap := budget.Snapshot()
							if snap.UsedBytes != 0 || snap.PeakBytes > opts.MemoryLimitBytes {
								t.Fatalf("memory %+v", snap)
							}
							if mi == 0 {
								wantPeak = snap.PeakBytes
							} else if snap.PeakBytes != wantPeak {
								t.Fatalf("%s %s %s null%d selected%t batch%d peak%d scalar%d", typ, op, mode, nullPct, selected, batch, snap.PeakBytes, wantPeak)
							}
						}
					}
				}
			}
		}
	}
}
