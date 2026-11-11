package pipeline

import (
	"context"
	"errors"
	"fmt"
	"gocolumn/kernel"
	"gocolumn/memory"
	"gocolumn/source"
	"gocolumn/source/inmemory"
	"runtime"
	"sync"
	"testing"
	"time"
)

type perfSourceMetrics struct{ batches, maxRows int }
type perfMeasuredSource struct {
	source.Source
	m *perfSourceMetrics
}
type perfMeasuredStream struct {
	source.BatchStream
	m *perfSourceMetrics
}

func (s perfMeasuredSource) NewBatchStreamWithPartitions(c context.Context, o source.ReadOptions, p []source.Partition) (source.BatchStream, error) {
	o.BatchPayloadTargetBytes = 64 << 10
	st, e := source.OpenBatchStreamWithPartitions(c, s.Source, o, p)
	if e != nil {
		return nil, e
	}
	return &perfMeasuredStream{st, s.m}, nil
}
func (s *perfMeasuredStream) Next(c context.Context, b *memory.Batch) (bool, error) {
	ok, e := s.BatchStream.Next(c, b)
	if ok {
		s.m.batches++
		if b.Rows > s.m.maxRows {
			s.m.maxRows = b.Rows
		}
	}
	return ok, e
}
func perfChecksum(kind string, b *memory.Batch) uint64 {
	var sum uint64
	for i := 0; i < b.Rows; i++ {
		switch kind {
		case "scalar":
			sum += uint64(b.Int64s["x"].Values[i]) + 1
		case "string":
			v := b.Strings["x"]
			if v.IsValid(i) {
				v := v.ValueBytes(i)
				sum += uint64(len(v)) + 1
				for _, x := range v {
					sum += uint64(x)
				}
			}
		default:
			s := b.Structs["s"]
			if !s.IsValid(i) {
				continue
			}
			sum++
			v := s.Values.StringLists["l"]
			a, z, ok := v.ValueRange(i)
			if !ok {
				continue
			}
			sum += uint64(z-a) + 1
			for j := a; j < z; j++ {
				if v.Values.IsValid(j) {
					x := v.Values.ValueBytes(j)
					sum += uint64(len(x)) + 1
					for _, k := range x {
						sum += uint64(k)
					}
				}
			}
		}
	}
	return sum
}
func TestRealFanOutResources(t *testing.T) {
	oldProcs := runtime.GOMAXPROCS(1)
	defer runtime.GOMAXPROCS(oldProcs)
	for _, kind := range []string{"scalar", "string", "struct_list"} {
		for _, slow := range []bool{false, true} {
			for _, limit := range []int64{256 << 10, 512 << 10, 768 << 10, 1 << 20} {
				t.Run(fmt.Sprintf("%s/slow%t/query%d", kind, slow, limit), func(t *testing.T) {
					input, schema := perfFixture(kind, 65536)
					want := perfChecksum(kind, input)
					base, e := inmemory.New(schema, input)
					if e != nil {
						t.Fatal(e)
					}
					metrics := new(perfSourceMetrics)
					src := perfMeasuredSource{base, metrics}
					engine := memory.NewEngineBudget(1 << 30)
					q, e := engine.BeginQuery(memory.BudgetOptions{HardLimitBytes: limit})
					if e != nil {
						t.Fatal(e)
					}
					defer q.Close()
					ctx, cancel := context.WithTimeout(memory.WithQueryBudget(context.Background(), q), 5*time.Second)
					defer cancel()
					parts, _ := src.Partitions(ctx)
					opts := Options{Workers: 1, TargetMorselBytes: 64 << 10, MaxMorselBytes: 256 << 10, MaxInFlightBytes: 1 << 20, MinMorselRows: 256, MaxMorselRows: 65536}
					start := time.Now()
					streams, e := OpenFanOut(ctx, src, source.ReadOptions{Kernels: kernel.New(kernel.ModeScalar), BatchRows: 65536}, parts, q, opts, 2, nil)
					if e != nil {
						t.Fatal(e)
					}
					type result struct {
						rows, items int
						sum         uint64
						err         error
					}
					results := make([]result, 2)
					var wg sync.WaitGroup
					for branch, st := range streams {
						wg.Add(1)
						go func(branch int, st source.BatchStream) {
							defer wg.Done()
							defer st.Close()
							b := memory.NewBatch()
							for {
								ok, e := st.Next(ctx, b)
								if e != nil {
									results[branch].err = e
									return
								}
								if !ok {
									return
								}
								if kind == "scalar" {
									for j, v := range b.Int64s["x"].Values {
										if v != int64(results[branch].rows+j) {
											results[branch].err = fmt.Errorf("order/value mismatch")
											return
										}
									}
								}
								results[branch].sum += perfChecksum(kind, b)
								results[branch].rows += b.Rows
								results[branch].items++
								if slow && branch == 1 {
									time.Sleep(50 * time.Microsecond)
								}
							}
						}(branch, st)
					}
					wg.Wait()
					elapsed := time.Since(start)
					success := true
					budgetFailure := false
					for _, r := range results {
						if r.err != nil {
							success = false
							if errors.Is(r.err, memory.ErrBudgetExceeded) {
								budgetFailure = true
							} else if !errors.Is(r.err, context.Canceled) {
								t.Fatalf("unexpected failure: %v", r.err)
							}
						} else if r.rows != 65536 || r.sum != want {
							t.Fatalf("bad rows/checksum: %+v want %d", r, want)
						}
					}
					if !success && limit >= 512<<10 {
						t.Error("required-budget fan-out failed")
					}
					if !success && !budgetFailure {
						t.Fatal("failure without budget error")
					}
					peaks := make([]int64, 2)
					var blocked time.Duration
					for i, st := range streams {
						e := st.(*fanOutBranch).exchange
						stats := e.Stats()
						peaks[i] = stats.PeakInFlightBytes
						blocked += stats.ProducerBlocked
						if stats.CurrentInFlightBytes != 0 || stats.PeakInFlightBytes > e.maxBytes {
							t.Fatal("exchange bound/leak")
						}
						if !e.bytes.TryAcquire(e.maxBytes) {
							t.Fatal("global leak")
						}
						e.bytes.Release(e.maxBytes)
						if !e.groupBytes[0].TryAcquire(e.groupMax) {
							t.Fatal("group leak")
						}
						e.groupBytes[0].Release(e.groupMax)
					}
					snap := q.Snapshot()
					if snap.UsedBytes != 0 || engine.Used() != 0 || snap.PeakBytes > limit {
						t.Fatal("query bound/leak")
					}
					t.Logf("success=%t source=%d/max%d items=%d/%d rows=%d/%d checksum=%d/%d want=%d qpeak=%d branch-observed-global-peaks=%v one-group-cap=%d global-cap=%d producer-blocked=%s elapsed=%s errors=%v/%v", success, metrics.batches, metrics.maxRows, results[0].items, results[1].items, results[0].rows, results[1].rows, results[0].sum, results[1].sum, want, snap.PeakBytes, peaks, opts.MaxInFlightBytes/2, opts.MaxInFlightBytes/2, blocked, elapsed, results[0].err, results[1].err)
				})
			}
		}
	}
}
