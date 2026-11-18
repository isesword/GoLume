package physical

import (
	"context"
	"errors"
	"gocolumn/memory"
	"gocolumn/source"
	"testing"
)

func TestPercentRankResidentBorrowedPages(t *testing.T) {
	for _, page := range []int{0, 1, 7, 64, 1024, 4096} {
		for _, n := range []int{0, 1, 65, 2051} {
			ctx, q := percentRankQuery(t, 8<<20, false)
			win := percentRankWindow(t, percentRankFixture(t, n, true), q, WindowExecutionResident)
			st, e := source.OpenBatchStream(ctx, win, source.ReadOptions{Kernels: offsetTestKernels(), BatchRows: page})
			if e != nil {
				t.Fatal(page, n, e)
			}
			seen, pages := 0, 0
			var previous *memory.Batch
			expected := page
			if expected == 0 {
				expected = 1024
			}
			for {
				out := memory.NewBatch()
				ok, e := st.Next(ctx, out)
				if e != nil {
					t.Fatal(page, n, e)
				}
				if previous != nil && (previous.Rows != 0 || len(previous.Float64s) != 0 || len(previous.Int64s) != 0) {
					t.Fatal("previous borrow survived Next")
				}
				if !ok {
					break
				}
				if out.Rows != min(expected, n-seen) {
					t.Fatal("page size", page, n, out.Rows, seen)
				}
				for i := 0; i < out.Rows; i++ {
					id := int64(n - 1 - seen - i)
					want := 0.0
					if n > 1 {
						want = float64(id/3*3) / float64(n-1)
					}
					if out.Int64s["id"].Values[i] != id || out.Float64s["pr"].Values[i] != want {
						t.Fatal("page value/order")
					}
				}
				seen += out.Rows
				pages++
				previous = out
				if seen == n && q.Used() == 0 {
					t.Fatal("last page owner released early")
				}
			}
			if seen != n || n == 0 && pages != 1 {
				t.Fatal("page totals", page, n, seen, pages)
			}
			if q.Used() != 0 {
				t.Fatal("EOF leak", q.Used())
			}
			if e := st.Close(); e != nil {
				t.Fatal(e)
			}
		}
	}
}
func TestPercentRankResidentPageCancelAndClose(t *testing.T) {
	for _, cancel := range []bool{false, true} {
		ctx, q := percentRankQuery(t, 8<<20, false)
		win := percentRankWindow(t, percentRankFixture(t, 65, false), q, WindowExecutionResident)
		st, e := source.OpenBatchStream(ctx, win, source.ReadOptions{Kernels: offsetTestKernels(), BatchRows: 7})
		if e != nil {
			t.Fatal(e)
		}
		out := memory.NewBatch()
		ok, e := st.Next(ctx, out)
		if e != nil || !ok || out.Rows != 7 {
			t.Fatal(e)
		}
		if cancel {
			canceled, stop := context.WithCancel(ctx)
			stop()
			ok, e = st.Next(canceled, memory.NewBatch())
			if ok || !errors.Is(e, context.Canceled) {
				t.Fatal("page cancel", e)
			}
		} else {
			if e = st.Close(); e != nil {
				t.Fatal(e)
			}
		}
		if out.Rows != 0 || len(out.Float64s) != 0 || q.Used() != 0 {
			t.Fatal("borrow/owner cleanup", out.Rows, q.Used())
		}
		if e = st.Close(); e != nil {
			t.Fatal(e)
		}
	}
}
