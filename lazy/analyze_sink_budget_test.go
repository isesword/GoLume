package lazy

import (
	"context"
	"errors"
	"gocolumn/engine"
	"gocolumn/memory"
	"io"
	"path/filepath"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	arrowmem "github.com/apache/arrow-go/v18/arrow/memory"
)

type analyzeFinalizeWriter struct {
	allocator arrowmem.Allocator
	closed    int
	failure   error
}

func (w *analyzeFinalizeWriter) Write(arrow.RecordBatch) error { return nil }
func (w *analyzeFinalizeWriter) Close() error {
	w.closed++
	// Native encoders may allocate only while finalizing, after upstream EOF.
	buffer := w.allocator.Allocate(16)
	w.allocator.Free(buffer)
	return w.failure
}

func TestFileSinkFinalizationBorrowsCallerBudget(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(map[bool]string{false: "success", true: "close-error"}[fail], func(t *testing.T) {
			budget, err := memory.NewEngineBudget(4096).BeginQuery(memory.BudgetOptions{})
			if err != nil {
				t.Fatal(err)
			}
			defer budget.Close()
			ctx := memory.WithQueryBudget(context.Background(), budget)
			caller, err := budget.Operator("Caller", memory.MemoryRetainedState).Reserve(ctx, 8)
			if err != nil {
				t.Fatal(err)
			}
			defer caller.Release()
			failure := errors.New("encoder finalize failure")
			writer := &analyzeFinalizeWriter{}
			if fail {
				writer.failure = failure
			}
			factory := func(_ io.Writer, _ *arrow.Schema, allocator arrowmem.Allocator) (recordSinkWriter, error) {
				writer.allocator = allocator
				return writer, nil
			}
			session := NewSession(engine.DefaultOptions())
			defer session.Close()
			src := &analyzeStreamSource{rows: 2, batches: 2, reserveBytes: 16}
			result, err := session.FromSource(src).sinkFiles(ctx, filepath.Join(t.TempDir(), "output.test"), ".test", SinkOptions{}, factory, "FixtureEncoder")
			if fail && !errors.Is(err, failure) || !fail && (err != nil || result.Rows != 4) {
				t.Fatalf("result=%+v error=%v", result, err)
			}
			if writer.closed != 1 || budget.Snapshot().Closed || budget.Used() != 8 {
				t.Fatalf("encoder closed=%d budget=%+v", writer.closed, budget.Snapshot())
			}
			assertAnalyzeReleased(t, session, src)
		})
	}
}
