package lazy

import (
	"context"
	"gocolumn/engine"
	"gocolumn/memory"
	"gocolumn/observer"
	"gocolumn/pipeline"
	"gocolumn/source/inmemory"
	"strings"
	"sync"
	"testing"
)

func TestPhase31ParallelPipelineCollectExplainAndObserver(t *testing.T) {
	schema := memory.NewSchema(memory.Field{Name: "x", Type: memory.TypeInt64})
	batches := make([]*memory.Batch, 8)
	for part := range batches {
		batch := memory.NewBatch()
		base := int64(part * 4)
		batch.Int64s["x"] = &memory.Int64Vector{Values: []int64{base, base + 1, base + 2, base + 3}}
		batch.Reset(4)
		batches[part] = batch
	}
	src, err := inmemory.New(schema, batches...)
	if err != nil {
		t.Fatal(err)
	}
	collector := &observer.Collector{}
	opts := engine.DefaultOptions()
	opts.Workers = 4
	opts.MemoryLimitBytes = 1 << 20
	opts.Observer = collector
	opts.Pipeline = pipeline.Options{
		Workers: 4, TargetMorselBytes: 32, MaxMorselBytes: 64, MaxInFlightBytes: 256,
		MinMorselRows: 1, MaxMorselRows: 4, Ordering: pipeline.OrderingStable,
	}
	session := NewSession(opts)
	defer session.Close()
	query := session.FromSource(src).
		Filter(Col("x").Gt(Lit(7))).
		SelectExprs(Named(Col("x").Add(Lit(1)), "y"))
	explain, err := query.ExplainPhysical(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"pipeline_workers=4 target_morsel_bytes=32 max_in_flight_bytes=256 ordering=stable",
		"PIPELINE id=p00 workers=4 ordering=stable target_morsel_bytes=32",
		"EXCHANGE id=x00 pipeline=p00 max_in_flight_bytes=256 max_morsel_bytes=64",
	} {
		if !strings.Contains(explain, want) {
			t.Fatalf("explain missing %q:\n%s", want, explain)
		}
	}
	df, err := query.Collect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	values := frameInt64Column(t, df, "y")
	if len(values) != 24 {
		t.Fatalf("rows=%d values=%v", len(values), values)
	}
	for i, got := range values {
		if want := int64(i + 9); got != want {
			t.Fatalf("value[%d]=%d want=%d", i, got, want)
		}
	}
	if err := df.Close(); err != nil {
		t.Fatal(err)
	}
	if used := session.Engine().Memory().Used(); used != 0 {
		t.Fatalf("pipeline/dataframe reservation leak=%d", used)
	}
	found := false
	for _, event := range collector.Events() {
		if event.Stage == "pipeline" {
			found = event.Workers == 4 && event.Rows == 24 && event.Attributes["peak_in_flight_bytes"] != ""
		}
	}
	if !found {
		t.Fatalf("pipeline observer event missing: %+v", collector.Events())
	}
}

func TestPhase31PreparedParallelExecutionsOwnIndependentGraphs(t *testing.T) {
	schema := memory.NewSchema(memory.Field{Name: "x", Type: memory.TypeInt64})
	batches := make([]*memory.Batch, 6)
	for part := range batches {
		batch := memory.NewBatch()
		batch.Int64s["x"] = &memory.Int64Vector{Values: []int64{int64(part*2 + 1), int64(part*2 + 2)}}
		batch.Reset(2)
		batches[part] = batch
	}
	src, _ := inmemory.New(schema, batches...)
	opts := engine.DefaultOptions()
	opts.Workers = 3
	opts.MemoryLimitBytes = 1 << 20
	opts.Pipeline = pipeline.Options{Workers: 3, TargetMorselBytes: 16, MaxMorselBytes: 32, MaxInFlightBytes: 96, MinMorselRows: 1, MaxMorselRows: 2}
	session := NewSession(opts)
	defer session.Close()
	prepared, err := session.FromSource(src).Select("x").Prepare(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer prepared.Close()
	var wg sync.WaitGroup
	errs := make(chan error, 2)
	for range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			df, err := prepared.Execute(context.Background())
			if err != nil {
				errs <- err
				return
			}
			if df.Height() != 12 {
				errs <- &phase31RowsError{got: df.Height(), want: 12}
			}
			_ = df.Close()
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	if used := session.Engine().Memory().Used(); used != 0 {
		t.Fatalf("concurrent execution leaked %d bytes", used)
	}
}

type phase31RowsError struct{ got, want int64 }

func (e *phase31RowsError) Error() string { return "parallel prepared row count mismatch" }
