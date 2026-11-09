package engine

import (
	"context"
	"gocolumn/expr"
	"gocolumn/memory"
	"reflect"
	"testing"
)

func TestExecutionMetadataAndEmbeddedInputOwnership(t *testing.T) {
	s, budget := globalBindingTestStream(t,
		memory.NewSchema(memory.Field{Name: "v", Type: memory.TypeInt64}),
		expr.Query{Aggregates: []expr.Aggregate{expr.Count("n")}})
	if s.inputBatch != &s.inputStorage {
		t.Fatal("stream input header is not owned by the stream")
	}
	// Preserve the public NewBatch contract even when its header is embedded.
	if s.inputBatch.Int64s == nil || s.inputBatch.Decimals == nil || s.inputBatch.Opaque == nil {
		t.Fatal("embedded input maps are not writable")
	}
	want := s.query.resultSchema()
	if !reflect.DeepEqual(s.query.executionSchema.Fields, want.Fields) {
		t.Fatal("cached execution schema differs from the public stream schema")
	}
	// The externally supplied stream descriptor and fresh result descriptors
	// must not expose the metadata shared only by internal Execute consumers.
	public := s.schema
	public.Fields[0].Name = "changed stream"
	fresh := s.query.resultSchema()
	fresh.Fields[0].Name = "changed result"
	if !reflect.DeepEqual(s.query.executionSchema.Fields, want.Fields) {
		t.Fatal("external schema mutation reached execution metadata")
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if s.inputBatch != nil || !reflect.DeepEqual(s.inputStorage, memory.Batch{}) || budget.Used() != 0 {
		t.Fatal("close retained embedded input bindings or reservations")
	}
	for range 2 {
		result, err := s.query.ExecuteTyped(context.Background(), ExecuteOptions{Materialize: true})
		if err != nil {
			t.Fatal(err)
		}
		if len(result.Rows) != 1 || len(result.Rows[0].Aggregates) != 1 || result.Rows[0].Aggregates[0].Alias != "n" || result.Rows[0].Aggregates[0].Value.Bits != 0 {
			t.Fatalf("reused execution metadata changed result: %+v", result)
		}
	}
}
