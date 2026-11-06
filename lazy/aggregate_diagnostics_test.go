package lazy

import (
	"context"
	"strings"
	"testing"

	"gocolumn/engine"
	"gocolumn/expr"
	"gocolumn/memory"
	"gocolumn/source"
	"gocolumn/source/inmemory"
)

func TestAggregateAnalyzeReportsActualNativeStrategy(t *testing.T) {
	for _, strategy := range []string{"bitmap-direct", "partitioned-hash"} {
		t.Run(strategy, func(t *testing.T) {
			batch := memory.NewBatch()
			batch.Int32s["key"] = &memory.Int32Vector{Values: []int32{1, 1, 2, 2}}
			batch.Reset(4)
			input, err := inmemory.New(memory.NewSchema(memory.Field{Name: "key", Type: memory.TypeInt32}), batch)
			if err != nil {
				t.Fatal(err)
			}
			opts := engine.DefaultOptions()
			opts.Workers = 4
			session := NewSession(opts)
			defer session.Close()
			frame := session.FromSource(input).WithHints(expr.Hints{ForceStrategy: strategy}).GroupBy("key").Agg(Count("count"))
			report, err := frame.Analyze(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if report.Strategy != strategy || report.GroupCount != 2 || report.SelectedRows != 4 {
				t.Fatalf("report=%+v", report)
			}
			prepared, err := frame.Prepare(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			defer prepared.Close()
			detail := source.StreamPlanOf(prepared.src).Detail
			workers := "workers=1"
			if strategy == "partitioned-hash" {
				workers = "workers=4"
			}
			if !strings.Contains(detail, "strategy="+strategy) || !strings.Contains(detail, workers) || !strings.Contains(detail, "mode=") {
				t.Fatalf("detail=%q", detail)
			}
		})
	}
}
