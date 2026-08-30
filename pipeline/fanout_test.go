package pipeline_test

import (
	"errors"
	"gocolumn/memory"
	"gocolumn/pipeline"
	"gocolumn/source"
	"gocolumn/source/inmemory"
	"testing"
)

func TestFanOutPlanNeverChoosesUnboundedCache(t *testing.T) {
	batch := memory.NewBatch()
	batch.Int64s["x"] = &memory.Int64Vector{Values: []int64{1}}
	batch.Reset(1)
	stable, err := inmemory.New(memory.NewSchema(memory.Field{Name: "x", Type: memory.TypeInt64}), batch)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := pipeline.PlanFanOut(stable, 2, 0)
	if err != nil || plan.Strategy != pipeline.FanOutRescan || plan.Replayability != source.ReplayStable {
		t.Fatalf("stable plan=%+v err=%v", plan, err)
	}
	unknown := &unknownReplaySource{Source: stable}
	if _, err := pipeline.PlanFanOut(unknown, 2, 0); !errors.Is(err, pipeline.ErrUnboundedFanOut) {
		t.Fatalf("unbounded unknown error=%v", err)
	}
	plan, err = pipeline.PlanFanOut(unknown, 3, 1<<20)
	if err != nil || plan.Strategy != pipeline.FanOutBoundedMultiplexer || plan.MaxBufferedBytes != 1<<20 {
		t.Fatalf("bounded plan=%+v err=%v", plan, err)
	}
}

type unknownReplaySource struct{ source.Source }
