package lazy

import (
	"context"
	"gocolumn/engine"
	"gocolumn/memory"
	"gocolumn/source/inmemory"
	"testing"
	"time"
)

func TestLazyTimestampUnitsAndTemporalArithmetic(t *testing.T) {
	stamp := time.Date(2024, time.February, 29, 23, 0, 0, 0, time.UTC)
	days := int32(stamp.Truncate(24*time.Hour).Unix() / int64(24*time.Hour/time.Second))
	batch := memory.NewBatch()
	batch.Int32s["d"] = &memory.Int32Vector{Values: []int32{days, days + 1}}
	batch.Int64s["ts"] = &memory.Int64Vector{Values: []int64{stamp.UnixNano(), stamp.Add(time.Second).UnixNano()}}
	batch.Reset(2)
	src, err := inmemory.New(memory.NewSchema(
		memory.Field{Name: "d", Type: memory.TypeDate32},
		memory.Field{Name: "ts", Type: memory.TypeTimestamp},
	), batch)
	if err != nil {
		t.Fatal(err)
	}
	session := NewSession(engine.DefaultOptions())
	defer session.Close()
	frame, err := session.FromSource(src).
		Filter(Col("ts").Eq(TimestampMillis(stamp.UnixMilli()))).
		WithColumns(
			Col("d").Add(Lit(1)).Alias("tomorrow"),
			Col("ts").Sub(Lit(int64(time.Second))).Alias("previous_second"),
		).
		SelectExprs(Col("tomorrow").Alias("tomorrow"), Col("previous_second").Alias("previous_second")).
		CollectFrame(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer frame.Close()
	if frame.Height() != 1 {
		t.Fatalf("rows=%d", frame.Height())
	}
	tomorrow, err := frame.batches()[0].Int32("tomorrow")
	if err != nil {
		t.Fatal(err)
	}
	previous, err := frame.batches()[0].Int64("previous_second")
	if err != nil {
		t.Fatal(err)
	}
	if tomorrow.Values[0] != days+1 || previous.Values[0] != stamp.Add(-time.Second).UnixNano() {
		t.Fatalf("tomorrow=%v previous=%v", tomorrow.Values, previous.Values)
	}
}
