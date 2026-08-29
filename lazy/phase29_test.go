package lazy

import (
	"context"
	"gocolumn/engine"
	"gocolumn/expr"
	"gocolumn/memory"
	"gocolumn/source/inmemory"
	"testing"
	"time"
)

func TestLazyStringAndCalendarExpressions(t *testing.T) {
	stamp := time.Date(2024, time.February, 29, 23, 58, 59, 0, time.UTC).UnixNano()
	valid := memory.EnsureBitmap(nil, 4)
	memory.Set(valid, 0)
	memory.Set(valid, 1)
	memory.Set(valid, 2)
	batch := memory.NewBatch()
	batch.Strings["s"] = &memory.StringViewVector{
		Offsets:  []uint32{0, 8, 18, 18, 18},
		Data:     []byte("Go语言prefix-end"),
		Validity: valid,
		Encoding: memory.EncodingView,
	}
	batch.Int64s["ts"] = &memory.Int64Vector{Values: []int64{stamp, stamp - int64(24*time.Hour), stamp, 0}, Validity: valid}
	batch.Reset(4)
	src, err := inmemory.New(memory.NewSchema(
		memory.Field{Name: "s", Type: memory.TypeString, Nullable: true, Encoding: memory.EncodingView},
		memory.Field{Name: "ts", Type: memory.TypeTimestamp, Nullable: true},
	), batch)
	if err != nil {
		t.Fatal(err)
	}
	session := NewSession(engine.DefaultOptions())
	defer session.Close()
	frame, err := session.FromSource(src).
		Filter(expr.And(Contains(Col("s"), Str("语")), Year(Col("ts")).Eq(Lit(2024)))).
		WithColumns(Length(Col("s")).Alias("chars"), Hour(Col("ts")).Alias("hour")).
		SelectExprs(Col("chars").Alias("chars"), Col("hour").Alias("hour")).
		CollectFrame(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer frame.Close()
	if frame.Height() != 1 {
		t.Fatalf("rows=%d", frame.Height())
	}
	chars, _ := frame.batches()[0].Int64("chars")
	hour, _ := frame.batches()[0].Int64("hour")
	if chars.Values[0] != 4 || hour.Values[0] != 23 {
		t.Fatalf("chars=%v hour=%v", chars.Values, hour.Values)
	}
}
