package lazy

import (
	"context"
	"testing"
	"time"

	"gocolumn/engine"
	"gocolumn/memory"
	"gocolumn/source/inmemory"
)

func TestLazyStringTimestampAndDurationPipeline(t *testing.T) {
	schema := memory.NewSchema(
		memory.Field{Name: "stamp", Type: memory.TypeString},
		memory.Field{Name: "span", Type: memory.TypeString},
	)
	batch := memory.NewBatch()
	batch.Strings["stamp"] = &memory.StringViewVector{
		Offsets:  []uint32{0, 25, 50},
		Data:     []byte("2024-03-10T01:59:59-08:002024-03-10T03:00:00-07:00"),
		Encoding: memory.EncodingView,
	}
	batch.Strings["span"] = &memory.StringViewVector{
		Offsets:  []uint32{0, 2, 5},
		Data:     []byte("1s-1s"),
		Encoding: memory.EncodingView,
	}
	batch.Reset(2)
	src, err := inmemory.New(schema, batch)
	if err != nil {
		t.Fatal(err)
	}
	start := time.Date(2024, time.March, 10, 9, 59, 59, 0, time.UTC)
	finish := start.Add(time.Second)
	session := NewSession(engine.DefaultOptions())
	defer session.Close()
	frame, err := session.FromSource(src).
		WithColumns(
			Cast(Col("stamp"), memory.TypeTimestamp).Alias("ts"),
			Cast(Col("span"), memory.TypeDuration).Alias("duration"),
		).
		WithColumns(
			Col("ts").Add(Col("duration")).Alias("finish"),
			Col("ts").Sub(Col("ts")).Alias("elapsed"),
		).
		Filter(Col("finish").Eq(Timestamp(finish.UnixNano()))).
		SelectExprs(
			Col("finish").Alias("finish"),
			Col("duration").Alias("duration"),
			Col("elapsed").Alias("elapsed"),
		).
		Collect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer frame.Close()
	if frame.Height() != 1 {
		t.Fatalf("rows=%d want 1", frame.Height())
	}
	for _, tt := range []struct {
		name string
		want int64
	}{
		{name: "finish", want: finish.UnixNano()},
		{name: "duration", want: int64(time.Second)},
		{name: "elapsed", want: 0},
	} {
		column, err := frame.batches()[0].Int64(tt.name)
		if err != nil {
			t.Fatal(err)
		}
		if column.Values[0] != tt.want {
			t.Fatalf("%s=%d want=%d", tt.name, column.Values[0], tt.want)
		}
	}
}
