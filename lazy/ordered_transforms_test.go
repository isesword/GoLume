package lazy

import (
	"context"
	"gocolumn/engine"
	"gocolumn/memory"
	"gocolumn/source"
	"gocolumn/source/inmemory"
	"math"
	"testing"
)

func TestShiftDiffAcrossStrategiesPartitionsAndBatches(t *testing.T) {
	schema := memory.NewSchema(memory.Field{Name: "id", Type: memory.TypeInt64}, memory.Field{Name: "g", Type: memory.TypeInt32}, memory.Field{Name: "v", Type: memory.TypeInt64, Nullable: true})
	batches := []*memory.Batch{}
	for _, part := range [][2]int{{0, 2}, {2, 6}} {
		b := memory.NewBatch()
		for i := part[0]; i < part[1]; i++ {
			if b.Int64s["id"] == nil {
				b.Int64s["id"] = &memory.Int64Vector{}
				b.Int64s["v"] = &memory.Int64Vector{}
				b.Int32s["g"] = &memory.Int32Vector{}
			}
			b.Int64s["id"].Values = append(b.Int64s["id"].Values, int64(i))
			b.Int64s["v"].Values = append(b.Int64s["v"].Values, []int64{1, 0, 3, 10, 20, 30}[i])
			b.Int32s["g"].Values = append(b.Int32s["g"].Values, int32(i/3))
		}
		b.Rows = part[1] - part[0]
		b.Int64s["v"].Validity = memory.EnsureBitmap(nil, b.Rows)
		for i := 0; i < b.Rows; i++ {
			if part[0]+i != 1 {
				memory.Set(b.Int64s["v"].Validity, i)
			}
		}
		batches = append(batches, b)
	}
	src, err := inmemory.New(schema, batches...)
	if err != nil {
		t.Fatal(err)
	}
	for _, mode := range []string{"resident", "ordered", "external"} {
		t.Run(mode, func(t *testing.T) {
			var session *Session
			var input source.Source = src
			switch mode {
			case "external":
				session = boundedJoinSession(t, 256<<10)
			case "ordered":
				opts := engine.DefaultOptions()
				opts.ExecutionPolicy = engine.ExecutionBounded
				opts.MemoryLimitBytes = 128 << 10
				session = NewSession(opts)
				input = &orderedWindowTestSource{Source: src, ordering: []source.OrderingColumn{{Name: "g"}, {Name: "id", NullsLast: true}}}
			default:
				session = NewSession(engine.DefaultOptions())
			}
			q := session.FromSource(input).WithWindows(NewWindow(PartitionBy("g"), OrderBy(Asc("id"))), Shift("v", 1, "s"), Diff("v", 1, "d"), Shift("v", 0, "identity"))
			f, err := q.Collect(context.Background())
			if err != nil {
				session.Close()
				t.Fatal(err)
			}
			values, valid := frameInt64WithValidity(t, f, "s")
			if !equalI64(values, []int64{0, 1, 0, 0, 10, 20}) || !equalBools(valid, []bool{false, true, false, false, true, true}) {
				t.Fatal(values, valid)
			}
			values, valid = frameInt64WithValidity(t, f, "d")
			if !equalI64(values, []int64{0, 0, 0, 0, 10, 10}) || !equalBools(valid, []bool{false, false, false, false, true, true}) {
				t.Fatal(values, valid)
			}
			f.Close()
			session.Close()
			if session.Engine().Memory().Used() != 0 {
				t.Fatal("budget leak")
			}
		})
	}
	session := NewSession(engine.DefaultOptions())
	defer session.Close()
	f, err := session.FromSource(src).WithWindows(NewWindow(PartitionBy("g"), OrderBy(Asc("id"))), Shift("v", -1, "s")).Collect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	values, valid := frameInt64WithValidity(t, f, "s")
	if !equalI64(values, []int64{0, 3, 0, 20, 30, 0}) || !equalBools(valid, []bool{false, true, false, true, true, false}) {
		t.Fatal(values, valid)
	}
	_, err = session.FromSource(src).WithWindows(NewWindow(nil, OrderBy(Asc("id"))), Shift("v", math.MinInt64, "bad")).Schema()
	if err == nil {
		t.Fatal("minimum offset accepted")
	}
	bounded := boundedJoinSession(t, 256<<10)
	defer bounded.Close()
	future, err := bounded.FromSource(src).WithWindows(NewWindow(PartitionBy("g"), OrderBy(Asc("id"))), Shift("v", -1, "future"), Diff("v", -1, "future_diff"), Shift("v", 1, "past")).Collect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer future.Close()
	fv, fok := frameInt64WithValidity(t, future, "future")
	if !equalI64(fv, []int64{0, 3, 0, 20, 30, 0}) || !equalBools(fok, []bool{false, true, false, true, true, false}) {
		t.Fatal(fv, fok)
	}
	dv, dok := frameInt64WithValidity(t, future, "future_diff")
	if !equalI64(dv, []int64{0, 0, 0, -10, -10, 0}) || !equalBools(dok, []bool{false, false, false, true, true, false}) {
		t.Fatal(dv, dok)
	}
}
