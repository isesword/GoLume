package physical

import (
	"context"
	"errors"
	"gocolumn/memory"
	"reflect"
	"testing"
)

func int64ListAccumulatorFixture() (memory.Schema, *memory.Batch) {
	field := memory.ListInt64Field("nums", true, true)
	b := memory.NewBatch()
	b.Rows = 4
	b.Int64Lists["nums"] = &memory.Int64ListVector{Offsets: []uint32{0, 2, 3, 3, 4}, Validity: memory.Bitmap{0b1101}, Values: &memory.Int64Vector{Values: []int64{7, 999, 88, -9}, Validity: memory.Bitmap{0b1101}}}
	child := memory.NewBatch()
	child.Rows = 4
	child.Int64Lists["nums"] = b.Int64Lists["nums"]
	b.Structs["s"] = &memory.StructVector{Rows: 4, Fields: []memory.Field{field}, Validity: memory.Bitmap{0b1110}, Values: child}
	return memory.NewSchema(field, memory.StructField("s", true, field)), b
}

func TestInt64ListAccumulatorsCanonicalBudgetAndCleanup(t *testing.T) {
	for _, resident := range []bool{false, true} {
		t.Run(map[bool]string{false: "external", true: "resident"}[resident], func(t *testing.T) {
			ctx := context.Background()
			schema, src := int64ListAccumulatorFixture()
			w, parent := externalAccumulatorTestWorkspace(t, 0)
			var a *externalSortAccumulator
			if resident {
				r, err := newResidentSortAccumulator(ctx, schema, w, src)
				if err != nil {
					t.Fatal(err)
				}
				defer r.close()
				a = r.externalSortAccumulator
			} else {
				var err error
				a, err = newExternalSortAccumulator(ctx, schema, w, 0)
				if err != nil {
					t.Fatal(err)
				}
				defer a.close()
			}
			for row := 0; row < 4; row++ {
				if err := a.appendRow(ctx, src, row); err != nil {
					t.Fatal(err)
				}
			}
			if err := a.finish(ctx); err != nil {
				t.Fatal(err)
			}
			v := a.batch.Int64Lists["nums"]
			nested := a.batch.Structs["s"].Values.Int64Lists["nums"]
			if err := v.Validate(4); err != nil {
				t.Fatal(err)
			}
			if err := nested.Validate(4); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(v.Offsets, []uint32{0, 2, 2, 2, 3}) || !reflect.DeepEqual(v.Values.Values, []int64{7, 0, -9}) || v.Values.IsValid(1) {
				t.Fatalf("noncanonical list: %+v %+v", v, v.Values)
			}
			if !reflect.DeepEqual(nested.Offsets, []uint32{0, 0, 0, 0, 1}) || nested.IsValid(0) {
				t.Fatalf("struct hidden child retained: %+v", nested)
			}
			actual := externalAccumulatorTestCapacity(reflect.ValueOf(a.batch))
			if actual != a.capacityBytes || a.reservation.Bytes() < a.metadataBytes+actual {
				t.Fatalf("capacity tracked=%d actual=%d admitted=%d", a.capacityBytes, actual, a.reservation.Bytes())
			}
			child := v.Values
			a.close()
			if v.Values != nil || child.Values != nil || child.Validity != nil {
				t.Fatal("cleanup left reachable buffers")
			}
			w.close()
			if parent.Used() != 0 {
				t.Fatal("cleanup leaked budget")
			}
			if src.Int64Lists["nums"].Values.Values[1] != 999 {
				t.Fatal("source modified")
			}
		})
	}
}

func TestInt64ListAccumulatorRefusalAndMalformedInput(t *testing.T) {
	schema, src := int64ListAccumulatorFixture()
	ctx := context.Background()
	for _, kind := range []string{"budget", "offset", "bitmap", "cancel"} {
		t.Run(kind, func(t *testing.T) {
			w, parent := externalAccumulatorTestWorkspace(t, 0)
			a, err := newExternalSortAccumulator(ctx, schema, w, 0)
			if err != nil {
				t.Fatal(err)
			}
			defer a.close()
			input, _ := memory.CloneBatch(src)
			workCtx := ctx
			switch kind {
			case "budget":
				parent.deny = true
			case "offset":
				input.Int64Lists["nums"].Offsets[1] = 99
			case "bitmap":
				input.Int64Lists["nums"].Values.Validity = memory.Bitmap{}
			case "cancel":
				var cancel context.CancelFunc
				workCtx, cancel = context.WithCancel(ctx)
				cancel()
			}
			if err := a.appendRow(workCtx, input, 0); err == nil {
				t.Fatal("invalid/refused input accepted")
			}
			if err := a.appendRow(ctx, src, 0); !errors.Is(err, errExternalSortAccumulatorFailed) {
				t.Fatalf("failed owner reusable: %v", err)
			}
			a.close()
			w.close()
			if parent.Used() != 0 {
				t.Fatal("failed owner leaked")
			}
		})
	}
}

func TestInt64ListAccumulatorEmptyFinish(t *testing.T) {
	schema := memory.NewSchema(memory.ListInt64Field("nums", true, true))
	w, _ := externalAccumulatorTestWorkspace(t, 0)
	a, err := newExternalSortAccumulator(context.Background(), schema, w, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer a.close()
	if err := a.finish(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := a.batch.Int64Lists["nums"].Validate(0); err != nil {
		t.Fatal(err)
	}
}
