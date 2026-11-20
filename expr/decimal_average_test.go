package expr

import "testing"

func TestN20AverageNormalization(t *testing.T) {
	spec := AvgDecimal128("d", 12, 3, DecimalRoundHalfEven, "a")
	input := []AggregateSpec{Count("n"), spec, Sum("d", "s")}
	a, q, o, e := NormalizeConfiguredAggregates(input)
	if e != nil || len(q) != 0 || len(o) != 1 || o[0].Output != 1 || o[0].Precision != 12 || o[0].Scale != 3 || o[0].Mode != DecimalRoundHalfEven || a[1].Kind != AggDecimalAverage {
		t.Fatal(a, q, o, e)
	}
	input[1] = AvgDecimal128("changed", 1, 0, DecimalRoundFloor, "changed")
	if a[1].Column != "d" || o[0].Precision != 12 {
		t.Fatal("alias")
	}
	if _, _, e := NormalizeAggregates([]AggregateSpec{spec}); e == nil {
		t.Fatal("old normalizer must not discard config")
	}
	if _, _, _, e := NormalizeConfiguredAggregates([]AggregateSpec{Aggregate{Kind: AggDecimalAverage, Column: "d"}}); e == nil {
		t.Fatal("bare kind")
	}
	for _, v := range []DecimalAverageSpec{AvgDecimal128("d", 0, 0, DecimalRoundHalfEven, "a"), AvgDecimal128("d", 39, 0, DecimalRoundHalfEven, "a"), AvgDecimal128("d", 2, -1, DecimalRoundHalfEven, "a"), AvgDecimal128("d", 2, 3, DecimalRoundHalfEven, "a"), AvgDecimal128("d", 2, 0, 0, "a"), AvgDecimal128("d", 2, 0, 255, "a"), AvgDecimal128("", 2, 0, DecimalRoundHalfEven, "a")} {
		if _, _, _, e := NormalizeConfiguredAggregates([]AggregateSpec{v}); e == nil {
			t.Fatal(v)
		}
	}
	if _, _, _, e := NormalizeConfiguredAggregates([]AggregateSpec{&spec}); e == nil {
		t.Fatal("mutable pointer not accepted")
	}
}
