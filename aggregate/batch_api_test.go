package aggregate

import "testing"

func TestInt64VectorBatchUpdateMergeAndFinalize(t *testing.T) {
	layout := Layout{Sum: true, CountRows: true, CountValues: true, Min: true, Max: true, Avg: true}
	vector := NewInt64Vector(3, layout)
	vector.InitValue(0, 10, true)
	vector.InitValue(1, 0, false)
	if err := vector.UpdateBatch([]int{0, 1}, []int64{5, 7}, []uint8{1, 0}); err != nil {
		t.Fatal(err)
	}
	if err := vector.MergeBatch([]int{0, 1}, []Int64Result{
		{Sum: 3, SumValid: true, CountRows: 2, CountValues: 1, Min: 3, MinValid: true, Max: 3, MaxValid: true},
		{Sum: 9, SumValid: true, CountRows: 1, CountValues: 1, Min: 9, MinValid: true, Max: 9, MaxValid: true},
	}); err != nil {
		t.Fatal(err)
	}
	results, err := vector.FinalizeBatch(nil, []int{0, 1})
	if err != nil {
		t.Fatal(err)
	}
	if got := results[0]; got.Sum != 18 || got.CountRows != 4 || got.CountValues != 3 || got.Min != 3 || got.Max != 10 || !got.AvgValid || got.Avg != 6 {
		t.Fatalf("slot 0=%+v", got)
	}
	if got := results[1]; got.Sum != 9 || got.CountRows != 3 || got.CountValues != 1 || got.Min != 9 || got.Max != 9 || !got.AvgValid || got.Avg != 9 {
		t.Fatalf("slot 1=%+v", got)
	}
}

func TestInt64VectorBatchAPIChecksLengthsAndSlots(t *testing.T) {
	vector := NewInt64Vector(1, DefaultLayout())
	if err := vector.UpdateBatch([]int{0}, nil, nil); err == nil {
		t.Fatal("expected update length error")
	}
	if err := vector.MergeBatch([]int{1}, []Int64Result{{}}); err == nil {
		t.Fatal("expected merge slot error")
	}
	if _, err := vector.FinalizeBatch(nil, []int{-1}); err == nil {
		t.Fatal("expected finalize slot error")
	}
}
