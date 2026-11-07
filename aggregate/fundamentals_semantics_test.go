package aggregate

import (
	"encoding/json"
	"os"
	"testing"
)

// This test uses recorded Polars observations without requiring Python in CI.
// Empty SUM deliberately retains GoLume's SQL-compatible null result; counts
// compare values rather than adopting Polars' UInt32 result representation.
func TestFundamentalsPolarsInt64Baseline(t *testing.T) {
	data, err := os.ReadFile("../testdata/fundamentals/polars-1.35.2.json")
	if err != nil {
		t.Fatal(err)
	}
	var reference struct {
		Version string `json:"version"`
		Cases   []struct {
			ID      string          `json:"id"`
			Input   json.RawMessage `json:"input"`
			Results json.RawMessage `json:"results"`
		} `json:"cases"`
	}
	if err := json.Unmarshal(data, &reference); err != nil {
		t.Fatal(err)
	}
	if reference.Version != "1.35.2" {
		t.Fatalf("unexpected oracle %q", reference.Version)
	}
	kinds := map[string]Kind{"sum": KindSum, "count": KindCountValues, "len": KindCountRows, "min": KindMin, "max": KindMax, "avg": KindAvg}
	cases := 0
	for _, tc := range reference.Cases {
		if tc.ID != "aggregate/Int64/empty" && tc.ID != "aggregate/Int64/null" && tc.ID != "aggregate/Int64/mixed" {
			continue
		}
		cases++
		var inputs []*int64
		if err := json.Unmarshal(tc.Input, &inputs); err != nil {
			t.Fatal(err)
		}
		var results map[string]struct {
			Values []*float64 `json:"values"`
		}
		if err := json.Unmarshal(tc.Results, &results); err != nil {
			t.Fatal(err)
		}
		for name, kind := range kinds {
			t.Run(tc.ID+"/"+name, func(t *testing.T) {
				sequential, err := NewState(kind)
				if err != nil {
					t.Fatal(err)
				}
				left, _ := NewState(kind)
				right, _ := NewState(kind)
				nonnull := 0
				for i, value := range inputs {
					var v int64
					valid := value != nil
					if valid {
						v = *value
						nonnull++
					}
					sequential.Add(v, valid)
					if i%2 == 0 {
						left.Add(v, valid)
					} else {
						right.Add(v, valid)
					}
				}
				if err := left.Merge(right); err != nil {
					t.Fatal(err)
				}
				got := sequential.Finalize()
				if merged := left.Finalize(); merged != got {
					t.Fatalf("merge=%+v sequential=%+v", merged, got)
				}
				expected := results[name].Values
				if len(expected) != 1 {
					t.Fatalf("oracle values=%v", expected)
				}
				want := expected[0]
				if kind == KindSum && nonnull == 0 {
					want = nil
				}
				if got.Valid != (want != nil) {
					t.Fatalf("validity=%v want=%v", got.Valid, want != nil)
				}
				if want != nil {
					numeric := float64(got.Int64)
					if got.IsFloat {
						numeric = got.Float64
					}
					if numeric != *want {
						t.Fatalf("value=%v want=%v", numeric, *want)
					}
				}
				sequential.Reset()
				reset := sequential.Finalize()
				if kind == KindCountRows || kind == KindCountValues {
					if !reset.Valid || reset.Int64 != 0 {
						t.Fatalf("reset count=%+v", reset)
					}
				} else if reset.Valid {
					t.Fatalf("reset unexpectedly valid=%+v", reset)
				}
			})
		}
	}
	if cases != 3 {
		t.Fatalf("expected three baseline cases, got %d", cases)
	}
}
