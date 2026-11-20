package engine

import (
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"math/big"
	"sort"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"gocolumn/aggregate"
)

type n31ReferenceFixture struct {
	*n30RefFixture
	merged []*multipassDecimalPartialRun
}

func n31NewReference(t *testing.T) *n31ReferenceFixture {
	f := &n31ReferenceFixture{n30RefFixture: n30ReferenceFixture(t)}
	// LIFO: output owners close before the underlying fixture closes its inputs/store.
	t.Cleanup(func() {
		for _, out := range f.merged {
			if e := out.close(context.Background()); e != nil {
				t.Error(e)
			}
		}
	})
	return f
}
func (f *n31ReferenceFixture) inputs(runs [][][]decimalPartialRecord) []decimalPartialLogicalRun {
	inputs := make([]decimalPartialLogicalRun, len(runs))
	for i, run := range runs {
		for _, records := range run {
			inputs[i].pages = append(inputs[i].pages, f.file(records))
		}
	}
	return inputs
}
func (f *n31ReferenceFixture) merge(inputs []decimalPartialLogicalRun) (*multipassDecimalPartialRun, error) {
	out, e := mergeDecimalPartialRunsMultiPass(f.ctx, f.store, f.meta, inputs, f.manager)
	if out != nil {
		f.merged = append(f.merged, out)
	}
	return out, e
}
func n31ExpectedPasses(runs int) int {
	if runs == 1 {
		return 1
	}
	passes := 0
	for runs > 1 {
		runs = (runs + 7) / 8
		passes++
	}
	return passes
}
func (f *n31ReferenceFixture) oracle(runs [][][]decimalPartialRecord) {
	f.t.Helper()
	want := make(map[n30RefKey]*n30RefValue)
	for _, run := range runs {
		for _, page := range run {
			for _, r := range page {
				key := n30RefKey{r.KeyValid, r.Key}
				if !key.valid {
					key.key = 0
				}
				value := want[key]
				if value == nil {
					value = &n30RefValue{sum: new(big.Int)}
					want[key] = value
				}
				value.sum.Add(value.sum, n30RefBig(f.t, r.Sum))
				value.valid += r.CountValid
				value.rows += r.CountRows
			}
		}
	}
	inputs := f.inputs(runs)
	out, e := f.merge(inputs)
	if e != nil {
		f.t.Fatal(e)
	}
	if out.passes != n31ExpectedPasses(len(runs)) {
		f.t.Fatalf("passes=%d for %d logical inputs", out.passes, len(runs))
	}
	keys := make([]n30RefKey, 0, len(want))
	for k, v := range want {
		if v.rows != 0 {
			keys = append(keys, k)
		}
	}
	sort.Slice(keys, func(i, j int) bool { return n30RefLess(keys[i], keys[j]) })
	if out.pageCount != (len(keys)+255)/256 {
		f.t.Fatalf("outputpages=%d groups=%d", out.pageCount, len(keys))
	}
	seen := 0
	for p := 0; p < out.pageCount; p++ {
		page, e := restoreDecimalPartialRun(f.ctx, out.pages[p], f.manager)
		if e != nil {
			f.t.Fatal(e)
		}
		if len(page.records) < 1 || len(page.records) > 256 {
			f.t.Fatal("invalid page size", len(page.records))
		}
		for _, r := range page.records {
			if seen >= len(keys) {
				f.t.Fatal("extra group")
			}
			k := keys[seen]
			v := want[k]
			if r.KeyValid != k.valid || r.Key != k.key || r.CountRows != v.rows || r.CountValid != v.valid || n30RefBig(f.t, r.Sum).Cmp(v.sum) != 0 {
				f.t.Fatalf("mismatch at group %d key %+v", seen, k)
			}
			if v.valid == 0 {
				_, valid, e := r.Sum.Finalize()
				if e != nil || valid {
					f.t.Fatal("all-NULL group lost NULL")
				}
			}
			seen++
		}
		page.close()
	}
	if seen != len(keys) {
		f.t.Fatal("missing groups", seen, len(keys))
	}
	if e := out.close(f.ctx); e != nil {
		f.t.Fatal(e)
	}
	for _, run := range inputs {
		for _, file := range run.pages {
			page, e := restoreDecimalPartialRun(f.ctx, file, f.manager)
			if e != nil {
				f.t.Fatal("borrowed input was consumed", e)
			}
			page.close()
		}
	}
}

func TestN31MultiPassReferenceDisjointTermination(t *testing.T) {
	for _, tc := range []struct{ runs, pages, rows int }{{9, 1, 1}, {17, 3, 17}, {64, 4, 256}, {1, 3, 256}} {
		t.Run(fmt.Sprintf("%dx%dx%d", tc.runs, tc.pages, tc.rows), func(t *testing.T) {
			f := n31NewReference(t)
			runs := make([][][]decimalPartialRecord, tc.runs)
			for i := range runs {
				for page := 0; page < tc.pages; page++ {
					records := make([]decimalPartialRecord, tc.rows)
					for row := range records {
						key := int64((page*tc.rows+row)*tc.runs+i) - 32768
						records[row] = n30RefRecord(t, key, true, uint64(1+row%3), decimal128.FromI64(key))
					}
					runs[i] = append(runs[i], records)
				}
			}
			f.oracle(runs)
		})
	}
}

func TestN31MultiPassReferenceSharedKeysNullAndIdentities(t *testing.T) {
	for _, reverse := range []bool{false, true} {
		t.Run(fmt.Sprint(reverse), func(t *testing.T) {
			f := n31NewReference(t)
			runs := make([][][]decimalPartialRecord, 17)
			maxValue := decimal128.GetScaleMultiplier(38).Sub(decimal128.FromI64(1))
			for i := range runs {
				lane := i
				if reverse {
					lane = 16 - i
				}
				records := []decimalPartialRecord{n30RefRecord(t, 37, false, uint64(lane%3))}
				for key := 0; key < 520; key++ {
					value := maxValue
					if lane%2 != 0 {
						value = value.Negate()
					}
					switch key % 7 {
					case 0:
						records = append(records, n30RefRecord(t, int64(key)-260, true, 0))
					case 1:
						records = append(records, n30RefRecord(t, int64(key)-260, true, 3))
					default:
						records = append(records, n30RefRecord(t, int64(key)-260, true, 2, value, value))
					}
				}
				// Empty pages between real pages must not reset the global ordering witness.
				runs[i] = [][]decimalPartialRecord{records[:200], nil, records[200:400], records[400:]}
			}
			f.oracle(runs)
		})
	}
}

func TestN31MultiPassReferenceWidePartialFinalization(t *testing.T) {
	for _, cancel := range []bool{false, true} {
		t.Run(fmt.Sprint(cancel), func(t *testing.T) {
			f := n31NewReference(t)
			maxValue := decimal128.GetScaleMultiplier(38).Sub(decimal128.FromI64(1))
			runs := make([][][]decimalPartialRecord, 16)
			for i := range runs {
				value := maxValue
				if cancel && i >= 8 {
					value = value.Negate()
				}
				runs[i] = [][]decimalPartialRecord{{n30RefRecord(t, math.MinInt64, true, 4, value, value, value, value)}}
			}
			out, e := f.merge(f.inputs(runs))
			if e != nil {
				t.Fatal("partial finalized early", e)
			}
			if out.passes != 2 || out.pageCount != 1 {
				t.Fatal("not a real multipass single group")
			}
			page, e := restoreDecimalPartialRun(f.ctx, out.pages[0], f.manager)
			if e != nil {
				t.Fatal(e)
			}
			defer page.close()
			if len(page.records) != 1 {
				t.Fatal("group count")
			}
			r := page.records[0]
			expected := new(big.Int)
			if !cancel {
				expected.Sub(new(big.Int).Exp(big.NewInt(10), big.NewInt(38), nil), big.NewInt(1))
				expected.Mul(expected, big.NewInt(64))
			}
			if r.CountRows != 64 || r.CountValid != 64 || n30RefBig(t, r.Sum).Cmp(expected) != 0 {
				t.Fatal("wide exact state lost")
			}
			value, valid, e := r.Sum.Finalize()
			if cancel {
				if e != nil || !valid || value != decimal128.FromI64(0) {
					t.Fatal("valid zero cancellation lost")
				}
			} else if e != aggregate.ErrDecimalSumOverflow {
				t.Fatal("expected only finalization to overflow", e)
			}
		})
	}
}

func TestN31MultiPassReferenceInputRejection(t *testing.T) {
	cases := map[string]func(*n31ReferenceFixture) []decimalPartialLogicalRun{
		"cross-page-descending": func(f *n31ReferenceFixture) []decimalPartialLogicalRun {
			return f.inputs([][][]decimalPartialRecord{{{n30RefRecord(f.t, 2, true, 1)}, nil, {n30RefRecord(f.t, 1, true, 1)}}})
		},
		"cross-page-duplicate": func(f *n31ReferenceFixture) []decimalPartialLogicalRun {
			return f.inputs([][][]decimalPartialRecord{{{n30RefRecord(f.t, 2, true, 1)}, {n30RefRecord(f.t, 2, true, 1)}}})
		},
		"null-after-value": func(f *n31ReferenceFixture) []decimalPartialLogicalRun {
			return f.inputs([][][]decimalPartialRecord{{{n30RefRecord(f.t, math.MinInt64, true, 1)}, {n30RefRecord(f.t, 0, false, 1)}}})
		},
		"null-repeated": func(f *n31ReferenceFixture) []decimalPartialLogicalRun {
			return f.inputs([][][]decimalPartialRecord{{{n30RefRecord(f.t, 0, false, 1)}, nil, {n30RefRecord(f.t, 0, false, 1)}}})
		},
		"65-runs": func(f *n31ReferenceFixture) []decimalPartialLogicalRun { return make([]decimalPartialLogicalRun, 65) },
		"257-pages": func(f *n31ReferenceFixture) []decimalPartialLogicalRun {
			run := decimalPartialLogicalRun{}
			for i := 0; i < 257; i++ {
				run.pages = append(run.pages, f.file(nil))
			}
			return []decimalPartialLogicalRun{run}
		},
		"same-handle-cross-run": func(f *n31ReferenceFixture) []decimalPartialLogicalRun {
			file := f.file(nil)
			return []decimalPartialLogicalRun{{pages: []*decimalPartialFile{file}}, {pages: []*decimalPartialFile{file}}}
		},
		"same-handle-cross-page": func(f *n31ReferenceFixture) []decimalPartialLogicalRun {
			file := f.file(nil)
			return []decimalPartialLogicalRun{{pages: []*decimalPartialFile{file, file}}}
		},
		"oversize-page": func(f *n31ReferenceFixture) []decimalPartialLogicalRun {
			rows := make([]decimalPartialRecord, 1000)
			for i := range rows {
				rows[i] = n30RefRecord(f.t, int64(i), true, 1)
			}
			return f.inputs([][][]decimalPartialRecord{{rows}})
		},
		"257-record-page": func(f *n31ReferenceFixture) []decimalPartialLogicalRun {
			rows := make([]decimalPartialRecord, 257)
			for i := range rows {
				rows[i] = n30RefRecord(f.t, int64(i), true, 1)
			}
			return f.inputs([][][]decimalPartialRecord{{rows}})
		},
	}
	for name, build := range cases {
		t.Run(name, func(t *testing.T) {
			f := n31NewReference(t)
			inputs := build(f)
			_, e := f.merge(inputs)
			if e == nil {
				t.Fatal("invalid input accepted")
			}
			for _, run := range inputs {
				for _, file := range run.pages {
					if file.handle == nil {
						t.Fatal("borrowed input deleted")
					}
				}
			}
		})
	}
}

func TestN31MultiPassReferenceCountOverflowAcrossPasses(t *testing.T) {
	for _, sumCount := range []bool{false, true} {
		t.Run(fmt.Sprint(sumCount), func(t *testing.T) {
			f := n31NewReference(t)
			runs := make([][][]decimalPartialRecord, 9)
			for i := range runs {
				runs[i] = [][]decimalPartialRecord{{n30RefRecord(t, 8, true, 0)}}
			}
			a := n30RefRecord(t, 8, true, math.MaxUint64)
			b := n30RefRecord(t, 8, true, 1)
			if sumCount {
				var wire [40]byte
				binary.LittleEndian.PutUint64(wire[32:], math.MaxInt64)
				if e := a.Sum.DecodeChecked(wire[:], 38); e != nil {
					t.Fatal(e)
				}
				a.CountValid = math.MaxInt64
				a.CountRows = math.MaxInt64
				b = n30RefRecord(t, 8, true, 1, decimal128.FromI64(0))
			}
			runs[0][0][0] = a
			runs[8][0][0] = b
			if _, e := f.merge(f.inputs(runs)); e == nil {
				t.Fatal("cross-pass count overflow accepted")
			}
		})
	}
}
