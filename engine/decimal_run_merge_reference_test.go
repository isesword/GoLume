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
	"gocolumn/memory"
)

type n30RefFixture struct {
	t       *testing.T
	ctx     context.Context
	store   *memory.QuerySpillStore
	manager *memory.BudgetManager
	meta    decimalPartialMetadata
	files   []*decimalPartialFile
	outputs []*mergedDecimalPartialRun
}

func n30ReferenceFixture(t *testing.T) *n30RefFixture {
	t.Helper()
	store, err := memory.NewQuerySpillStore(memory.SpillOptions{Directory: t.TempDir(), MaxBytes: 16 << 20})
	if err != nil {
		t.Fatal(err)
	}
	f := &n30RefFixture{t: t, ctx: context.Background(), store: store, manager: memory.NewBudgetManager(8 << 20), meta: decimalPartialMetadata{38, 9}}
	t.Cleanup(func() {
		for _, o := range f.outputs {
			if e := o.close(context.Background()); e != nil {
				t.Error(e)
			}
		}
		for _, file := range f.files {
			if e := file.close(context.Background()); e != nil {
				t.Error(e)
			}
		}
		if e := store.Close(); e != nil {
			t.Error(e)
		}
		if f.manager.Used() != 0 || store.Used() != 0 {
			t.Errorf("leaked memory/disk: %d/%d", f.manager.Used(), store.Used())
		}
	})
	return f
}
func (f *n30RefFixture) file(records []decimalPartialRecord) *decimalPartialFile {
	f.t.Helper()
	out, err := spillDecimalPartialRun(f.ctx, f.store, f.meta, records, f.manager)
	if out != nil {
		f.files = append(f.files, out)
	}
	if err != nil {
		f.t.Fatal(err)
	}
	return out
}
func n30RefRecord(t *testing.T, key int64, valid bool, rows uint64, values ...memory.Decimal128) decimalPartialRecord {
	t.Helper()
	r := decimalPartialRecord{Key: key, KeyValid: valid, CountRows: rows}
	for _, v := range values {
		if e := r.Sum.Add(v); e != nil {
			t.Fatal(e)
		}
	}
	r.CountValid = r.Sum.Count()
	return r
}
func n30RefBig(t *testing.T, s aggregate.DecimalSumState) *big.Int {
	t.Helper()
	var wire [40]byte
	if e := s.EncodeChecked(wire[:], 38); e != nil {
		t.Fatal(e)
	}
	n := new(big.Int)
	for i := 31; i >= 0; i-- {
		n.Lsh(n, 8)
		n.Add(n, big.NewInt(int64(wire[i])))
	}
	if wire[31]&128 != 0 {
		n.Sub(n, new(big.Int).Lsh(big.NewInt(1), 256))
	}
	return n
}

type n30RefKey struct {
	valid bool
	key   int64
}
type n30RefValue struct {
	sum         *big.Int
	valid, rows uint64
}

func n30RefLess(a, b n30RefKey) bool {
	if a.valid != b.valid {
		return !a.valid
	}
	return a.key < b.key
}
func (f *n30RefFixture) check(inputs [][]decimalPartialRecord) {
	f.t.Helper()
	var files []*decimalPartialFile
	oracle := map[n30RefKey]*n30RefValue{}
	for _, records := range inputs {
		files = append(files, f.file(records))
		for _, r := range records {
			key := n30RefKey{r.KeyValid, r.Key}
			if !key.valid {
				key.key = 0
			}
			o := oracle[key]
			if o == nil {
				o = &n30RefValue{sum: new(big.Int)}
				oracle[key] = o
			}
			o.sum.Add(o.sum, n30RefBig(f.t, r.Sum))
			o.valid += r.CountValid
			o.rows += r.CountRows
		}
	}
	out, err := mergeDecimalPartialRuns(f.ctx, f.store, f.meta, files, f.manager)
	if out != nil {
		f.outputs = append(f.outputs, out)
	}
	if err != nil {
		f.t.Fatal(err)
	}
	keys := []n30RefKey{}
	for k, v := range oracle {
		if v.rows != 0 {
			keys = append(keys, k)
		}
	}
	sort.Slice(keys, func(i, j int) bool { return n30RefLess(keys[i], keys[j]) })
	if out.pageCount != (len(keys)+255)/256 {
		f.t.Fatalf("pages %d for %d groups", out.pageCount, len(keys))
	}
	seen := 0
	for page := 0; page < out.pageCount; page++ {
		decoded, e := restoreDecimalPartialRun(f.ctx, out.pages[page], f.manager)
		if e != nil {
			f.t.Fatal(e)
		}
		if len(decoded.records) == 0 || len(decoded.records) > 256 {
			f.t.Fatal("bad page size", len(decoded.records))
		}
		for _, r := range decoded.records {
			if seen >= len(keys) {
				f.t.Fatal("too many records")
			}
			key := keys[seen]
			want := oracle[key]
			if r.KeyValid != key.valid || r.Key != key.key || r.CountRows != want.rows || r.CountValid != want.valid || n30RefBig(f.t, r.Sum).Cmp(want.sum) != 0 {
				f.t.Fatalf("group %d got %+v wantkey %+v", seen, r, key)
			}
			seen++
		}
		decoded.close()
	}
	if seen != len(keys) {
		f.t.Fatal("missing groups", seen, len(keys))
	}
	// Borrowed inputs remain independently restorable after output deletion.
	if e := out.close(f.ctx); e != nil {
		f.t.Fatal(e)
	}
	for _, file := range files {
		restored, e := restoreDecimalPartialRun(f.ctx, file, f.manager)
		if e != nil {
			f.t.Fatal("input consumed", e)
		}
		restored.close()
	}
}
func TestN30RunMergeReferencePageBoundaries(t *testing.T) {
	for _, count := range []int{0, 1, 255, 256, 257, 511, 512, 2048} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			f := n30ReferenceFixture(t)
			fanin := max(1, (count+255)/256)
			inputs := make([][]decimalPartialRecord, fanin)
			for i := 0; i < count; i++ {
				key := int64(i) - 1024
				inputs[i%fanin] = append(inputs[i%fanin], n30RefRecord(t, key, true, uint64(1+i%3), decimal128.FromI64(int64(i)-900)))
			}
			f.check(inputs)
		})
	}
}
func TestN30RunMergeReferenceWideNullAndOrder(t *testing.T) {
	f := n30ReferenceFixture(t)
	maximum := decimal128.GetScaleMultiplier(38).Sub(decimal128.FromI64(1))
	a := n30RefRecord(t, math.MinInt64, true, 4, maximum, maximum, maximum, maximum)
	b := n30RefRecord(t, math.MinInt64, true, 4, maximum.Negate(), maximum.Negate(), maximum.Negate(), maximum.Negate())
	if _, _, e := a.Sum.Finalize(); e == nil {
		t.Fatal("fixture not wide")
	}
	inputs := [][]decimalPartialRecord{
		{n30RefRecord(t, 999, false, 2), a, n30RefRecord(t, 0, true, 0), n30RefRecord(t, math.MaxInt64, true, 2, decimal128.FromI64(-3))},
		{n30RefRecord(t, -9, false, 0), b, n30RefRecord(t, -1, true, 1), n30RefRecord(t, 0, true, 0), n30RefRecord(t, math.MaxInt64, true, 4, decimal128.FromI64(7))},
	}
	f.check(inputs)
	// Cancellation is retained as a valid zero, not an all-NULL state.
	if e := a.Sum.Merge(b.Sum); e != nil {
		t.Fatal(e)
	}
	v, valid, e := a.Sum.Finalize()
	if e != nil || !valid || v != decimal128.FromI64(0) {
		t.Fatal("cancellation finalization", v, valid, e)
	}
}
func TestN30RunMergeReferenceRejectsShapeAndOrder(t *testing.T) {
	cases := map[string]func(*n30RefFixture) []*decimalPartialFile{
		"descending": func(f *n30RefFixture) []*decimalPartialFile {
			return []*decimalPartialFile{f.file([]decimalPartialRecord{n30RefRecord(t, 2, true, 1), n30RefRecord(t, 1, true, 1)})}
		},
		"duplicate-key": func(f *n30RefFixture) []*decimalPartialFile {
			return []*decimalPartialFile{f.file([]decimalPartialRecord{n30RefRecord(t, 1, true, 1), n30RefRecord(t, 1, true, 1)})}
		},
		"null-after-valid": func(f *n30RefFixture) []*decimalPartialFile {
			return []*decimalPartialFile{f.file([]decimalPartialRecord{n30RefRecord(t, -1, true, 1), n30RefRecord(t, 0, false, 1)})}
		},
		"duplicate-null": func(f *n30RefFixture) []*decimalPartialFile {
			return []*decimalPartialFile{f.file([]decimalPartialRecord{n30RefRecord(t, 0, false, 1), n30RefRecord(t, 0, false, 1)})}
		},
		"257-records": func(f *n30RefFixture) []*decimalPartialFile {
			r := make([]decimalPartialRecord, 257)
			for i := range r {
				r[i] = n30RefRecord(t, int64(i), true, 1)
			}
			return []*decimalPartialFile{f.file(r)}
		},
		"over64k": func(f *n30RefFixture) []*decimalPartialFile {
			r := make([]decimalPartialRecord, 1000)
			for i := range r {
				r[i] = n30RefRecord(t, int64(i), true, 1)
			}
			return []*decimalPartialFile{f.file(r)}
		},
		"duplicate-handle": func(f *n30RefFixture) []*decimalPartialFile { a := f.file(nil); return []*decimalPartialFile{a, a} },
		"nine-inputs": func(f *n30RefFixture) []*decimalPartialFile {
			r := make([]*decimalPartialFile, 9)
			for i := range r {
				r[i] = f.file(nil)
			}
			return r
		},
		"nil-input": func(f *n30RefFixture) []*decimalPartialFile { return []*decimalPartialFile{nil} },
		"metadata": func(f *n30RefFixture) []*decimalPartialFile {
			f.meta.Scale++
			a := f.file(nil)
			f.meta.Scale--
			return []*decimalPartialFile{a}
		},
	}
	for name, build := range cases {
		t.Run(name, func(t *testing.T) {
			f := n30ReferenceFixture(t)
			files := build(f)
			out, e := mergeDecimalPartialRuns(f.ctx, f.store, f.meta, files, f.manager)
			if out != nil {
				f.outputs = append(f.outputs, out)
			}
			if e == nil {
				t.Fatal("invalid input admitted")
			}
			for _, file := range files {
				if file != nil && file.handle == nil {
					t.Fatal("borrowed input deleted")
				}
			}
		})
	}
}
func TestN30RunMergeReferenceCountOverflow(t *testing.T) {
	f := n30ReferenceFixture(t)
	a := n30RefRecord(t, 7, true, math.MaxUint64)
	b := n30RefRecord(t, 7, true, 1)
	out, e := mergeDecimalPartialRuns(f.ctx, f.store, f.meta, []*decimalPartialFile{f.file([]decimalPartialRecord{a}), f.file([]decimalPartialRecord{b})}, f.manager)
	if out != nil {
		f.outputs = append(f.outputs, out)
	}
	if e == nil {
		t.Fatal("row count overflow accepted")
	}
	// Independently construct a legal zero sum with the largest SUM count.
	var wire [40]byte
	binary.LittleEndian.PutUint64(wire[32:], math.MaxInt64)
	if e := a.Sum.DecodeChecked(wire[:], 38); e != nil {
		t.Fatal(e)
	}
	a.CountValid = math.MaxInt64
	a.CountRows = math.MaxInt64
	b = n30RefRecord(t, 7, true, 1, decimal128.FromI64(0))
	out, e = mergeDecimalPartialRuns(f.ctx, f.store, f.meta, []*decimalPartialFile{f.file([]decimalPartialRecord{a}), f.file([]decimalPartialRecord{b})}, f.manager)
	if out != nil {
		f.outputs = append(f.outputs, out)
	}
	if e == nil {
		t.Fatal("SUM count overflow accepted")
	}
}

func TestN30RunMergeReferenceEightWaySharedKeys(t *testing.T) {
	for _, reverse := range []bool{false, true} {
		t.Run(fmt.Sprint(reverse), func(t *testing.T) {
			f := n30ReferenceFixture(t)
			inputs := make([][]decimalPartialRecord, 8)
			maxValue := decimal128.GetScaleMultiplier(38).Sub(decimal128.FromI64(1))
			for input := 0; input < 8; input++ {
				lane := input
				if reverse {
					lane = 7 - input
				}
				for key := 0; key < 256; key++ {
					value := maxValue
					if lane%2 != 0 {
						value = value.Negate()
					}
					if key%5 == 0 {
						inputs[input] = append(inputs[input], n30RefRecord(t, int64(key)-128, true, 2))
					} else {
						inputs[input] = append(inputs[input], n30RefRecord(t, int64(key)-128, true, 2, value, value))
					}
				}
			}
			f.check(inputs)
		})
	}
}

func TestN30RunMergeReferenceWideOutputNotFinalized(t *testing.T) {
	f := n30ReferenceFixture(t)
	maxValue := decimal128.GetScaleMultiplier(38).Sub(decimal128.FromI64(1))
	a := n30RefRecord(t, 5, true, 3, maxValue, maxValue, maxValue)
	b := n30RefRecord(t, 5, true, 2, maxValue, maxValue)
	inputs := []*decimalPartialFile{f.file([]decimalPartialRecord{a}), f.file([]decimalPartialRecord{b})}
	out, e := mergeDecimalPartialRuns(f.ctx, f.store, f.meta, inputs, f.manager)
	if out != nil {
		f.outputs = append(f.outputs, out)
	}
	if e != nil {
		t.Fatal("partial merge finalized too early", e)
	}
	if out.pageCount != 1 {
		t.Fatal("missing wide output")
	}
	decoded, e := restoreDecimalPartialRun(f.ctx, out.pages[0], f.manager)
	if e != nil {
		t.Fatal(e)
	}
	defer decoded.close()
	if len(decoded.records) != 1 {
		t.Fatal("wrong group count")
	}
	expected := new(big.Int).Mul(new(big.Int).Sub(new(big.Int).Exp(big.NewInt(10), big.NewInt(38), nil), big.NewInt(1)), big.NewInt(5))
	r := decoded.records[0]
	if n30RefBig(t, r.Sum).Cmp(expected) != 0 || r.CountValid != 5 || r.CountRows != 5 {
		t.Fatal("wide state changed")
	}
	if _, _, e := r.Sum.Finalize(); e != aggregate.ErrDecimalSumOverflow {
		t.Fatal("only explicit finalization must fail", e)
	}
}
