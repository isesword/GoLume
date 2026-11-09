package physical

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"sort"
	"strings"
	"testing"

	"gocolumn/memory"
	"gocolumn/source"
	"gocolumn/source/inmemory"
)

type binaryDictionarySortRow struct {
	value []byte
	valid bool
	id    int64
	batch int
	row   int
}

// Deliberately reuse domains with different dictionaries. Ordering must use
// decoded bytes, including empty values and invalid UTF-8, never codes/domains.
func binaryDictionarySortFixture(parts int) ([]*memory.Batch, []binaryDictionarySortRow) {
	var batches []*memory.Batch
	var rows []binaryDictionarySortRow
	for part := 0; part < parts; part++ {
		values := [][]byte{{0xff}, {0x00}, {0x80}, {}, {'a', 0x00}, {'a'}, {'a', 0x00}, {0x00, 0xff}, {0xfe, byte(part)}}
		dictionary := &memory.DictionaryStringVector{
			Offsets: []uint32{0}, Validity: memory.EnsureBitmap(nil, len(values)),
			DictionaryDomain: "shared", Encoding: memory.EncodingDictionary,
		}
		if part%3 == 2 {
			dictionary.DictionaryDomain = fmt.Sprintf("domain-%d", part)
		}
		for code := range values {
			value := values[(code+part)%len(values)]
			dictionary.Data = append(dictionary.Data, value...)
			dictionary.Offsets = append(dictionary.Offsets, uint32(len(dictionary.Data)))
		}
		batch := memory.NewBatch()
		batch.Rows = len(values)
		batch.DictStrings["key"] = dictionary
		batch.Int64s["id"] = &memory.Int64Vector{}
		for row := range values {
			code := uint32(row * 5 % len(values))
			value := values[(int(code)+part)%len(values)]
			valid := row != 4 && row != 8
			if valid {
				memory.Set(dictionary.Validity, row)
			} else {
				// Both out-of-range and ordinary-looking codes are meaningless
				// for NULLs, and must not become empty or non-NULL values.
				value = nil
				code = math.MaxUint32
				if row == 8 {
					code = 1
				}
			}
			id := int64(len(rows))
			dictionary.Indices = append(dictionary.Indices, code)
			batch.Int64s["id"].Values = append(batch.Int64s["id"].Values, id)
			rows = append(rows, binaryDictionarySortRow{value: value, valid: valid, id: id, batch: part, row: row})
		}
		batches = append(batches, batch)
	}
	return batches, rows
}

func compareBinaryDictionarySortRows(a, b binaryDictionarySortRow, key SortKey) int {
	if a.valid != b.valid {
		if a.valid == key.NullsLast {
			return -1
		}
		return 1
	}
	if !a.valid {
		return 0
	}
	comparison := bytes.Compare(a.value, b.value)
	if key.Descending {
		comparison = -comparison
	}
	return comparison
}

func TestBinaryDictionarySortKeyBindingAndSnapshots(t *testing.T) {
	batches, rows := binaryDictionarySortFixture(3)
	for _, typ := range []memory.DataType{memory.TypeString, memory.TypeBinary} {
		t.Run(typ.String(), func(t *testing.T) {
			schema := memory.NewSchema(memory.Field{Name: "key", Type: typ, Encoding: memory.EncodingDictionary, Nullable: true})
			for _, descending := range []bool{false, true} {
				for _, nullsLast := range []bool{false, true} {
					key := SortKey{Column: "key", Descending: descending, NullsLast: nullsLast}
					bound := make([]boundSortKey, len(batches))
					for i, batch := range batches {
						var err error
						bound[i], err = bindSortKey(batch, schema, key)
						if err != nil {
							t.Fatal(err)
						}
					}
					candidates := make([]rowCandidate, len(rows))
					for i, row := range rows {
						cell, err := snapshotCell(batches[row.batch], schema, "key", row.row)
						if err != nil {
							t.Fatal(err)
						}
						if cell.typ != typ || cell.null == row.valid || !bytes.Equal(cell.text, row.value) {
							t.Fatalf("row=%d snapshot=(%s,%v,%x) want=(%s,%v,%x)", i, cell.typ, cell.null, cell.text, typ, !row.valid, row.value)
						}
						if !row.valid && cell.text != nil {
							t.Fatalf("NULL row %d decoded a meaningless dictionary code", i)
						}
						if err := snapshotCandidateInto(&candidates[i], batches[row.batch], schema, []SortKey{key}, row.row, row.id); err != nil {
							t.Fatal(err)
						}
					}
					for i, a := range rows {
						for j, b := range rows {
							want := compareBinaryDictionarySortRows(a, b, key)
							if got := compareSortCells(candidates[i].cells[0], candidates[j].cells[0], key); got != want {
								t.Fatalf("descending=%v nullsLast=%v rows=(%d,%d) snapshot=%d want=%d", descending, nullsLast, i, j, got, want)
							}
							wantLess := want < 0 || want == 0 && a.id < b.id
							if candidateLess(candidates[i], candidates[j], []SortKey{key}) != wantLess {
								t.Fatalf("candidate rows=(%d,%d) lost value/ordinal order", i, j)
							}
							less, err := batchRowLessCandidate(batches[a.batch], schema, []SortKey{key}, a.row, a.id, candidates[j])
							if err != nil || less != wantLess {
								t.Fatalf("batch/candidate rows=(%d,%d) less=%v want=%v err=%v", i, j, less, wantLess, err)
							}
							if a.batch == b.batch {
								if got := bound[a.batch].compareRows(a.row, b.row); got != want {
									t.Fatalf("descending=%v nullsLast=%v rows=(%d,%d) bound=%d want=%d", descending, nullsLast, i, j, got, want)
								}
								cmp := batchComparator{keys: []boundSortKey{bound[a.batch]}}
								if cmp.less(a.row, b.row) != wantLess {
									t.Fatalf("bound rows=(%d,%d) lost value/ordinal order", i, j)
								}
							}
						}
					}
				}
			}
		})
	}
}

func TestExternalSortBinaryDictionaryValuesAndStableOrdinals(t *testing.T) {
	// Each input batch produces an initial run. More than sixteen batches
	// exercise comparison and payload preservation across two merge levels.
	batches, rows := binaryDictionarySortFixture(24)
	schema := memory.NewSchema(
		memory.Field{Name: "key", Type: memory.TypeBinary, Encoding: memory.EncodingDictionary, Nullable: true},
		memory.Field{Name: "id", Type: memory.TypeInt64},
	)
	input, err := inmemory.New(schema, batches...)
	if err != nil {
		t.Fatal(err)
	}
	for _, descending := range []bool{false, true} {
		for _, nullsLast := range []bool{false, true} {
			t.Run(fmt.Sprintf("descending=%v/nullsLast=%v", descending, nullsLast), func(t *testing.T) {
				key := SortKey{Column: "key", Descending: descending, NullsLast: nullsLast}
				want := append([]binaryDictionarySortRow(nil), rows...)
				sort.SliceStable(want, func(i, j int) bool { return compareBinaryDictionarySortRows(want[i], want[j], key) < 0 })
				const hard = int64(1 << 20)
				root := memory.NewEngineBudget(hard)
				query, err := root.BeginQuery(memory.BudgetOptions{
					HardLimitBytes: hard, Spill: memory.SpillOptions{Directory: t.TempDir(), MaxBytes: 16 << 20},
				})
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = query.Close() })
				ordered, err := NewSortSourceWithOptions(input, []SortKey{key}, root, SortSourceOptions{Spillable: true})
				if err != nil {
					t.Fatal(err)
				}
				ctx := memory.WithQueryBudget(context.Background(), query)
				stream, err := source.OpenBatchStream(ctx, ordered, testReadOptions())
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = stream.Close() })
				out := memory.NewBatch()
				defer out.ReleaseOwned()
				seen := 0
				for {
					ok, err := stream.Next(ctx, out)
					if err != nil {
						t.Fatal(err)
					}
					if !ok {
						break
					}
					dictionary, err := out.DictionaryString("key")
					if err != nil {
						t.Fatal(err)
					}
					ids, err := out.Int64("id")
					if err != nil {
						t.Fatal(err)
					}
					for row := 0; row < out.Rows; row++ {
						if seen >= len(want) {
							t.Fatal("sort emitted extra rows")
						}
						expected := want[seen]
						if ids.Values[row] != expected.id || dictionary.IsValid(row) != expected.valid {
							t.Fatalf("row=%d id=%d valid=%v want id=%d valid=%v", seen, ids.Values[row], dictionary.IsValid(row), expected.id, expected.valid)
						}
						if expected.valid && !bytes.Equal(dictionary.DictionaryValue(dictionary.Indices[row]), expected.value) {
							t.Fatalf("row=%d value=%x want=%x", seen, dictionary.DictionaryValue(dictionary.Indices[row]), expected.value)
						}
						seen++
					}
				}
				if err := stream.Close(); err != nil {
					t.Fatal(err)
				}
				external, ok := stream.(*externalSortStream)
				if !ok || external.mergeLevels < 2 {
					t.Fatalf("sort did not exercise hierarchical external merging: %T", stream)
				}
				if seen != len(want) || query.Used() != 0 || query.Snapshot().PeakBytes > hard {
					t.Fatalf("rows=%d want=%d budget=%+v", seen, len(want), query.Snapshot())
				}
			})
		}
	}
}

func TestBinaryDictionarySortKeepsNativeDecimalBoundary(t *testing.T) {
	decimal, err := memory.Decimal128Field("amount", true, 9, 2)
	if err != nil {
		t.Fatal(err)
	}
	input, err := inmemory.New(memory.NewSchema(
		memory.Field{Name: "key", Type: memory.TypeBinary, Encoding: memory.EncodingDictionary}, decimal,
	))
	if err != nil {
		t.Fatal(err)
	}
	keys := []SortKey{{Column: "key"}}
	for _, spillable := range []bool{false, true} {
		_, err := NewSortSourceWithOptions(input, keys, nil, SortSourceOptions{Spillable: spillable})
		if err == nil || !strings.Contains(err.Error(), "native decimal execution is not supported") {
			t.Fatalf("spillable=%v native decimal error=%v", spillable, err)
		}
	}
	if _, err := NewTopKSourceWithManager(input, 1, keys, nil); err == nil || !strings.Contains(err.Error(), "native decimal execution is not supported") {
		t.Fatalf("top-k native decimal error=%v", err)
	}
}
