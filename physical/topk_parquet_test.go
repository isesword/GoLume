package physical

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"

	arrowmem "github.com/apache/arrow-go/v18/arrow/memory"
	arrowparquet "github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"gocolumn/memory"
	"gocolumn/source"
	parquetSource "gocolumn/source/parquet"
)

// Shared deterministic fixture: duplicate and nullable keys, nullable strings,
// and a wide payload make whole-input retention exceed the regression budget.
func topKSnapshotFixture(rows int) (memory.Schema, *memory.Batch) {
	schema := memory.NewSchema(
		memory.Field{Name: "id", Type: memory.TypeInt64},
		memory.Field{Name: "score", Type: memory.TypeInt64, Nullable: true},
		memory.Field{Name: "tag", Type: memory.TypeString, Encoding: memory.EncodingView, Nullable: true},
		memory.Field{Name: "payload", Type: memory.TypeString, Encoding: memory.EncodingView},
	)
	batch := memory.NewBatch()
	ids := &memory.Int64Vector{Values: make([]int64, rows)}
	scores := &memory.Int64Vector{Values: make([]int64, rows), Validity: make(memory.Bitmap, (rows+63)/64)}
	tags := &memory.StringViewVector{Offsets: []uint32{0}, Validity: make(memory.Bitmap, (rows+63)/64)}
	payload := &memory.StringViewVector{Offsets: []uint32{0}}
	for i := 0; i < rows; i++ {
		ids.Values[i] = int64(i)
		scores.Values[i] = int64((i * 7919) % 97)
		if i%19 != 0 {
			memory.Set(scores.Validity, i)
		}
		if i%23 != 0 {
			memory.Set(tags.Validity, i)
			tags.Data = append(tags.Data, fmt.Sprintf("tag-%02d", (i*17)%11)...)
		}
		tags.Offsets = append(tags.Offsets, uint32(len(tags.Data)))
		payload.Data = append(payload.Data, fmt.Sprintf("row-%08d:%s", i, strings.Repeat("payload-", 24))...)
		payload.Offsets = append(payload.Offsets, uint32(len(payload.Data)))
	}
	batch.Int64s["id"], batch.Int64s["score"] = ids, scores
	batch.Strings["tag"], batch.Strings["payload"] = tags, payload
	batch.Reset(rows)
	return schema, batch
}

func writeTopKSnapshotParquet(tb testing.TB, schema memory.Schema, batch *memory.Batch) *parquetSource.Source {
	tb.Helper()
	path := filepath.Join(tb.TempDir(), "topk.parquet")
	f, err := os.Create(path)
	if err != nil {
		tb.Fatal(err)
	}
	as, err := memory.ArrowSchema(schema)
	if err != nil {
		tb.Fatal(err)
	}
	writer, err := pqarrow.NewFileWriter(as, f, arrowparquet.NewWriterProperties(arrowparquet.WithMaxRowGroupLength(1024)), pqarrow.DefaultWriterProps())
	if err != nil {
		tb.Fatal(err)
	}
	for offset := 0; offset < batch.Rows; offset += 1024 {
		n := min(1024, batch.Rows-offset)
		record, err := memory.ArrowRecordRange(arrowmem.NewGoAllocator(), schema, batch, offset, n)
		if err != nil {
			tb.Fatal(err)
		}
		err = writer.Write(record)
		record.Release()
		if err != nil {
			tb.Fatal(err)
		}
	}
	if err := writer.Close(); err != nil {
		tb.Fatal(err)
	}
	src, err := parquetSource.OpenFile(path)
	if err != nil {
		tb.Fatal(err)
	}
	if got := source.ReplayabilityOf(src); got != source.ReplayUnknown {
		tb.Fatalf("Parquet replayability=%v, want unknown", got)
	}
	return src
}

func TestTopKParquetBoundedSnapshots(t *testing.T) {
	const rows, k, budget = 32768, 37, 256 << 10
	schema, input := topKSnapshotFixture(rows)
	defer input.ReleaseOwned()
	base := writeTopKSnapshotParquet(t, schema, input)
	if !reflect.DeepEqual(base.Schema().Fields, schema.Fields) {
		t.Fatalf("Parquet schema=%v, want %v", base.Schema().Fields, schema.Fields)
	}
	for _, tc := range []struct {
		name string
		key  SortKey
	}{
		{"numeric_nulls_first", SortKey{Column: "score"}},
		{"numeric_desc_nulls_last", SortKey{Column: "score", Descending: true, NullsLast: true}},
		{"string_nulls_last", SortKey{Column: "tag", NullsLast: true}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			want := make([]int, rows)
			for i := range want {
				want[i] = i
			}
			sort.SliceStable(want, func(a, b int) bool {
				i, j := want[a], want[b]
				var vi, vj bool
				var cmp int
				if tc.key.Column == "score" {
					v := input.Int64s["score"]
					vi, vj = v.IsValid(i), v.IsValid(j)
					if v.Values[i] < v.Values[j] {
						cmp = -1
					} else if v.Values[i] > v.Values[j] {
						cmp = 1
					}
				} else {
					v := input.Strings["tag"]
					vi, vj = v.IsValid(i), v.IsValid(j)
					cmp = strings.Compare(string(v.ValueBytes(i)), string(v.ValueBytes(j)))
				}
				if vi != vj {
					if tc.key.NullsLast {
						return vi
					}
					return !vi
				}
				if !vi {
					return false
				}
				if tc.key.Descending {
					return cmp > 0
				}
				return cmp < 0
			})
			for _, earlyClose := range []bool{false, true} {
				manager := memory.NewBudgetManager(budget)
				top, err := NewTopKSourceWithManager(base, k, []SortKey{tc.key}, manager)
				if err != nil {
					t.Fatal(err)
				}
				if !reflect.DeepEqual(top.Schema().Fields, schema.Fields) {
					t.Fatal("top-k changed schema")
				}
				opts := testReadOptions()
				opts.BatchRows = 113
				opts.BatchBytes = 64 << 10
				stream, err := source.OpenBatchStream(context.Background(), top, opts)
				if err != nil {
					t.Fatal(err)
				}
				dst := memory.NewBatch()
				got := 0
				for {
					ok, err := stream.Next(context.Background(), dst)
					if err != nil {
						_ = stream.Close()
						t.Fatal(err)
					}
					if !ok {
						break
					}
					if manager.Used() <= 0 || manager.Used() > budget {
						t.Fatalf("live reservation=%d", manager.Used())
					}
					for row := 0; row < dst.Rows; row++ {
						if got >= k {
							t.Fatal("too many rows")
						}
						id := want[got]
						if dst.Int64s["id"].Values[row] != int64(id) {
							t.Fatalf("row %d id=%d want %d", got, dst.Int64s["id"].Values[row], id)
						}
						score := dst.Int64s["score"]
						if score.IsValid(row) != input.Int64s["score"].IsValid(id) || (score.IsValid(row) && score.Values[row] != input.Int64s["score"].Values[id]) {
							t.Fatalf("score mismatch at %d", got)
						}
						for _, name := range []string{"tag", "payload"} {
							v := dst.Strings[name]
							original := input.Strings[name]
							if v.IsValid(row) != original.IsValid(id) || (v.IsValid(row) && string(v.ValueBytes(row)) != string(original.ValueBytes(id))) {
								t.Fatalf("%s mismatch at %d", name, got)
							}
						}
						got++
					}
					dst.ReleaseOwned()
					if earlyClose {
						break
					}
				}
				if !earlyClose {
					if got != k {
						t.Fatalf("rows=%d want %d", got, k)
					}
					if manager.Used() != 0 {
						t.Fatalf("reservation after exhaustion=%d", manager.Used())
					}
				}
				if err := stream.Close(); err != nil {
					t.Fatal(err)
				}
				dst.ReleaseOwned()
				if manager.Used() != 0 {
					t.Fatalf("reservation after Close=%d", manager.Used())
				}
			}
		})
	}
}

func TestTopKParquetSharedQueryBudget(t *testing.T) {
	const rows, k, hard = 32768, 37, 1 << 20
	schema, input := topKSnapshotFixture(rows)
	defer input.ReleaseOwned()
	base := writeTopKSnapshotParquet(t, schema, input)
	root := memory.NewEngineBudget(hard)
	query, err := root.BeginQuery(memory.BudgetOptions{HardLimitBytes: hard})
	if err != nil {
		t.Fatal(err)
	}
	defer query.Close()
	ctx := memory.WithQueryBudget(context.Background(), query)
	top, err := NewTopKSourceWithManager(base, k, []SortKey{{Column: "score", NullsLast: false}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	opts := testReadOptions()
	opts.BatchRows = 113
	opts.BatchBytes = 64 << 10
	stream, err := source.OpenBatchStream(ctx, top, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	dst := memory.NewBatch()
	defer dst.ClearColumns()
	count := 0
	for {
		ok, err := stream.Next(ctx, dst)
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			break
		}
		if query.Used() <= 0 || query.Used() > hard || root.Used() != query.Used() {
			t.Fatalf("live query=%d root=%d", query.Used(), root.Used())
		}
		for row := 0; row < dst.Rows; row++ {
			if got, want := dst.Int64s["id"].Values[row], int64(count*19); got != want {
				t.Fatalf("stable NULL winner %d: id=%d want=%d", count, got, want)
			}
			if dst.Int64s["score"].IsValid(row) {
				t.Fatal("NULL-first result lost NULL")
			}
			count++
		}
		dst.ReleaseOwned()
	}
	if count != k {
		t.Fatalf("rows=%d want=%d", count, k)
	}
	snapshot := query.Snapshot()
	if snapshot.PeakBytes <= 0 || snapshot.PeakBytes > hard {
		t.Fatalf("query peak=%d hard=%d", snapshot.PeakBytes, hard)
	}
	if query.Used() != 0 || root.Used() != 0 {
		t.Fatalf("after EOF query=%d root=%d", query.Used(), root.Used())
	}
	if err := stream.Close(); err != nil {
		t.Fatal(err)
	}
	if query.Used() != 0 || root.Used() != 0 {
		t.Fatalf("after Close query=%d root=%d", query.Used(), root.Used())
	}
	t.Logf("shared query peak %d bytes for %d rows, K=%d, hard=%d", snapshot.PeakBytes, rows, k, hard)
}
