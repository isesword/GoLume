package physical

import (
	"context"
	"fmt"
	"gocolumn/expr"
	"gocolumn/memory"
	"gocolumn/source"
	"gocolumn/source/inmemory"
	"sort"
	"strings"
	"testing"
)

// Replaying a stable row sequence need not reproduce native batch boundaries.
type topKRebatchSource struct {
	source.Source
	rows            int
	payload, target int64
	empty           bool
}

func (s *topKRebatchSource) Replayability() source.Replayability { return source.ReplayStable }
func (s *topKRebatchSource) NewBatchStream(ctx context.Context, opts source.ReadOptions) (source.BatchStream, error) {
	opts.BatchRows = s.rows
	opts.BatchPayloadBytes = s.payload
	opts.BatchPayloadTargetBytes = s.target
	child, err := source.OpenBatchStream(ctx, s.Source, opts)
	if err != nil {
		return nil, err
	}
	if s.empty {
		return &topKEmptyBatchStream{child: child, emitEmpty: true}, nil
	}
	return child, nil
}

type topKEmptyBatchStream struct {
	child     source.BatchStream
	emitEmpty bool
}

func (s *topKEmptyBatchStream) Next(ctx context.Context, dst *memory.Batch) (bool, error) {
	if s.emitEmpty {
		s.emitEmpty = false
		dst.ClearColumns()
		return true, nil
	}
	ok, err := s.child.Next(ctx, dst)
	if ok {
		s.emitEmpty = true
	}
	return ok, err
}
func (s *topKEmptyBatchStream) Close() error { return s.child.Close() }

func TestTopKReplayUsesGlobalOrdinalsAcrossBatchBoundaries(t *testing.T) {
	const rows = 75
	batch := memory.NewBatch()
	batch.Rows = rows
	batch.Int64s["id"] = &memory.Int64Vector{Values: make([]int64, rows)}
	batch.Int64s["key"] = &memory.Int64Vector{Values: make([]int64, rows), Validity: memory.EnsureBitmap(nil, rows)}
	batch.Strings["payload"] = &memory.StringViewVector{Offsets: []uint32{0}}
	for row := 0; row < rows; row++ {
		batch.Int64s["id"].Values[row] = int64(row)
		batch.Int64s["key"].Values[row] = int64(row % 9)
		if row%4 != 0 {
			memory.Set(batch.Int64s["key"].Validity, row)
		}
		value := fmt.Sprintf("%02d:", row) + strings.Repeat("x", 80+row)
		batch.Strings["payload"].Data = append(batch.Strings["payload"].Data, value...)
		batch.Strings["payload"].Offsets = append(batch.Strings["payload"].Offsets, uint32(len(batch.Strings["payload"].Data)))
	}
	base, err := inmemory.New(memory.NewSchema(memory.Field{Name: "id", Type: memory.TypeInt64}, memory.Field{Name: "key", Type: memory.TypeInt64, Nullable: true}, memory.Field{Name: "payload", Type: memory.TypeString}), batch)
	if err != nil {
		t.Fatal(err)
	}
	keyOnly, err := NewProjectSource(base, []expr.NamedExpr{expr.Named(expr.Col("key"), "key")}, false)
	if err != nil {
		t.Fatal(err)
	}
	want := make([]int, rows)
	for i := range want {
		want[i] = i
	}
	sort.SliceStable(want, func(i, j int) bool {
		a, b := want[i], want[j]
		an, bn := a%4 == 0, b%4 == 0
		if an != bn {
			return an
		}
		if an {
			return false
		}
		return a%9 > b%9
	})
	for _, tc := range []struct {
		name                    string
		keyRows, payloadRows    int
		emptyKeys, emptyPayload bool
	}{
		{"key_larger", 17, 2, false, false}, {"payload_larger", 1, 13, false, false}, {"empty_key_batches", 7, 3, true, false}, {"empty_payload_batches", 3, 11, false, true}, {"both_empty", 1, 1, true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			keys := &topKRebatchSource{Source: keyOnly, rows: tc.keyRows, payload: 4096, target: 2048, empty: tc.emptyKeys}
			payload := &topKRebatchSource{Source: base, rows: tc.payloadRows, payload: 512, target: 256, empty: tc.emptyPayload}
			manager := memory.NewBudgetManager(1 << 20)
			top, err := NewTopKSourceWithKeyInput(payload, keys, 32, []SortKey{{Column: "key", Descending: true}}, manager)
			if err != nil {
				t.Fatal(err)
			}
			stream, err := source.OpenBatchStream(context.Background(), top, testReadOptions())
			if err != nil {
				t.Fatal(err)
			}
			defer stream.Close()
			out := memory.NewBatch()
			if ok, err := stream.Next(context.Background(), out); !ok || err != nil {
				t.Fatalf("top-k=(%v,%v)", ok, err)
			}
			defer out.ReleaseOwned()
			if out.Rows != 32 {
				t.Fatalf("rows=%d", out.Rows)
			}
			for row := 0; row < 32; row++ {
				original := want[row]
				if got := out.Int64s["id"].Values[row]; got != int64(original) {
					t.Fatalf("winner %d id=%d want=%d", row, got, original)
				}
				if out.Int64s["key"].IsValid(row) != (original%4 != 0) {
					t.Fatalf("winner %d NULL", row)
				}
				if string(out.Strings["payload"].ValueBytes(row)) != string(batch.Strings["payload"].ValueBytes(original)) {
					t.Fatalf("winner %d payload", row)
				}
			}
			if err := stream.Close(); err != nil {
				t.Fatal(err)
			}
			if manager.Used() != 0 {
				t.Fatalf("retained top-k memory=%d", manager.Used())
			}
		})
	}
}
