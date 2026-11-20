package parquet

import (
	"context"
	"errors"
	"math"
	"os"
	"testing"

	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/metadata"
	"gocolumn/expr"
	"gocolumn/memory"
)

type item24GuardHasher struct{ bytes [][]byte }

func (h *item24GuardHasher) Sum64(b []byte) uint64 {
	h.bytes = append(h.bytes, append([]byte(nil), b...))
	return 0
}
func (h *item24GuardHasher) Sum64s(v [][]byte) []uint64 {
	out := make([]uint64, len(v))
	for i, b := range v {
		out[i] = h.Sum64(b)
	}
	return out
}

type item24GuardBloom struct {
	h       item24GuardHasher
	present bool
	checks  int
}

func (f *item24GuardBloom) Hasher() metadata.Hasher { return &f.h }
func (f *item24GuardBloom) CheckHash(uint64) bool   { f.checks++; return f.present }
func (f *item24GuardBloom) Size() int64             { return 32 }

func TestItem24Uint32BloomLiteralGuards(t *testing.T) {
	for _, nullable := range []bool{false, true} {
		b := columnBinding{Name: "x", Type: memory.TypeUint32, Nullable: nullable}
		negative := truthFalse
		if nullable {
			negative |= truthNull
		}
		for _, tc := range []struct {
			name string
			e    expr.Expr
			want truthSet
		}{
			{"negative", expr.I64(-1), negative}, {"above-max", expr.I64(int64(math.MaxUint32) + 1), negative}, {"max-i64", expr.I64(math.MaxInt64), negative},
			{"u64-cast-unsupported", expr.CastTo(expr.I64(1), memory.TypeUint64), truthAny}, {"float-unsupported", expr.F64(1), truthAny}, {"column-unsupported", expr.Col("other"), truthAny}, {"null", expr.Null(memory.TypeUint32), truthNull},
		} {
			t.Run(tc.name+map[bool]string{true: "/nullable", false: "/required"}[nullable], func(t *testing.T) {
				f := &item24GuardBloom{present: true}
				if got := bloomEquality(tc.e, b, f); got != tc.want {
					t.Fatalf("truth=%b want=%b", got, tc.want)
				}
				if f.checks != 0 || len(f.h.bytes) != 0 {
					t.Fatal("unsupported/out-of-range literal hashed")
				}
			})
		}
	}
}

func TestItem24Uint32BloomThreeValuedGuards(t *testing.T) {
	b := columnBinding{Name: "x", Type: memory.TypeUint32, Nullable: true}
	eq := expr.EQ(expr.Col("x"), expr.I64(17))
	other := expr.EQ(expr.Col("other"), expr.I64(17))
	nul := expr.EQ(expr.Col("x"), expr.Null(memory.TypeUint32))
	for _, tc := range []struct {
		name     string
		e        expr.Expr
		positive bool
		want     truthSet
	}{
		{"absent", eq, false, truthFalse | truthNull}, {"possible", eq, true, truthAny},
		{"not-absent", expr.Not(eq), false, truthTrue | truthNull}, {"not-possible", expr.Not(eq), true, truthAny},
		{"absent-and-unknown", expr.And(eq, other), false, truthFalse | truthNull}, {"absent-or-unknown", expr.Or(eq, other), false, truthAny},
		{"null", nul, false, truthNull}, {"not-null", expr.Not(nul), false, truthNull},
		{"in-absent-null", expr.In(expr.Col("x"), expr.I64(17), expr.Null(memory.TypeUint32)), false, truthNull},
		{"in-unknown-u64-cast", expr.In(expr.Col("x"), expr.I64(17), expr.CastTo(expr.I64(17), memory.TypeUint64)), false, truthAny},
		{"empty-in", expr.In(expr.Col("x")), false, truthAny},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := &item24GuardBloom{present: tc.positive}
			if got := bloomTruth(context.Background(), tc.e, b, f, 0); got != tc.want {
				t.Fatalf("truth=%b want=%b", got, tc.want)
			}
		})
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	f := &item24GuardBloom{}
	if bloomTruth(ctx, eq, b, f, 0) != truthAny || f.checks != 0 {
		t.Fatal("cancel must remain unknown without membership")
	}
	if bloomTruth(context.Background(), eq, b, f, 257) != truthAny || f.checks != 0 {
		t.Fatal("depth cap must remain unknown")
	}
}

func TestItem24Uint32BloomPhysicalAndCancellationGuards(t *testing.T) {
	s := item24Fixture(t, true)
	f, e := os.Open(s.path)
	if e != nil {
		t.Fatal(e)
	}
	counted := &pagePruningIOReader{f: f}
	r, e := file.NewParquetReader(counted)
	if e != nil {
		f.Close()
		t.Fatal(e)
	}
	defer r.Close()
	// id is physically INT64. A deliberately inconsistent U32 binding must fail
	// open before any optional Bloom read, not hash this as physical INT32.
	counted.reads = nil
	wrong := []columnBinding{{Name: "id", Index: 1, Type: memory.TypeUint32}}
	if bloomGroupExcluded(context.Background(), r, 0, wrong, expr.EQ(expr.Col("id"), expr.I64(999999))) {
		t.Fatal("physical mismatch excluded group")
	}
	if len(counted.reads) != 0 {
		t.Fatal("physical mismatch read optional data")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	counted.reads = nil
	if bloomGroupExcluded(ctx, r, 0, s.bindings, expr.EQ(expr.Col("x"), expr.I64(17))) || len(counted.reads) != 0 {
		t.Fatal("pre-cancel did work/excluded")
	}
	for _, raw := range [][]byte{nil, {}, {0}, {0xff}, {0x15, 0x40, 0}} {
		if bloomHeaderValid(context.Background(), raw) {
			t.Fatalf("malformed header accepted %x", raw)
		}
	}
	if bloomHeaderValid(ctx, []byte{0}) {
		t.Fatal("canceled header accepted")
	}
}

// Faults are armed after footer parsing and target only the actual Bloom extent.
// Each subtest opens an independent reader, so cancellation/corruption cannot
// contaminate another reader's metadata or optional-reader cache.
type item24BloomFaultReader struct {
	*os.File
	offset int64
	mode   string
	cancel context.CancelFunc
	hits   int
}

func (r *item24BloomFaultReader) ReadAt(p []byte, off int64) (int, error) {
	n, e := r.File.ReadAt(p, off)
	if r.offset > 0 && off == r.offset {
		r.hits++
		switch r.mode {
		case "cancel":
			r.cancel()
		case "read-error":
			return 0, errors.New("item24 injected Bloom read error")
		case "bad-header":
			if n > 0 {
				p[0] = 0xff
			}
		}
	}
	return n, e
}
func TestItem24Uint32BloomRunningFaults(t *testing.T) {
	s := item24Fixture(t, true)
	predicate := expr.EQ(expr.Col("x"), expr.I64(2147483649))
	bindings := make(map[string]columnBinding)
	for _, b := range s.bindings {
		bindings[b.Name] = b
	}
	anchor, ok := bloomAnchor(context.Background(), predicate, bindings, 0)
	if !ok || anchor.Type != memory.TypeUint32 || anchor.Index != 0 {
		t.Fatalf("not UINT32 anchor: %+v %v", anchor, ok)
	}
	for _, mode := range []string{"cancel", "read-error", "bad-header", "bad-length", "alias-data"} {
		t.Run(mode, func(t *testing.T) {
			f, e := os.Open(s.path)
			if e != nil {
				t.Fatal(e)
			}
			input := &item24BloomFaultReader{File: f}
			r, e := file.NewParquetReader(input)
			if e != nil {
				f.Close()
				t.Fatal(e)
			}
			defer r.Close()
			// The same unmodified reader must really exclude first; no test passes just
			// because its anchor, Bloom metadata, min-row bound or membership is inert.
			if !bloomGroupExcluded(context.Background(), r, 0, s.bindings, predicate) {
				t.Fatal("normal UINT32 control did not exclude")
			}
			cc, e := r.MetaData().RowGroup(0).ColumnChunk(0)
			if e != nil {
				t.Fatal(e)
			}
			input.offset = cc.BloomFilterOffset()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			input.cancel = cancel
			switch mode {
			case "bad-length":
				v := int32(-1)
				r.MetaData().RowGroups[0].Columns[0].MetaData.BloomFilterLength = &v
			case "alias-data":
				v := cc.DataPageOffset()
				r.MetaData().RowGroups[0].Columns[0].MetaData.BloomFilterOffset = &v
			default:
				input.mode = mode
			}
			if bloomGroupExcluded(ctx, r, 0, s.bindings, predicate) {
				t.Fatal("fault excluded UINT32 group")
			}
			if mode == "bad-length" || mode == "alias-data" {
				if input.hits != 0 {
					t.Fatal("bad metadata reached optional Bloom read")
				}
			} else if input.hits == 0 {
				t.Fatal("fault never reached actual Bloom read")
			}
			if mode == "cancel" && !errors.Is(ctx.Err(), context.Canceled) {
				t.Fatal("not canceled during read")
			}
		})
	}
}
