package optimizer

import (
	"context"
	"encoding/binary"
	"errors"
	"gocolumn/expr"
	"gocolumn/kernel"
	"gocolumn/memory"
	"reflect"
	"testing"
)

func civilRewriteResolver(t *testing.T, offset int32) expr.CivilTimeResolver {
	t.Helper()
	b := make([]byte, 44)
	copy(b, "TZif")
	binary.BigEndian.PutUint32(b[36:], 1)
	binary.BigEndian.PutUint32(b[40:], 4)
	b = binary.BigEndian.AppendUint32(b, uint32(offset))
	b = append(b, 0, 0, 'X', 'X', 'X', 0)
	z, e := expr.TimeZoneFromTZData("same-label", b)
	if e != nil {
		t.Fatal(e)
	}
	r, e := expr.NewCivilTimeResolver(context.Background(), z, expr.CivilResolveOptions{Fold: expr.FoldReject, Gap: expr.GapReject})
	if e != nil {
		t.Fatal(e)
	}
	return r
}
func TestCivilTimestampRewritePreservesResolver(t *testing.T) {
	schema := memory.NewSchema(memory.Field{Name: "renamed", Type: memory.TypeString, Nullable: true})
	b := memory.NewBatch()
	b.Reset(1)
	b.Strings["renamed"] = &memory.StringViewVector{Offsets: []uint32{0, 19}, Data: []byte("1970-01-01T00:00:00")}
	for _, offset := range []int32{0, 3600} {
		for _, tolerant := range []bool{false, true} {
			r := civilRewriteResolver(t, offset)
			var original expr.Expr = expr.ParseCivilTimestamp(expr.Col("original"), r)
			if tolerant {
				original = expr.TryParseCivilTimestamp(expr.Col("original"), r)
			}
			before := original.String()
			rewritten, ok := renamePredicateColumns(original, map[string]string{"original": "renamed"})
			if !ok {
				t.Fatal("rename refused")
			}
			if original.String() != before || !reflect.DeepEqual(expr.Columns(original), []string{"original"}) || !reflect.DeepEqual(expr.Columns(rewritten), []string{"renamed"}) {
				t.Fatal("dependency/mutation")
			}
			if _, ok := renamePredicateColumns(original, map[string]string{}); ok {
				t.Fatal("missing rename admitted")
			}
			p, e := expr.CompileExpressions(schema, []expr.NamedExpr{expr.Named(rewritten, "out")})
			if e != nil {
				t.Fatal(e)
			}
			m := memory.NewBudgetManager(0)
			w, e := expr.NewWorkspace(context.Background(), p, m)
			if e != nil {
				t.Fatal(e)
			}
			out, e := w.Evaluate(context.Background(), b, kernel.New(kernel.ModeScalar))
			if e != nil || out[0].Int64.Values[0] != -int64(offset)*1e9 {
				t.Fatal(offset, e, out)
			}
			b.Strings["renamed"] = &memory.StringViewVector{Offsets: []uint32{0, 3}, Data: []byte("bad")}
			out, e = w.Evaluate(context.Background(), b, kernel.New(kernel.ModeScalar))
			if tolerant {
				if e != nil || out[0].Int64.IsValid(0) {
					t.Fatal("try bit lost", e)
				}
			} else if !errors.Is(e, expr.ErrCivilSyntax) {
				t.Fatal("strict bit lost", e)
			}
			w.Close()
			if m.Used() != 0 {
				t.Fatal("leak")
			}
			b.Strings["renamed"] = &memory.StringViewVector{Offsets: []uint32{0, 19}, Data: []byte("1970-01-01T00:00:00")}
		}
	}
}

func TestCivilTimestampMayErrorContract(t *testing.T) {
	r := civilRewriteResolver(t, 0)
	s := memory.NewSchema(memory.Field{Name: "x", Type: memory.TypeString})
	for _, tc := range []struct {
		e    expr.Expr
		want bool
	}{{expr.ParseCivilTimestamp(expr.Col("x"), r), true}, {expr.TryParseCivilTimestamp(expr.Col("x"), r), false}, {expr.TryParseCivilTimestamp(expr.CastTo(expr.CastTo(expr.Col("x"), memory.TypeInt64), memory.TypeString), r), true}} {
		b, e := expr.Bind(s, tc.e)
		if e != nil {
			t.Fatal(e)
		}
		if b.MayError != tc.want {
			t.Fatalf("%s MayError=%v want %v", tc.e, b.MayError, tc.want)
		}
	}
}
