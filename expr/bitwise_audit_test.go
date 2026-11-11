package expr

import (
	"context"
	"errors"
	"testing"

	"gocolumn/kernel"
	"gocolumn/memory"
)

// Keep cancellation deterministic: refuse the second chunk and verify no
// lane belonging to that chunk has been touched.
func TestBitwiseAuditCancellationBoundary(t *testing.T) {
	const n = 8193
	for _, mode := range []kernel.Mode{kernel.ModeScalar, kernel.ModeAuto} {
		for _, op := range []kernel.BitwiseOp{kernel.BitwiseAnd, kernel.BitwiseOr, kernel.BitwiseXor, kernel.BitwiseNot} {
			left, right := make([]uint64, n), make([]uint64, n)
			output := make([]uint64, n)
			for i := range output {
				left[i], right[i], output[i] = 7, 3, 99
			}
			checks := 0
			r := &scalarRegister{checkContext: func() error {
				checks++
				if checks == 2 {
					return context.Canceled
				}
				return nil
			}}
			err := evalBitwiseBatch(r, left, right, output, n, op, kernel.New(mode).BitwiseUint64)
			if !errors.Is(err, context.Canceled) || checks != 2 {
				t.Fatalf("error=%v checks=%d", err, checks)
			}
			for i, x := range output {
				if i >= 4096 && x != 99 {
					t.Fatalf("mode %v op %v row %d touched after cancellation", mode, op, i)
				}
			}
		}
	}
}

func TestBitwiseAuditNullPayloadAndChildErrors(t *testing.T) {
	for _, typ := range []memory.DataType{memory.TypeInt32, memory.TypeUint32, memory.TypeInt64, memory.TypeUint64} {
		schema := memory.NewSchema(memory.Field{Name: "a", Type: typ, Nullable: true})
		for _, e := range []Expr{BitNot(Col("a")), BitAnd(Col("a"), I64(7)), BitOr(I64(7), Col("a")), BitXor(Col("a"), Col("a"))} {
			p, err := Compile(schema, e)
			if err != nil {
				t.Fatal(err)
			}
			b := memory.NewBatch()
			b.Rows = 4097
			bits := make([]uint64, b.Rows)
			for i := range bits {
				bits[i] = ^uint64(0)
			}
			valid := make(memory.Bitmap, memory.WordsFor(b.Rows))
			for i := 0; i < b.Rows; i += 2 {
				memory.Set(valid, i)
			}
			bitReviewPut(b, "a", typ, bits, valid)
			w, err := NewWorkspace(context.Background(), p, memory.NewBudgetManager(0))
			if err != nil {
				t.Fatal(err)
			}
			for _, mode := range []kernel.Mode{kernel.ModeScalar, kernel.ModeAuto} {
				got, err := w.Evaluate(context.Background(), b, kernel.New(mode))
				if err != nil {
					t.Fatal(err)
				}
				for i := 1; i < b.Rows; i += 2 {
					x, v := bitReviewRead(got[0], i)
					if v || x != 0 {
						t.Fatalf("type=%v mode=%v row=%d invalid payload=%x valid=%v", typ, mode, i, x, v)
					}
				}
			}
			w.Close()
		}
	}
	b, err := Bind(memory.NewSchema(memory.Field{Name: "a", Type: memory.TypeInt64}), BitNot(CastTo(Col("a"), memory.TypeUint64)))
	if err != nil || !b.MayError {
		t.Fatalf("fallible child metadata lost: bound=%+v error=%v", b, err)
	}
}
