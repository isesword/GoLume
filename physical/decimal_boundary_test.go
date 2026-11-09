package physical

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"gocolumn/expr"
	"gocolumn/join"
	"gocolumn/memory"
	"gocolumn/source"
	"gocolumn/source/inmemory"
)

func TestNativeDecimalJoinContextValidation(t *testing.T) {
	field, err := memory.Decimal128Field("amount", true, 9, 2)
	if err != nil {
		t.Fatal(err)
	}
	base, err := inmemory.New(memory.NewSchema(field))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := NewJoinSourceWithOptions(nil, base, base, join.Inner, nil, "_right", nil, JoinSourceOptions{}); err == nil || !strings.Contains(err.Error(), "context is nil") {
		t.Fatalf("nil context error=%v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := NewJoinSourceWithOptions(ctx, base, base, join.Inner, nil, "_right", nil, JoinSourceOptions{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled context error=%v", err)
	}
	if _, err := NewJoinSourceWithOptions(context.Background(), base, nil, join.Inner, nil, "_right", nil, JoinSourceOptions{}); err == nil || !strings.Contains(err.Error(), "requires left and right sources") {
		t.Fatalf("nil source error=%v", err)
	}
}

func TestNativeDecimalExecutionBoundaries(t *testing.T) {
	field, _ := memory.Decimal128Field("amount", true, 9, 2)
	schema := memory.NewSchema(field, memory.Field{Name: "id", Type: memory.TypeInt64})
	batch := memory.NewBatch()
	batch.Rows = 3
	batch.Decimals["amount"] = &memory.Decimal128Vector{Values: []memory.Decimal128{decimal128.FromI64(123), {}, decimal128.FromI64(-123)}, Validity: memory.Bitmap{5}, Precision: 9, Scale: 2}
	batch.Int64s["id"] = &memory.Int64Vector{Values: []int64{1, 2, 3}}
	base, err := inmemory.New(schema, batch)
	if err != nil {
		t.Fatal(err)
	}
	for name, build := range map[string]func() error{
		"comparison":    func() error { _, e := expr.Bind(schema, expr.EQ(expr.Col("amount"), expr.Col("amount"))); return e },
		"sort payload":  func() error { _, e := NewSortSource(base, []SortKey{{Column: "id"}}, nil); return e },
		"top-k payload": func() error { _, e := NewTopKSourceWithManager(base, 1, []SortKey{{Column: "id"}}, nil); return e },
		"join payload": func() error {
			_, e := NewJoinSourceWithOptions(context.Background(), base, base, join.Inner, nil, "_right", nil, JoinSourceOptions{})
			return e
		},
		"window payload": func() error {
			_, e := NewWindowSourceWithOptions(base, nil, nil, nil, nil, nil, WindowSourceOptions{})
			return e
		},
	} {
		t.Run(name, func(t *testing.T) {
			err := build()
			if err == nil || !strings.Contains(err.Error(), "native decimal") || !strings.Contains(err.Error(), "not supported") {
				t.Fatalf("boundary error=%v", err)
			}
		})
	}
	// Explicit simple transport paths may not drop the decimal map.
	dropped, err := NewDropColumnsSource(base, []string{"id"})
	if err != nil {
		t.Fatal(err)
	}
	limited, err := NewLimitSource(dropped, 2)
	if err != nil {
		t.Fatal(err)
	}
	st, err := source.OpenBatchStream(context.Background(), limited, testReadOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	out := memory.NewBatch()
	if ok, err := st.Next(context.Background(), out); !ok || err != nil {
		t.Fatalf("next=%v error=%v", ok, err)
	}
	if out.Rows != 2 || len(out.Int64s) != 0 || len(out.Decimals) != 1 || out.Decimals["amount"].Values[0] != decimal128.FromI64(123) || out.Decimals["amount"].IsValid(1) {
		t.Fatal("limit/drop lost decimal transport")
	}
}
