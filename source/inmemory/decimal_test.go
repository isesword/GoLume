package inmemory

import (
	"context"
	"gocolumn/kernel"
	"gocolumn/memory"
	"gocolumn/source"
	"strings"
	"testing"
)

func TestNativeDecimalSchemaContract(t *testing.T) {
	field, _ := memory.Decimal128Field("amount", true, 9, 2)
	vector := &memory.Decimal128Vector{Values: []memory.Decimal128{{}}, Precision: 9, Scale: 2}
	for _, tc := range []struct {
		name   string
		field  memory.Field
		vector *memory.Decimal128Vector
	}{
		{"opaque metadata with native values", memory.Field{Name: "amount", Type: memory.TypeDecimal}, vector},
		{"wrong logical type", memory.Field{Name: "amount", Type: memory.TypeInt64}, vector},
		{"nil native binding", memory.Field{Name: "amount", Type: memory.TypeDecimal}, nil},
		{"scale mismatch", field, &memory.Decimal128Vector{Values: []memory.Decimal128{{}}, Precision: 9, Scale: 0}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := memory.NewBatch()
			b.Rows = 1
			b.Decimals["amount"] = tc.vector
			if _, err := New(memory.NewSchema(tc.field), b); err == nil {
				t.Fatal("invalid decimal schema/storage pair accepted")
			}
		})
	}
	invalid := field
	invalid.DecimalPrecision = 39
	if _, err := New(memory.NewSchema(invalid)); err == nil {
		t.Fatal("invalid empty native decimal schema accepted")
	}
	// Legacy Arrow decimal storage is still deliberately opaque.
	b := memory.NewBatch()
	b.Rows = 1
	b.Opaque["amount"] = &memory.OpaqueVector{Rows: 1, Type: memory.TypeDecimal, Value: struct{}{}}
	if _, err := New(memory.NewSchema(memory.Field{Name: "amount", Type: memory.TypeDecimal, ArrowType: "decimal128(9, 2)"}), b); err != nil {
		t.Fatalf("legacy opaque decimal changed: %v", err)
	}
}

func TestNativeDecimalBindingUsesDeclaredType(t *testing.T) {
	field, _ := memory.Decimal128Field("amount", true, 9, 2)
	batch := memory.NewBatch()
	batch.Rows = 1
	vector := &memory.Decimal128Vector{Values: []memory.Decimal128{{}}, Precision: 9, Scale: 2}
	batch.Decimals["amount"] = vector
	// Reused batches may carry other storage under the same name. A declared
	// native decimal must bind its validated vector, never an integer fallback.
	batch.Int64s["amount"] = &memory.Int64Vector{Values: []int64{999}}
	src, err := New(memory.NewSchema(field), batch)
	if err != nil {
		t.Fatal(err)
	}
	st, err := src.NewBatchStream(context.Background(), source.ReadOptions{Kernels: kernel.New(kernel.ModeScalar)})
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	out := memory.NewBatch()
	if ok, err := st.Next(context.Background(), out); !ok || err != nil {
		t.Fatalf("next=%v err=%v", ok, err)
	}
	if len(out.Int64s) != 0 || len(out.Decimals) != 1 || out.Decimals["amount"].Scale != 2 || !out.Decimals["amount"].IsValid(0) {
		t.Fatal("native decimal type was reinterpreted")
	}
}

func TestNativeDecimalNestedSchemasRejected(t *testing.T) {
	decimal, err := memory.Decimal128Field("amount", true, 38, 30)
	if err != nil {
		t.Fatal(err)
	}
	element := decimal
	element.Name = "item"
	list := memory.Field{Name: "amounts", Type: memory.TypeList, Nullable: true, Encoding: memory.EncodingView, Children: []memory.Field{element}}
	for _, tc := range []struct {
		name  string
		field memory.Field
		want  string
	}{
		{"struct decimal", memory.StructField("record", true, decimal), "unsupported type decimal"},
		{"list decimal", list, "requires string child"},
		{"struct list decimal", memory.StructField("record", true, list), "requires string child"},
		{"recursive struct decimal", memory.StructField("outer", true, memory.StructField("inner", true, decimal)), "unsupported type struct"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			schema := memory.NewSchema(tc.field)
			// Reject the schema before payload validation, whether no batch, an
			// empty batch, or a nonempty batch would otherwise be examined.
			for _, rows := range []int{-1, 0, 1} {
				var batches []*memory.Batch
				if rows >= 0 {
					batch := memory.NewBatch()
					batch.Rows = rows
					batches = []*memory.Batch{batch}
				}
				if _, err := New(schema, batches...); err == nil || !strings.Contains(err.Error(), tc.want) {
					t.Fatalf("rows=%d: unsupported nested schema returned %v, want %q", rows, err, tc.want)
				}
			}
			if _, err := memory.ArrowSchema(schema); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("Arrow accepted unsupported nested schema: %v, want %q", err, tc.want)
			}
		})
	}
	// These bounded nested shapes remain valid for an empty source and for
	// Arrow export; the preflight must not reject supported primitive children.
	for _, field := range []memory.Field{
		memory.ListStringField("tags", true, true),
		memory.StructField("record", true, memory.Field{Name: "count", Type: memory.TypeInt64}, memory.ListStringField("tags", true, true)),
	} {
		schema := memory.NewSchema(field)
		if _, err := New(schema); err != nil {
			t.Fatalf("supported empty source %q: %v", field.Name, err)
		}
		if _, err := memory.ArrowSchema(schema); err != nil {
			t.Fatalf("supported Arrow schema %q: %v", field.Name, err)
		}
	}
}
