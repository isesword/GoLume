package logical

import (
	"gocolumn/join"
	"gocolumn/memory"
	"gocolumn/source/inmemory"
	"testing"
)

func TestJoinSchemaOuterNullabilityAndManyKeys(t *testing.T) {
	fields := []memory.Field{{Name: "a", Type: memory.TypeString}, {Name: "b", Type: memory.TypeInt64}, {Name: "c", Type: memory.TypeUint32}}
	src, err := inmemory.New(memory.NewSchema(fields...))
	if err != nil {
		t.Fatal(err)
	}
	for typ := join.Inner; typ <= join.Full; typ++ {
		schema, err := InferSchema(&Join{Left: &Scan{Source: src}, Right: &Scan{Source: src}, Type: typ, Keys: []JoinKey{{"a", "a"}, {"b", "b"}, {"c", "c"}}})
		if err != nil {
			t.Fatal(typ, err)
		}
		for _, field := range fields {
			got, _ := schema.Field(field.Name)
			want := typ == join.Right || typ == join.Full
			if got.Nullable != want {
				t.Fatalf("%s left %s nullable=%t want=%t", typ, field.Name, got.Nullable, want)
			}
			got, ok := schema.Field(field.Name + "_right")
			if typ == join.Semi || typ == join.Anti {
				if ok {
					t.Fatal("semi/anti right output")
				}
				continue
			}
			want = typ == join.Left || typ == join.Full
			if !ok || got.Nullable != want {
				t.Fatalf("%s right %s nullable=%t want=%t", typ, field.Name, got.Nullable, want)
			}
		}
	}
}
