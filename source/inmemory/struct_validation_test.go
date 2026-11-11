package inmemory

import (
	"gocolumn/memory"
	"strings"
	"testing"
)

func TestNewRejectsMalformedStructBeforeStreamRead(t *testing.T) {
	for _, kind := range []string{"short-values", "empty-validity", "bad-string-offsets"} {
		t.Run(kind, func(t *testing.T) {
			child := memory.Field{Name: "value", Type: memory.TypeInt64, Nullable: true}
			values := memory.NewBatch()
			values.Rows = 2
			values.Int64s["value"] = &memory.Int64Vector{Values: []int64{1, 2}}
			switch kind {
			case "short-values":
				values.Int64s["value"].Values = values.Int64s["value"].Values[:1]
			case "empty-validity":
				values.Int64s["value"].Validity = memory.Bitmap{}
			case "bad-string-offsets":
				child.Type = memory.TypeString
				delete(values.Int64s, "value")
				values.Strings["value"] = &memory.StringViewVector{Offsets: []uint32{0, 2, 1}, Data: []byte("ab")}
			}
			f := memory.StructField("record", true, child)
			b := memory.NewBatch()
			b.Rows = 2
			b.Structs["record"] = &memory.StructVector{Rows: 2, Fields: f.Children, Values: values}
			defer func() {
				if p := recover(); p != nil {
					t.Fatalf("New panicked: %v", p)
				}
			}()
			src, err := New(memory.NewSchema(f), b)
			if err == nil || src != nil {
				t.Fatalf("malformed nested input accepted: source=%v err=%v", src, err)
			}
			if !strings.Contains(err.Error(), "record") || !strings.Contains(err.Error(), "value") {
				t.Fatalf("missing error path: %v", err)
			}
		})
	}
}
