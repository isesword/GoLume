package inmemory

import (
	"testing"

	"gocolumn/memory"
)

func TestNewValidatesListInt64SchemaOffsetsAndNullability(t *testing.T) {
	valid := memory.NewBatch()
	valid.Int64Lists["tags"] = &memory.Int64ListVector{
		Offsets:  []uint32{0, 1},
		Values:   &memory.Int64Vector{Values: []int64{42}, Encoding: memory.EncodingView},
		Encoding: memory.EncodingView,
	}
	valid.Reset(1)
	if _, err := New(memory.NewSchema(memory.ListInt64Field("tags", false, false)), valid); err != nil {
		t.Fatal(err)
	}

	malformed, _ := memory.CloneBatch(valid)
	malformed.Int64Lists["tags"].Offsets[1] = 2
	if _, err := New(memory.NewSchema(memory.ListInt64Field("tags", false, false)), malformed); err == nil {
		t.Fatal("malformed list offsets should be rejected")
	}

	nullChild, _ := memory.CloneBatch(valid)
	nullChild.Int64Lists["tags"].Values.Validity = memory.EnsureBitmap(nil, 1)
	if _, err := New(memory.NewSchema(memory.ListInt64Field("tags", false, false)), nullChild); err == nil {
		t.Fatal("NULL child in non-nullable item field should be rejected")
	}

	nullParent, _ := memory.CloneBatch(valid)
	nullParent.Int64Lists["tags"].Validity = memory.EnsureBitmap(nil, 1)
	if _, err := New(memory.NewSchema(memory.ListInt64Field("tags", false, false)), nullParent); err == nil {
		t.Fatal("NULL parent in non-nullable list field should be rejected")
	}
}
