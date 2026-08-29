package inmemory

import (
	"testing"

	"gocolumn/memory"
)

func TestNewValidatesListStringSchemaOffsetsAndNullability(t *testing.T) {
	valid := memory.NewBatch()
	valid.StringLists["tags"] = &memory.StringListVector{
		Offsets:  []uint32{0, 1},
		Values:   &memory.StringViewVector{Offsets: []uint32{0, 1}, Data: []byte("x"), Encoding: memory.EncodingView},
		Encoding: memory.EncodingView,
	}
	valid.Reset(1)
	if _, err := New(memory.NewSchema(memory.ListStringField("tags", false, false)), valid); err != nil {
		t.Fatal(err)
	}

	malformed, _ := memory.CloneBatch(valid)
	malformed.StringLists["tags"].Offsets[1] = 2
	if _, err := New(memory.NewSchema(memory.ListStringField("tags", false, false)), malformed); err == nil {
		t.Fatal("malformed list offsets should be rejected")
	}

	nullChild, _ := memory.CloneBatch(valid)
	nullChild.StringLists["tags"].Values.Validity = memory.EnsureBitmap(nil, 1)
	if _, err := New(memory.NewSchema(memory.ListStringField("tags", false, false)), nullChild); err == nil {
		t.Fatal("NULL child in non-nullable item field should be rejected")
	}

	nullParent, _ := memory.CloneBatch(valid)
	nullParent.StringLists["tags"].Validity = memory.EnsureBitmap(nil, 1)
	if _, err := New(memory.NewSchema(memory.ListStringField("tags", false, false)), nullParent); err == nil {
		t.Fatal("NULL parent in non-nullable list field should be rejected")
	}
}
