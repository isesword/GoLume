package memory

import "testing"

func TestCloneBatchOwnsStringBuffers(t *testing.T) {
	b := NewBatch()
	b.Strings["s"] = &StringViewVector{Offsets: []uint32{0, 1}, Data: []byte("a"), Encoding: EncodingView}
	b.Reset(1)
	c, err := CloneBatch(b)
	if err != nil {
		t.Fatal(err)
	}
	b.Strings["s"].Data[0] = 'z'
	if string(c.Strings["s"].Data) != "a" {
		t.Fatalf("clone shares string data")
	}
}
