package memory

import (
	"context"
	"errors"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"os"
	"reflect"
	"strings"
	"testing"
)

// Existing TestDecimal128SpillAndArrowRoundTrip separately asserts truncation,
// precision/scale corruption and valid coefficient overflow. Here NULL poison
// and the real file-store SHA-256 layer are exercised independently.
func TestN13DecimalSpillNullPoisonAndChecksum(t *testing.T) {
	ctx := context.Background()
	f, e := Decimal128Field("d", true, 1, 1)
	if e != nil {
		t.Fatal(e)
	}
	schema := NewSchema(f)
	b := NewBatch()
	b.Rows = 3
	poison := decimal128.New(-1, 0)
	b.Decimals["d"] = &Decimal128Vector{Values: []Decimal128{decimal128.FromI64(9), poison, decimal128.FromI64(-9)}, Validity: Bitmap{5}, Precision: 1, Scale: 1}
	m := NewBudgetManager(1 << 20)
	encoded, e := EncodeColumnarRun(ctx, schema, b, m)
	if e != nil {
		t.Fatal(e)
	}
	store, e := NewQuerySpillStore(SpillOptions{Directory: t.TempDir(), MaxBytes: 1 << 20})
	if e != nil {
		t.Fatal(e)
	}
	defer store.Close()
	fp := SchemaFingerprint(schema)
	h, e := store.SpillWithMetadata(ctx, SpillObject{Kind: SpillColumnarRun, Codec: ColumnarRunCodec, SchemaFingerprint: fp, Payload: encoded.Payload}, m)
	encoded.Close()
	if e != nil {
		t.Fatal(e)
	}
	restore, e := store.Restore(ctx, h, fp, m)
	if e != nil {
		t.Fatal(e)
	}
	decoded, e := DecodeColumnarRun(ctx, schema, restore, m)
	if e != nil {
		t.Fatal(e)
	}
	if !reflect.DeepEqual(decoded.Batch.Decimals["d"], b.Decimals["d"]) {
		t.Fatal("NULL poison/coefficients/metadata changed")
	}
	decoded.Close()
	restore.Close()
	before := m.Used()
	path := h.(*spillHandle).path
	data, e := os.ReadFile(path)
	if e != nil {
		t.Fatal(e)
	}
	data[len(data)-1] ^= 1
	if e = os.WriteFile(path, data, 0600); e != nil {
		t.Fatal(e)
	}
	out, e := store.Restore(ctx, h, fp, m)
	if out != nil {
		out.Close()
	}
	if !errors.Is(e, ErrSpillCorrupt) || !strings.Contains(e.Error(), "checksum") {
		t.Fatal("SHA256 not checked", e)
	}
	if m.Used() != before {
		t.Fatal("failed restore leak", before, m.Used())
	}
	if e = store.Delete(ctx, h); e != nil {
		t.Fatal(e)
	}
	if store.Used() != 0 {
		t.Fatal("disk charge retained")
	}
	if e = store.Close(); e != nil {
		t.Fatal(e)
	}
	if m.Used() != 0 {
		t.Fatal("memory retained", m.Used())
	}
}
