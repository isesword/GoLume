package engine

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"hash/fnv"
	"math"
	"reflect"
	"testing"
	"unsafe"

	"gocolumn/memory"
)

// This oracle deliberately does not call the encoder's width, float, visitor,
// or hash helpers, so a shared implementation mistake cannot mask a regression.
func TestCanonicalWorkspaceIndependentBytes(t *testing.T) {
	batch := memory.NewBatch()
	batch.Rows = 5
	validity := memory.Bitmap{15}
	batch.Int8s["x"] = &memory.Int8Vector{Values: []int8{-128, -128, -128, -128, 0}, Validity: validity}
	batch.Int16s["x"] = &memory.Int16Vector{Values: []int16{-32768, -32768, -32768, -32768, 0}, Validity: validity}
	batch.Int32s["x"] = &memory.Int32Vector{Values: []int32{math.MinInt32, math.MinInt32, math.MinInt32, math.MinInt32, 0}, Validity: validity}
	batch.Int64s["x"] = &memory.Int64Vector{Values: []int64{math.MinInt64, math.MinInt64, math.MinInt64, math.MinInt64, 0}, Validity: validity}
	batch.Uint8s["x"] = &memory.Uint8Vector{Values: []uint8{255, 255, 255, 255, 0}, Validity: validity}
	batch.Uint16s["x"] = &memory.Uint16Vector{Values: []uint16{65535, 65535, 65535, 65535, 0}, Validity: validity}
	batch.Uint32s["x"] = &memory.Uint32Vector{Values: []uint32{math.MaxUint32, math.MaxUint32, math.MaxUint32, math.MaxUint32, 0}, Validity: validity}
	batch.Uint64s["x"] = &memory.Uint64Vector{Values: []uint64{math.MaxUint64, math.MaxUint64, math.MaxUint64, math.MaxUint64, 0}, Validity: validity}
	batch.Uint32s["bool"] = &memory.Uint32Vector{Values: []uint32{1, 1, 1, 1, 99}, Validity: validity}
	batch.Float32s["x"] = &memory.Float32Vector{Values: []float32{0, math.Float32frombits(0x80000000), math.Float32frombits(0x7f800001), math.Float32frombits(0xffc12345), 0}, Validity: validity}
	batch.Float64s["x"] = &memory.Float64Vector{Values: []float64{0, math.Copysign(0, -1), math.Float64frombits(0x7ff0000000000001), math.Float64frombits(0xfff8123456789012), 0}, Validity: validity}
	batch.Strings["x"] = &memory.StringViewVector{Offsets: []uint32{0, 3, 6, 9, 12, 12}, Data: []byte("a\x00ba\x00ba\x00ba\x00b"), Validity: validity}
	cases := []struct {
		typ   memory.DataType
		width int
		bits  uint64
	}{
		{memory.TypeInt8, 1, 128}, {memory.TypeInt16, 2, 32768}, {memory.TypeInt32, 4, 1 << 31}, {memory.TypeInt64, 8, 1 << 63},
		{memory.TypeUint8, 1, 255}, {memory.TypeUint16, 2, 65535}, {memory.TypeUint32, 4, math.MaxUint32}, {memory.TypeUint64, 8, math.MaxUint64},
		{memory.TypeBool, 1, 1}, {memory.TypeFloat32, 4, 0}, {memory.TypeFloat64, 8, 0},
		{memory.TypeDate32, 4, 1 << 31}, {memory.TypeTime32, 4, 1 << 31}, {memory.TypeDate64, 8, 1 << 63},
		{memory.TypeTimestamp, 8, 1 << 63}, {memory.TypeTime64, 8, 1 << 63}, {memory.TypeDuration, 8, 1 << 63},
		{memory.TypeString, 3, 0}, {memory.TypeBinary, 3, 0},
	}
	fields := make([]memory.Field, len(cases))
	for i, c := range cases {
		fields[i] = memory.Field{Name: "x", Type: c.typ}
		if c.typ == memory.TypeBool {
			fields[i].Name = "bool"
		}
	}
	var workspace canonicalGroupWorkspace
	manager := memory.NewBudgetManager(0)
	reserve := func(n int64) (memory.Reservation, error) { return manager.Reserve(context.Background(), n) }
	if err := workspace.ensure(fields, reserve); err != nil {
		t.Fatal(err)
	}
	defer workspace.close()
	for _, dictionary := range []bool{false, true} {
		if dictionary {
			batch.Strings = nil
			batch.DictStrings["x"] = &memory.DictionaryStringVector{Indices: []uint32{1, 0, 1, 0, math.MaxUint32}, Offsets: []uint32{0, 3, 6}, Data: []byte("a\x00ba\x00b"), Validity: validity, DictionaryDomain: "different"}
		}
		binding, err := workspace.bind(fields, batch)
		if err != nil {
			t.Fatal(err)
		}
		for row := 0; row < 5; row++ {
			var expected []byte
			for _, c := range cases {
				width, bits, flag := c.width, c.bits, byte(1)
				if row == 4 {
					width, flag = 0, 0
				} else if row >= 2 {
					if c.typ == memory.TypeFloat32 {
						bits = 0x7fc00000
					}
					if c.typ == memory.TypeFloat64 {
						bits = 0x7ff8000000000000
					}
				}
				expected = append(expected, byte(c.typ), flag, byte(width), 0, 0, 0)
				if flag == 0 {
					continue
				}
				if c.typ == memory.TypeString || c.typ == memory.TypeBinary {
					expected = append(expected, 'a', 0, 'b')
					continue
				}
				var payload [8]byte
				binary.LittleEndian.PutUint64(payload[:], bits)
				expected = append(expected, payload[:width]...)
			}
			n, err := binding.KeySize(row)
			if err != nil || n != len(expected) {
				t.Fatalf("size %d: %d %v", row, n, err)
			}
			got, err := binding.appendSizedKey(make([]byte, 0, n), row, n)
			if err != nil || !bytes.Equal(got, expected) {
				t.Fatalf("bytes dict=%v row=%d: got %x want %x (%v)", dictionary, row, got, expected, err)
			}
			validated, err := binding.AppendKey(make([]byte, 0, n), row)
			if err != nil || !bytes.Equal(validated, expected) {
				t.Fatal("validated append", err)
			}
			h := fnv.New64a()
			_, _ = h.Write(expected)
			actual, err := binding.HashRow(row)
			if err != nil || actual != h.Sum64() {
				t.Fatal("independent hash mismatch", err)
			}
		}
	}
}

func TestCanonicalWorkspaceLifecycleAndBudget(t *testing.T) {
	fields := []memory.Field{{Name: "x", Type: memory.TypeInt64}, {Name: "s", Type: memory.TypeString}}
	charge := int64(len(fields)) * int64(unsafe.Sizeof(canonicalGroupComponent{}))
	manager := memory.NewBudgetManager(charge)
	reserve := func(n int64) (memory.Reservation, error) { return manager.Reserve(context.Background(), n) }
	var w canonicalGroupWorkspace
	if err := w.ensure(nil, nil); err != nil || manager.Used() != 0 {
		t.Fatal("empty ensure", err)
	}
	if err := w.ensure(fields, reserve); err != nil || manager.Used() != charge {
		t.Fatal("charged ensure", err, manager.Used())
	}
	batch := memory.NewBatch()
	batch.Rows = 1
	batch.Int64s["x"] = &memory.Int64Vector{Values: []int64{42}}
	batch.Strings["s"] = &memory.StringViewVector{Offsets: []uint32{0, 1}, Data: []byte("a")}
	binding, err := w.bind(fields, batch)
	if err != nil {
		t.Fatal(err)
	}
	original := &w.components[0]
	oldLease := w.lease
	if err := w.ensure(append(fields, fields[0]), reserve); !errors.Is(err, memory.ErrBudgetExceeded) {
		t.Fatal("growth budget", err)
	}
	if &w.components[0] != original || w.lease != oldLease || w.binding.batch != batch || manager.Used() != charge {
		t.Fatal("failed reservation mutated workspace")
	}
	if _, err := binding.KeySize(0); err != nil {
		t.Fatal("old binding after failed reservation", err)
	}
	if err := w.ensure(fields, func(int64) (memory.Reservation, error) { t.Fatal("warm ensure reserved"); return nil, nil }); err != nil {
		t.Fatal(err)
	}
	w.clear()
	if w.binding.batch != nil || w.binding.fields != nil || w.binding.components != nil {
		t.Fatal("binding retained")
	}
	for _, component := range w.components {
		if !reflect.DeepEqual(component, canonicalGroupComponent{}) {
			t.Fatal("vector retained")
		}
	}
	if manager.Used() != charge {
		t.Fatal("clear prematurely released storage")
	}
	batch.Strings = nil
	batch.DictStrings["s"] = &memory.DictionaryStringVector{Indices: []uint32{0}, Offsets: []uint32{0, 1}, Data: []byte("b")}
	if _, err := w.bind(fields, batch); err != nil {
		t.Fatal("dictionary rebind", err)
	}
	if w.components[1].strings != nil || w.components[1].dictStrings != batch.DictStrings["s"] {
		t.Fatal("stale string reference")
	}
	// The first column binds successfully before the second column fails.
	batch.DictStrings["s"].Indices[0] = 99
	if _, err := w.bind(fields, batch); err == nil {
		t.Fatal("invalid dictionary accepted")
	}
	if w.binding.batch != nil {
		t.Fatal("failed binding retained batch")
	}
	for _, component := range w.components {
		if !reflect.DeepEqual(component, canonicalGroupComponent{}) {
			t.Fatal("failed binding retained vector")
		}
	}
	w.close()
	w.close()
	if manager.Used() != 0 || w.components != nil || w.lease != nil {
		t.Fatal("close leak")
	}
	// Reopening after Close is supported, and a failed initial reservation owns nothing.
	tiny := memory.NewBudgetManager(1)
	if err := w.ensure(fields, func(n int64) (memory.Reservation, error) { return tiny.Reserve(context.Background(), n) }); !errors.Is(err, memory.ErrBudgetExceeded) {
		t.Fatal(err)
	}
	if w.components != nil || w.lease != nil || tiny.Used() != 0 {
		t.Fatal("initial failure leak")
	}
	if err := w.ensure(fields, reserve); err != nil {
		t.Fatal(err)
	}
	w.close()
}

func TestCanonicalWorkspaceWarmAllocations(t *testing.T) {
	fields := []memory.Field{{Name: "x", Type: memory.TypeInt64}, {Name: "s", Type: memory.TypeString}}
	batch := memory.NewBatch()
	batch.Rows = 1
	batch.Int64s["x"] = &memory.Int64Vector{Values: []int64{42}}
	batch.DictStrings["s"] = &memory.DictionaryStringVector{Indices: []uint32{0}, Offsets: []uint32{0, 3}, Data: []byte("abc")}
	manager := memory.NewBudgetManager(0)
	reserve := func(n int64) (memory.Reservation, error) { return manager.Reserve(context.Background(), n) }
	var w canonicalGroupWorkspace
	if err := w.ensure(fields, reserve); err != nil {
		t.Fatal(err)
	}
	defer w.close()
	dst := make([]byte, 0, 64)
	allocations := testing.AllocsPerRun(1000, func() {
		if err := w.ensure(fields, reserve); err != nil {
			panic(err)
		}
		b, err := w.bind(fields, batch)
		if err != nil {
			panic(err)
		}
		n, err := b.KeySize(0)
		if err != nil {
			panic(err)
		}
		if _, err = b.appendSizedKey(dst, 0, n); err != nil {
			panic(err)
		}
		if _, err = b.HashRow(0); err != nil {
			panic(err)
		}
		w.clear()
	})
	if allocations != 0 {
		t.Fatalf("warm binding/encoding allocated %g times", allocations)
	}
}

func TestCanonicalSizedAppendRejectsInvalidArguments(t *testing.T) {
	batch := memory.NewBatch()
	batch.Rows = 1
	batch.Uint64s["x"] = &memory.Uint64Vector{Values: []uint64{1}}
	b, err := bindCanonicalGroupKeys([]memory.Field{{Name: "x", Type: memory.TypeUint64}}, batch)
	if err != nil {
		t.Fatal(err)
	}
	for _, args := range []struct{ row, size int }{{-1, 14}, {1, 14}, {0, -1}, {0, 65}, {0, math.MaxInt}} {
		backing := bytes.Repeat([]byte{0xa5}, 64)
		before := bytes.Clone(backing)
		if _, err := b.appendSizedKey(backing[:3], args.row, args.size); err == nil {
			t.Fatal("accepted invalid arguments", args)
		}
		if !bytes.Equal(backing, before) {
			t.Fatal("argument rejection wrote destination", args)
		}
	}
	// Incorrect trusted sizes must also return an error rather than panic.
	for _, size := range []int{0, 1, 13, 15} {
		if _, err := b.appendSizedKey(make([]byte, 0, 64), 0, size); err == nil {
			t.Fatal("accepted incorrect trusted size", size)
		}
	}
}

// bindCanonicalGroupKeys is an uncharged convenience for small unit tests only.
func bindCanonicalGroupKeys(fields []memory.Field, batch *memory.Batch) (canonicalGroupBinding, error) {
	return bindCanonicalGroupComponents(fields, batch, make([]canonicalGroupComponent, len(fields)))
}
