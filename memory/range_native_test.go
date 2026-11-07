package memory

import (
	"fmt"
	arrowmem "github.com/apache/arrow-go/v18/arrow/memory"
	"math"
	"reflect"
	"testing"
)

func TestNativeNumericBatchRanges(t *testing.T) {
	t.Run("int8", func(t *testing.T) {
		testNativeNumericRange(t, TypeInt8, 1, func(b *Batch, values []int8, validity Bitmap) {
			b.Int8s["value"] = &Int8Vector{Values: values, Validity: validity, Encoding: EncodingPlain}
		}, func(b *Batch) ([]int8, Bitmap, Encoding) {
			v := b.Int8s["value"]
			if v == nil {
				t.Fatal("native vector missing")
			}
			return v.Values, v.Validity, v.Encoding
		})
	})
	t.Run("int16", func(t *testing.T) {
		testNativeNumericRange(t, TypeInt16, 2, func(b *Batch, values []int16, validity Bitmap) {
			b.Int16s["value"] = &Int16Vector{Values: values, Validity: validity, Encoding: EncodingPlain}
		}, func(b *Batch) ([]int16, Bitmap, Encoding) {
			v := b.Int16s["value"]
			if v == nil {
				t.Fatal("native vector missing")
			}
			return v.Values, v.Validity, v.Encoding
		})
	})
	t.Run("uint8", func(t *testing.T) {
		testNativeNumericRange(t, TypeUint8, 1, func(b *Batch, values []uint8, validity Bitmap) {
			b.Uint8s["value"] = &Uint8Vector{Values: values, Validity: validity, Encoding: EncodingPlain}
		}, func(b *Batch) ([]uint8, Bitmap, Encoding) {
			v := b.Uint8s["value"]
			if v == nil {
				t.Fatal("native vector missing")
			}
			return v.Values, v.Validity, v.Encoding
		})
	})
	t.Run("uint16", func(t *testing.T) {
		testNativeNumericRange(t, TypeUint16, 2, func(b *Batch, values []uint16, validity Bitmap) {
			b.Uint16s["value"] = &Uint16Vector{Values: values, Validity: validity, Encoding: EncodingPlain}
		}, func(b *Batch) ([]uint16, Bitmap, Encoding) {
			v := b.Uint16s["value"]
			if v == nil {
				t.Fatal("native vector missing")
			}
			return v.Values, v.Validity, v.Encoding
		})
	})
	t.Run("float32", func(t *testing.T) {
		testNativeNumericRange(t, TypeFloat32, 4, func(b *Batch, values []float32, validity Bitmap) {
			b.Float32s["value"] = &Float32Vector{Values: values, Validity: validity, Encoding: EncodingPlain}
		}, func(b *Batch) ([]float32, Bitmap, Encoding) {
			v := b.Float32s["value"]
			if v == nil {
				t.Fatal("native vector missing")
			}
			return v.Values, v.Validity, v.Encoding
		})
	})
}

type nativeRangeNumber interface {
	~int8 | ~int16 | ~uint8 | ~uint16 | ~float32
}

func testNativeNumericRange[T nativeRangeNumber](t *testing.T, dtype DataType, width int64, bind func(*Batch, []T, Bitmap), read func(*Batch) ([]T, Bitmap, Encoding)) {
	t.Helper()
	testNativeNumericDerivedPaths(t, dtype, bind, read)
	for _, nullable := range []bool{false, true} {
		for _, bounds := range [][2]int{{0, 70}, {1, 65}, {63, 3}, {64, 1}, {69, 1}, {70, 0}, {0, 0}} {
			t.Run(fmt.Sprintf("nullable=%v/offset=%d/length=%d", nullable, bounds[0], bounds[1]), func(t *testing.T) {
				values := make([]T, 70)
				var validity Bitmap
				if nullable {
					validity = EnsureBitmap(nil, 70)
				}
				for i := range values {
					values[i] = T(i + 1)
					if nullable && i%3 != 1 {
						Set(validity, i)
					}
				}
				src := NewBatch()
				src.Reset(70)
				bind(src, values, validity)
				offset, length := bounds[0], bounds[1]
				estimated, err := EstimateBatchRangeBytes(src, offset, length)
				if err != nil {
					t.Fatal(err)
				}
				wantBytes := int64(length) * width
				if nullable && length > 0 {
					wantBytes += int64(WordsFor(length)) * 8
				}
				if estimated != wantBytes {
					t.Fatalf("estimate=%d want=%d", estimated, wantBytes)
				}
				clone, err := CloneBatchRange(src, offset, length)
				if err != nil {
					t.Fatal(err)
				}
				defer clone.ReleaseOwned()
				got, bits, encoding := read(clone)
				if clone.Rows != length || len(got) != length || encoding != EncodingPlain {
					t.Fatalf("rows=%d values=%v encoding=%v", clone.Rows, got, encoding)
				}
				for i, v := range got {
					if v != values[offset+i] {
						t.Fatalf("value[%d]=%v want=%v", i, v, values[offset+i])
					}
					if (bits == nil || IsSet(bits, i)) != (!nullable || (offset+i)%3 != 1) {
						t.Fatalf("validity mismatch at %d", i)
					}
				}
				if BatchBytes(clone) != estimated {
					t.Fatalf("actual=%d estimate=%d", BatchBytes(clone), estimated)
				}
				if !nullable && bits != nil {
					t.Fatal("nil validity was not preserved")
				}
				if length > 0 {
					original := values[offset]
					got[0] = T(99)
					if values[offset] != original {
						t.Fatal("range values alias source")
					}
					if bits != nil {
						before := validity[offset>>6]
						bits[0] ^= 1
						if validity[offset>>6] != before {
							t.Fatal("range validity aliases source")
						}
					}
				}
				full, err := CloneBatch(src)
				if err != nil {
					t.Fatal(err)
				}
				defer full.ReleaseOwned()
				fullValues, fullBits, fullEncoding := read(full)
				if full.Rows != 70 || !reflect.DeepEqual(fullValues, values) || !reflect.DeepEqual(fullBits, validity) || fullEncoding != EncodingPlain {
					t.Fatal("full clone differs from source")
				}
				fullValues[0] = T(99)
				if values[0] != T(1) {
					t.Fatal("full clone values alias source")
				}
				if nullable {
					before := validity[0]
					fullBits[0] ^= 1
					if validity[0] != before {
						t.Fatal("full clone validity aliases source")
					}
				}
			})
		}
	}
	// Malformed native values must fail before allocation or slicing.
	src := NewBatch()
	src.Reset(2)
	bind(src, []T{1}, nil)
	if _, err := EstimateBatchRangeBytes(src, 0, 2); err == nil {
		t.Fatal("estimate accepted short vector")
	}
	if _, err := CloneBatchRange(src, 0, 2); err == nil {
		t.Fatal("clone accepted short vector")
	}
	for _, bounds := range [][2]int{{-1, 1}, {0, -1}, {2, 1}, {1, 2}} {
		if _, err := EstimateBatchRangeBytes(src, bounds[0], bounds[1]); err == nil {
			t.Fatalf("estimate accepted %v", bounds)
		}
		if _, err := CloneBatchRange(src, bounds[0], bounds[1]); err == nil {
			t.Fatalf("clone accepted %v", bounds)
		}
	}
}

func testNativeNumericDerivedPaths[T nativeRangeNumber](t *testing.T, dtype DataType, bind func(*Batch, []T, Bitmap), read func(*Batch) ([]T, Bitmap, Encoding)) {
	t.Helper()
	values := []T{11, 22, 33, 44}
	src := NewBatch()
	src.Reset(4)
	bind(src, values, Bitmap{0b1101})
	field := Field{Name: "value", Type: dtype, Nullable: true, Encoding: EncodingPlain}
	t.Run("ArrowOwnedRange", func(t *testing.T) {
		allocator := arrowmem.NewCheckedAllocator(arrowmem.DefaultAllocator)
		defer allocator.AssertSize(t, 0)
		record, err := ArrowRecordRange(allocator, NewSchema(field), src, 1, 3)
		if err != nil {
			t.Fatal(err)
		}
		defer record.Release()
		column := record.Column(0)
		typed, ok := column.(interface{ Value(int) T })
		if !ok {
			t.Fatalf("wrong native Arrow column: %T", column)
		}
		if record.NumRows() != 3 || !column.IsNull(0) || column.IsNull(1) || column.IsNull(2) || typed.Value(1) != T(33) || typed.Value(2) != T(44) {
			t.Fatalf("Arrow range=%s", column)
		}
		values[2] = T(99)
		if typed.Value(1) != T(33) {
			t.Fatal("Arrow output aliases source")
		}
		values[2] = T(33)
	})
	parent := &StructVector{Rows: 4, Fields: []Field{field}, Values: src, Validity: Bitmap{0b1011}, Encoding: EncodingView}
	structField := StructField("payload", true, field)
	if err := parent.Validate(structField, 4); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name string
		rows []int
		run  func() (*StructVector, error)
	}{
		{"StructRange", []int{1, 2, 3}, func() (*StructVector, error) { return CloneStructRange(parent, 1, 3) }},
		{"StructTake", []int{3, 0, 1, 2, 3}, func() (*StructVector, error) { return TakeStructRows(parent, []int{3, 0, 1, 2, 3}) }},
		{"StructBatchClone", []int{0, 1, 2, 3}, func() (*StructVector, error) {
			b := NewBatch()
			b.Reset(4)
			b.Structs["payload"] = parent
			clone, err := CloneBatch(b)
			if err != nil {
				return nil, err
			}
			return clone.Structs["payload"], nil
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := tc.run()
			if err != nil {
				t.Fatal(err)
			}
			defer got.Values.ReleaseOwned()
			if err := got.Validate(structField, len(tc.rows)); err != nil {
				t.Fatal(err)
			}
			output, bits, _ := read(got.Values)
			for i, row := range tc.rows {
				wantParent := row != 2
				wantValid := wantParent && row != 1
				if got.IsValid(i) != wantParent || (bits == nil || IsSet(bits, i)) != wantValid {
					t.Fatalf("row %d validity mismatch", i)
				}
				want := T(0)
				if wantValid {
					want = values[row]
				}
				if output[i] != want {
					t.Fatalf("row %d=%v want %v", i, output[i], want)
				}
			}
			original := values[tc.rows[0]]
			output[0] = T(99)
			if values[tc.rows[0]] != original {
				t.Fatal("struct clone aliases values")
			}
		})
	}
}

func TestFloat32ClonePreservesBits(t *testing.T) {
	src := NewBatch()
	src.Reset(4)
	bits := []uint32{0x80000000, 0x7fc01234, 0x7f800000, 0xff800000}
	values := make([]float32, len(bits))
	for i, b := range bits {
		values[i] = math.Float32frombits(b)
	}
	src.Float32s["value"] = &Float32Vector{Values: values}
	for _, cloneFn := range []func() (*Batch, error){func() (*Batch, error) { return CloneBatch(src) }, func() (*Batch, error) { return CloneBatchRange(src, 0, src.Rows) }} {
		clone, err := cloneFn()
		if err != nil {
			t.Fatal(err)
		}
		for i, value := range clone.Float32s["value"].Values {
			if math.Float32bits(value) != bits[i] {
				t.Fatalf("row %d bits=%x want=%x", i, math.Float32bits(value), bits[i])
			}
		}
		clone.ReleaseOwned()
	}
}
