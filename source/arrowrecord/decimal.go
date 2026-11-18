package arrowrecord

import (
	"fmt"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"gocolumn/memory"
)

// Options controls explicit per-source conversions. The zero value preserves
// legacy Arrow transport and does not enable native Decimal expressions.
type Options struct {
	// NativeNested selects the checked unified container profile, including two List axes.
	// It composes with all older opt-ins; top-level Decimal still requires NativeDecimal128.
	NativeNested bool

	// NativeStructList enables flat fourteen-type Struct elements in native Lists,
	// including their bounded outer Struct ancestry. It composes with other opt-ins;
	// nested Decimal and dictionary tables containing NULL remain unsupported.
	NativeStructList bool

	// NativeRecursiveStruct enables bounded recursive native Struct transport.
	// Struct depth is limited by memory.MaxStructDepth; List elements remain primitive.
	// String dictionary tables containing NULL entries are unsupported, even
	// when the NULL entry is unreferenced. NativeDecimal128 may be combined
	// for top-level Decimal fields; nested Decimal remains unsupported.
	NativeRecursiveStruct bool

	// NativeDecimal128 maps top-level Decimal128 fields to native exact vectors.
	// Precision must be 1..38 and scale 0..precision; unsupported Decimal128
	// metadata is rejected. Decimal256, dictionary and nested forms retain their
	// existing mapping and admission behavior. No rounding or narrowing occurs.
	NativeDecimal128 bool
}

// Only top-level callers use this mapper. memoryField's recursive nested
// mapping deliberately remains unchanged.
func sourceMemoryField(f arrow.Field, options Options) (memory.Field, error) {
	if options.NativeDecimal128 {
		if dt, ok := f.Type.(*arrow.Decimal128Type); ok {
			if dt == nil {
				return memory.Field{}, fmt.Errorf("arrowrecord: field %q has nil Decimal128 type", f.Name)
			}
			out, err := memory.Decimal128Field(f.Name, f.Nullable, dt.Precision, dt.Scale)
			if err != nil {
				return memory.Field{}, fmt.Errorf("arrowrecord: native Decimal128: %w", err)
			}
			out.ArrowType = dt.String()
			return out, out.ValidateDecimal128()
		}
	}
	if options.NativeNested {
		return nestedMemoryField(f)
	}
	if options.NativeStructList {
		return structListMemoryField(f)
	}
	if options.NativeRecursiveStruct {
		if err := checkRecursiveArrowType(f.Type, 0); err != nil {
			return memory.Field{}, err
		}
	}
	out := memoryField(f)
	if options.NativeRecursiveStruct && out.Type == memory.TypeStruct {
		normalizeRecursiveStringFields(&out)
	}
	if options.NativeRecursiveStruct && out.Type == memory.TypeStruct {
		if err := out.ValidateRecursiveStruct(); err != nil {
			return memory.Field{}, err
		}
	}
	return out, nil
}

func nativeDecimalVector(arr arrow.Array, field memory.Field, validity memory.Bitmap, rows int) (*memory.Decimal128Vector, error) {
	a, ok := arr.(*array.Decimal128)
	if !ok || a == nil {
		return nil, fmt.Errorf("native Decimal128 field has nil or incompatible array %T", arr)
	}
	expected := &arrow.Decimal128Type{Precision: field.DecimalPrecision, Scale: field.DecimalScale}
	if !arrow.TypeEqual(a.DataType(), expected) {
		return nil, fmt.Errorf("native Decimal128 array type %s differs from %s", a.DataType(), expected)
	}
	if rows < 0 || a.Len() < rows || len(a.Values()) < rows {
		return nil, fmt.Errorf("native Decimal128 array shorter than logical rows")
	}
	v := &memory.Decimal128Vector{Values: a.Values()[:rows], Validity: validity, Precision: field.DecimalPrecision, Scale: field.DecimalScale}
	if err := v.ValidateField(field, rows); err != nil {
		return nil, err
	}
	return v, nil
}

func checkRecursiveArrowType(dt arrow.DataType, depth int) error {
	if dt == nil {
		return fmt.Errorf("nil Arrow datatype")
	}
	switch t := dt.(type) {
	case *arrow.StructType:
		depth++
		if depth > memory.MaxStructDepth {
			return fmt.Errorf("Arrow Struct depth exceeds %d", memory.MaxStructDepth)
		}
		for _, f := range t.Fields() {
			if err := checkRecursiveArrowType(f.Type, depth); err != nil {
				return err
			}
		}
	case *arrow.ListType:
		if t.Elem().ID() != arrow.INT64 && t.Elem().ID() != arrow.STRING {
			return fmt.Errorf("recursive native List requires Int64 or String elements")
		}
	}
	return nil
}

func normalizeRecursiveStringFields(f *memory.Field) {
	if f.Type == memory.TypeString {
		f.Encoding = memory.EncodingView
		f.DictionaryDomain = ""
	}
	for i := range f.Children {
		normalizeRecursiveStringFields(&f.Children[i])
	}
}
