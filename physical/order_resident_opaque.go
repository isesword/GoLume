package physical

import (
	"context"
	"fmt"
	"gocolumn/memory"
	"hash/maphash"
	"math"
	"reflect"
	"unsafe"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// residentOpaqueRows deliberately does not concatenate Arrow arrays. Generic
// concatenation can allocate dictionary-unification scratch outside its allocator.
// A resident owner emits single-row borrowed bindings instead, keeping all Arrow
// types and their full backing alive until its output lease ends.
type residentOpaqueRows struct {
	fields      []residentOpaqueField
	rows        int
	reservation *sortWorkspaceReservation
}
type residentOpaqueField struct {
	name      string
	prototype memory.OpaqueVector
	output    *memory.OpaqueVector
	values    []*residentOpaqueValue
	sources   residentOpaqueSourceIndex
}

type residentOpaqueValue struct {
	memory.OpaqueVector
	source arrow.ArrayData
}

func newResidentOpaqueRows(ctx context.Context, schema memory.Schema, first *memory.Batch, workspace *sortWorkspace) (*residentOpaqueRows, error) {
	if ctx == nil || first == nil || workspace == nil {
		return nil, fmt.Errorf("resident opaque rows require context, batch and workspace")
	}
	count := 0
	for _, field := range schema.Fields {
		if residentUsesOpaque(field, first) {
			count++
		}
	}
	if count == 0 {
		return nil, nil
	}
	n, err := sortWorkspaceSize(int64(unsafe.Sizeof(residentOpaqueRows{})), int64(unsafe.Sizeof(sortWorkspaceReservation{})), int64(count)*(int64(unsafe.Sizeof(residentOpaqueField{}))+2*int64(unsafe.Sizeof(memory.OpaqueVector{}))))
	if err != nil {
		return nil, err
	}
	for _, field := range schema.Fields {
		name := field.Name
		v := first.Opaque[name]
		if !residentUsesOpaque(field, first) {
			continue
		}
		if a, ok := v.Value.(arrow.Array); !ok || residentOpaqueIsNil(a) {
			return nil, fmt.Errorf("resident opaque column %q is not an Arrow array", name)
		}
		n, err = sortWorkspaceSize(n, int64(len(name)), int64(len(v.ArrowType)))
		if err != nil {
			return nil, err
		}
	}
	r, err := workspace.Reserve(ctx, n)
	if err != nil {
		return nil, err
	}
	o := &residentOpaqueRows{reservation: r.(*sortWorkspaceReservation), fields: make([]residentOpaqueField, 0, count)}
	for _, field := range schema.Fields {
		name := field.Name
		v := first.Opaque[name]
		if !residentUsesOpaque(field, first) {
			continue
		}
		p := *v
		p.Value = nil
		p.Validity = nil
		p.Rows = 1
		p.Owned = false
		o.fields = append(o.fields, residentOpaqueField{name: name, prototype: p, sources: residentOpaqueSourceIndex{seed: maphash.MakeSeed()}})
	}
	return o, nil
}

func (o *residentOpaqueRows) appendRow(ctx context.Context, src *memory.Batch, row int) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if o == nil || o.reservation == nil {
		return fmt.Errorf("resident opaque rows closed")
	}
	if row < 0 || row >= src.Rows {
		return fmt.Errorf("resident opaque rows shape changed")
	}
	// Admit each allocation before constructing a slice. Partial appends on error
	// remain owned and are cleaned by close; callers must discard a failed owner.
	for i := range o.fields {
		f := &o.fields[i]
		v := src.Opaque[f.name]
		if v == nil {
			return fmt.Errorf("resident opaque column %q missing", f.name)
		}
		if err := o.appendValue(ctx, f, v, row, nil); err != nil {
			return err
		}
	}
	o.rows++
	return nil
}

func (o *residentOpaqueRows) appendValue(ctx context.Context, f *residentOpaqueField, v *memory.OpaqueVector, row int, origin arrow.ArrayData) error {
	a, ok := v.Value.(arrow.Array)
	if !ok || residentOpaqueIsNil(a) || row < 0 || row >= a.Len() {
		return fmt.Errorf("resident opaque column %q has invalid Arrow backing", f.name)
	}
	if v.Type != f.prototype.Type || v.ArrowType != f.prototype.ArrowType || v.Encoding != f.prototype.Encoding {
		return fmt.Errorf("resident opaque column %q metadata changed", f.name)
	}
	// Arrow NewSliceData asserts *array.Data after retaining its buffers. Reject
	// custom data implementations before that call can panic and leak references.
	arrowData, validData := a.Data().(*array.Data)
	if !validData || arrowData == nil {
		return fmt.Errorf("resident opaque column %q has unsupported Arrow array data", f.name)
	}
	data := origin
	if data == nil {
		data = arrowData
	}
	hash, comparable, err := f.sources.key(data)
	if err != nil {
		return err
	}
	_, found := f.sources.find(data, hash, comparable)
	// topKArrowDataBytes includes all buffer capacities and parent buffers,
	// descendants, dictionaries and their metadata, not just logical slice size.
	backing, err := topKArrowDataBytes(data)
	if err != nil {
		return err
	}
	// A row slice can construct a recursive Arrow array/header tree. Charge that
	// tree conservatively from the source graph even when backing is already held.
	headers, err := residentOpaqueHeaderBytes(data)
	if err != nil {
		return err
	}
	extra := headers
	if !found {
		extra, err = sortWorkspaceSize(extra, backing)
		if err != nil {
			return err
		}
	}
	extra, err = sortWorkspaceSize(extra, int64(unsafe.Sizeof(residentOpaqueValue{})), 8)
	if err != nil {
		return err
	}
	valueCap := cap(f.values)
	sourceCap := len(f.sources.slots)
	if len(f.values) == valueCap {
		if valueCap > math.MaxInt/2 {
			return memory.ErrBudgetExceeded
		}
		valueCap = max(1, 2*valueCap)
		width := int64(unsafe.Sizeof((*residentOpaqueValue)(nil)))
		if int64(valueCap) > math.MaxInt64/width {
			return memory.ErrBudgetExceeded
		}
		extra, err = sortWorkspaceSize(extra, int64(valueCap)*width)
		if err != nil {
			return err
		}
	}
	if !found {
		var indexBytes int64
		sourceCap, indexBytes, err = f.sources.growth()
		if err != nil {
			return err
		}
		extra, err = sortWorkspaceSize(extra, indexBytes)
		if err != nil {
			return err
		}
	}
	if err = o.reservation.grow(ctx, extra); err != nil {
		return err
	}
	if valueCap != cap(f.values) {
		values := make([]*residentOpaqueValue, len(f.values), valueCap)
		copy(values, f.values)
		f.values = values
	}
	if sourceCap != len(f.sources.slots) {
		f.sources.resize(sourceCap)
	}
	if !found {
		slot, _ := f.sources.find(data, hash, false)
		data.Retain()
		f.sources.slots[slot] = residentOpaqueSourceSlot{data: data, hash: hash}
		f.sources.count++
	}
	slice := array.NewSlice(a, int64(row), int64(row+1))
	out := f.prototype
	out.Value = slice
	out.Validity = validityFromArrow(slice)
	f.values = append(f.values, &residentOpaqueValue{OpaqueVector: out, source: data})
	return nil
}

func (o *residentOpaqueRows) appendFrom(ctx context.Context, src *residentOpaqueRows, row int) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if o == nil || src == nil || row < 0 || row >= src.rows || len(o.fields) != len(src.fields) {
		return fmt.Errorf("resident opaque source row invalid")
	}
	for i := range o.fields {
		f := &o.fields[i]
		var from *residentOpaqueField
		for j := range src.fields {
			if src.fields[j].name == f.name {
				from = &src.fields[j]
				break
			}
		}
		if from == nil {
			return fmt.Errorf("resident opaque source field missing")
		}
		v := from.values[row]
		if err := o.appendValue(ctx, f, &v.OpaqueVector, 0, v.source); err != nil {
			return err
		}
	}
	o.rows++
	return nil
}

func residentOpaqueHeaderBytes(data arrow.ArrayData) (int64, error) {
	if data == nil || (reflect.ValueOf(data).Kind() == reflect.Ptr && reflect.ValueOf(data).IsNil()) {
		return 0, nil
	}
	// Same Arrow metadata envelope as topKArrowDataBytes, without buffer payload.
	n := int64(256)
	var err error
	n, err = sortWorkspaceSize(n, int64(cap(data.Buffers()))*int64(unsafe.Sizeof(uintptr(0))), int64(cap(data.Children()))*int64(unsafe.Sizeof(arrow.ArrayData(nil))))
	if err != nil {
		return 0, err
	}
	for _, child := range data.Children() {
		v, e := residentOpaqueHeaderBytes(child)
		if e != nil {
			return 0, e
		}
		n, err = sortWorkspaceSize(n, v)
		if err != nil {
			return 0, err
		}
	}
	dictionary, err := residentOpaqueHeaderBytes(data.Dictionary())
	if err != nil {
		return 0, err
	}
	return sortWorkspaceSize(n, dictionary)
}

// bindRow borrows a row until the owner closes. The caller supplies a batch with
// an initialized Opaque map and clears its previous bindings before calling.
func (o *residentOpaqueRows) bindRow(dst *memory.Batch, row int) error {
	if o == nil || o.reservation == nil || row < 0 || row >= o.rows {
		return fmt.Errorf("resident opaque row out of range")
	}
	if dst.Opaque == nil && len(o.fields) > 0 {
		return fmt.Errorf("resident opaque output map is missing")
	}
	for i := range o.fields {
		f := &o.fields[i]
		if f.output != nil {
			*f.output = memory.OpaqueVector{}
		}
		out := f.values[row].OpaqueVector
		f.output = &out
		dst.Opaque[f.name] = f.output
	}
	return nil
}
func (o *residentOpaqueRows) close() {
	if o == nil {
		return
	}
	for i := range o.fields {
		f := &o.fields[i]
		if f.output != nil {
			*f.output = memory.OpaqueVector{}
			f.output = nil
		}
		for _, v := range f.values {
			if a, ok := v.Value.(arrow.Array); ok {
				v.Value = nil
				a.Release()
			}
			v.Validity = nil
			v.source = nil
		}
		f.sources.close()
		f.values = nil
	}
	o.fields = nil
	o.rows = 0
	if o.reservation != nil {
		o.reservation.Release()
		o.reservation = nil
	}
}

// A custom source may expose extra runtime aliases. Match the schema's physical
// family first; an unrelated map cannot displace either its native binding or
// the legitimate opaque fallback. This is the original resident precedence.
func residentUsesOpaque(field memory.Field, b *memory.Batch) bool {
	if b == nil || b.Opaque[field.Name] == nil {
		return false
	}
	name := field.Name
	switch field.Type {
	case memory.TypeInt8:
		return b.Int8s[name] == nil
	case memory.TypeInt16:
		return b.Int16s[name] == nil
	case memory.TypeInt32, memory.TypeDate32, memory.TypeTime32:
		return b.Int32s[name] == nil
	case memory.TypeInt64, memory.TypeDate64, memory.TypeTimestamp, memory.TypeTime64, memory.TypeDuration:
		return b.Int64s[name] == nil
	case memory.TypeUint8:
		return b.Uint8s[name] == nil
	case memory.TypeUint16:
		return b.Uint16s[name] == nil
	case memory.TypeUint32, memory.TypeBool:
		return b.Uint32s[name] == nil
	case memory.TypeUint64:
		return b.Uint64s[name] == nil
	case memory.TypeFloat32:
		return b.Float32s[name] == nil
	case memory.TypeFloat64:
		return b.Float64s[name] == nil
	case memory.TypeString, memory.TypeBinary:
		if field.Encoding == memory.EncodingDictionary {
			return b.DictStrings[name] == nil
		}
		return b.Strings[name] == nil && b.DictStrings[name] == nil
	case memory.TypeList:
		return b.StringLists[name] == nil
	case memory.TypeStruct:
		return b.Structs[name] == nil
	default:
		return true
	}
}
