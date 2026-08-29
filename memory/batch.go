package memory

import "fmt"

// Batch is a reusable columnar morsel whose vectors are addressed by logical column name.
// Vector bindings are shallow; sources retain ownership unless documented otherwise.
type Batch struct {
	Rows        int
	Int64s      map[string]*Int64Vector
	Int32s      map[string]*Int32Vector
	Uint32s     map[string]*Uint32Vector
	Uint64s     map[string]*Uint64Vector
	Float64s    map[string]*Float64Vector
	Strings     map[string]*StringViewVector
	DictStrings map[string]*DictionaryStringVector
	StringLists map[string]*StringListVector
	Opaque      map[string]*OpaqueVector
}

// NewBatch allocates an empty batch with reusable column maps.
func NewBatch() *Batch {
	return &Batch{
		Int64s:      make(map[string]*Int64Vector),
		Int32s:      make(map[string]*Int32Vector),
		Uint32s:     make(map[string]*Uint32Vector),
		Uint64s:     make(map[string]*Uint64Vector),
		Float64s:    make(map[string]*Float64Vector),
		Strings:     make(map[string]*StringViewVector),
		DictStrings: make(map[string]*DictionaryStringVector),
		StringLists: make(map[string]*StringListVector),
		Opaque:      make(map[string]*OpaqueVector),
	}
}

// Reset updates the logical row count without discarding vector allocations.
func (b *Batch) Reset(rows int) { b.Rows = rows }

// ReleaseOwned releases source-native values explicitly owned by b. Specialized
// Go vectors do not require release; currently this lifecycle is used by
// materialized Arrow arrays stored in OpaqueVector.
func (b *Batch) ReleaseOwned() {
	if b == nil {
		return
	}
	for _, v := range b.Opaque {
		if v == nil || !v.Owned || v.Value == nil {
			continue
		}
		if rr, ok := v.Value.(interface{ Release() }); ok {
			rr.Release()
		}
		v.Owned = false
	}
}

// ClearColumns removes vector bindings but keeps the maps allocated. Any
// materialized opaque Arrow arrays owned by the batch are released first.
func (b *Batch) ClearColumns() {
	b.ReleaseOwned()
	clear(b.Int64s)
	clear(b.Int32s)
	clear(b.Uint32s)
	clear(b.Uint64s)
	clear(b.Float64s)
	clear(b.Strings)
	clear(b.DictStrings)
	clear(b.StringLists)
	clear(b.Opaque)
	b.Rows = 0
}

// Int64 returns the named int64 vector or an error when the binding is absent.
func (b *Batch) Int64(name string) (*Int64Vector, error) {
	v, ok := b.Int64s[name]
	if !ok {
		return nil, fmt.Errorf("int64 column %q not present in batch", name)
	}
	return v, nil
}

// Int32 returns the named int32 vector or an error when the binding is absent.
func (b *Batch) Int32(name string) (*Int32Vector, error) {
	v, ok := b.Int32s[name]
	if !ok {
		return nil, fmt.Errorf("int32 column %q not present in batch", name)
	}
	return v, nil
}

// Uint32 returns the named uint32 vector or an error when the binding is absent.
func (b *Batch) Uint32(name string) (*Uint32Vector, error) {
	v, ok := b.Uint32s[name]
	if !ok {
		return nil, fmt.Errorf("uint32 column %q not present in batch", name)
	}
	return v, nil
}

// String returns the named offset-and-bytes string vector or an error when absent.
func (b *Batch) String(name string) (*StringViewVector, error) {
	v, ok := b.Strings[name]
	if !ok {
		return nil, fmt.Errorf("string-view column %q not present in batch", name)
	}
	return v, nil
}

// DictionaryString returns the named dictionary-encoded string vector or an error when absent.
func (b *Batch) DictionaryString(name string) (*DictionaryStringVector, error) {
	v, ok := b.DictStrings[name]
	if !ok {
		return nil, fmt.Errorf("dictionary-string column %q not present in batch", name)
	}
	return v, nil
}

// StringList returns the named specialized list<string> vector.
func (b *Batch) StringList(name string) (*StringListVector, error) {
	v, ok := b.StringLists[name]
	if !ok {
		return nil, fmt.Errorf("list<string> column %q not present in batch", name)
	}
	return v, nil
}

// Uint64 returns the named uint64 vector or an error when the binding is absent.
func (b *Batch) Uint64(name string) (*Uint64Vector, error) {
	v, ok := b.Uint64s[name]
	if !ok {
		return nil, fmt.Errorf("uint64 column %q not present in batch", name)
	}
	return v, nil
}

// Float64 returns the named float64 vector or an error when the binding is absent.
func (b *Batch) Float64(name string) (*Float64Vector, error) {
	v, ok := b.Float64s[name]
	if !ok {
		return nil, fmt.Errorf("float64 column %q not present in batch", name)
	}
	return v, nil
}

// OpaqueColumn returns a source-native vector that has no specialized engine
// representation yet.
func (b *Batch) OpaqueColumn(name string) (*OpaqueVector, error) {
	v, ok := b.Opaque[name]
	if !ok {
		return nil, fmt.Errorf("opaque column %q not present in batch", name)
	}
	return v, nil
}
