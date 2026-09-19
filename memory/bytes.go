package memory

// BatchBytes returns the bytes owned by specialized vector buffers in b. It
// excludes source-native opaque buffers because CloneBatch retains those
// buffers instead of copying them.
func BatchBytes(b *Batch) int64 {
	if b == nil {
		return 0
	}
	var n int64
	for _, v := range b.Int8s {
		n += int64(len(v.Values)) + int64(len(v.Validity))*8
	}
	for _, v := range b.Int16s {
		n += int64(len(v.Values))*2 + int64(len(v.Validity))*8
	}
	for _, v := range b.Int64s {
		n += int64(len(v.Values))*8 + int64(len(v.Validity))*8
	}
	for _, v := range b.Int32s {
		n += int64(len(v.Values))*4 + int64(len(v.Validity))*8
	}
	for _, v := range b.Uint32s {
		n += int64(len(v.Values))*4 + int64(len(v.Validity))*8
	}
	for _, v := range b.Uint8s {
		n += int64(len(v.Values)) + int64(len(v.Validity))*8
	}
	for _, v := range b.Uint16s {
		n += int64(len(v.Values))*2 + int64(len(v.Validity))*8
	}
	for _, v := range b.Uint64s {
		n += int64(len(v.Values))*8 + int64(len(v.Validity))*8
	}
	for _, v := range b.Float64s {
		n += int64(len(v.Values))*8 + int64(len(v.Validity))*8
	}
	for _, v := range b.Float32s {
		n += int64(len(v.Values))*4 + int64(len(v.Validity))*8
	}
	for _, v := range b.Strings {
		n += int64(len(v.Offsets))*4 + int64(len(v.Data)) + int64(len(v.Validity))*8
	}
	for _, v := range b.DictStrings {
		n += int64(len(v.Indices))*4 + int64(len(v.Validity))*8 + int64(len(v.Offsets))*4 + int64(len(v.Data))
	}
	for _, v := range b.StringLists {
		n += int64(len(v.Offsets))*4 + int64(len(v.Validity))*8
		if v.Values != nil {
			n += int64(len(v.Values.Offsets))*4 + int64(len(v.Values.Data)) + int64(len(v.Values.Validity))*8
		}
	}
	for _, v := range b.Structs {
		if v != nil {
			n += int64(len(v.Validity))*8 + BatchBytes(v.Values)
		}
	}
	for _, v := range b.Opaque {
		if v != nil {
			n += int64(len(v.Validity)) * 8
		}
	}
	return n
}
