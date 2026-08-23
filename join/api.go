package join

import "fmt"

// KeyCodec is the extension boundary for dictionary strings, composite keys,
// and future row encodings. The first production operator is a specialized
// int32 table; generic codecs should be selected once per operator, never
// dispatched through an interface for every primitive comparison.
type KeyCodec interface {
	Name() string
	FixedWidthBytes() int
}

func unsupportedJoinType(t Type) error {
	return fmt.Errorf("join: unsupported type %d", t)
}
