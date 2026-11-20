package memory

import (
	"context"
	"fmt"
	"math"
)

// OwnedNestedList owns a canonical independent array tree and its reservation.
// Bytes includes exact payload capacities and conservative metadata, not process RSS.
type OwnedNestedList struct {
	Vector *ArrayNode
	Field  Field
	Bytes  int64
	lease  Reservation
}

// Close releases this owner and invalidates Vector and Field; it is idempotent.
func (o *OwnedNestedList) Close() {
	if o == nil {
		return
	}
	o.Vector = nil
	o.Field = Field{}
	if o.lease != nil {
		o.lease.Release()
		o.lease = nil
	}
}

// CloneNestedListRangeOwned copies a parent range after checking all structure
// and only the selected values. Hidden NULL spans are canonicalized away.
func CloneNestedListRangeOwned(ctx context.Context, manager Manager, src *ArrayNode, f Field, d NestedListParentDomain, offset, length int) (*OwnedNestedList, error) {
	if src == nil || offset < 0 || length < 0 || offset > src.Len() || length > src.Len()-offset {
		return nil, fmt.Errorf("nested List range out of bounds")
	}
	return copyNestedListOwned(ctx, manager, src, f, d, length, func(i int) int { return offset + i })
}

// TakeNestedListRowsOwned copies reordered/repeated parent rows. Both List
// offsets are remapped; ancestor validity is indexed only by original parent rows.
func TakeNestedListRowsOwned(ctx context.Context, manager Manager, src *ArrayNode, f Field, d NestedListParentDomain, rows []int) (*OwnedNestedList, error) {
	return copyNestedListOwned(ctx, manager, src, f, d, len(rows), func(i int) int { return rows[i] })
}

func nestedListCountOK(n int) bool { return n >= 0 && n < math.MaxInt-63 && uint64(n) < math.MaxUint32 }
func nestedListAddCount(a, b int) (int, error) {
	if !nestedListCountOK(a) || b < 0 || b > math.MaxInt-a || !nestedListCountOK(a+b) {
		return 0, fmt.Errorf("nested List count overflow")
	}
	return a + b, nil
}
func nestedListAddBytes(a, b int) (int, error) {
	if a < 0 || b < 0 || b > math.MaxInt-a || uint64(a)+uint64(b) > math.MaxUint32 {
		return 0, fmt.Errorf("nested List String bytes overflow")
	}
	return a + b, nil
}
func nestedListOwnedCharge(f Field, p, i, e, data int) (int64, error) {
	if !nestedListCountOK(p) || !nestedListCountOK(i) || !nestedListCountOK(e) || data < 0 || uint64(data) > math.MaxUint32 {
		return 0, fmt.Errorf("nested List owned size overflow")
	}
	total := int64(3*4096 + 2*1024)
	add := func(n int64) error {
		if n < 0 || total > math.MaxInt64-n {
			return fmt.Errorf("nested List owned bytes overflow")
		}
		total += n
		return nil
	}
	// Field strings are immutable shared references, not copied byte payloads.
	// Charge their visible lengths conservatively for this owner's retained metadata.
	node := f
	for j := 0; j < 3; j++ {
		for _, s := range []string{node.Name, node.ArrowType, node.DictionaryDomain} {
			if err := add(int64(len(s))); err != nil {
				return 0, err
			}
		}
		if j < 2 {
			node = node.Children[0]
		}
	}
	for _, n := range []int{p, i} {
		if err := add((int64(n)+1)*4 + int64(WordsFor(n))*8); err != nil {
			return 0, err
		}
	}
	if err := add(int64(WordsFor(e)) * 8); err != nil {
		return 0, err
	}
	if node.Type == TypeInt64 {
		if err := add(int64(e) * 8); err != nil {
			return 0, err
		}
	} else {
		if err := add((int64(e)+1)*4 + int64(data)); err != nil {
			return 0, err
		}
	}
	return total, nil
}
func copyNestedListOwned(ctx context.Context, manager Manager, src *ArrayNode, f Field, d NestedListParentDomain, n int, row func(int) int) (*OwnedNestedList, error) {
	return copyRepeatedListOwned(ctx, manager, src, f, d, n, row)
}
func copyNestedListBytes(ctx context.Context, dst, src []byte) error {
	for len(src) > 0 {
		if err := ctx.Err(); err != nil {
			return err
		}
		n := len(src)
		if n > 32768 {
			n = 32768
		}
		copy(dst[:n], src[:n])
		dst = dst[n:]
		src = src[n:]
	}
	return nil
}
