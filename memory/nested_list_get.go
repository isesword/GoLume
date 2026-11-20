package memory

import (
	"context"
	"fmt"
)

// OwnedArray owns a one- or two-axis inner List extracted from the checked array profile.
// Its node can be adapted to existing primitive List vectors without copying.
type OwnedArray struct {
	Vector *ArrayNode
	Field  Field
	Bytes  int64
	lease  Reservation
}

// Close releases this owner's reservation and drops owned references once.
func (o *OwnedArray) Close() {
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

// TakeNullableArrayRowsOwned copies one- or two-axis Lists into independent storage.
// A -1 index and a selected NULL root both emit NULL regardless of root requiredness.
// All storage spans are checked; required descendants are checked only under
// selected valid roots. Ordinary Clone/Take retain their strict input contract.
func TakeNullableArrayRowsOwned(ctx context.Context, m Manager, src *ArrayNode, f Field, rows []int) (*OwnedArray, error) {
	if ctx == nil || m == nil || src == nil || !nestedListCountOK(len(rows)) {
		return nil, fmt.Errorf("invalid nullable inner List take")
	}
	depth, err := repeatedListDepthMinimum(f, ctx.Err, 1)
	if err != nil {
		return nil, err
	}
	if depth > MaxNestedListDepth {
		return nil, fmt.Errorf("nullable inner List take supports at most two axes")
	}
	owned, err := copyRepeatedListOwnedMode(ctx, m, src, f, NestedListParentDomain{}, len(rows), func(i int) int { return rows[i] }, true)
	if err != nil {
		return nil, err
	}
	result := &OwnedArray{Vector: owned.Vector, Field: owned.Field, Bytes: owned.Bytes, lease: owned.lease}
	owned.lease = nil
	return result, nil
}
