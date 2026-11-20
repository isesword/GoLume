package memory

import (
	"context"
	"fmt"
)

type repeatedListCounts struct {
	rows  [MaxRepeatedListDepth + 1]int
	data  int
	depth int
}

func measureRepeatedRows(ctx context.Context, src *ArrayNode, f Field, d NestedListParentDomain, n int, row func(int) int) (repeatedListCounts, error) {
	return measureRepeatedRowsMode(ctx, src, f, d, n, row, false)
}

func measureRepeatedRowsMode(ctx context.Context, src *ArrayNode, f Field, d NestedListParentDomain, n int, row func(int) int, nullable bool) (repeatedListCounts, error) {
	var counts repeatedListCounts
	minimum := 2
	if nullable {
		minimum = 1
		f.Nullable = true
	}
	depth, err := repeatedListDepthMinimum(f, ctx.Err, minimum)
	if err != nil {
		return counts, err
	}
	counts.depth = depth
	if depth == 1 {
		if !nestedListCountOK(src.Len()) {
			return counts, fmt.Errorf("inner row count overflow")
		}
		child := f
		child.Name = "item"
		outer := Field{Name: "array", Type: TypeList, Nullable: true, Encoding: EncodingView, Children: []Field{child}}
		offsets := [2]uint32{0, uint32(src.Len())}
		wrapper := ArrayNode{Kind: TypeList, Offsets: offsets[:], Child: src, Encoding: EncodingView}
		if err := wrapper.validateNestedStructure(outer, 1, NestedListParentDomain{}, ctx.Err); err != nil {
			return counts, err
		}
	} else if err := src.validateNestedStructure(f, src.Len(), d, ctx.Err); err != nil {
		return counts, err
	}
	var visit func(*ArrayNode, int, int, bool) error
	visit = func(v *ArrayNode, r, axis int, visible bool) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		var err error
		counts.rows[axis], err = nestedListAddCount(counts.rows[axis], 1)
		if err != nil {
			return err
		}
		if !visible || !v.IsValid(r) {
			return nil
		}
		if v.Kind == TypeList {
			for i := int(v.Offsets[r]); i < int(v.Offsets[r+1]); i++ {
				if err := visit(v.Child, i, axis+1, true); err != nil {
					return err
				}
			}
		} else if v.Kind == TypeString {
			counts.data, err = nestedListAddBytes(counts.data, int(v.Strings.Offsets[r+1]-v.Strings.Offsets[r]))
			return err
		}
		return nil
	}
	for k := 0; k < n; k++ {
		if err := ctx.Err(); err != nil {
			return counts, err
		}
		r := row(k)
		if nullable && r == -1 {
			counts.rows[0], err = nestedListAddCount(counts.rows[0], 1)
			if err != nil {
				return counts, err
			}
			continue
		}
		if r < 0 || r >= src.Len() {
			return counts, fmt.Errorf("nested List selected row out of bounds")
		}
		if err := src.validateNestedParent(f, d, r, ctx.Err); err != nil {
			return counts, err
		}
		if err := visit(src, r, 0, d.visible(r)); err != nil {
			return counts, err
		}
	}
	return counts, ctx.Err()
}
func repeatedOwnedCharge(f Field, c repeatedListCounts) (int64, error) {
	total := int64((c.depth+1)*4096 + c.depth*1024)
	var err error
	for axis := 0; axis <= c.depth; axis++ {
		n := c.rows[axis]
		if !nestedListCountOK(n) {
			return 0, fmt.Errorf("nested owned row overflow")
		}
		for _, s := range []string{f.Name, f.ArrowType, f.DictionaryDomain} {
			total, err = ownedRangeAdd(total, int64(len(s)), 1)
			if err != nil {
				return 0, err
			}
		}
		total, err = ownedRangeAdd(total, int64(WordsFor(n)), 8)
		if err != nil {
			return 0, err
		}
		if axis < c.depth || f.Type == TypeString {
			total, err = ownedRangeAdd(total, int64(n)+1, 4)
		} else {
			total, err = ownedRangeAdd(total, int64(n), 8)
		}
		if err != nil {
			return 0, err
		}
		if axis < c.depth {
			f = f.Children[0]
		}
	}
	return ownedRangeAdd(total, int64(c.data), 1)
}
func copyRepeatedListOwned(ctx context.Context, manager Manager, src *ArrayNode, f Field, d NestedListParentDomain, n int, row func(int) int) (*OwnedNestedList, error) {
	return copyRepeatedListOwnedMode(ctx, manager, src, f, d, n, row, false)
}

func copyRepeatedListOwnedMode(ctx context.Context, manager Manager, src *ArrayNode, f Field, d NestedListParentDomain, n int, row func(int) int, nullable bool) (*OwnedNestedList, error) {
	if ctx == nil || manager == nil || src == nil || !nestedListCountOK(n) {
		return nil, fmt.Errorf("nested List copy requires context, manager and bounded rows")
	}
	counts, err := measureRepeatedRowsMode(ctx, src, f, d, n, row, nullable)
	if err != nil {
		return nil, err
	}
	charge, err := repeatedOwnedCharge(f, counts)
	if err != nil {
		return nil, err
	}
	lease, err := manager.Reserve(ctx, charge)
	if err != nil {
		return nil, err
	}
	success := false
	defer func() {
		if !success {
			lease.Release()
		}
	}()
	field, err := cloneRecursiveFieldContext(f, ctx.Err)
	if err != nil {
		return nil, err
	}
	if nullable || d.count > 0 {
		field.Nullable = true
	}
	var nodes [MaxRepeatedListDepth + 1]*ArrayNode
	nodeField := f
	for axis := 0; axis <= counts.depth; axis++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		count := counts.rows[axis]
		v := &ArrayNode{Kind: nodeField.Type, Encoding: EncodingView}
		nodes[axis] = v
		if axis > 0 {
			nodes[axis-1].Child = v
		}
		if axis < counts.depth {
			v.Offsets = make([]uint32, count+1)
			v.Validity = make(Bitmap, WordsFor(count))
			nodeField = nodeField.Children[0]
		} else if v.Kind == TypeInt64 {
			v.Int64 = &Int64Vector{Values: make([]int64, count), Validity: make(Bitmap, WordsFor(count)), Encoding: EncodingView}
		} else {
			v.Strings = &StringViewVector{Offsets: make([]uint32, count+1), Data: make([]byte, counts.data), Validity: make(Bitmap, WordsFor(count)), Encoding: EncodingView}
		}
	}
	var at [MaxRepeatedListDepth + 1]int
	data := 0
	var copyRow func(*ArrayNode, int, int, bool) error
	copyRow = func(v *ArrayNode, r, axis int, visible bool) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		i := at[axis]
		if i >= counts.rows[axis] {
			return fmt.Errorf("nested source changed")
		}
		at[axis]++
		out := nodes[axis]
		// A synthetic NULL never dereferences the source row or offsets.
		valid := r != -1 && visible && v.IsValid(r)
		if axis < counts.depth {
			if valid {
				Set(out.Validity, i)
				for j := int(v.Offsets[r]); j < int(v.Offsets[r+1]); j++ {
					if err := copyRow(v.Child, j, axis+1, true); err != nil {
						return err
					}
				}
			}
			out.Offsets[i+1] = uint32(at[axis+1])
			return nil
		}
		if out.Kind == TypeInt64 {
			if valid {
				out.Int64.Values[i] = v.Int64.Values[r]
				Set(out.Int64.Validity, i)
			}
		} else {
			if valid {
				value := v.Strings.ValueBytes(r)
				if len(value) > counts.data-data {
					return fmt.Errorf("nested source changed")
				}
				if err := copyNestedListBytes(ctx, out.Strings.Data[data:data+len(value)], value); err != nil {
					return err
				}
				data += len(value)
				Set(out.Strings.Validity, i)
			}
			out.Strings.Offsets[i+1] = uint32(data)
		}
		return nil
	}
	for k := 0; k < n; k++ {
		r := row(k)
		if err := copyRow(src, r, 0, d.visible(r)); err != nil {
			return nil, err
		}
	}
	if at != counts.rows || data != counts.data {
		return nil, fmt.Errorf("nested source changed during copy")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	success = true
	return &OwnedNestedList{Vector: nodes[0], Field: field, Bytes: charge, lease: lease}, nil
}
