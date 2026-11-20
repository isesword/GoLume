package expr

import (
	"fmt"
	"gocolumn/memory"
	"math"
)

// ListGetExpr extracts a zero-based element from a native list.
type ListGetExpr struct{ Value, Index Expr }

func (ListGetExpr) exprNode() {}

// String returns the list expression and explicit element index.
func (e ListGetExpr) String() string { return fmt.Sprintf("LIST_GET(%s,%s)", e.Value, e.Index) }

// ListGet extracts an element from List<String> or List<Int64>. Index must be
// a native integer expression. NULL lists, NULL indices, negative indices,
// out-of-bounds indices, and NULL elements produce NULL. Negative indices do
// not count from the end. Results own their scalar or string payload.
func ListGet(value, index Expr) ListGetExpr { return ListGetExpr{Value: value, Index: index} }

// Alias names an extracted list element.
func (e ListGetExpr) Alias(name string) NamedExpr { return Named(e, name) }

func bindListGet(bound *BoundExpr) error {
	parent := bound.Children[0].Field
	if parent.Type != memory.TypeList {
		return fmt.Errorf("LIST_GET requires native List")
	}
	if err := validateListAccessContainers(parent); err != nil {
		return fmt.Errorf("LIST_GET: %w", err)
	}
	child := parent.Children[0]
	if (len(child.Children) != 0 && !memory.IsStructList(parent) && !memory.IsNestedList(parent)) || parent.DictionaryDomain != "" || child.DictionaryDomain != "" ||
		(parent.Encoding != memory.EncodingPlain && parent.Encoding != memory.EncodingView) ||
		(child.Encoding != memory.EncodingPlain && child.Encoding != memory.EncodingView) {
		return fmt.Errorf("LIST_GET requires a native non-recursive list")
	}
	indexType := bound.Children[1].Field.Type
	if !isSignedInteger(indexType) && !unsignedInteger(indexType) {
		return fmt.Errorf("LIST_GET index must be a native integer, got %s", indexType)
	}
	bound.Field = cloneBoundField(child)
	bound.Field.Name, bound.Field.Nullable = "", true
	bound.Field.DictionaryDomain = ""
	bound.Field.Encoding = memory.EncodingPlain
	if child.Type == memory.TypeString || child.Type == memory.TypeStruct || child.Type == memory.TypeList {
		bound.Field.Encoding = memory.EncodingView
	}
	return nil
}

type listGetView struct {
	offsets                       []uint32
	parentValidity, childValidity memory.Bitmap
	ints                          []int64
	strings                       *memory.StringViewVector
}

// validateListGet checks the entire logical prefix's structure, including
// NULL rows, but never reads scalar or string payload bytes. Unreferenced
// physical tails and prefixes are outside this evaluation's domain.
func validateListGet(in *scalarRegister, field *memory.Field, rows int, check func() error) (listGetView, error) {
	var view listGetView
	if rows < 0 || rows == math.MaxInt || field == nil {
		return view, fmt.Errorf("LIST_GET invalid row count or plan")
	}
	children := 0
	switch field.Children[0].Type {
	case memory.TypeInt64:
		if in.intList == nil || in.strList != nil || in.intList.Values == nil {
			return view, fmt.Errorf("LIST_GET requires native list<int64> storage")
		}
		v := in.intList
		view.offsets, view.parentValidity, view.childValidity, view.ints = v.Offsets, v.Validity, v.Values.Validity, v.Values.Values
		children = len(view.ints)
	case memory.TypeString:
		if in.strList == nil || in.intList != nil || in.strList.Values == nil {
			return view, fmt.Errorf("LIST_GET requires native list<string> storage")
		}
		v := in.strList
		view.offsets, view.parentValidity, view.childValidity, view.strings = v.Offsets, v.Validity, v.Values.Validity, v.Values
		children = v.Values.Len()
	default:
		return view, fmt.Errorf("LIST_GET invalid child type")
	}
	if len(view.offsets) < rows+1 {
		return view, fmt.Errorf("LIST_GET parent offsets are too short")
	}
	if (view.parentValidity != nil && len(view.parentValidity) < memory.WordsFor(rows)) || (in.validity != nil && len(in.validity) < memory.WordsFor(rows)) {
		return view, fmt.Errorf("LIST_GET parent validity is too short")
	}
	for i := 0; i <= rows; i++ {
		if check != nil {
			if err := check(); err != nil {
				return view, err
			}
		}
		if uint64(view.offsets[i]) > uint64(children) || i > 0 && view.offsets[i] < view.offsets[i-1] {
			return view, fmt.Errorf("LIST_GET invalid parent offset at %d", i)
		}
	}
	base, end := int(view.offsets[0]), int(view.offsets[rows])
	if view.childValidity != nil && len(view.childValidity) < memory.WordsFor(end) {
		return view, fmt.Errorf("LIST_GET child validity is too short")
	}
	if view.strings != nil {
		offsets := view.strings.Offsets
		// Empty child string vectors may omit their otherwise singleton offset.
		if end == 0 && len(offsets) == 0 {
			return view, nil
		}
		if len(offsets) <= end {
			return view, fmt.Errorf("LIST_GET child string offsets are too short")
		}
		for i := base; i <= end; i++ {
			if check != nil {
				if err := check(); err != nil {
					return view, err
				}
			}
			if uint64(offsets[i]) > uint64(len(view.strings.Data)) || i > base && offsets[i] < offsets[i-1] {
				return view, fmt.Errorf("LIST_GET invalid child string offset at %d", i)
			}
		}
	}
	return view, nil
}

func listGetIndex(index *scalarRegister, row int, length uint64) (uint64, bool, error) {
	if !isValidRegister(index, row) {
		return 0, false, nil
	}
	var n uint64
	if isSignedInteger(index.typ) {
		value, err := registerInt64(index, row)
		if err != nil {
			return 0, false, err
		}
		if value < 0 {
			return 0, false, nil
		}
		n = uint64(value)
	} else {
		value, err := registerUint64(index, row)
		if err != nil {
			return 0, false, err
		}
		n = value
	}
	return n, n < length, nil
}

func evalListGet(out, in, index *scalarRegister, field *memory.Field, rows int, active memory.Bitmap) error {
	if field != nil && memory.IsNestedList(*field) {
		return evalNestedListGet(out, in, index, *field, rows, active)
	}
	if field != nil && memory.IsStructList(*field) {
		return evalStructListGet(out, in, index, *field, rows, active)
	}
	check := out.checkContext
	view, err := validateListGet(in, field, rows, check)
	if err != nil {
		return err
	}
	if active != nil && len(active) < memory.WordsFor(rows) {
		return fmt.Errorf("LIST_GET active bitmap is too short")
	}
	if index.validity != nil && len(index.validity) < memory.WordsFor(rows) {
		return fmt.Errorf("LIST_GET index validity is too short")
	}
	// Validate required elements for every active, parent-valid list, regardless
	// of whether this particular index selects an element.
	for row := 0; row < rows; row++ {
		if check != nil {
			if err := check(); err != nil {
				return err
			}
		}
		if active != nil && !memory.IsSet(active, row) {
			continue
		}
		parentValid := view.parentValidity == nil || memory.IsSet(view.parentValidity, row)
		if !parentValid {
			if !field.Nullable {
				return fmt.Errorf("LIST_GET non-nullable parent contains NULL at row %d", row)
			}
			continue
		}
		if !isValidRegister(in, row) {
			continue
		}
		if !field.Children[0].Nullable && view.childValidity != nil {
			for child := int(view.offsets[row]); child < int(view.offsets[row+1]); child++ {
				if check != nil {
					if err := check(); err != nil {
						return err
					}
				}
				if !memory.IsSet(view.childValidity, child) {
					return fmt.Errorf("LIST_GET non-nullable element contains NULL at row %d", row)
				}
			}
		}
	}
	selected := func(row int) (int, bool, error) {
		if active != nil && !memory.IsSet(active, row) || !isValidRegister(in, row) || view.parentValidity != nil && !memory.IsSet(view.parentValidity, row) {
			return 0, false, nil
		}
		lo, hi := view.offsets[row], view.offsets[row+1]
		n, valid, err := listGetIndex(index, row, uint64(hi-lo))
		if err != nil || !valid {
			return 0, false, err
		}
		child := int(uint64(lo) + n)
		if view.childValidity != nil && !memory.IsSet(view.childValidity, child) {
			return 0, false, nil
		}
		return child, true, nil
	}
	var bytes uint64
	if view.strings != nil {
		for row := 0; row < rows; row++ {
			if check != nil {
				if err := check(); err != nil {
					return err
				}
			}
			child, valid, err := selected(row)
			if err != nil {
				return err
			}
			if !valid {
				continue
			}
			bytes += uint64(view.strings.Offsets[child+1] - view.strings.Offsets[child])
			if bytes > math.MaxUint32 || bytes > uint64(math.MaxInt) {
				return fmt.Errorf("LIST_GET string output size overflow")
			}
		}
		if err := scalarResize(&out.data, int(bytes), &out.buffers[12], out.acquire); err != nil {
			return err
		}
	}
	out.validity = ensureValidity(out, rows)
	clear(out.validity)
	if view.strings != nil {
		out.offsets[0] = 0
	} else {
		clear(out.i64)
	}
	position := 0
	for row := 0; row < rows; row++ {
		if check != nil {
			if err := check(); err != nil {
				return err
			}
		}
		child, valid, err := selected(row)
		if err != nil {
			return err
		}
		if valid {
			memory.Set(out.validity, row)
			if view.strings == nil {
				out.i64[row] = view.ints[child]
			} else {
				lo, hi := int(view.strings.Offsets[child]), int(view.strings.Offsets[child+1])
				for lo < hi {
					if check != nil {
						if err := check(); err != nil {
							return err
						}
					}
					count := min(hi-lo, 32<<10)
					copy(out.data[position:position+count], view.strings.Data[lo:lo+count])
					lo += count
					position += count
				}
			}
		}
		if view.strings != nil {
			out.offsets[row+1] = uint32(position)
		}
	}
	return nil
}
