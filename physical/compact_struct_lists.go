package physical

import (
	"context"
	"fmt"
	"gocolumn/kernel"
	"gocolumn/memory"
)

type compactExtendedOwner struct {
	leases  []memory.Reservation
	lists   []*memory.OwnedStructList
	arrays  []*memory.OwnedNestedList
	structs []*memory.OwnedRecursiveStruct
}

func (o *compactExtendedOwner) release() {
	if o == nil {
		return
	}
	for _, v := range o.arrays {
		v.Close()
	}
	for _, v := range o.lists {
		v.Close()
	}
	for _, v := range o.structs {
		v.Close()
	}
	for _, l := range o.leases {
		l.Release()
	}
}
func (w *compactWorkspace) compactExtended(ctx context.Context, src *memory.Batch, rows []int, selection memory.Bitmap, kernels kernel.Set) (*memory.Batch, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	count := int64(len(src.Structs) + len(src.StructLists) + len(src.Arrays))
	if count > (int64(^uint64(0)>>1)-8192)/2048 {
		return nil, fmt.Errorf("extended compact metadata overflow")
	}
	r, err := w.manager.Reserve(ctx, 8192+count*2048)
	if err != nil {
		return nil, err
	}
	owner := &compactExtendedOwner{leases: []memory.Reservation{r}}
	ok := false
	defer func() {
		if !ok {
			owner.release()
		}
	}()
	flat := *src
	flat.StructLists = nil
	flat.Arrays = nil
	flat.Structs = make(map[string]*memory.StructVector, len(src.Structs))
	added := memory.NewBatch()
	for name, v := range src.Structs {
		if err = ctx.Err(); err != nil {
			return nil, err
		}
		if !memory.BatchHasStructLists(v.Values) && !memory.BatchHasArrays(v.Values) {
			flat.Structs[name] = v
			continue
		}
		f := memory.Field{Name: name, Type: memory.TypeStruct, Nullable: true, Encoding: memory.EncodingView, Children: v.Fields}
		take := memory.TakeStructListsStructRowsOwned
		if memory.BatchHasArrays(v.Values) {
			take = memory.TakeNestedListsStructRowsOwned
		}
		o, e := take(ctx, w.manager, v, f, rows)
		if e != nil {
			return nil, e
		}
		owner.structs = append(owner.structs, o)
		added.Structs[name] = o.Vector
	}
	for name, v := range src.Arrays {
		if err = ctx.Err(); err != nil {
			return nil, err
		}
		if v == nil || v.Child == nil || v.Child.Child == nil {
			return nil, fmt.Errorf("invalid compact array node")
		}
		leaf := memory.Field{Name: "item", Type: v.Child.Child.Kind, Nullable: true, Encoding: memory.EncodingView}
		inner := memory.Field{Name: "item", Type: memory.TypeList, Nullable: true, Encoding: memory.EncodingView, Children: []memory.Field{leaf}}
		f := memory.Field{Name: name, Type: memory.TypeList, Nullable: true, Encoding: memory.EncodingView, Children: []memory.Field{inner}}
		o, e := memory.TakeNestedListRowsOwned(ctx, w.manager, v, f, memory.NestedListParentDomain{}, rows)
		if e != nil {
			return nil, e
		}
		owner.arrays = append(owner.arrays, o)
		added.Arrays[name] = o.Vector
	}
	for name, v := range src.StructLists {
		if err = ctx.Err(); err != nil {
			return nil, err
		}
		f := memory.Field{Name: name, Type: memory.TypeList, Nullable: true, Encoding: memory.EncodingView, Children: []memory.Field{{Name: "item", Type: memory.TypeStruct, Nullable: true, Encoding: memory.EncodingView, Children: v.Values.Fields}}}
		o, e := memory.TakeStructListRowsOwned(ctx, w.manager, v, f, memory.StructListParentDomain{}, rows)
		if e != nil {
			return nil, e
		}
		owner.lists = append(owner.lists, o)
		added.StructLists[name] = o.Vector
	}
	// Preserve the existing flat compactor and its old budget model. New owners
	// remain charged throughout its old/new replacement; publication is last.
	out, err := w.compactFlat(ctx, &flat, rows, selection, kernels)
	if err != nil {
		return nil, err
	}
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	for n, v := range added.Arrays {
		out.Arrays[n] = v
	}
	for n, v := range added.Structs {
		out.Structs[n] = v
	}
	for n, v := range added.StructLists {
		out.StructLists[n] = v
	}
	w.extended = owner
	ok = true
	return out, nil
}
func (w *compactWorkspace) clearExtended() {
	if w.extended == nil {
		return
	}
	if w.batch != nil {
		clear(w.batch.StructLists)
		clear(w.batch.Arrays)
		for n, v := range w.batch.Structs {
			if v != nil && (memory.BatchHasStructLists(v.Values) || memory.BatchHasArrays(v.Values)) {
				delete(w.batch.Structs, n)
			}
		}
	}
	w.extended.release()
	w.extended = nil
}
