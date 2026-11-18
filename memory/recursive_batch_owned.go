package memory

import (
	"context"
	"fmt"
)

// HasRecursiveStruct reports whether the schema contains a Struct child of a
// top-level Struct. It does not traverse unvalidated child graphs.
func (s Schema) HasRecursiveStruct() bool {
	for _, f := range s.Fields {
		if f.Type == TypeStruct {
			for _, c := range f.Children {
				if c.Type == TypeStruct {
					return true
				}
			}
		}
	}
	return false
}

type recursiveBatchLease struct {
	leases []Reservation
	bytes  int64
}

// Bytes returns the retained admission charge.
func (r *recursiveBatchLease) Bytes() int64 { return r.bytes }

// Release idempotently releases the retained owned reservations.
func (r *recursiveBatchLease) Release() {
	for _, l := range r.leases {
		l.Release()
	}
	r.leases = nil
}

// CloneCollectBatchRecursive preserves legacy flat/depth-one copies and uses
// explicit checked owned copies for recursive roots. Every reservation remains
// live until the returned reservation is released. It does not broaden Opaque
// ownership guarantees or support arbitrary recursive list elements.
func CloneCollectBatchRecursive(ctx context.Context, manager Manager, src *Batch, schema Schema) (*Batch, Reservation, error) {
	if ctx == nil || manager == nil || src == nil {
		return nil, nil, fmt.Errorf("recursive Collect requires context, manager and batch")
	}
	if err := checkRecursiveCollectGraph(src, 0, ctx.Err); err != nil {
		return nil, nil, err
	}
	for _, f := range schema.Fields {
		if f.Type == TypeStruct {
			if err := f.ValidateRecursiveStruct(); err != nil {
				return nil, nil, err
			}
		}
	}
	if err := checkRecursiveDeclaredStructs(src, schema.Fields, ctx.Err); err != nil {
		return nil, nil, err
	}
	setup, err := ownedRangeAdd(4096, int64(len(src.Structs)), 1024)
	if err != nil {
		return nil, nil, err
	}
	first, err := manager.Reserve(ctx, setup)
	if err != nil {
		return nil, nil, err
	}
	lease := &recursiveBatchLease{leases: []Reservation{first}, bytes: setup}
	success := false
	defer func() {
		if !success {
			lease.Release()
		}
	}()
	flat := *src
	flat.Structs = make(map[string]*StructVector, len(src.Structs))
	for name, v := range src.Structs {
		flat.Structs[name] = v
	}
	for _, f := range schema.Fields {
		if f.Type == TypeStruct {
			for _, c := range f.Children {
				if c.Type == TypeStruct {
					delete(flat.Structs, f.Name)
					break
				}
			}
		}
	}
	n, err := EstimateCollectBatchBytes(&flat)
	if err != nil {
		return nil, nil, err
	}
	r, err := manager.Reserve(ctx, n)
	if err != nil {
		return nil, nil, err
	}
	lease.leases = append(lease.leases, r)
	lease.bytes, err = ownedRangeAdd(lease.bytes, n, 1)
	if err != nil {
		return nil, nil, err
	}
	out, err := CloneCollectBatch(&flat)
	if err != nil {
		return nil, nil, err
	}
	defer func() {
		if !success {
			out.ReleaseOwned()
		}
	}()
	for _, f := range schema.Fields {
		if f.Type != TypeStruct || flat.Structs[f.Name] != nil {
			continue
		}
		owned, err := CloneRecursiveStructRangeOwned(ctx, manager, src.Structs[f.Name], f, 0, src.Rows)
		if err != nil {
			return nil, nil, err
		}
		out.Structs[f.Name] = owned.Vector
		lease.leases = append(lease.leases, owned.lease)
		lease.bytes, err = ownedRangeAdd(lease.bytes, owned.Bytes, 1)
		if err != nil {
			return nil, nil, err
		}
		owned.lease = nil
	}
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	success = true
	return out, lease, nil
}

func checkRecursiveCollectGraph(b *Batch, depth int, check func() error) error {
	if err := recursiveCheck(check); err != nil {
		return err
	}
	if b == nil {
		return fmt.Errorf("recursive Collect has nil child batch")
	}
	for _, v := range b.Structs {
		if depth >= MaxStructDepth || v == nil {
			return fmt.Errorf("recursive Collect runtime graph exceeds depth or is nil")
		}
		if err := checkRecursiveCollectGraph(v.Values, depth+1, check); err != nil {
			return err
		}
	}
	return nil
}

func checkRecursiveDeclaredStructs(b *Batch, fields []Field, check func() error) error {
	for name, v := range b.Structs {
		if err := recursiveCheck(check); err != nil {
			return err
		}
		found := false
		for _, f := range fields {
			if err := recursiveCheck(check); err != nil {
				return err
			}
			if f.Name == name && f.Type == TypeStruct {
				found = true
				if err := checkRecursiveDeclaredStructs(v.Values, f.Children, check); err != nil {
					return err
				}
				break
			}
		}
		if !found {
			return fmt.Errorf("recursive Collect has undeclared Struct %q", name)
		}
	}
	return nil
}
