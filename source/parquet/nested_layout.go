package parquet

import (
	"context"
	"fmt"
	"gocolumn/memory"
)

func cropStructBatch(b *memory.Batch, rows int) {
	b.Reset(rows)
	words := memory.WordsFor(rows)
	for _, v := range b.Int8s {
		v.Values = v.Values[:rows]
		if v.Validity != nil {
			v.Validity = v.Validity[:words]
		}
	}
	for _, v := range b.Int16s {
		v.Values = v.Values[:rows]
		if v.Validity != nil {
			v.Validity = v.Validity[:words]
		}
	}
	for _, v := range b.Int32s {
		v.Values = v.Values[:rows]
		if v.Validity != nil {
			v.Validity = v.Validity[:words]
		}
	}
	for _, v := range b.Int64s {
		v.Values = v.Values[:rows]
		if v.Validity != nil {
			v.Validity = v.Validity[:words]
		}
	}
	for _, v := range b.Uint8s {
		v.Values = v.Values[:rows]
		if v.Validity != nil {
			v.Validity = v.Validity[:words]
		}
	}
	for _, v := range b.Uint16s {
		v.Values = v.Values[:rows]
		if v.Validity != nil {
			v.Validity = v.Validity[:words]
		}
	}
	for _, v := range b.Uint32s {
		v.Values = v.Values[:rows]
		if v.Validity != nil {
			v.Validity = v.Validity[:words]
		}
	}
	for _, v := range b.Uint64s {
		v.Values = v.Values[:rows]
		if v.Validity != nil {
			v.Validity = v.Validity[:words]
		}
	}
	for _, v := range b.Float32s {
		v.Values = v.Values[:rows]
		if v.Validity != nil {
			v.Validity = v.Validity[:words]
		}
	}
	for _, v := range b.Float64s {
		v.Values = v.Values[:rows]
		if v.Validity != nil {
			v.Validity = v.Validity[:words]
		}
	}
	for _, v := range b.Strings {
		v.Offsets = v.Offsets[:rows+1]
		if v.Validity != nil {
			v.Validity = v.Validity[:words]
		}
	}
	for _, v := range b.Structs {
		v.Rows = rows
		if v.Validity != nil {
			v.Validity = v.Validity[:words]
		}
		cropStructBatch(v.Values, rows)
	}
}

func (s *nestedListStream) allocateForest(ctx context.Context, b *memory.Batch, fields []memory.Field, rows, parent int) error {
	for _, f := range fields {
		if err := s.check(ctx); err != nil {
			return err
		}
		if f.Type == memory.TypeStruct {
			id := -1
			for i, n := range s.nodes {
				if err := s.check(ctx); err != nil {
					return err
				}
				if n.parent == parent && n.name == f.Name {
					id = i
					break
				}
			}
			if id < 0 {
				return fmt.Errorf("missing recursive Struct layout node")
			}
			child := memory.NewBatch()
			child.Reset(rows)
			v := &memory.StructVector{Rows: rows, Fields: memory.NewSchema(f.Children...).Fields, Values: child, Validity: make(memory.Bitmap, memory.WordsFor(rows)), Encoding: memory.EncodingView}
			b.Structs[f.Name] = v
			s.parentVectors[id] = v
			s.parentBatches[id] = child
			if err := s.allocateForest(ctx, child, f.Children, rows, id); err != nil {
				return err
			}
		} else if f.Type != memory.TypeString && f.Type != memory.TypeList {
			allocateFixedColumn(b, f, rows)
		}
	}
	return nil
}
