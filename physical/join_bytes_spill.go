package physical

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"gocolumn/hashagg"
	"gocolumn/internal/spillmeta"
	"gocolumn/join"
	"gocolumn/memory"
	"math"
	"unsafe"
)

// bytePartitionHash hashes the canonical encoding without allocating a key.
// The digest only chooses a partition; table and skew equality use every byte.
func (s *JoinSource) bytePartitionHash(batch *memory.Batch, left bool, row int) (uint64, error) {
	h := uint64(14695981039346656037)
	put := func(b byte) { h = (h ^ uint64(b)) * 1099511628211 }
	word := func(v uint64) {
		for i := 0; i < 8; i++ {
			put(byte(v))
			v >>= 8
		}
	}
	valid := true
	for i, k := range s.keys {
		name := k.Right
		if left {
			name = k.Left
		}
		put(byte(s.keyFields[i].Type))
		if s.keyFields[i].Type == memory.TypeString {
			v, ok, err := joinKeyString(batch, name, row)
			if err != nil {
				return 0, err
			}
			valid = valid && ok
			word(uint64(len(v)))
			for _, b := range v {
				put(b)
			}
		} else {
			v, ok, err := fixedPhysicalValue(batch, name, row)
			if err != nil {
				return 0, err
			}
			valid = valid && ok
			word(v)
		}
	}
	if !valid {
		return 0, nil
	}
	return hashagg.Hash64(h), nil
}

// byteKeyEqual compares a row to a valid retained canonical key without
// allocating scratch, including on adversarially long string values.
func (s *JoinSource) byteKeyEqual(key []byte, batch *memory.Batch, left bool, row int) (bool, error) {
	for i, k := range s.keys {
		name := k.Right
		if left {
			name = k.Left
		}
		if len(key) < 9 || key[0] != byte(s.keyFields[i].Type) {
			return false, nil
		}
		value := binary.LittleEndian.Uint64(key[1:9])
		key = key[9:]
		if s.keyFields[i].Type == memory.TypeString {
			v, valid, err := joinKeyString(batch, name, row)
			if err != nil || !valid {
				return false, err
			}
			if value != uint64(len(v)) || value > uint64(len(key)) || !bytes.Equal(v, key[:len(v)]) {
				return false, nil
			}
			key = key[len(v):]
		} else {
			v, valid, err := fixedPhysicalValue(batch, name, row)
			if err != nil || !valid {
				return false, err
			}
			if v != value {
				return false, nil
			}
		}
	}
	return len(key) == 0, nil
}

func (s *spillableJoinStream) loadByteBuild(ctx context.Context, refs []joinRunRef) (_ *loadedJoinBuild, resultErr error) {
	// Leave room for a restored probe, matched bitmap, materialized output and
	// its encoded run. Variable-width retained keys must not consume the whole
	// query and strand a partition that could otherwise be repartitioned.
	headroom, err := s.output.Reserve(ctx, s.query.Limit()/2)
	if err != nil {
		return nil, err
	}
	defer headroom.Release()
	var total int64
	for _, ref := range refs {
		if ref.rows < 0 || int64(ref.rows) > math.MaxInt32-total {
			return nil, fmt.Errorf("%w: generic join build partition row count exceeds table capacity", memory.ErrBudgetExceeded)
		}
		total += int64(ref.rows)
	}
	retained := int64(len(refs))*16 + total*16 + 256 + 2*int64(unsafe.Sizeof(loadedJoinBuild{})) + 2*int64(unsafe.Sizeof(JoinSource{}))
	reservation, err := s.state.Reserve(ctx, retained)
	if err != nil {
		return nil, err
	}
	build := &loadedJoinBuild{decoded: make([]*memory.DecodedColumnarRun, 0, len(refs)), batches: make([]*memory.Batch, 0, len(refs)), locations: make([]rowLocation, 0, int(total)), reservation: reservation}
	defer func() {
		if resultErr != nil {
			build.close()
		}
	}()
	for _, ref := range refs {
		decoded, err := s.restoreRun(ctx, s.rightSchema, s.rightFP, ref)
		if err != nil {
			return nil, err
		}
		index := len(build.batches)
		build.decoded = append(build.decoded, decoded)
		build.batches = append(build.batches, decoded.Batch)
		for row := 0; row < decoded.Batch.Rows; row++ {
			build.locations = append(build.locations, rowLocation{index, row})
		}
	}
	owner := s.ownerWithBuild(build)
	owner.reservations, owner.bytesTable = nil, nil
	build.byteOwner = owner
	if err := owner.buildByteKeys(ctx, s.state); err != nil {
		return nil, err
	}
	if int(total) > s.maxBuildRows {
		s.maxBuildRows = int(total)
	}
	if multiplicity := owner.bytesTable.MaxMultiplicity(); multiplicity > s.maxMultiplicity {
		s.maxMultiplicity = multiplicity
	}
	return build, nil
}

func (s *spillableJoinStream) probeBuild(ctx context.Context, build *loadedJoinBuild, batch *memory.Batch, emit join.EmitFunc) (join.Summary, error) {
	if build.byteOwner != nil {
		return build.byteOwner.probeByteKeys(ctx, batch, emit, s.state)
	}
	keys, masks, scratch, err := newJoinProbeKeys(ctx, s.state, batch.Rows)
	if err != nil {
		return join.Summary{}, err
	}
	defer scratch.Release()
	for row := 0; row < batch.Rows; row++ {
		keys[row], masks[row], err = s.encodedKey(batch, true, row)
		if err != nil {
			return join.Summary{}, err
		}
	}
	return build.table.Probe(keys, masks, s.owner.typ, emit)
}

// skewByteBuildKey admits only one complete distinct non-NULL key. A hash
// collision cannot select the Cartesian-product fallback. The caller owns the
// returned key reservation until all run-by-run comparisons have finished.
func (s *spillableJoinStream) skewByteBuildKey(ctx context.Context, refs []joinRunRef) (key []byte, reservation memory.Reservation, resultErr error) {
	validRows := 0
	defer func() {
		if resultErr != nil && reservation != nil {
			reservation.Release()
			reservation = nil
		}
	}()
	for _, ref := range refs {
		decoded, err := s.restoreRun(ctx, s.rightSchema, s.rightFP, ref)
		if err != nil {
			return nil, reservation, err
		}
		err = func() error {
			defer decoded.Close()
			for row := 0; row < decoded.Batch.Rows; row++ {
				if err := ctx.Err(); err != nil {
					return err
				}
				size, valid, err := s.owner.byteKeySize(decoded.Batch, false, row)
				if err != nil {
					return err
				}
				if !valid {
					continue
				}
				validRows++
				if key == nil {
					reservation, err = s.state.Reserve(ctx, int64(size))
					if err != nil {
						return err
					}
					key, _, err = s.owner.encodeByteKey(make([]byte, 0, size), decoded.Batch, false, row)
					if err != nil {
						return err
					}
				} else {
					equal, err := s.owner.byteKeyEqual(key, decoded.Batch, false, row)
					if err != nil {
						return err
					}
					if !equal {
						return ErrJoinSkewExhausted
					}
				}
			}
			return nil
		}()
		if err != nil {
			return nil, reservation, err
		}
	}
	if validRows > s.maxMultiplicity {
		s.maxMultiplicity = validRows
	}
	return key, reservation, nil
}

func (s *spillableJoinStream) processByteSkew(ctx context.Context, leftRefs, rightRefs []joinRunRef, partition int) error {
	key, reservation, err := s.skewByteBuildKey(ctx, rightRefs)
	if err != nil {
		return err
	}
	if reservation != nil {
		defer reservation.Release()
	}
	var created spillmeta.Slice[joinRunRef]
	defer created.Release()
	validKeyMatched := false
	for _, leftRef := range leftRefs {
		left, err := s.restoreRun(ctx, s.leftSchema, s.leftFP, leftRef)
		if err != nil {
			return err
		}
		err = func() error {
			defer left.Close()
			classification, err := s.state.Reserve(ctx, int64(left.Batch.Rows)*16)
			if err != nil {
				return err
			}
			defer classification.Release()
			matching, missing := make([]int, 0, left.Batch.Rows), make([]int, 0, left.Batch.Rows)
			for row := 0; row < left.Batch.Rows; row++ {
				if err := ctx.Err(); err != nil {
					return err
				}
				equal, err := s.owner.byteKeyEqual(key, left.Batch, true, row)
				if err != nil {
					return err
				}
				if equal {
					matching = append(matching, row)
				} else {
					missing = append(missing, row)
				}
			}
			if len(matching) != 0 {
				validKeyMatched = true
				s.matchedProbe += int64(len(matching))
			}
			switch s.owner.typ {
			case join.Semi:
				return s.appendLeftOnly(ctx, left.Batch, matching, partition, &created)
			case join.Anti:
				return s.appendLeftOnly(ctx, left.Batch, missing, partition, &created)
			default:
				if len(matching) != 0 {
					for _, rightRef := range rightRefs {
						right, err := s.restoreRun(ctx, s.rightSchema, s.rightFP, rightRef)
						if err != nil {
							return err
						}
						_, err = s.appendSkewMatches(ctx, left.Batch, matching, right.Batch, hashagg.Key128{}, key, partition, &created)
						_ = right.Close()
						if err != nil {
							return err
						}
					}
				}
				if s.owner.typ == join.Left || s.owner.typ == join.Full {
					return s.appendLeftOnly(ctx, left.Batch, missing, partition, &created)
				}
			}
			return nil
		}()
		if err != nil {
			return err
		}
	}
	if s.owner.typ == join.Right || s.owner.typ == join.Full {
		for _, ref := range rightRefs {
			right, err := s.restoreRun(ctx, s.rightSchema, s.rightFP, ref)
			if err != nil {
				return err
			}
			err = s.appendSkewUnmatchedRight(ctx, right.Batch, hashagg.Key128{}, key, validKeyMatched, partition, &created)
			_ = right.Close()
			if err != nil {
				return err
			}
		}
	}
	if err := errors.Join(s.deleteRefsStrict(ctx, leftRefs), s.deleteRefsStrict(ctx, rightRefs)); err != nil {
		return err
	}
	return s.outputRuns.Append(ctx, s.metadata, created.Values...)
}

func (s *spillableJoinStream) skewRowEqual(batch *memory.Batch, row int, key hashagg.Key128, byteKey []byte) (bool, error) {
	if s.owner.shape == joinBytes {
		return s.owner.byteKeyEqual(byteKey, batch, false, row)
	}
	candidate, mask, err := s.encodedKey(batch, false, row)
	return mask == 3 && candidate == key, err
}

// Variable-width output is split by its measured payload estimate rather than
// only a schema row-width guess. At most one materialized/encoded morsel lives
// at once; a single oversized row still fails the finite admission contract.
func (s *spillableJoinStream) appendOutputPairs(ctx context.Context, build *loadedJoinBuild, left *memory.Batch, pairs []join.Pair, partition int, created *spillmeta.Slice[joinRunRef]) error {
	if len(pairs) == 0 {
		return nil
	}
	if s.owner.shape == joinBytes && len(pairs) > 1 {
		estimate, err := estimateJoinMaterializationBytes(s.ownerWithBuild(build), left, pairs)
		if err != nil {
			return err
		}
		if estimate > s.query.Limit()/8 {
			middle := len(pairs) / 2
			if err := s.appendOutputPairs(ctx, build, left, pairs[:middle], partition, created); err != nil {
				return err
			}
			return s.appendOutputPairs(ctx, build, left, pairs[middle:], partition, created)
		}
	}
	ref, err := s.flushPairs(ctx, build, left, pairs, partition)
	if err != nil {
		return err
	}
	return created.Append(ctx, s.metadata, ref)
}
