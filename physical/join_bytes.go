package physical

import (
	"context"
	"encoding/binary"
	"fmt"
	"gocolumn/join"
	"gocolumn/memory"
	"gocolumn/source"
	"math"
)

func validateJoinKeyFields(left, right memory.Field) error {
	if left.Type != right.Type {
		return fmt.Errorf("join key type mismatch %s vs %s", left.Type, right.Type)
	}
	switch left.Type {
	case memory.TypeInt8, memory.TypeInt16, memory.TypeInt32, memory.TypeInt64,
		memory.TypeUint8, memory.TypeUint16, memory.TypeUint32, memory.TypeUint64, memory.TypeString:
	case memory.TypeDate32, memory.TypeDate64, memory.TypeTime32, memory.TypeTime64, memory.TypeTimestamp, memory.TypeDuration:
		// Units/timezones are not normalized by join execution. Reject distinct
		// logical Arrow types rather than silently compare differently scaled bits.
		if left.ArrowType != right.ArrowType {
			return fmt.Errorf("join temporal key metadata mismatch %q vs %q", left.ArrowType, right.ArrowType)
		}
	default:
		return fmt.Errorf("join key type %s is not supported; use integer, temporal or string keys", left.Type)
	}
	return nil
}

func joinKeyString(batch *memory.Batch, name string, row int) ([]byte, bool, error) {
	if v, ok := batch.Strings[name]; ok {
		if row < 0 || row+1 >= len(v.Offsets) {
			return nil, false, fmt.Errorf("join string key %q row %d out of range", name, row)
		}
		if !v.IsValid(row) {
			return nil, false, nil
		}
		lo, hi := v.Offsets[row], v.Offsets[row+1]
		if lo > hi || uint64(hi) > uint64(len(v.Data)) {
			return nil, false, fmt.Errorf("join string key %q has malformed offsets", name)
		}
		return v.Data[lo:hi], true, nil
	}
	if v, ok := batch.DictStrings[name]; ok {
		if row < 0 || row >= len(v.Indices) {
			return nil, false, fmt.Errorf("join dictionary key %q row %d out of range", name, row)
		}
		if !v.IsValid(row) {
			return nil, false, nil
		}
		code := uint64(v.Indices[row])
		if code+1 >= uint64(len(v.Offsets)) {
			return nil, false, fmt.Errorf("join dictionary key %q code %d out of range", name, code)
		}
		lo, hi := v.Offsets[code], v.Offsets[code+1]
		if lo > hi || uint64(hi) > uint64(len(v.Data)) {
			return nil, false, fmt.Errorf("join dictionary key %q has malformed offsets", name)
		}
		return v.Data[lo:hi], true, nil
	}
	return nil, false, fmt.Errorf("join string key %q has no string vector", name)
}

// byteKeySize is allocation-free and checks all components. Any NULL makes
// the whole SQL equality key invalid, including NULL versus NULL.
func (s *JoinSource) byteKeySize(batch *memory.Batch, left bool, row int) (int, bool, error) {
	size := 0
	valid := true
	for i, k := range s.keys {
		name := k.Right
		if left {
			name = k.Left
		}
		n := 9 // one type byte and either a value or a string length
		if s.keyFields[i].Type == memory.TypeString {
			value, ok, err := joinKeyString(batch, name, row)
			if err != nil {
				return 0, false, err
			}
			valid = valid && ok
			if len(value) > math.MaxInt-9 {
				return 0, false, fmt.Errorf("join key length exceeds platform capacity")
			}
			n += len(value)
		} else {
			_, ok, err := fixedPhysicalValue(batch, name, row)
			if err != nil {
				return 0, false, err
			}
			valid = valid && ok
		}
		if size > math.MaxInt-n {
			return 0, false, fmt.Errorf("join key length exceeds platform capacity")
		}
		size += n
	}
	return size, valid, nil
}

// encodeByteKey is injective over the supported, schema-checked key types:
// each component has a type tag, and strings have an explicit byte length.
// Dictionary values are compared by bytes, never by dictionary-local codes.
func (s *JoinSource) encodeByteKey(dst []byte, batch *memory.Batch, left bool, row int) ([]byte, bool, error) {
	dst = dst[:0]
	for i, k := range s.keys {
		name := k.Right
		if left {
			name = k.Left
		}
		dst = append(dst, byte(s.keyFields[i].Type))
		if s.keyFields[i].Type == memory.TypeString {
			value, valid, err := joinKeyString(batch, name, row)
			if err != nil || !valid {
				return dst, false, err
			}
			dst = binary.LittleEndian.AppendUint64(dst, uint64(len(value)))
			dst = append(dst, value...)
		} else {
			value, valid, err := fixedPhysicalValue(batch, name, row)
			if err != nil || !valid {
				return dst, false, err
			}
			dst = binary.LittleEndian.AppendUint64(dst, value)
		}
	}
	return dst, true, nil
}

func (s *JoinSource) buildByteKeys(ctx context.Context, manager memory.Manager) error {
	// Count before allocating the directory or encoding scratch. The retained
	// payload has already been reserved and copied by buildRight.
	var bytes int64
	maxKey := 0
	for row, loc := range s.buildLoc {
		if row&1023 == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		size, valid, err := s.byteKeySize(s.rightBatches[loc.batch], false, loc.row)
		if err != nil {
			return err
		}
		if size > maxKey {
			maxKey = size
		}
		if valid {
			if err := addJoinEstimate(&bytes, int64(size)); err != nil {
				return err
			}
		}
	}
	tableBytes, err := join.EstimateBytesTableBytes(len(s.buildLoc), bytes)
	if err != nil {
		return err
	}
	if manager != nil {
		reservation, err := manager.Reserve(ctx, tableBytes)
		if err != nil {
			return fmt.Errorf("generic join table memory: %w", err)
		}
		s.reservations = append(s.reservations, reservation)
		scratch, err := manager.Reserve(ctx, int64(maxKey))
		if err != nil {
			return fmt.Errorf("generic join key scratch memory: %w", err)
		}
		defer scratch.Release()
	}
	scratch := make([]byte, 0, maxKey)
	s.bytesTable, err = join.BuildBytesTable(len(s.buildLoc), func(row int) ([]byte, bool, error) {
		if row&1023 == 0 {
			if err := ctx.Err(); err != nil {
				return nil, false, err
			}
		}
		loc := s.buildLoc[row]
		var valid bool
		var e error
		scratch, valid, e = s.encodeByteKey(scratch, s.rightBatches[loc.batch], false, loc.row)
		return scratch, valid, e
	})
	if err != nil {
		return err
	}
	s.strategy = "canonical-bytes-hash"
	return nil
}

func (s *JoinSource) probeByteKeys(ctx context.Context, batch *memory.Batch, emit join.EmitFunc, manager memory.Manager) (join.Summary, error) {
	maxKey := 0
	for row := 0; row < batch.Rows; row++ {
		if row&1023 == 0 {
			if err := ctx.Err(); err != nil {
				return join.Summary{}, err
			}
		}
		size, _, err := s.byteKeySize(batch, true, row)
		if err != nil {
			return join.Summary{}, err
		}
		if size > maxKey {
			maxKey = size
		}
	}
	if manager != nil {
		reservation, err := manager.Reserve(ctx, int64(maxKey))
		if err != nil {
			return join.Summary{}, fmt.Errorf("generic join probe key memory: %w", err)
		}
		defer reservation.Release()
	}
	scratch := make([]byte, 0, maxKey)
	emitted := 0
	checkedEmit := func(pair join.Pair) error {
		if emitted&1023 == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		emitted++
		if emit != nil {
			return emit(pair)
		}
		return nil
	}
	return s.bytesTable.Probe(batch.Rows, func(row int) ([]byte, bool, error) {
		if row&1023 == 0 {
			if err := ctx.Err(); err != nil {
				return nil, false, err
			}
		}
		var valid bool
		var err error
		scratch, valid, err = s.encodeByteKey(scratch, batch, true, row)
		return scratch, valid, err
	}, s.typ, checkedEmit)
}

// resetBytePairs reserves the conservative output-pair bound before allocating.
// Resident generic readers fail on the finite budget rather than silently
// switching strategies after preparation. Bounded queries use the spill stream.
func (r *joinReader) resetBytePairs(ctx context.Context, rows int64) error {
	if r.pairReservation != nil {
		r.pairReservation.Release()
		r.pairReservation = nil
	}
	r.pairs = nil
	if rows < 0 || rows > math.MaxInt64/16 || uint64(rows) > uint64(math.MaxInt) {
		return fmt.Errorf("generic join output pair capacity overflows")
	}
	if r.byteManager != nil {
		reservation, err := r.byteManager.Reserve(ctx, rows*16)
		if err != nil {
			return fmt.Errorf("generic join output pairs memory: %w", err)
		}
		r.pairReservation = reservation
	}
	r.pairs = make([]join.Pair, 0, int(rows))
	return nil
}

func (s *residentJoinBatchStream) reserveByteOutput(ctx context.Context, left *memory.Batch, pairs []join.Pair) error {
	estimate, err := estimateJoinMaterializationBytes(s.reader.owner, left, pairs)
	if err != nil {
		return err
	}
	manager := s.reader.byteManager
	if s.workspace != nil {
		manager = s.workspace
	}
	if manager == nil {
		return nil
	}
	reservation, err := manager.Reserve(ctx, estimate)
	if err != nil {
		return fmt.Errorf("generic join output payload memory: %w", err)
	}
	s.outputReservation = reservation
	return nil
}

// Dictionary output retains its encoding. Require the source-wide dictionary
// contract and own the bytes so a joined result cannot borrow reader storage.
func (s *JoinSource) ownByteJoinDictionaries(ctx context.Context) error {
	for i := range s.bindings {
		binding := &s.bindings[i]
		if binding.field.Type != memory.TypeString || binding.field.Encoding != memory.EncodingDictionary {
			continue
		}
		input := s.left
		if binding.right {
			input = s.right
		}
		provider, ok := input.(source.DictionaryProvider)
		if !ok {
			return fmt.Errorf("generic join dictionary payload %q requires a stable source dictionary", binding.in)
		}
		dictionary, err := provider.Dictionary(ctx, binding.in)
		if err != nil {
			return fmt.Errorf("generic join dictionary payload %q: %w", binding.in, err)
		}
		if dictionary.Domain == "" {
			return fmt.Errorf("generic join dictionary payload %q has no stable domain", binding.in)
		}
		if binding.field.DictionaryDomain != "" && binding.field.DictionaryDomain != dictionary.Domain {
			return fmt.Errorf("generic join dictionary payload %q domain does not match schema", binding.in)
		}
		if err := (join.Dictionary{Domain: dictionary.Domain, Offsets: dictionary.Offsets, Data: dictionary.Data}).Validate(); err != nil {
			return err
		}
		bytes := int64(len(dictionary.Data))
		if err := addJoinEstimate(&bytes, int64(len(dictionary.Offsets))*4); err != nil {
			return err
		}
		if s.keyManager != nil {
			reservation, err := s.keyManager.Reserve(ctx, bytes)
			if err != nil {
				return fmt.Errorf("generic join dictionary memory: %w", err)
			}
			s.reservations = append(s.reservations, reservation)
		}
		dictionary.Offsets = copyByteJoinSlice(dictionary.Offsets)
		dictionary.Data = copyByteJoinSlice(dictionary.Data)
		binding.dict = dictionary
	}
	return nil
}

func (s *JoinSource) reserveBytePartitions(ctx context.Context, count int) error {
	// source.Partition is an int ID plus an int64 row count (16 bytes on
	// supported platforms). Retained input and output slices are exact-sized.
	bytes, err := safeMulRows(int64(count), 32)
	if err != nil {
		return err
	}
	if s.keyManager != nil {
		reservation, err := s.keyManager.Reserve(ctx, bytes)
		if err != nil {
			return fmt.Errorf("generic join partition metadata memory: %w", err)
		}
		s.reservations = append(s.reservations, reservation)
	}
	return nil
}

func (s *JoinSource) byteExecutionManager(ctx context.Context, opts source.ReadOptions) memory.Manager {
	if query := memory.QueryBudgetFromContext(ctx); query != nil {
		return query.Operator("GenericJoinWorkspace", memory.MemoryWorkspace)
	}
	if opts.MemoryManager != nil {
		return opts.MemoryManager
	}
	return s.keyManager
}
