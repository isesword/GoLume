package physical

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"gocolumn/join"
	"gocolumn/memory"
	"gocolumn/observer"
	"gocolumn/source"
	"time"
)

func mergeJoinOrdering(left, right source.Source, keys []JoinKey) bool {
	leftOrderingProvider, lok := left.(source.OrderingProvider)
	rightOrderingProvider, rok := right.(source.OrderingProvider)
	if !lok || !rok || len(keys) == 0 {
		return false
	}
	leftOrdering, rightOrdering := leftOrderingProvider.Ordering(), rightOrderingProvider.Ordering()
	if len(leftOrdering) < len(keys) || len(rightOrdering) < len(keys) {
		return false
	}
	for index, key := range keys {
		lo, ro := leftOrdering[index], rightOrdering[index]
		if lo.Name != key.Left || ro.Name != key.Right || lo.Descending != ro.Descending || lo.NullsLast != ro.NullsLast {
			return false
		}
	}
	return true
}

type mergeKeyPart struct {
	valid bool
	typ   memory.DataType
	i64   int64
	u64   uint64
	bytes []byte
}

type mergeKey struct{ parts []mergeKeyPart }

func (s *JoinSource) mergeKey(batch *memory.Batch, left bool, row int) (mergeKey, error) {
	out := mergeKey{parts: make([]mergeKeyPart, len(s.keys))}
	for index, key := range s.keys {
		name := key.Right
		if left {
			name = key.Left
		}
		field, _ := s.right.Schema().Field(name)
		if left {
			field, _ = s.left.Schema().Field(name)
		}
		part := mergeKeyPart{typ: field.Type}
		switch field.Type {
		case memory.TypeInt8:
			vector, err := batch.Int8(name)
			if err != nil {
				return mergeKey{}, err
			}
			part.valid, part.i64 = vector.IsValid(row), int64(vector.Values[row])
		case memory.TypeInt16:
			vector, err := batch.Int16(name)
			if err != nil {
				return mergeKey{}, err
			}
			part.valid, part.i64 = vector.IsValid(row), int64(vector.Values[row])
		case memory.TypeInt32, memory.TypeDate32, memory.TypeTime32:
			vector, err := batch.Int32(name)
			if err != nil {
				return mergeKey{}, err
			}
			part.valid, part.i64 = vector.IsValid(row), int64(vector.Values[row])
		case memory.TypeInt64, memory.TypeDate64, memory.TypeTimestamp, memory.TypeTime64, memory.TypeDuration:
			vector, err := batch.Int64(name)
			if err != nil {
				return mergeKey{}, err
			}
			part.valid, part.i64 = vector.IsValid(row), vector.Values[row]
		case memory.TypeUint8:
			vector, err := batch.Uint8(name)
			if err != nil {
				return mergeKey{}, err
			}
			part.valid, part.u64 = vector.IsValid(row), uint64(vector.Values[row])
		case memory.TypeUint16:
			vector, err := batch.Uint16(name)
			if err != nil {
				return mergeKey{}, err
			}
			part.valid, part.u64 = vector.IsValid(row), uint64(vector.Values[row])
		case memory.TypeUint32:
			vector, err := batch.Uint32(name)
			if err != nil {
				return mergeKey{}, err
			}
			part.valid, part.u64 = vector.IsValid(row), uint64(vector.Values[row])
		case memory.TypeUint64:
			vector, err := batch.Uint64(name)
			if err != nil {
				return mergeKey{}, err
			}
			part.valid, part.u64 = vector.IsValid(row), vector.Values[row]
		case memory.TypeString:
			vector, err := batch.DictionaryString(name)
			if err != nil {
				return mergeKey{}, err
			}
			part.valid = vector.IsValid(row)
			if part.valid {
				part.bytes = append([]byte(nil), vector.DictionaryValue(vector.Indices[row])...)
			}
		default:
			return mergeKey{}, fmt.Errorf("merge join key %q type %s is unsupported", name, field.Type)
		}
		out.parts[index] = part
	}
	return out, nil
}

func mergeKeysEqual(a, b mergeKey) bool {
	if len(a.parts) != len(b.parts) {
		return false
	}
	for index := range a.parts {
		x, y := a.parts[index], b.parts[index]
		if !x.valid || !y.valid || x.typ != y.typ || x.i64 != y.i64 || x.u64 != y.u64 || !bytes.Equal(x.bytes, y.bytes) {
			return false
		}
	}
	return true
}

func mergeKeysSameGroup(a, b mergeKey) bool {
	if len(a.parts) != len(b.parts) {
		return false
	}
	for index := range a.parts {
		x, y := a.parts[index], b.parts[index]
		if x.valid != y.valid || x.typ != y.typ {
			return false
		}
		if x.valid && (x.i64 != y.i64 || x.u64 != y.u64 || !bytes.Equal(x.bytes, y.bytes)) {
			return false
		}
	}
	return true
}

func compareMergeKeys(a, b mergeKey, ordering []source.OrderingColumn) int {
	for index := range a.parts {
		x, y := a.parts[index], b.parts[index]
		cmp := 0
		if !x.valid || !y.valid {
			if x.valid != y.valid {
				if ordering[index].NullsLast {
					if !x.valid {
						cmp = 1
					} else {
						cmp = -1
					}
				} else if !x.valid {
					cmp = -1
				} else {
					cmp = 1
				}
			}
		} else {
			switch x.typ {
			case memory.TypeInt8, memory.TypeInt16, memory.TypeInt32, memory.TypeDate32, memory.TypeTime32,
				memory.TypeInt64, memory.TypeDate64, memory.TypeTimestamp, memory.TypeTime64, memory.TypeDuration:
				if x.i64 < y.i64 {
					cmp = -1
				} else if x.i64 > y.i64 {
					cmp = 1
				}
			case memory.TypeUint8, memory.TypeUint16, memory.TypeUint32, memory.TypeUint64:
				if x.u64 < y.u64 {
					cmp = -1
				} else if x.u64 > y.u64 {
					cmp = 1
				}
			case memory.TypeString:
				cmp = bytes.Compare(x.bytes, y.bytes)
			}
			if ordering[index].Descending {
				cmp = -cmp
			}
		}
		if cmp != 0 {
			return cmp
		}
	}
	return 0
}

type mergeInputCursor struct {
	owner  *JoinSource
	left   bool
	stream source.BatchStream
	batch  *memory.Batch
	budget *memory.OperatorBudget
	row    int
	done   bool
}

func (c *mergeInputCursor) ensure(ctx context.Context) error {
	for !c.done && (c.batch.Rows == 0 || c.row >= c.batch.Rows) {
		ok, err := c.stream.Next(ctx, c.batch)
		if err != nil {
			return err
		}
		if !ok {
			c.done = true
			break
		}
		c.row = 0
	}
	return nil
}

func (c *mergeInputCursor) group(ctx context.Context) (*memory.Batch, []memory.Reservation, mergeKey, bool, error) {
	if err := c.ensure(ctx); err != nil || c.done {
		return nil, nil, mergeKey{}, false, err
	}
	key, err := c.owner.mergeKey(c.batch, c.left, c.row)
	if err != nil {
		return nil, nil, mergeKey{}, false, err
	}
	accumulator := newBatchAccumulator(func() memory.Schema {
		if c.left {
			return c.owner.left.Schema()
		}
		return c.owner.right.Schema()
	}())
	reservations := make([]memory.Reservation, 0, 2)
	fail := func(err error) (*memory.Batch, []memory.Reservation, mergeKey, bool, error) {
		for _, reservation := range reservations {
			reservation.Release()
		}
		return nil, nil, mergeKey{}, false, err
	}
	for !c.done {
		start := c.row
		for c.row < c.batch.Rows {
			current, keyErr := c.owner.mergeKey(c.batch, c.left, c.row)
			if keyErr != nil {
				return fail(keyErr)
			}
			if !mergeKeysSameGroup(key, current) {
				break
			}
			c.row++
		}
		length := c.row - start
		if length > 0 {
			estimate, estimateErr := memory.EstimateBatchRangeBytes(c.batch, start, length)
			if estimateErr != nil {
				return fail(estimateErr)
			}
			if estimate > (int64(^uint64(0)>>1)-1024)/2 {
				return fail(fmt.Errorf("merge join group byte estimate overflows int64"))
			}
			if c.budget != nil {
				reservation, reserveErr := c.budget.Reserve(ctx, estimate*2+1024)
				if reserveErr != nil {
					return fail(reserveErr)
				}
				reservations = append(reservations, reservation)
			}
			for row := start; row < c.row; row++ {
				if appendErr := accumulator.appendRow(c.batch, row); appendErr != nil {
					return fail(appendErr)
				}
			}
		}
		if c.row < c.batch.Rows {
			break
		}
		if ensureErr := c.ensure(ctx); ensureErr != nil {
			return fail(ensureErr)
		}
		if c.done {
			break
		}
		current, keyErr := c.owner.mergeKey(c.batch, c.left, c.row)
		if keyErr != nil {
			return fail(keyErr)
		}
		if !mergeKeysSameGroup(key, current) {
			break
		}
	}
	batch, err := accumulator.finish()
	if err != nil {
		return fail(err)
	}
	return batch, reservations, key, true, nil
}

func (c *mergeInputCursor) close() error {
	if c.batch != nil {
		c.batch.ReleaseOwned()
	}
	return c.stream.Close()
}

type mergeJoinStream struct {
	owner             *JoinSource
	left              *mergeInputCursor
	right             *mergeInputCursor
	leftGroup         *memory.Batch
	rightGroup        *memory.Batch
	leftReservations  []memory.Reservation
	rightReservations []memory.Reservation
	rightLocations    []rowLocation
	rightLocationMem  memory.Reservation
	leftKey           mergeKey
	rightKey          mergeKey
	haveLeft          bool
	haveRight         bool
	matchLeft         int
	matchRight        int
	outputRows        int
	outputBudget      *memory.OperatorBudget
	outputReservation memory.Reservation
	closed            bool
	exhausted         bool
	started           time.Time
	probeRows         int64
	probeBytes        int64
	buildRows         int64
	buildBytes        int64
	matched           int64
	output            int64
	outputBytes       int64
	maxMultiplicity   int
	statsRecorded     bool
}

func newMergeJoinStream(ctx context.Context, owner *JoinSource, opts source.ReadOptions) (*mergeJoinStream, error) {
	left, err := source.OpenBatchStream(ctx, owner.left, opts)
	if err != nil {
		return nil, err
	}
	right, err := source.OpenBatchStream(ctx, owner.right, opts)
	if err != nil {
		_ = left.Close()
		return nil, err
	}
	var leftBudget, rightBudget, outputBudget *memory.OperatorBudget
	if query := memory.QueryBudgetFromContext(ctx); query != nil && query.Limit() > 0 {
		leftBudget = query.Operator("MergeJoinLeftGroup", memory.MemoryRetainedState)
		rightBudget = query.Operator("MergeJoinRightGroup", memory.MemoryRetainedState)
		outputBudget = query.Operator("MergeJoinOutput", memory.MemoryWorkspace)
	}
	return &mergeJoinStream{
		owner: owner, left: &mergeInputCursor{owner: owner, left: true, stream: left, batch: memory.NewBatch(), budget: leftBudget},
		right:      &mergeInputCursor{owner: owner, stream: right, batch: memory.NewBatch(), budget: rightBudget},
		outputRows: 4096, outputBudget: outputBudget, started: time.Now(),
	}, nil
}

func (s *mergeJoinStream) loadGroups(ctx context.Context) error {
	var err error
	if !s.haveLeft {
		s.leftGroup, s.leftReservations, s.leftKey, s.haveLeft, err = s.left.group(ctx)
		if err != nil {
			return err
		}
		if s.haveLeft {
			s.probeRows += int64(s.leftGroup.Rows)
			s.probeBytes += memory.BatchBytes(s.leftGroup)
		}
	}
	if !s.haveRight {
		s.rightGroup, s.rightReservations, s.rightKey, s.haveRight, err = s.right.group(ctx)
		if err == nil && s.haveRight {
			if s.rightGroup.Rows > int(^uint(0)>>1)/16 {
				return fmt.Errorf("merge join right-group location bytes overflow int")
			}
			if s.right.budget != nil {
				s.rightLocationMem, err = s.right.budget.Reserve(ctx, int64(s.rightGroup.Rows)*16)
				if err != nil {
					return err
				}
			}
			s.rightLocations = make([]rowLocation, s.rightGroup.Rows)
			for row := range s.rightLocations {
				s.rightLocations[row] = rowLocation{row: row}
			}
			s.buildRows += int64(s.rightGroup.Rows)
			s.buildBytes += memory.BatchBytes(s.rightGroup)
		}
	}
	return err
}

func (s *mergeJoinStream) releaseLeft() {
	if s.leftGroup != nil {
		s.leftGroup.ReleaseOwned()
	}
	for _, reservation := range s.leftReservations {
		reservation.Release()
	}
	s.leftReservations = nil
	s.leftGroup, s.haveLeft, s.matchLeft = nil, false, 0
}

func (s *mergeJoinStream) releaseRight() {
	if s.rightGroup != nil {
		s.rightGroup.ReleaseOwned()
	}
	for _, reservation := range s.rightReservations {
		reservation.Release()
	}
	if s.rightLocationMem != nil {
		s.rightLocationMem.Release()
		s.rightLocationMem = nil
	}
	s.rightReservations = nil
	s.rightLocations = nil
	s.rightGroup, s.haveRight, s.matchRight = nil, false, 0
}

func (s *mergeJoinStream) emit(ctx context.Context, dst *memory.Batch, left, right *memory.Batch, pairs []join.Pair) (bool, error) {
	materializer := *s.owner
	if right != nil {
		materializer.rightBatches = []*memory.Batch{right}
		materializer.buildLoc = s.rightLocations
	}
	var estimate int64
	if s.outputBudget != nil {
		var err error
		estimate, err = estimateJoinMaterializationBytes(&materializer, left, pairs)
		if err != nil {
			return false, err
		}
		s.outputReservation, err = s.outputBudget.Reserve(ctx, estimate)
		if err != nil {
			return false, err
		}
	}
	if err := materializeJoinBatch(dst, &materializer, left, pairs); err != nil {
		if s.outputReservation != nil {
			s.outputReservation.Release()
			s.outputReservation = nil
		}
		return false, err
	}
	actual := memory.BatchBytes(dst)
	if s.outputBudget != nil && actual > estimate {
		dst.ReleaseOwned()
		s.outputReservation.Release()
		s.outputReservation = nil
		return false, fmt.Errorf("merge join materialization used %d bytes beyond pre-reserved %d", actual, estimate)
	}
	s.output += int64(len(pairs))
	s.outputBytes += actual
	return true, nil
}

// Next emits a bounded cross-product slice while retaining at most the current
// sorted key group from each input. This strategy is selected only outside the
// strict Bounded contract because one adversarial duplicate group is unbounded.
func (s *mergeJoinStream) Next(ctx context.Context, dst *memory.Batch) (bool, error) {
	if dst == nil {
		return false, fmt.Errorf("merge join destination is nil")
	}
	dst.ClearColumns()
	if s.outputReservation != nil {
		s.outputReservation.Release()
		s.outputReservation = nil
	}
	if s.closed {
		return false, source.ErrBatchStreamClosed
	}
	if s.exhausted {
		return false, nil
	}
	leftOrdering := s.owner.left.(source.OrderingProvider).Ordering()
	for {
		if err := s.loadGroups(ctx); err != nil {
			return false, err
		}
		if !s.haveLeft && !s.haveRight {
			s.exhausted = true
			s.recordStats()
			return false, nil
		}
		if !s.haveRight {
			if s.owner.typ == join.Left || s.owner.typ == join.Full || s.owner.typ == join.Anti {
				group := s.leftGroup
				pairs := make([]join.Pair, 0, minInt(s.outputRows, group.Rows-s.matchLeft))
				for s.matchLeft < group.Rows && len(pairs) < s.outputRows {
					pairs = append(pairs, join.Pair{ProbeRow: s.matchLeft, BuildRow: -1})
					s.matchLeft++
				}
				done := s.matchLeft == group.Rows
				ok, emitErr := s.emit(ctx, dst, group, nil, pairs)
				if done {
					s.releaseLeft()
				}
				return ok, emitErr
			}
			s.releaseLeft()
			continue
		}
		if !s.haveLeft {
			if s.owner.typ == join.Right || s.owner.typ == join.Full {
				group := s.rightGroup
				pairs := make([]join.Pair, 0, minInt(s.outputRows, group.Rows-s.matchRight))
				for s.matchRight < group.Rows && len(pairs) < s.outputRows {
					pairs = append(pairs, join.Pair{ProbeRow: -1, BuildRow: s.matchRight})
					s.matchRight++
				}
				done := s.matchRight == group.Rows
				ok, emitErr := s.emit(ctx, dst, nil, group, pairs)
				if done {
					s.releaseRight()
				}
				return ok, emitErr
			}
			s.releaseRight()
			continue
		}
		cmp := compareMergeKeys(s.leftKey, s.rightKey, leftOrdering)
		if cmp == 0 && mergeKeysEqual(s.leftKey, s.rightKey) {
			if s.matchLeft == 0 && s.matchRight == 0 {
				s.matched += int64(s.leftGroup.Rows)
				if s.rightGroup.Rows > s.maxMultiplicity {
					s.maxMultiplicity = s.rightGroup.Rows
				}
			}
			switch s.owner.typ {
			case join.Semi:
				group := s.leftGroup
				pairs := make([]join.Pair, 0, minInt(s.outputRows, group.Rows-s.matchLeft))
				for s.matchLeft < group.Rows && len(pairs) < s.outputRows {
					pairs = append(pairs, join.Pair{ProbeRow: s.matchLeft, BuildRow: -1})
					s.matchLeft++
				}
				done := s.matchLeft == group.Rows
				ok, emitErr := s.emit(ctx, dst, group, nil, pairs)
				if done {
					s.releaseLeft()
					s.releaseRight()
				}
				return ok, emitErr
			case join.Anti:
				s.releaseLeft()
				s.releaseRight()
				continue
			default:
				leftGroup, rightGroup := s.leftGroup, s.rightGroup
				pairs := make([]join.Pair, 0, s.outputRows)
				for s.matchLeft < leftGroup.Rows && len(pairs) < s.outputRows {
					for s.matchRight < rightGroup.Rows && len(pairs) < s.outputRows {
						pairs = append(pairs, join.Pair{ProbeRow: s.matchLeft, BuildRow: s.matchRight})
						s.matchRight++
					}
					if s.matchRight == rightGroup.Rows {
						s.matchRight = 0
						s.matchLeft++
					}
				}
				done := s.matchLeft == leftGroup.Rows
				ok, emitErr := s.emit(ctx, dst, leftGroup, rightGroup, pairs)
				if done {
					s.releaseLeft()
					s.releaseRight()
				}
				return ok, emitErr
			}
		}
		if cmp <= 0 {
			if s.owner.typ == join.Left || s.owner.typ == join.Full || s.owner.typ == join.Anti {
				group := s.leftGroup
				pairs := make([]join.Pair, 0, minInt(s.outputRows, group.Rows-s.matchLeft))
				for s.matchLeft < group.Rows && len(pairs) < s.outputRows {
					pairs = append(pairs, join.Pair{ProbeRow: s.matchLeft, BuildRow: -1})
					s.matchLeft++
				}
				done := s.matchLeft == group.Rows
				ok, emitErr := s.emit(ctx, dst, group, nil, pairs)
				if done {
					s.releaseLeft()
				}
				return ok, emitErr
			}
			s.releaseLeft()
			continue
		}
		if s.owner.typ == join.Right || s.owner.typ == join.Full {
			group := s.rightGroup
			pairs := make([]join.Pair, 0, minInt(s.outputRows, group.Rows-s.matchRight))
			for s.matchRight < group.Rows && len(pairs) < s.outputRows {
				pairs = append(pairs, join.Pair{ProbeRow: -1, BuildRow: s.matchRight})
				s.matchRight++
			}
			done := s.matchRight == group.Rows
			ok, emitErr := s.emit(ctx, dst, nil, group, pairs)
			if done {
				s.releaseRight()
			}
			return ok, emitErr
		}
		s.releaseRight()
	}
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// Close releases current groups and both child streams.
func (s *mergeJoinStream) Close() error {
	if s == nil || s.closed {
		return nil
	}
	s.closed = true
	s.recordStats()
	if s.outputReservation != nil {
		s.outputReservation.Release()
		s.outputReservation = nil
	}
	s.releaseLeft()
	s.releaseRight()
	return errors.Join(s.left.close(), s.right.close())
}

// StreamDiagnostics reports the merge strategy's actual group and output data.
func (s *mergeJoinStream) StreamDiagnostics() map[string]string {
	return map[string]string{
		"strategy": "merge", "build_rows": fmt.Sprint(s.buildRows), "build_bytes": fmt.Sprint(s.buildBytes),
		"probe_rows": fmt.Sprint(s.probeRows), "probe_bytes": fmt.Sprint(s.probeBytes),
		"matched_probe_rows": fmt.Sprint(s.matched), "output_rows": fmt.Sprint(s.output),
		"output_bytes": fmt.Sprint(s.outputBytes), "max_multiplicity": fmt.Sprint(s.maxMultiplicity),
	}
}

func (s *mergeJoinStream) recordStats() {
	if s == nil || s.statsRecorded {
		return
	}
	s.statsRecorded = true
	if s.owner.feedback != nil {
		s.owner.feedback(JoinRuntimeFeedback{
			BuildBytes: s.buildBytes, ProbeBytes: s.probeBytes, ProbeRows: s.probeRows,
			OutputRows: s.output, MaxMultiplicity: s.maxMultiplicity,
		})
	}
	if _, nop := s.owner.observer.(observer.Nop); nop {
		return
	}
	s.owner.observer.Observe(observer.Event{
		Stage: "merge-join", Duration: time.Since(s.started), Rows: s.probeRows, Bytes: s.outputBytes,
		Attributes: s.StreamDiagnostics(),
	})
}

var _ source.BatchStreamDiagnostics = (*mergeJoinStream)(nil)
