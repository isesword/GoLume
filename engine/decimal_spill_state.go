package engine

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"unsafe"

	"gocolumn/aggregate"
	"gocolumn/memory"
)

const decimalPartialCodec = "decimal-partial-state-v1"
const decimalPartialHeaderBytes = 32
const decimalPartialRecordBytes = 72

var decimalPartialMagic = [8]byte{'G', 'L', 'D', 'P', 'A', 'R', 'T', '1'}

type decimalPartialMetadata struct{ Precision, Scale int32 }
type decimalPartialRecord struct {
	Key                   int64
	KeyValid              bool
	Sum                   aggregate.DecimalSumState
	CountValid, CountRows uint64
}

func (m decimalPartialMetadata) validate() error {
	_, err := memory.Decimal128Field("partial", true, m.Precision, m.Scale)
	return err
}
func (m decimalPartialMetadata) fingerprint() [32]byte {
	var b [16]byte
	copy(b[:8], decimalPartialMagic[:])
	binary.LittleEndian.PutUint32(b[8:12], uint32(m.Precision))
	binary.LittleEndian.PutUint32(b[12:16], uint32(m.Scale))
	return sha256.Sum256(b[:])
}
func validateDecimalPartialRecord(r decimalPartialRecord, m decimalPartialMetadata) error {
	if r.Sum.Count() != r.CountValid || r.CountValid > r.CountRows {
		return fmt.Errorf("decimal partial count relationship invalid")
	}
	var b [aggregate.DecimalSumStateEncodedBytes]byte
	return r.Sum.EncodeChecked(b[:], m.Precision)
}
func mergeDecimalPartialRecord(ctx context.Context, dst *decimalPartialRecord, dm, sm decimalPartialMetadata, src decimalPartialRecord) error {
	if ctx == nil || dst == nil {
		return fmt.Errorf("decimal partial merge requires context and destination")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := dm.validate(); err != nil {
		return err
	}
	if dm != sm || dst.KeyValid != src.KeyValid || (dst.KeyValid && dst.Key != src.Key) {
		return fmt.Errorf("decimal partial merge key or metadata mismatch")
	}
	if err := validateDecimalPartialRecord(*dst, dm); err != nil {
		return err
	}
	if err := validateDecimalPartialRecord(src, sm); err != nil {
		return err
	}
	if dst.CountRows > math.MaxUint64-src.CountRows {
		return fmt.Errorf("decimal partial row count overflow")
	}
	next := *dst
	if err := next.Sum.Merge(src.Sum); err != nil {
		return err
	}
	next.CountValid = next.Sum.Count()
	next.CountRows += src.CountRows
	if !next.KeyValid {
		next.Key = 0
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	*dst = next
	return nil
}

type encodedDecimalPartialRun struct {
	payload []byte
	lease   memory.Reservation
}

func (r *encodedDecimalPartialRun) close() {
	if r == nil {
		return
	}
	r.payload = nil
	if r.lease != nil {
		r.lease.Release()
		r.lease = nil
	}
}

type decodedDecimalPartialRun struct {
	metadata decimalPartialMetadata
	records  []decimalPartialRecord
	lease    memory.Reservation
}

func (r *decodedDecimalPartialRun) close() {
	if r == nil {
		return
	}
	r.records = nil
	if r.lease != nil {
		r.lease.Release()
		r.lease = nil
	}
}

func encodeDecimalPartialRun(ctx context.Context, m decimalPartialMetadata, records []decimalPartialRecord, manager memory.Manager) (*encodedDecimalPartialRun, error) {
	if ctx == nil || manager == nil || manager.Limit() <= 0 {
		return nil, fmt.Errorf("decimal partial encode requires context and finite manager")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := m.validate(); err != nil {
		return nil, err
	}
	if len(records) > (math.MaxInt-decimalPartialHeaderBytes)/decimalPartialRecordBytes {
		return nil, fmt.Errorf("decimal partial length overflow")
	}
	size := decimalPartialHeaderBytes + len(records)*decimalPartialRecordBytes
	if uint64(size) > math.MaxInt64-256 {
		return nil, fmt.Errorf("decimal partial budget overflow")
	}
	lease, err := manager.Reserve(ctx, int64(size)+256)
	if err != nil {
		return nil, err
	}
	out := &encodedDecimalPartialRun{lease: lease}
	fail := func(e error) (*encodedDecimalPartialRun, error) { out.close(); return nil, e }
	// Validate every record before allocating its payload.
	for i, r := range records {
		if i&255 == 0 {
			if err := ctx.Err(); err != nil {
				return fail(err)
			}
		}
		if err := validateDecimalPartialRecord(r, m); err != nil {
			return fail(err)
		}
	}
	out.payload = make([]byte, size)
	b := out.payload
	copy(b[:8], decimalPartialMagic[:])
	binary.LittleEndian.PutUint16(b[8:10], 1)
	binary.LittleEndian.PutUint32(b[12:16], 1) // one nullable Int64 key + SUM/COUNT kind
	binary.LittleEndian.PutUint32(b[16:20], uint32(m.Precision))
	binary.LittleEndian.PutUint32(b[20:24], uint32(m.Scale))
	binary.LittleEndian.PutUint64(b[24:32], uint64(len(records)))
	for i, r := range records {
		if i&255 == 0 {
			if err := ctx.Err(); err != nil {
				return fail(err)
			}
		}
		p := b[decimalPartialHeaderBytes+i*decimalPartialRecordBytes:][:decimalPartialRecordBytes]
		if r.KeyValid {
			p[8] = 1
			binary.LittleEndian.PutUint64(p[:8], uint64(r.Key))
		}
		if err := r.Sum.EncodeChecked(p[16:56], m.Precision); err != nil {
			return fail(err)
		}
		binary.LittleEndian.PutUint64(p[56:64], r.CountValid)
		binary.LittleEndian.PutUint64(p[64:72], r.CountRows)
	}
	if err := ctx.Err(); err != nil {
		return fail(err)
	}
	return out, nil
}

func decodeDecimalPartialRun(ctx context.Context, m decimalPartialMetadata, b []byte, manager memory.Manager) (*decodedDecimalPartialRun, error) {
	if ctx == nil || manager == nil || manager.Limit() <= 0 {
		return nil, fmt.Errorf("decimal partial decode requires context and finite manager")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := m.validate(); err != nil {
		return nil, err
	}
	corrupt := func(s string) error { return fmt.Errorf("%w: decimal partial %s", memory.ErrSpillCorrupt, s) }
	if len(b) < decimalPartialHeaderBytes {
		return nil, corrupt("short header")
	}
	if string(b[:8]) != string(decimalPartialMagic[:]) || binary.LittleEndian.Uint16(b[8:10]) != 1 || binary.LittleEndian.Uint16(b[10:12]) != 0 || binary.LittleEndian.Uint32(b[12:16]) != 1 {
		return nil, corrupt("magic/version/flags/kind")
	}
	if int32(binary.LittleEndian.Uint32(b[16:20])) != m.Precision || int32(binary.LittleEndian.Uint32(b[20:24])) != m.Scale {
		return nil, corrupt("metadata mismatch")
	}
	count := binary.LittleEndian.Uint64(b[24:32])
	if count > uint64((math.MaxInt-decimalPartialHeaderBytes)/decimalPartialRecordBytes) || uint64(len(b)) != decimalPartialHeaderBytes+count*decimalPartialRecordBytes {
		return nil, corrupt("record count/length")
	}
	width := uint64(unsafe.Sizeof(decimalPartialRecord{}))
	if count > (math.MaxInt64-256)/width {
		return nil, corrupt("owner size overflow")
	}
	lease, err := manager.Reserve(ctx, int64(count*width)+256)
	if err != nil {
		return nil, err
	}
	out := &decodedDecimalPartialRun{metadata: m, lease: lease}
	fail := func(e error) (*decodedDecimalPartialRun, error) { out.close(); return nil, e }
	out.records = make([]decimalPartialRecord, int(count))
	for i := range out.records {
		if i&255 == 0 {
			if err := ctx.Err(); err != nil {
				return fail(err)
			}
		}
		p := b[decimalPartialHeaderBytes+i*decimalPartialRecordBytes:][:decimalPartialRecordBytes]
		if p[8] > 1 {
			return fail(corrupt("key validity"))
		}
		for _, v := range p[9:16] {
			if v != 0 {
				return fail(corrupt("reserved record flags"))
			}
		}
		r := &out.records[i]
		r.Key = int64(binary.LittleEndian.Uint64(p[:8]))
		r.KeyValid = p[8] == 1
		if !r.KeyValid && r.Key != 0 {
			return fail(corrupt("noncanonical NULL key"))
		}
		if err := r.Sum.DecodeChecked(p[16:56], m.Precision); err != nil {
			return fail(errors.Join(corrupt("SUM state"), err))
		}
		r.CountValid = binary.LittleEndian.Uint64(p[56:64])
		r.CountRows = binary.LittleEndian.Uint64(p[64:72])
		if r.CountValid != r.Sum.Count() || r.CountValid > r.CountRows {
			return fail(corrupt("count relationship"))
		}
	}
	if err := ctx.Err(); err != nil {
		return fail(err)
	}
	return out, nil
}

// decimalPartialFile owns one committed file, not an aggregation stream.
// A failed close keeps the handle and lease intact so deletion can be retried.
type decimalPartialFile struct {
	store    *memory.QuerySpillStore
	handle   memory.SpillHandle
	metadata decimalPartialMetadata
	lease    memory.Reservation
}

func (f *decimalPartialFile) close(ctx context.Context) error {
	if f == nil || f.handle == nil {
		return nil
	}
	if err := f.store.Delete(ctx, f.handle); err != nil {
		return err
	}
	f.handle = nil
	if f.lease != nil {
		f.lease.Release()
		f.lease = nil
	}
	return nil
}
func spillDecimalPartialRun(ctx context.Context, store *memory.QuerySpillStore, m decimalPartialMetadata, records []decimalPartialRecord, manager memory.Manager) (*decimalPartialFile, error) {
	if ctx == nil || store == nil || store.Limit() <= 0 || manager == nil || manager.Limit() <= 0 {
		return nil, fmt.Errorf("decimal partial spill requires finite store and manager")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	ownerLease, err := manager.Reserve(ctx, 256)
	if err != nil {
		return nil, err
	}
	encoded, err := encodeDecimalPartialRun(ctx, m, records, manager)
	if err != nil {
		ownerLease.Release()
		return nil, err
	}
	defer encoded.close()
	handle, err := store.SpillWithMetadata(ctx, memory.SpillObject{Kind: memory.SpillAggregateState, Codec: decimalPartialCodec, SchemaFingerprint: m.fingerprint(), Payload: encoded.payload}, manager)
	if err != nil {
		ownerLease.Release()
		return nil, err
	}
	out := &decimalPartialFile{store: store, handle: handle, metadata: m, lease: ownerLease}
	if err := ctx.Err(); err != nil {
		cleanup := out.close(context.Background())
		if cleanup != nil {
			return out, errors.Join(err, cleanup)
		}
		return nil, err
	}
	return out, nil
}
func restoreDecimalPartialRun(ctx context.Context, f *decimalPartialFile, manager memory.Manager) (*decodedDecimalPartialRun, error) {
	if ctx == nil || f == nil || f.handle == nil || manager == nil || manager.Limit() <= 0 {
		return nil, fmt.Errorf("decimal partial restore requires live file and finite manager")
	}
	restored, err := f.store.Restore(ctx, f.handle, f.metadata.fingerprint(), manager)
	if err != nil {
		return nil, err
	}
	if restored.Object.Kind != memory.SpillAggregateState || restored.Object.Codec != decimalPartialCodec {
		return nil, errors.Join(fmt.Errorf("%w: decimal partial object kind/codec", memory.ErrSpillCorrupt), restored.Close())
	}
	out, decodeErr := decodeDecimalPartialRun(ctx, f.metadata, restored.Object.Payload, manager)
	closeErr := restored.Close()
	if err := errors.Join(decodeErr, closeErr, ctx.Err()); err != nil {
		if out != nil {
			out.close()
		}
		return nil, err
	}
	return out, nil
}
