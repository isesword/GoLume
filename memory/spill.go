package memory

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"hash"
	"io"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var (
	// ErrSpillUnsupported reports that no external spill store is configured.
	ErrSpillUnsupported = errors.New("spill is not configured")
	// ErrSpillQuotaExceeded reports a write that would exceed the query disk cap.
	ErrSpillQuotaExceeded = errors.New("spill disk quota exceeded")
	// ErrSpillCorrupt reports truncated, incompatible, or checksum-invalid data.
	ErrSpillCorrupt = errors.New("spill object is corrupt")
	// ErrSpillClosed reports use of a closed query spill store.
	ErrSpillClosed = errors.New("spill store closed")
	// ErrInvalidSpillHandle reports a handle from another store or deleted file.
	ErrInvalidSpillHandle = errors.New("invalid spill handle")
)

const (
	spillVersion         = uint16(1)
	spillEndianLittle    = byte(1)
	spillFixedHeaderLen  = 90
	spillDirectoryPrefix = "golume-q-"
)

var spillMagic = [8]byte{'G', 'L', 'S', 'P', 'I', 'L', 'L', '1'}

// SpillKind identifies the physical payload family. Every kind must use a
// versioned codec; arbitrary gob/interface encoding is deliberately excluded.
type SpillKind uint8

const (
	// SpillColumnarRun stores row data in a columnar/Arrow-shaped codec.
	SpillColumnarRun SpillKind = iota + 1
	// SpillAggregateState stores a versioned mergeable aggregate partition.
	SpillAggregateState
	// SpillJoinState stores a versioned hash/radix join partition.
	SpillJoinState
)

// SpillObject is one immutable versioned payload submitted to the query store.
type SpillObject struct {
	Kind              SpillKind
	Partition         int32
	Codec             string
	SchemaFingerprint [32]byte
	Payload           []byte
}

// SpillHandle identifies one committed query-scoped spill object.
type SpillHandle interface {
	ID() string
	Bytes() int64
	Kind() SpillKind
}

// RestoredSpill owns decoded payload memory and a conservative spill-buffer
// reservation that also covers codec metadata read during restore. Close must
// be called once the consumer has merged or emitted the payload.
type RestoredSpill struct {
	Object      SpillObject
	reservation Reservation
	once        sync.Once
}

// Close releases restored payload memory accounting. It is idempotent.
func (r *RestoredSpill) Close() error {
	if r == nil {
		return nil
	}
	r.once.Do(func() {
		r.Object.Payload = nil
		if r.reservation != nil {
			r.reservation.Release()
			r.reservation = nil
		}
	})
	return nil
}

// SpillManager persists, validates, restores, and deletes query spill objects.
type SpillManager interface {
	Spill(ctx context.Context, object SpillObject) (SpillHandle, error)
	Restore(ctx context.Context, handle SpillHandle, expectedSchema [32]byte, bufferManager Manager) (*RestoredSpill, error)
	Delete(ctx context.Context, handle SpillHandle) error
	Close() error
}

// MetadataSpillManager additionally admits all retained store metadata before
// allocation. The store owns these reservations until Delete/Close. Map storage
// stays charged at its high-water capacity until the store has no live handles.
// Wrappers must explicitly forward this capability to preserve its guarantee.
type MetadataSpillManager interface {
	SpillWithMetadata(context.Context, SpillObject, Manager) (SpillHandle, error)
}

// NoSpill rejects every spill operation without touching the filesystem.
type NoSpill struct{}

// Spill returns ErrSpillUnsupported.
func (NoSpill) Spill(context.Context, SpillObject) (SpillHandle, error) {
	return nil, ErrSpillUnsupported
}

// Restore returns ErrSpillUnsupported.
func (NoSpill) Restore(context.Context, SpillHandle, [32]byte, Manager) (*RestoredSpill, error) {
	return nil, ErrSpillUnsupported
}

// Delete returns ErrSpillUnsupported.
func (NoSpill) Delete(context.Context, SpillHandle) error { return ErrSpillUnsupported }

// Close is a no-op for an unconfigured store.
func (NoSpill) Close() error { return nil }

// SpillOptions defines an explicit disk directory and hard byte quota.
// Directory may be empty to use the process temporary directory; MaxBytes must
// always be positive so GoLume never silently consumes unbounded disk.
type SpillOptions struct {
	Directory string
	MaxBytes  int64
}

// Enabled reports whether disk spill is explicitly configured.
func (o SpillOptions) Enabled() bool { return o.MaxBytes > 0 }

// Validate rejects an unbounded spill configuration.
func (o SpillOptions) Validate() error {
	if o.MaxBytes <= 0 {
		return fmt.Errorf("spill max bytes must be positive")
	}
	return nil
}

// ValidateOptional accepts the all-zero disabled configuration and otherwise
// requires the same finite disk quota as Validate.
func (o SpillOptions) ValidateOptional() error {
	if o.Directory == "" && o.MaxBytes == 0 {
		return nil
	}
	return o.Validate()
}

// SpillResourceError reports disk quota failures without exposing payload or
// source paths in the error string.
type SpillResourceError struct {
	RequestedBytes int64
	UsedBytes      int64
	LimitBytes     int64
}

// Error describes requested, used, and configured spill bytes.
func (e *SpillResourceError) Error() string {
	if e == nil {
		return ErrSpillQuotaExceeded.Error()
	}
	return fmt.Sprintf("spill disk quota exceeded: requested=%d used=%d limit=%d", e.RequestedBytes, e.UsedBytes, e.LimitBytes)
}

// Unwrap supports errors.Is(err, ErrSpillQuotaExceeded).
func (*SpillResourceError) Unwrap() error { return ErrSpillQuotaExceeded }

type spillWriteFile interface {
	io.Writer
	Sync() error
	Close() error
	Chmod(os.FileMode) error
}

type spillFileOps struct {
	createTemp func(string, string) (spillWriteFile, string, error)
	open       func(string) (*os.File, error)
	rename     func(string, string) error
	syncDir    func(string) error
	remove     func(string) error
	removeAll  func(string) error
	chmod      func(string, os.FileMode) error
}

func defaultSpillFileOps() spillFileOps {
	return spillFileOps{
		createTemp: func(dir, pattern string) (spillWriteFile, string, error) {
			file, err := os.CreateTemp(dir, pattern)
			if err != nil {
				return nil, "", err
			}
			return file, file.Name(), nil
		},
		open:   os.Open,
		rename: os.Rename,
		syncDir: func(path string) error {
			dir, err := os.Open(path)
			if err != nil {
				return err
			}
			defer dir.Close()
			return dir.Sync()
		},
		remove: os.Remove, removeAll: os.RemoveAll, chmod: os.Chmod,
	}
}

// QuerySpillStore owns one secure temporary directory and its committed files.
type QuerySpillStore struct {
	dir      string
	maxBytes int64
	used     atomic.Int64
	nextID   atomic.Uint64
	closed   atomic.Bool

	mu             sync.Mutex
	handles        map[string]*spillHandle
	handleCapacity int
	mapReservation Reservation
	mapManager     Manager
	closeErr       error
	ops            spillFileOps
}

// NewQuerySpillStore creates a mode-0700 query directory. No directory should
// be created unless spill was explicitly configured by the embedding process.
func NewQuerySpillStore(options SpillOptions) (*QuerySpillStore, error) {
	if err := options.Validate(); err != nil {
		return nil, err
	}
	base := options.Directory
	if base == "" {
		base = os.TempDir()
	}
	dir, err := os.MkdirTemp(base, spillDirectoryPrefix)
	if err != nil {
		return nil, fmt.Errorf("create query spill directory: %w", err)
	}
	ops := defaultSpillFileOps()
	if err := ops.chmod(dir, 0o700); err != nil {
		_ = ops.removeAll(dir)
		return nil, fmt.Errorf("secure query spill directory: %w", err)
	}
	info, err := os.Stat(dir)
	if err != nil || info.Mode().Perm() != 0o700 {
		_ = ops.removeAll(dir)
		if err != nil {
			return nil, fmt.Errorf("stat query spill directory: %w", err)
		}
		return nil, fmt.Errorf("query spill directory permissions=%#o want=0700", info.Mode().Perm())
	}
	return &QuerySpillStore{dir: dir, maxBytes: options.MaxBytes, handles: make(map[string]*spillHandle), ops: ops}, nil
}

// Directory returns the query-scoped spill directory for diagnostics and
// cleanup tooling. File names never contain source paths or column values.
func (s *QuerySpillStore) Directory() string {
	if s == nil {
		return ""
	}
	return s.dir
}

// Used returns committed on-disk bytes, including headers and checksums.
func (s *QuerySpillStore) Used() int64 {
	if s == nil {
		return 0
	}
	return s.used.Load()
}

// Limit returns the configured query disk quota.
func (s *QuerySpillStore) Limit() int64 {
	if s == nil {
		return 0
	}
	return s.maxBytes
}

type spillHandle struct {
	owner     *QuerySpillStore
	id        string
	path      string
	bytes     int64
	kind      SpillKind
	codec     string
	partition int32
	schema    [32]byte
	deleted   atomic.Bool
	metadata  Reservation
}

// ID returns the query-local opaque object identifier.
func (h *spillHandle) ID() string {
	if h == nil {
		return ""
	}
	return h.id
}

// Bytes returns committed disk bytes including metadata and payload.
func (h *spillHandle) Bytes() int64 {
	if h == nil {
		return 0
	}
	return h.bytes
}

// Kind returns the versioned physical payload family.
func (h *spillHandle) Kind() SpillKind {
	if h == nil {
		return 0
	}
	return h.kind
}

// Spill commits an immutable file using write-sync-close-rename. Disk quota is
// reserved before the temporary file is created and rolled back exactly once.
func (s *QuerySpillStore) Spill(ctx context.Context, object SpillObject) (SpillHandle, error) {
	return s.spill(ctx, object, nil)
}

// SpillWithMetadata admits handle, path, ID, and map storage to manager before
// allocating it. Payload admission remains the caller's responsibility.
func (s *QuerySpillStore) SpillWithMetadata(ctx context.Context, object SpillObject, manager Manager) (SpillHandle, error) {
	if manager == nil {
		return nil, fmt.Errorf("spill metadata requires a memory manager")
	}
	return s.spill(ctx, object, manager)
}
func (s *QuerySpillStore) spill(ctx context.Context, object SpillObject, manager Manager) (SpillHandle, error) {
	if s == nil {
		return nil, ErrSpillUnsupported
	}
	if ctx == nil {
		return nil, fmt.Errorf("spill context is nil")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if s.closed.Load() {
		return nil, ErrSpillClosed
	}
	if object.Kind < SpillColumnarRun || object.Kind > SpillJoinState {
		return nil, fmt.Errorf("invalid spill kind %d", object.Kind)
	}
	if object.Codec == "" || len(object.Codec) > math.MaxUint16 {
		return nil, fmt.Errorf("spill codec length must be between 1 and %d", math.MaxUint16)
	}
	var metadata Reservation
	if manager != nil {
		// Includes the handle, reservation, ID, immutable path, temporary path,
		// header, and conservative fixed write-time bookkeeping. The directory is
		// caller-controlled, so its byte length must never be hidden in a constant.
		if int64(len(s.dir)) > (math.MaxInt64-512-2*int64(len(object.Codec)))/2 {
			return nil, ErrBudgetExceeded
		}
		var err error
		metadata, err = manager.Reserve(ctx, 512+2*int64(len(s.dir))+2*int64(len(object.Codec)))
		if err != nil {
			return nil, err
		}
		defer func() {
			if metadata != nil {
				metadata.Release()
			}
		}()
	}
	// Admission precedes creating or committing a file. Serialize the handle
	// capacity claim through insertion so concurrent writers cannot consume it.
	// This also lets Close wait for an in-flight commit and own its cleanup.
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed.Load() {
		return nil, ErrSpillClosed
	}
	if err := s.admitHandleMap(ctx, manager); err != nil {
		return nil, err
	}
	inserted := false
	defer func() {
		if !inserted {
			s.releaseEmptyMap()
		}
	}()
	total, ok := checkedSpillSize(len(object.Codec), len(object.Payload))
	if !ok {
		return nil, fmt.Errorf("spill object size overflows int64")
	}
	if err := s.reserveDisk(total); err != nil {
		return nil, err
	}
	reserved := true
	defer func() {
		if reserved {
			s.used.Add(-total)
		}
	}()

	id := fmt.Sprintf("spill-%06d", s.nextID.Add(1))
	finalPath := filepath.Join(s.dir, id+".gls")
	file, tempPath, err := s.ops.createTemp(s.dir, ".tmp-*")
	if err != nil {
		return nil, fmt.Errorf("create spill temp file: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = file.Close()
			_ = s.ops.remove(tempPath)
		}
	}()
	if err := file.Chmod(0o600); err != nil {
		return nil, fmt.Errorf("secure spill temp file: %w", err)
	}
	header := encodeSpillHeader(object)
	for _, part := range [][]byte{header, []byte(object.Codec), object.Payload} {
		if err := writeFull(ctx, file, part); err != nil {
			return nil, fmt.Errorf("write spill object: %w", err)
		}
	}
	if err := file.Sync(); err != nil {
		return nil, fmt.Errorf("sync spill object: %w", err)
	}
	if err := file.Close(); err != nil {
		return nil, fmt.Errorf("close spill object: %w", err)
	}
	if err := s.ops.rename(tempPath, finalPath); err != nil {
		return nil, fmt.Errorf("commit spill object: %w", err)
	}
	committed = true
	handle := &spillHandle{owner: s, id: id, path: finalPath, bytes: total, kind: object.Kind, codec: object.Codec, partition: object.Partition, schema: object.SchemaFingerprint, metadata: metadata}
	s.handles[id] = handle
	inserted = true
	metadata = nil
	reserved = false
	// Once rename succeeds the store owns the file and quota, even when a
	// durability failure is followed by a failed unlink. Retain that handle
	// for Close retry rather than falsely releasing quota or losing the path.
	if syncErr := s.ops.syncDir(s.dir); syncErr != nil {
		removeErr := s.ops.remove(finalPath)
		if removeErr == nil || errors.Is(removeErr, os.ErrNotExist) {
			delete(s.handles, id)
			s.used.Add(-total)
			if handle.metadata != nil {
				handle.metadata.Release()
				handle.metadata = nil
			}
			s.releaseEmptyMap()
		}
		return nil, fmt.Errorf("sync spill directory: %w", errors.Join(syncErr, removeErr))
	}
	return handle, nil
}

// admitHandleMap enters and returns with mu held. Reservation callbacks run
// without mu: an OperatorBudget may synchronously reclaim through this store.
// Capacity is checked again after each unlocked call; the caller retains mu
// from final admission through insertion, before creating any spill file.
func (s *QuerySpillStore) admitHandleMap(ctx context.Context, requested Manager) error {
	for {
		if s.closed.Load() {
			return ErrSpillClosed
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		manager := requested
		if manager == nil {
			manager = s.mapManager
		}
		if manager == nil {
			if s.handles == nil {
				s.handles = make(map[string]*spillHandle)
			}
			return nil // Legacy Spill callers retain their old contract.
		}
		if s.mapReservation != nil && len(s.handles) < s.handleCapacity {
			return nil
		}
		capacity := 8
		for capacity <= len(s.handles) {
			if capacity > math.MaxInt/2 {
				return ErrBudgetExceeded
			}
			capacity *= 2
		}
		if int64(capacity) > (math.MaxInt64-128)/128 {
			return ErrBudgetExceeded
		}
		// Includes load-factor slack, buckets and internal rehash overlap. The
		// complete replacement is charged while the original map remains live.
		s.mu.Unlock()
		reservation, err := manager.Reserve(ctx, 128+int64(capacity)*128)
		s.mu.Lock()
		if err != nil {
			return err
		}
		if s.closed.Load() || ctx.Err() != nil || len(s.handles) >= capacity || (s.mapReservation != nil && len(s.handles) < s.handleCapacity) {
			// The callback or another writer changed the needed capacity. No file has
			// been created. Release this unused claim outside mu and recompute.
			s.mu.Unlock()
			reservation.Release()
			s.mu.Lock()
			continue
		}
		replacement := make(map[string]*spillHandle, capacity)
		for id, h := range s.handles {
			replacement[id] = h
		}
		old := s.mapReservation
		s.handles = replacement
		s.handleCapacity = capacity
		s.mapReservation = reservation
		s.mapManager = manager
		if old == nil {
			return nil
		}
		// Release is external too. Recheck after reacquiring mu because another
		// operation can fill, clear, or replace the newly installed map meanwhile.
		s.mu.Unlock()
		old.Release()
		s.mu.Lock()
	}
}

func (s *QuerySpillStore) releaseEmptyMap() {
	if len(s.handles) != 0 {
		return
	}
	s.handles = nil
	s.handleCapacity = 0
	if s.mapReservation != nil {
		s.mapReservation.Release()
		s.mapReservation = nil
	}
	s.mapManager = nil
}

func checkedSpillSize(codec, payload int) (int64, bool) {
	if codec < 0 || payload < 0 || int64(codec) > math.MaxInt64-spillFixedHeaderLen {
		return 0, false
	}
	total := int64(spillFixedHeaderLen + codec)
	if int64(payload) > math.MaxInt64-total {
		return 0, false
	}
	return total + int64(payload), true
}

func (s *QuerySpillStore) reserveDisk(bytes int64) error {
	for {
		used := s.used.Load()
		if bytes > s.maxBytes || used > s.maxBytes-bytes {
			return &SpillResourceError{RequestedBytes: bytes, UsedBytes: used, LimitBytes: s.maxBytes}
		}
		if s.used.CompareAndSwap(used, used+bytes) {
			return nil
		}
	}
}

func encodeSpillHeader(object SpillObject) []byte {
	header := make([]byte, spillFixedHeaderLen)
	copy(header[:8], spillMagic[:])
	binary.LittleEndian.PutUint16(header[8:10], spillVersion)
	header[10] = spillEndianLittle
	header[11] = byte(object.Kind)
	binary.LittleEndian.PutUint16(header[12:14], uint16(len(object.Codec)))
	copy(header[14:46], object.SchemaFingerprint[:])
	binary.LittleEndian.PutUint32(header[46:50], uint32(object.Partition))
	binary.LittleEndian.PutUint64(header[50:58], uint64(len(object.Payload)))
	checksum := sha256.Sum256(object.Payload)
	copy(header[58:90], checksum[:])
	return header
}

func writeFull(ctx context.Context, writer io.Writer, data []byte) error {
	for len(data) > 0 {
		if err := ctx.Err(); err != nil {
			return err
		}
		n, err := writer.Write(data)
		if n < 0 || n > len(data) {
			return fmt.Errorf("invalid write count %d", n)
		}
		data = data[n:]
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
	}
	return nil
}

// Restore validates metadata and checksum, reserving spill-buffer memory
// before allocating payload bytes. expectedSchema must match the file header.
func (s *QuerySpillStore) Restore(ctx context.Context, handle SpillHandle, expectedSchema [32]byte, bufferManager Manager) (*RestoredSpill, error) {
	if s == nil {
		return nil, ErrSpillUnsupported
	}
	if ctx == nil {
		return nil, fmt.Errorf("spill restore context is nil")
	}
	if bufferManager == nil {
		return nil, fmt.Errorf("spill restore requires a buffer memory manager")
	}
	h, err := s.validateHandle(handle)
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	file, err := s.ops.open(h.path)
	if err != nil {
		return nil, fmt.Errorf("open spill object: %w", err)
	}
	closed := false
	defer func() {
		if !closed {
			_ = file.Close()
		}
	}()
	header := make([]byte, spillFixedHeaderLen)
	if _, err := io.ReadFull(file, header); err != nil {
		return nil, corruptSpill("read header", err)
	}
	metadata, payloadBytes, checksum, err := decodeSpillHeader(header)
	if err != nil {
		return nil, err
	}
	if metadata.Kind != h.kind || metadata.Partition != h.partition || metadata.SchemaFingerprint != h.schema || metadata.SchemaFingerprint != expectedSchema {
		return nil, corruptSpill("handle or expected schema mismatch", nil)
	}
	info, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("stat spill object: %w", err)
	}
	if payloadBytes > uint64(math.MaxInt) {
		return nil, corruptSpill("payload exceeds addressable memory", nil)
	}
	wantSize, ok := checkedSpillSize(metadata.codecLength, int(payloadBytes))
	if !ok || info.Size() != wantSize || wantSize != h.bytes {
		return nil, corruptSpill("file length mismatch", nil)
	}
	reserveBytes := int64(payloadBytes) + int64(metadata.codecLength)
	reservation, err := bufferManager.Reserve(ctx, reserveBytes)
	if err != nil {
		return nil, fmt.Errorf("reserve spill restore buffer: %w", err)
	}
	codecBytes := make([]byte, metadata.codecLength)
	if _, err := io.ReadFull(file, codecBytes); err != nil {
		reservation.Release()
		return nil, corruptSpill("read codec", err)
	}
	if string(codecBytes) != h.codec {
		reservation.Release()
		return nil, corruptSpill("codec mismatch", nil)
	}
	metadata.Codec = h.codec
	payload := make([]byte, int(payloadBytes))
	if _, err := io.ReadFull(file, payload); err != nil {
		reservation.Release()
		return nil, corruptSpill("read payload", err)
	}
	if sha256.Sum256(payload) != checksum {
		reservation.Release()
		return nil, corruptSpill("payload checksum mismatch", nil)
	}
	if err := file.Close(); err != nil {
		reservation.Release()
		return nil, fmt.Errorf("close restored spill object: %w", err)
	}
	closed = true
	metadata.Payload = payload
	return &RestoredSpill{Object: metadata.SpillObject, reservation: reservation}, nil
}

type decodedSpillObject struct {
	SpillObject
	codecLength int
}

func decodeSpillHeader(header []byte) (decodedSpillObject, uint64, [32]byte, error) {
	var checksum [32]byte
	if len(header) != spillFixedHeaderLen || string(header[:8]) != string(spillMagic[:]) {
		return decodedSpillObject{}, 0, checksum, corruptSpill("magic mismatch", nil)
	}
	if binary.LittleEndian.Uint16(header[8:10]) != spillVersion {
		return decodedSpillObject{}, 0, checksum, corruptSpill("version mismatch", nil)
	}
	if header[10] != spillEndianLittle {
		return decodedSpillObject{}, 0, checksum, corruptSpill("endianness mismatch", nil)
	}
	object := decodedSpillObject{SpillObject: SpillObject{Kind: SpillKind(header[11]), Partition: int32(binary.LittleEndian.Uint32(header[46:50]))}, codecLength: int(binary.LittleEndian.Uint16(header[12:14]))}
	if object.Kind < SpillColumnarRun || object.Kind > SpillJoinState {
		return decodedSpillObject{}, 0, checksum, corruptSpill("invalid kind", nil)
	}
	copy(object.SchemaFingerprint[:], header[14:46])
	payloadBytes := binary.LittleEndian.Uint64(header[50:58])
	copy(checksum[:], header[58:90])
	return object, payloadBytes, checksum, nil
}

func corruptSpill(detail string, cause error) error {
	if cause == nil {
		return fmt.Errorf("%w: %s", ErrSpillCorrupt, detail)
	}
	return fmt.Errorf("%w: %s: %v", ErrSpillCorrupt, detail, cause)
}

func (s *QuerySpillStore) validateHandle(handle SpillHandle) (*spillHandle, error) {
	if s.closed.Load() {
		return nil, ErrSpillClosed
	}
	h, ok := handle.(*spillHandle)
	if !ok || h == nil || h.owner != s || h.deleted.Load() {
		return nil, ErrInvalidSpillHandle
	}
	s.mu.Lock()
	current := s.handles[h.id]
	s.mu.Unlock()
	if current != h {
		return nil, ErrInvalidSpillHandle
	}
	return h, nil
}

// Delete removes one committed object and returns its disk quota. It is safe
// to call repeatedly; a failed filesystem delete remains retryable.
func (s *QuerySpillStore) Delete(ctx context.Context, handle SpillHandle) error {
	if s == nil {
		return ErrSpillUnsupported
	}
	if ctx == nil {
		return fmt.Errorf("spill delete context is nil")
	}
	h, ok := handle.(*spillHandle)
	if !ok || h == nil || h.owner != s {
		return ErrInvalidSpillHandle
	}
	if h.deleted.Load() {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if h.deleted.Load() {
		return nil
	}
	if s.handles[h.id] != h {
		return ErrInvalidSpillHandle
	}
	if err := s.ops.remove(h.path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("delete spill object: %w", err)
	}
	delete(s.handles, h.id)
	h.deleted.Store(true)
	if h.metadata != nil {
		h.metadata.Release()
		h.metadata = nil
	}
	s.releaseEmptyMap()
	s.used.Add(-h.bytes)
	return nil
}

// Close deletes every query spill object and then its directory. It is
// idempotent and preserves the first cleanup error for diagnostics.
func (s *QuerySpillStore) Close() error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed.Swap(true) {
		return s.closeErr
	}
	if err := s.ops.removeAll(s.dir); err != nil {
		s.closeErr = fmt.Errorf("remove query spill directory: %w", err)
		return s.closeErr
	}
	for _, h := range s.handles {
		h.deleted.Store(true)
		if h.metadata != nil {
			h.metadata.Release()
			h.metadata = nil
		}
	}
	s.handles = nil
	s.releaseEmptyMap()
	s.used.Store(0)
	return nil
}

// CleanupOrphanSpillDirs removes only GoLume query directories older than the
// cutoff. Symlinks and non-matching names are ignored.
func CleanupOrphanSpillDirs(base string, olderThan time.Duration, now time.Time) (int, error) {
	if base == "" {
		base = os.TempDir()
	}
	entries, err := os.ReadDir(base)
	if err != nil {
		return 0, err
	}
	removed := 0
	for _, entry := range entries {
		if !entry.IsDir() || entry.Type()&os.ModeSymlink != 0 || !strings.HasPrefix(entry.Name(), spillDirectoryPrefix) {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			return removed, err
		}
		if olderThan > 0 && now.Sub(info.ModTime()) < olderThan {
			continue
		}
		if err := os.RemoveAll(filepath.Join(base, entry.Name())); err != nil {
			return removed, err
		}
		removed++
	}
	return removed, nil
}

// SchemaFingerprint returns a stable SHA-256 over logical fields, nesting,
// nullability, encoding, Arrow spelling, and dictionary domain.
func SchemaFingerprint(schema Schema) [32]byte {
	h := sha256.New()
	writeFingerprintUint32(h, uint32(len(schema.Fields)))
	for _, field := range schema.Fields {
		writeFieldFingerprint(h, field)
	}
	var out [32]byte
	copy(out[:], h.Sum(nil))
	return out
}

func writeFieldFingerprint(h hash.Hash, field Field) {
	writeFingerprintString(h, field.Name)
	writeFingerprintUint32(h, uint32(field.Type))
	if field.Nullable {
		_, _ = h.Write([]byte{1})
	} else {
		_, _ = h.Write([]byte{0})
	}
	writeFingerprintUint32(h, uint32(field.Encoding))
	writeFingerprintString(h, field.ArrowType)
	writeFingerprintString(h, field.DictionaryDomain)
	writeFingerprintUint32(h, uint32(len(field.Children)))
	for _, child := range field.Children {
		writeFieldFingerprint(h, child)
	}
}

func writeFingerprintString(h hash.Hash, value string) {
	writeFingerprintUint32(h, uint32(len(value)))
	_, _ = h.Write([]byte(value))
}

func writeFingerprintUint32(h hash.Hash, value uint32) {
	var buf [4]byte
	binary.LittleEndian.PutUint32(buf[:], value)
	_, _ = h.Write(buf[:])
}

var _ SpillManager = (*QuerySpillStore)(nil)
var _ SpillManager = NoSpill{}
