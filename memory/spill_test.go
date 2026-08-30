package memory

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"
)

func TestQuerySpillStoreRoundTripPermissionsQuotaAndCleanup(t *testing.T) {
	base := t.TempDir()
	store, err := NewQuerySpillStore(SpillOptions{Directory: base, MaxBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	dir := store.Directory()
	info, err := os.Stat(dir)
	if err != nil || info.Mode().Perm() != 0o700 {
		t.Fatalf("directory info=%v err=%v", info, err)
	}
	schema := NewSchema(Field{Name: "key", Type: TypeUint64}, Field{Name: "value", Type: TypeInt64, Nullable: true})
	fingerprint := SchemaFingerprint(schema)
	object := SpillObject{Kind: SpillAggregateState, Partition: 7, Codec: "aggregate-u64-i64-v1", SchemaFingerprint: fingerprint, Payload: []byte("columnar-state-payload")}
	handle, err := store.Spill(context.Background(), object)
	if err != nil {
		t.Fatal(err)
	}
	internal := handle.(*spillHandle)
	fileInfo, err := os.Stat(internal.path)
	if err != nil || fileInfo.Mode().Perm() != 0o600 {
		t.Fatalf("file info=%v err=%v", fileInfo, err)
	}
	if store.Used() != handle.Bytes() || handle.Bytes() <= int64(len(object.Payload)) {
		t.Fatalf("used=%d handle=%d payload=%d", store.Used(), handle.Bytes(), len(object.Payload))
	}

	engine := NewEngineBudget(1 << 20)
	query, _ := engine.BeginQuery(BudgetOptions{HardLimitBytes: 1 << 20})
	ctx := WithQueryBudget(context.Background(), query)
	bufferManager := query.Operator("SpillRestore", MemorySpillBuffer)
	restored, err := store.Restore(ctx, handle, fingerprint, bufferManager)
	if err != nil {
		t.Fatal(err)
	}
	if string(restored.Object.Payload) != string(object.Payload) || restored.Object.Codec != object.Codec || restored.Object.Partition != 7 {
		t.Fatalf("restored=%+v", restored.Object)
	}
	wantRestoreBytes := int64(len(object.Payload) + len(object.Codec))
	if bufferManager.Used() != wantRestoreBytes {
		t.Fatalf("restore reservation=%d", bufferManager.Used())
	}
	if err := restored.Close(); err != nil {
		t.Fatal(err)
	}
	if err := restored.Close(); err != nil {
		t.Fatal(err)
	}
	if bufferManager.Used() != 0 || engine.Used() != 0 {
		t.Fatalf("restore release operator=%d engine=%d", bufferManager.Used(), engine.Used())
	}
	if err := store.Delete(context.Background(), handle); err != nil {
		t.Fatal(err)
	}
	if err := store.Delete(context.Background(), handle); err != nil {
		t.Fatal(err)
	}
	if store.Used() != 0 {
		t.Fatalf("delete used=%d", store.Used())
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(dir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("spill directory remains err=%v", err)
	}
}

func TestQuerySpillStoreQuotaFailureDoesNotCreateOrCharge(t *testing.T) {
	store, err := NewQuerySpillStore(SpillOptions{Directory: t.TempDir(), MaxBytes: 100})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	_, err = store.Spill(context.Background(), SpillObject{Kind: SpillColumnarRun, Codec: "arrow-shaped-v1", Payload: make([]byte, 64)})
	var resourceErr *SpillResourceError
	if !errors.Is(err, ErrSpillQuotaExceeded) || !errors.As(err, &resourceErr) {
		t.Fatalf("quota error=%T %v", err, err)
	}
	if store.Used() != 0 {
		t.Fatalf("quota failure used=%d", store.Used())
	}
	entries, err := os.ReadDir(store.Directory())
	if err != nil || len(entries) != 0 {
		t.Fatalf("quota failure entries=%v err=%v", entries, err)
	}
}

func TestQuerySpillStoreCancellationAndRestoreBudgetFailureAreLeakFree(t *testing.T) {
	store, err := NewQuerySpillStore(SpillOptions{Directory: t.TempDir(), MaxBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	object := SpillObject{Kind: SpillColumnarRun, Codec: ColumnarRunCodec, Payload: []byte("payload")}
	if _, err := store.Spill(cancelled, object); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled spill error=%v", err)
	}
	if store.Used() != 0 {
		t.Fatalf("cancelled spill used=%d", store.Used())
	}
	handle, err := store.Spill(context.Background(), object)
	if err != nil {
		t.Fatal(err)
	}
	manager := NewBudgetManager(1)
	if _, err := store.Restore(context.Background(), handle, object.SchemaFingerprint, manager); !errors.Is(err, ErrBudgetExceeded) {
		t.Fatalf("restore budget error=%v", err)
	}
	if manager.Used() != 0 {
		t.Fatalf("restore budget leak=%d", manager.Used())
	}
	if _, err := store.Restore(cancelled, handle, object.SchemaFingerprint, NewBudgetManager(1<<20)); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled restore error=%v", err)
	}
	before := store.Used()
	if err := store.Delete(cancelled, handle); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled delete error=%v", err)
	}
	if store.Used() != before {
		t.Fatalf("cancelled delete released quota=%d want=%d", store.Used(), before)
	}
	if err := store.Delete(context.Background(), handle); err != nil {
		t.Fatal(err)
	}
}

func TestQuerySpillStoreRejectsCorruptionAndSchemaMismatchWithoutMemoryLeak(t *testing.T) {
	tests := []struct {
		name   string
		mutate func([]byte) []byte
		wrong  bool
	}{
		{name: "magic", mutate: func(data []byte) []byte { data[0] ^= 0xff; return data }},
		{name: "version", mutate: func(data []byte) []byte { data[8]++; return data }},
		{name: "endianness", mutate: func(data []byte) []byte { data[10] = 2; return data }},
		{name: "checksum", mutate: func(data []byte) []byte { data[len(data)-1] ^= 0xff; return data }},
		{name: "truncated", mutate: func(data []byte) []byte { return data[:len(data)-1] }},
		{name: "schema-mismatch", mutate: func(data []byte) []byte { return data }, wrong: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			store, err := NewQuerySpillStore(SpillOptions{Directory: t.TempDir(), MaxBytes: 1 << 20})
			if err != nil {
				t.Fatal(err)
			}
			defer store.Close()
			fingerprint := SchemaFingerprint(NewSchema(Field{Name: "x", Type: TypeInt64}))
			handle, err := store.Spill(context.Background(), SpillObject{Kind: SpillColumnarRun, Codec: "arrow-shaped-v1", SchemaFingerprint: fingerprint, Payload: []byte("payload")})
			if err != nil {
				t.Fatal(err)
			}
			internal := handle.(*spillHandle)
			data, err := os.ReadFile(internal.path)
			if err != nil {
				t.Fatal(err)
			}
			data = tc.mutate(data)
			if err := os.WriteFile(internal.path, data, 0o600); err != nil {
				t.Fatal(err)
			}
			expected := fingerprint
			if tc.wrong {
				expected[0] ^= 0xff
			}
			manager := NewBudgetManager(1 << 20)
			_, err = store.Restore(context.Background(), handle, expected, manager)
			if !errors.Is(err, ErrSpillCorrupt) {
				t.Fatalf("restore error=%T %v", err, err)
			}
			if manager.Used() != 0 {
				t.Fatalf("corrupt restore reservation leak=%d", manager.Used())
			}
		})
	}
}

func TestQuerySpillStoreWriteFailuresRollbackQuotaAndTempFile(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
	}{
		{name: "short-write", err: nil},
		{name: "disk-full", err: syscall.ENOSPC},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store, err := NewQuerySpillStore(SpillOptions{Directory: t.TempDir(), MaxBytes: 1 << 20})
			if err != nil {
				t.Fatal(err)
			}
			defer store.Close()
			original := store.ops.createTemp
			store.ops.createTemp = func(dir, pattern string) (spillWriteFile, string, error) {
				file, name, err := original(dir, pattern)
				if err != nil {
					return nil, "", err
				}
				return &failingSpillWriteFile{spillWriteFile: file, err: tc.err}, name, nil
			}
			_, err = store.Spill(context.Background(), SpillObject{Kind: SpillColumnarRun, Codec: "arrow-shaped-v1", Payload: []byte("payload")})
			if tc.err == nil && !errors.Is(err, io.ErrShortWrite) {
				t.Fatalf("short write error=%v", err)
			}
			if tc.err != nil && !errors.Is(err, tc.err) {
				t.Fatalf("disk error=%v", err)
			}
			if store.Used() != 0 {
				t.Fatalf("failure used=%d", store.Used())
			}
			entries, readErr := os.ReadDir(store.Directory())
			if readErr != nil || len(entries) != 0 {
				t.Fatalf("failure entries=%v err=%v", entries, readErr)
			}
		})
	}
}

func TestQuerySpillStoreDirectorySyncFailureRollsBackCommittedFile(t *testing.T) {
	store, err := NewQuerySpillStore(SpillOptions{Directory: t.TempDir(), MaxBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	injected := errors.New("directory sync failed")
	store.ops.syncDir = func(string) error { return injected }
	_, err = store.Spill(context.Background(), SpillObject{Kind: SpillColumnarRun, Codec: ColumnarRunCodec, Payload: []byte("payload")})
	if !errors.Is(err, injected) {
		t.Fatalf("sync error=%v", err)
	}
	if store.Used() != 0 {
		t.Fatalf("sync failure used=%d", store.Used())
	}
	entries, err := os.ReadDir(store.Directory())
	if err != nil || len(entries) != 0 {
		t.Fatalf("sync failure entries=%v err=%v", entries, err)
	}
}

func TestQuerySpillStoreDeleteFailureIsRetryable(t *testing.T) {
	store, err := NewQuerySpillStore(SpillOptions{Directory: t.TempDir(), MaxBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	handle, err := store.Spill(context.Background(), SpillObject{Kind: SpillAggregateState, Codec: "state-v1", Payload: []byte("payload")})
	if err != nil {
		t.Fatal(err)
	}
	original := store.ops.remove
	store.ops.remove = func(string) error { return syscall.EACCES }
	if err := store.Delete(context.Background(), handle); !errors.Is(err, syscall.EACCES) {
		t.Fatalf("delete failure=%v", err)
	}
	if store.Used() != handle.Bytes() {
		t.Fatalf("failed delete released quota=%d", store.Used())
	}
	store.ops.remove = original
	if err := store.Delete(context.Background(), handle); err != nil {
		t.Fatal(err)
	}
	if store.Used() != 0 {
		t.Fatalf("retry delete used=%d", store.Used())
	}
}

func TestCleanupOrphanSpillDirsIgnoresLiveUnrelatedAndSymlink(t *testing.T) {
	base := t.TempDir()
	old := filepath.Join(base, spillDirectoryPrefix+"old")
	live := filepath.Join(base, spillDirectoryPrefix+"live")
	unrelated := filepath.Join(base, "other")
	for _, dir := range []string{old, live, unrelated} {
		if err := os.Mkdir(dir, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	now := time.Now()
	if err := os.Chtimes(old, now.Add(-2*time.Hour), now.Add(-2*time.Hour)); err != nil {
		t.Fatal(err)
	}
	symlink := filepath.Join(base, spillDirectoryPrefix+"link")
	if err := os.Symlink(unrelated, symlink); err != nil {
		t.Fatal(err)
	}
	removed, err := CleanupOrphanSpillDirs(base, time.Hour, now)
	if err != nil {
		t.Fatal(err)
	}
	if removed != 1 {
		t.Fatalf("removed=%d", removed)
	}
	if _, err := os.Stat(old); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("old directory remains err=%v", err)
	}
	for _, path := range []string{live, unrelated, symlink} {
		if _, err := os.Lstat(path); err != nil {
			t.Fatalf("preserved path %s err=%v", path, err)
		}
	}
}

func TestSchemaFingerprintIncludesNestedContract(t *testing.T) {
	base := NewSchema(Field{Name: "payload", Type: TypeStruct, Nullable: true, Children: []Field{{Name: "items", Type: TypeList, Children: []Field{{Name: "item", Type: TypeString, Nullable: true}}}}})
	same := NewSchema(base.Fields...)
	changed := NewSchema(Field{Name: "payload", Type: TypeStruct, Nullable: false, Children: base.Fields[0].Children})
	if SchemaFingerprint(base) != SchemaFingerprint(same) {
		t.Fatal("equal schemas have different fingerprints")
	}
	if SchemaFingerprint(base) == SchemaFingerprint(changed) {
		t.Fatal("nullability change did not alter fingerprint")
	}
}

type failingSpillWriteFile struct {
	spillWriteFile
	err  error
	once bool
}

func (f *failingSpillWriteFile) Write([]byte) (int, error) {
	if !f.once {
		f.once = true
		return 0, f.err
	}
	return 0, f.err
}
