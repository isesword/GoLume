package lazy

import (
	"gocolumn/engine"
	"gocolumn/logical"
	"gocolumn/source"
	arrowrecord "gocolumn/source/arrowrecord"
	parquetsource "gocolumn/source/parquet"
	"io"
	"sync"

	"github.com/apache/arrow-go/v18/arrow"
)

// Session owns or references the execution engine used to prepare lazy plans.
// A Session created with NewSession owns its engine and should be closed.
type Session struct {
	engine   *engine.Engine
	owned    bool
	once     sync.Once
	closeErr error
	mu       sync.Mutex
	closers  []io.Closer
}

// NewSession constructs a lazy session and an owned execution engine.
func NewSession(opts engine.Options) *Session {
	return &Session{engine: engine.New(opts), owned: true}
}

// NewSessionWithEngine constructs a lazy session around an existing engine.
// Closing the session does not close the caller-owned engine.
func NewSessionWithEngine(eng *engine.Engine) *Session {
	return &Session{engine: eng}
}

// Close releases the engine only when the session owns it.
func (s *Session) Close() error {
	if s == nil {
		return nil
	}
	s.once.Do(func() {
		s.mu.Lock()
		closers := append([]io.Closer(nil), s.closers...)
		s.closers = nil
		s.mu.Unlock()
		for _, closer := range closers {
			if err := closer.Close(); err != nil && s.closeErr == nil {
				s.closeErr = err
			}
		}
		if s.owned && s.engine != nil {
			if err := s.engine.Close(); err != nil && s.closeErr == nil {
				s.closeErr = err
			}
		}
	})
	return s.closeErr
}

// ScanParquet discovers a flat primitive Parquet schema and returns a lazy scan.
// No row group or column data is decoded until Collect, Prepare, or physical
// explain is requested.
func (s *Session) ScanParquet(path string) (LazyFrame, error) {
	src, err := parquetsource.OpenFile(path)
	if err != nil {
		return LazyFrame{}, err
	}
	return s.FromSource(src), nil
}

// ParquetScanOptions selects bounded native nested Parquet decoding. Its zero
// value preserves the flat primitive ScanParquet contract.
type ParquetScanOptions = parquetsource.Options

// ScanParquetWithOptions discovers a schema using explicit source options.
// NativeNested admits primitive lists and bounded recursive Struct forests, not
// arbitrary nested containers. Decoding remains deferred until execution.
func (s *Session) ScanParquetWithOptions(path string, options ParquetScanOptions) (LazyFrame, error) {
	src, err := parquetsource.OpenFileWithOptions(path, options)
	if err != nil {
		return LazyFrame{}, err
	}
	return s.FromSource(src), nil
}

// ScanArrow retains Arrow RecordBatches and returns a lazy scan over them.
// Every Arrow field is preserved in the logical schema; fields without a
// specialized engine vector remain available as opaque Arrow arrays. The
// session releases its RecordBatch retains when Close is called.
func (s *Session) ScanArrow(records ...arrow.RecordBatch) (LazyFrame, error) {
	return s.ScanArrowWithOptions(ArrowScanOptions{}, records...)
}

// ArrowScanOptions selects explicit per-source Arrow conversions. Its zero
// value preserves legacy opaque Decimal input behavior.
type ArrowScanOptions = arrowrecord.Options

// ScanArrowWithOptions retains records with explicit Arrow conversion options.
// NativeDecimal128 selects only top-level Decimal128; downstream Decimal
// aggregate, join, sort and window admission restrictions remain unchanged.
// NativeStructList separately enables flat fourteen-type Struct list items.
// NativeRecursiveStruct separately opts into depth-bounded recursive Struct
// transport; default Arrow admission remains one-level.
func (s *Session) ScanArrowWithOptions(options ArrowScanOptions, records ...arrow.RecordBatch) (LazyFrame, error) {
	src, err := arrowrecord.NewSourceWithOptions(options, records...)
	if err != nil {
		return LazyFrame{}, err
	}
	s.mu.Lock()
	s.closers = append(s.closers, src)
	s.mu.Unlock()
	return s.FromSource(src), nil
}

// FromSource creates a lazy frame over an existing columnar source.
func (s *Session) FromSource(src source.Source) LazyFrame {
	return LazyFrame{session: s, plan: &logical.Scan{Source: src}}
}

// Engine returns the execution engine backing the session.
func (s *Session) Engine() *engine.Engine {
	if s == nil {
		return nil
	}
	return s.engine
}
