package parquet

import (
	"context"
	"io"
	"testing"

	"github.com/apache/thrift/lib/go/thrift"
)

func recoveryBloomHeader(t *testing.T, order []int16, size int32, union int16, nonempty bool) []byte {
	t.Helper()
	ctx := context.Background()
	transport := thrift.NewTMemoryBufferLen(128)
	p := thrift.NewTCompactProtocol(transport)
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	must(p.WriteStructBegin(ctx, "BloomFilterHeader"))
	for _, id := range order {
		if id == 1 {
			must(p.WriteFieldBegin(ctx, "numBytes", thrift.I32, id))
			must(p.WriteI32(ctx, size))
		} else {
			must(p.WriteFieldBegin(ctx, "union", thrift.STRUCT, id))
			must(p.WriteStructBegin(ctx, "union"))
			must(p.WriteFieldBegin(ctx, "marker", thrift.STRUCT, union))
			must(p.WriteStructBegin(ctx, "marker"))
			if nonempty {
				must(p.WriteFieldBegin(ctx, "unknown", thrift.I32, 1))
				must(p.WriteI32(ctx, 0))
				must(p.WriteFieldEnd(ctx))
			}
			must(p.WriteFieldStop(ctx))
			must(p.WriteStructEnd(ctx))
			must(p.WriteFieldEnd(ctx))
			must(p.WriteFieldStop(ctx))
			must(p.WriteStructEnd(ctx))
		}
		must(p.WriteFieldEnd(ctx))
	}
	must(p.WriteFieldStop(ctx))
	must(p.WriteStructEnd(ctx))
	must(p.Flush(ctx))
	return append(append([]byte(nil), transport.Bytes()...), make([]byte, 32)...)
}

func TestRecoveryBloomHeaderValidation(t *testing.T) {
	ctx := context.Background()
	valid := recoveryBloomHeader(t, []int16{1, 2, 3, 4}, 32, 1, false)
	if !bloomHeaderValid(ctx, valid) {
		t.Fatal("valid header rejected")
	}
	if !bloomHeaderValid(ctx, recoveryBloomHeader(t, []int16{4, 1, 3, 2}, 32, 1, false)) {
		t.Fatal("reordered fields rejected")
	}
	for i := 0; i < len(valid); i++ {
		if bloomHeaderValid(ctx, valid[:i]) {
			t.Fatalf("accepted truncation %d", i)
		}
	}
	for _, data := range [][]byte{
		append(append([]byte(nil), valid...), 0),
		recoveryBloomHeader(t, []int16{1, 2, 3}, 32, 1, false),
		recoveryBloomHeader(t, []int16{1, 2, 3, 4, 4}, 32, 1, false),
		recoveryBloomHeader(t, []int16{1, 2, 3, 4}, -1, 1, false),
		recoveryBloomHeader(t, []int16{1, 2, 3, 4}, 1<<30, 1, false),
		recoveryBloomHeader(t, []int16{1, 2, 3, 4}, 31, 1, false),
		recoveryBloomHeader(t, []int16{1, 2, 3, 4}, 32, 2, false),
		recoveryBloomHeader(t, []int16{1, 2, 3, 4}, 32, 1, true),
		recoveryBloomHeader(t, []int16{1, 2, 3, 5}, 32, 1, false),
	} {
		if bloomHeaderValid(ctx, data) {
			t.Fatalf("accepted malformed %x", data)
		}
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if bloomHeaderValid(canceled, valid) {
		t.Fatal("accepted canceled parse")
	}
}
func TestRecoveryBloomCachedReader(t *testing.T) {
	r := bloomCachedReader{data: []byte{1, 2, 3}, offset: 100}
	p := make([]byte, 2)
	if n, e := r.ReadAt(p, 99); n != 0 || e != io.EOF {
		t.Fatalf("prefix %d %v", n, e)
	}
	if n, e := r.ReadAt(p, 102); n != 1 || e != io.EOF {
		t.Fatalf("suffix %d %v", n, e)
	}
	if n, e := r.ReadAt(p, 100); n != 2 || e != nil || p[0] != 1 || p[1] != 2 {
		t.Fatalf("bounded read %d %v %v", n, e, p)
	}
	if _, e := r.Read(p); e == nil {
		t.Fatal("Read must fail")
	}
	if _, e := r.Seek(0, 0); e == nil {
		t.Fatal("Seek must fail")
	}
}
