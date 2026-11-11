package pipeline

import (
	"context"
	"errors"
	"gocolumn/memory"
	"testing"
)

func TestExchangeEmptyListCloneAdmissionAndCleanup(t *testing.T) {
	b := memory.NewBatch()
	b.Rows = 3
	b.StringLists["list"] = &memory.StringListVector{Offsets: []uint32{0, 0, 0, 0}, Values: &memory.StringViewVector{}, Validity: memory.Bitmap{0b101}}
	bytes, err := nativeOwnedRangeBytes(b, 0, 3)
	if err != nil {
		t.Fatal(err)
	}
	for _, mode := range []string{"consume", "close", "cancel", "denied", "clone failure"} {
		t.Run(mode, func(t *testing.T) {
			limit := bytes
			if mode == "denied" {
				limit--
			}
			manager := memory.NewBudgetManager(limit)
			exchange, err := NewExchange(manager, bytes, 1, 1)
			if err != nil {
				t.Fatal(err)
			}
			ctx := context.Background()
			if mode == "cancel" {
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			}
			if mode == "clone failure" {
				b.Opaque["bad"] = &memory.OpaqueVector{Rows: 3, Value: struct{}{}}
				defer delete(b.Opaque, "bad")
			}
			err = exchange.enqueueRange(ctx, Sequence{}, b, MorselRange{Rows: 3, Bytes: bytes})
			switch mode {
			case "cancel":
				if !errors.Is(err, context.Canceled) {
					t.Fatal(err)
				}
			case "denied":
				if !errors.Is(err, memory.ErrBudgetExceeded) {
					t.Fatal(err)
				}
			case "clone failure":
				if err == nil {
					t.Fatal("expected opaque clone error")
				}
			default:
				if err != nil {
					t.Fatal(err)
				}
				if manager.Used() != bytes || exchange.Stats().CurrentInFlightBytes != bytes {
					t.Fatal("admission mismatch")
				}
				if mode == "close" {
					exchange.closeSignal()
				}
				item := <-exchange.items
				list := item.batch.StringLists["list"]
				if err := list.Validate(3); err != nil {
					t.Fatal(err)
				}
				if !list.IsValid(0) || list.IsValid(1) || !list.IsValid(2) || len(list.Values.Offsets) != 1 {
					t.Fatal("NULL/empty clone semantics")
				}
				exchange.release(item)
			}
			if manager.Used() != 0 || exchange.Stats().CurrentInFlightBytes != 0 || len(exchange.items) != 0 {
				t.Fatal("failed or consumed clone leaked")
			}
			if !exchange.bytes.TryAcquire(bytes) || !exchange.groupBytes[0].TryAcquire(bytes) {
				t.Fatal("semaphore capacity leaked")
			}
			exchange.bytes.Release(bytes)
			exchange.groupBytes[0].Release(bytes)
		})
	}
	if b.StringLists["list"].Values.Offsets != nil {
		t.Fatal("mutated source child")
	}
}
