package physical

import (
	"context"
	"errors"
	"gocolumn/expr"
	"gocolumn/memory"
	"reflect"
	"testing"
)

func TestFilterSelectionOwnsMaskWithoutChangingBooleanResult(t *testing.T) {
	manager := memory.NewBudgetManager(1 << 20)
	stream := &filterBatchStream{input: memory.NewBatch(), manager: manager}
	stream.input.Rows = 65
	truth := memory.Bitmap{^uint64(0), ^uint64(0)}
	validity := memory.Bitmap{3, ^uint64(0)}
	beforeTruth, beforeValidity := append(memory.Bitmap(nil), truth...), append(memory.Bitmap(nil), validity...)
	selected, err := stream.selectRows(context.Background(), expr.BoolVector{Truth: truth, Validity: validity})
	if err != nil {
		t.Fatal(err)
	}
	if selected != 3 || !reflect.DeepEqual(stream.rows, []int{0, 1, 64}) || !reflect.DeepEqual(stream.selection, memory.Bitmap{3, 1}) {
		t.Fatalf("selected=%d rows=%v mask=%v", selected, stream.rows, stream.selection)
	}
	if !reflect.DeepEqual(truth, beforeTruth) || !reflect.DeepEqual(validity, beforeValidity) {
		t.Fatal("filter mutated a shared boolean result")
	}
	stream.selectionLease.Release()
	stream.rowsLease.Release()
	if manager.Used() != 0 {
		t.Fatal("selection or row index leaked")
	}
}

func TestFilterSelectionAdmissionPreservesPreviousCapacity(t *testing.T) {
	manager := &filterRecordingManager{base: memory.NewBudgetManager(0)}
	stream := &filterBatchStream{input: memory.NewBatch(), manager: manager}
	stream.input.Rows = 1
	if _, err := stream.selectRows(context.Background(), expr.BoolVector{Truth: memory.Bitmap{1}}); err != nil {
		t.Fatal(err)
	}
	prior := &stream.selection[0]
	stream.input.Rows = 65
	manager.deny = true
	if _, err := stream.selectRows(context.Background(), expr.BoolVector{Truth: memory.Bitmap{1, 1}}); !errors.Is(err, memory.ErrBudgetExceeded) {
		t.Fatalf("expected denied replacement, got %v", err)
	}
	if manager.Used() != 8 || &stream.selection[0] != prior || manager.requests[len(manager.requests)-1] != 16 || manager.live[len(manager.live)-1] != 8 {
		t.Fatal("selection growth did not reserve replacement before releasing original")
	}
	stream.selectionLease.Release()
}
