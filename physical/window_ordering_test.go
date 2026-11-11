package physical

import (
	"fmt"
	"gocolumn/memory"
	"gocolumn/source"
	"gocolumn/source/inmemory"
	"reflect"
	"testing"
)

type windowOrderingProofSource struct {
	source.Source
	ordering []source.OrderingColumn
}

func (s windowOrderingProofSource) Ordering() []source.OrderingColumn { return s.ordering }

func TestWindowOrderingPreservesOnlyUnchangedPrefix(t *testing.T) {
	ordering := []source.OrderingColumn{
		{Name: "a", Descending: true, NullsLast: true},
		{Name: "b"},
		{Name: "c", NullsLast: true},
	}
	for _, mode := range []WindowExecutionMode{WindowExecutionResident, WindowExecutionOrdered, WindowExecutionExternalSort} {
		for _, tc := range []struct {
			name    string
			aliases []string
			prefix  int
		}{
			{"new-output", []string{"rank"}, 3},
			{"non-key-replaced", []string{"value"}, 3},
			{"first-key-replaced", []string{"a"}, 0},
			{"middle-key-replaced", []string{"b"}, 1},
			{"last-key-replaced", []string{"c"}, 2},
			{"multiple-keys-replaced", []string{"c", "a"}, 0},
			{"new-and-replaced", []string{"rank", "c", "b"}, 1},
		} {
			t.Run(fmt.Sprintf("mode-%d/%s", mode, tc.name), func(t *testing.T) {
				input, err := inmemory.New(memory.NewSchema(
					memory.Field{Name: "a", Type: memory.TypeInt64},
					memory.Field{Name: "b", Type: memory.TypeInt64},
					memory.Field{Name: "c", Type: memory.TypeInt64},
					memory.Field{Name: "value", Type: memory.TypeInt64},
				))
				if err != nil {
					t.Fatal(err)
				}
				proof := windowOrderingProofSource{Source: input, ordering: append([]source.OrderingColumn(nil), ordering...)}
				expressions := make([]WindowExpression, len(tc.aliases))
				fields := make([]memory.Field, len(tc.aliases))
				for index, alias := range tc.aliases {
					expressions[index] = WindowExpression{Function: WindowRowNumber, Alias: alias}
					fields[index] = memory.Field{Name: alias, Type: memory.TypeInt64}
				}
				window, err := NewWindowSourceWithOptions(proof, nil, []SortKey{{Column: "a"}}, expressions, fields, nil, WindowSourceOptions{Mode: mode})
				if err != nil {
					t.Fatal(err)
				}
				provider := window.(source.OrderingProvider)
				want := append([]source.OrderingColumn(nil), ordering[:tc.prefix]...)
				if got := provider.Ordering(); !reflect.DeepEqual(got, want) {
					t.Fatalf("ordering = %+v, want %+v", got, want)
				}
				if got := provider.Ordering(); len(got) > 0 {
					got[0].Name = "modified-by-caller"
				}
				if got := provider.Ordering(); !reflect.DeepEqual(got, want) {
					t.Fatalf("caller mutated ordering: %+v", got)
				}
				if got := proof.Ordering(); !reflect.DeepEqual(got, ordering) {
					t.Fatalf("window mutated input ordering: %+v", got)
				}
			})
		}
	}
}

func TestWindowOrderingDoesNotInventProof(t *testing.T) {
	input, err := inmemory.New(memory.NewSchema(memory.Field{Name: "value", Type: memory.TypeInt64}))
	if err != nil {
		t.Fatal(err)
	}
	// Hide optional interfaces, so the child supplies no ordering proof at all.
	window, err := NewWindowSource(struct{ source.Source }{input}, nil, []SortKey{{Column: "value"}},
		[]WindowExpression{{Function: WindowRowNumber, Alias: "rank"}},
		[]memory.Field{{Name: "rank", Type: memory.TypeInt64}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got := window.(source.OrderingProvider).Ordering(); len(got) != 0 {
		t.Fatalf("invented ordering = %+v", got)
	}
}
