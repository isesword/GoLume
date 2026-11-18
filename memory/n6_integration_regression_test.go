package memory

import (
	"context"
	"errors"
	"testing"
)

func TestN6ExtendedProfilePreservesPrimitiveListMetadata(t *testing.T) {
	nested, err := NestedListField("nested", true, ListStringField("item", true, true))
	if err != nil {
		t.Fatal(err)
	}
	for _, kind := range []string{"parent_domain", "item_domain", "parent_encoding", "item_encoding"} {
		bad := ListInt64Field("xs", true, true)
		switch kind {
		case "parent_domain":
			bad.DictionaryDomain = "bad"
		case "item_domain":
			bad.Children[0].DictionaryDomain = "bad"
		case "parent_encoding":
			bad.Encoding = EncodingDictionary
		case "item_encoding":
			bad.Children[0].Encoding = EncodingDictionary
		}
		root := StructField("r", true, nested, StructField("deep", true, bad))
		if _, err := NewSchemaWithNestedListsChecked(root); err == nil {
			t.Fatalf("accepted %s alongside nested List", kind)
		}
	}
}

func TestN6SizingCancellationEntryPoints(t *testing.T) {
	b := NewBatch()
	check := func() error { return context.Canceled }
	if _, err := EstimateBatchRangeBytesWithCheck(b, 0, 0, check); !errors.Is(err, context.Canceled) {
		t.Fatalf("range cancellation: %v", err)
	}
	if _, err := SelectBatchRangeRowsWithCheck(b, 0, 0, 0, 0, check); !errors.Is(err, context.Canceled) {
		t.Fatalf("selection cancellation: %v", err)
	}
}
