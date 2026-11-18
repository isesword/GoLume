package memory

import (
	"context"
	"errors"
	"testing"
)

func TestN4KeepsRecursivePrimitiveListMetadataRules(t *testing.T) {
	good, err := ListStructField("records", true, true, Field{Name: "x", Type: TypeInt64})
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
		root := StructField("r", true, good, StructField("deep", true, bad))
		if _, err := NewSchemaWithStructListsChecked(root); err == nil {
			t.Fatalf("accepted %s beside StructList", kind)
		}
	}
}

func TestN4BorrowKeepsCancellationEntryPoints(t *testing.T) {
	b := NewBatch()
	check := func() error { return context.Canceled }
	if _, err := BorrowedRangeMetadataBytesWithCheck(b, 0, 0, check); !errors.Is(err, context.Canceled) {
		t.Fatalf("metadata cancellation: %v", err)
	}
	if _, err := BorrowBatchRangeWithCheck(b, 0, 0, check); !errors.Is(err, context.Canceled) {
		t.Fatalf("borrow cancellation: %v", err)
	}
}
