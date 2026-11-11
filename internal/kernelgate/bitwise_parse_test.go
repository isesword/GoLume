package kernelgate

import "testing"

func TestBitwiseIDParsing(t *testing.T) {
	if !ValidFamily("bitwise") {
		t.Fatal("missing bitwise family")
	}
	for _, typ := range []string{"int32", "uint32", "int64", "uint64"} {
		for _, op := range []string{"and", "or", "xor", "not"} {
			id := "bitwise-" + typ + "-" + op
			family, gotType, gotOp, err := ParseKernelID(id)
			if err != nil || family != "bitwise" || gotType != typ || gotOp != op {
				t.Fatalf("%s: %q/%q/%q %v", id, family, gotType, gotOp, err)
			}
		}
	}
	for _, id := range []string{"bitwise-", "bitwise-int32", "bitwise-int32-", "bitwise-float32-and", "bitwise-int8-and", "bitwise-bool-and", "bitwise-int64-shl", "bitwise-int64-and-extra"} {
		if _, _, _, err := ParseKernelID(id); err == nil {
			t.Fatalf("accepted unsupported ID %q", id)
		}
	}
}
