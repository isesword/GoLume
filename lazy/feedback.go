package lazy

import (
	"crypto/sha256"
	"fmt"
	"gocolumn/logical"
)

func planFeedbackKey(plan logical.Plan) string {
	h := sha256.New()
	_, _ = h.Write([]byte(logical.Explain(plan)))
	writeSourceIdentity(h, plan)
	return fmt.Sprintf("lazy:%x", h.Sum(nil)[:16])
}

type byteWriter interface{ Write([]byte) (int, error) }

func writeSourceIdentity(w byteWriter, plan logical.Plan) {
	switch n := plan.(type) {
	case *logical.Scan:
		_, _ = fmt.Fprintf(w, "|source=%T:%p", n.Source, n.Source)
	case *logical.Filter:
		writeSourceIdentity(w, n.Input)
	case *logical.Projection:
		writeSourceIdentity(w, n.Input)
	case *logical.ExprProjection:
		writeSourceIdentity(w, n.Input)
	case *logical.Sort:
		writeSourceIdentity(w, n.Input)
	case *logical.TopK:
		writeSourceIdentity(w, n.Input)
	case *logical.Limit:
		writeSourceIdentity(w, n.Input)
	case *logical.Aggregate:
		writeSourceIdentity(w, n.Input)
	case *logical.Window:
		writeSourceIdentity(w, n.Input)
	case *logical.Join:
		writeSourceIdentity(w, n.Left)
		writeSourceIdentity(w, n.Right)
	}
}
