package expr

import (
	"reflect"
	"sort"
)

// Columns returns the sorted, de-duplicated column names referenced by e.
func Columns(e Expr) []string {
	if e == nil {
		return nil
	}
	set := make(map[string]struct{})
	seen := make(map[reflect.Value]struct{})
	var visit func(Expr)
	visit = func(n Expr) {
		key := reflect.ValueOf(n)
		if _, ok := seen[key]; ok {
			return
		}
		seen[key] = struct{}{}
		switch x := n.(type) {
		case Column:
			set[x.Name] = struct{}{}
		case Binary:
			visit(x.Left)
			visit(x.Right)
		case Unary:
			visit(x.Value)
		case Decimal128CastExpr:
			visit(x.Value)
		case Cast:
			visit(x.Value)
		case Decimal128DivideRoundExpr:
			visit(x.Left)
			visit(x.Right)
		case Decimal128DivideExpr:
			visit(x.Left)
			visit(x.Right)
		case Decimal128RoundExpr:
			visit(x.Value)
		case Decimal128RescaleExpr:
			visit(x.Value)
		case TryCastExpr:
			visit(x.Value)
		case Ternary:
			visit(x.Value)
			visit(x.First)
			visit(x.Second)
		case StructFromExpr:
			for _, field := range x.Fields {
				visit(field.Expr)
			}
		case StructGetExpr:
			visit(x.Value)
		case ListGetExpr:
			visit(x.Value)
			visit(x.Index)
		case YMDDateExpr:
			visit(x.Year)
			visit(x.Month)
			visit(x.Day)
		case ISOWeekDateExpr:
			visit(x.Year)
			visit(x.Week)
			visit(x.Weekday)
		case CalendarDaysExpr:
			visit(x.Timestamp)
			visit(x.Days)
		case CivilComposeExpr:
			visit(x.Date)
			visit(x.Clock)
		case CivilFormatExpr:
			visit(x.Value)
		case DateAtMidnightExpr:
			visit(x.Value)
		case CivilTimestampExpr:
			visit(x.Value)
		case DateInZoneExpr:
			visit(x.Value)
		case ZonedExtractExpr:
			visit(x.Value)
		case JSONDecodeExpr:
			visit(x.Value)
		case IfExpr:
			visit(x.Condition)
			visit(x.Then)
			visit(x.Else)
		case CaseExpr:
			for _, branch := range x.Whens {
				visit(branch.When)
				visit(branch.Then)
			}
			visit(x.Else)
		case CoalesceExpr:
			for _, value := range x.Values {
				visit(value)
			}
		case InExpr:
			visit(x.Value)
			for _, value := range x.Values {
				visit(value)
			}
		}
	}
	visit(e)
	out := make([]string, 0, len(set))
	for name := range set {
		out = append(out, name)
	}
	sort.Strings(out)
	return out
}

// MergeAnd combines predicates with SQL AND while preserving nil as the
// identity element. It is used by optimizer rules that merge adjacent filters
// or attach multiple residual predicates to one scan.
func MergeAnd(left, right Expr) Expr {
	if left == nil {
		return right
	}
	if right == nil {
		return left
	}
	return And(left, right)
}
