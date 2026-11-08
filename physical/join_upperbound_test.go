package physical

import (
	"context"
	"errors"
	"fmt"
	"gocolumn/expr"
	"gocolumn/join"
	"gocolumn/kernel"
	"gocolumn/memory"
	"gocolumn/source"
	"gocolumn/source/inmemory"
	"reflect"
	"sort"
	"strings"
	"testing"
)

// Dictionary domains distinguish independently encoded inputs, even when
// their codes happen to overlap. Clone source batches to set a valid domain.
func upperBoundJoinInput(t *testing.T, rows []genericJoinRow, dict bool, chunk int, domain string) source.Source {
	t.Helper()
	src := genericJoinSource(t, rows, dict, chunk)
	if !dict {
		return src
	}
	ctx := context.Background()
	rd, err := source.OpenBatchStream(ctx, src, source.ReadOptions{Kernels: kernel.New(kernel.ModeScalar)})
	if err != nil {
		t.Fatal(err)
	}
	defer rd.Close()
	schema := src.Schema()
	schema.Fields = append([]memory.Field(nil), schema.Fields...)
	schema.Fields[0].DictionaryDomain = domain
	var batches []*memory.Batch
	for {
		b := memory.NewBatch()
		ok, err := rd.Next(ctx, b)
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			break
		}
		copy, err := cloneBatch(b)
		if err != nil {
			t.Fatal(err)
		}
		copy.DictStrings["s"].DictionaryDomain = domain
		batches = append(batches, copy)
	}
	result, err := inmemory.New(schema, batches...)
	if err != nil {
		t.Fatal(err)
	}
	d, err := src.Dictionary(ctx, "s")
	if err != nil {
		t.Fatal(err)
	}
	d.Domain = domain
	return result.WithDictionary("s", d)
}

// References use original payload row IDs, not the compacted build indices.
func upperBoundJoinReference(left, right []genericJoinRow, ids []int, shape string, typ join.Type) [][2]int64 {
	var pairs [][2]int64
	matched := make([]bool, len(ids))
	for li, l := range left {
		found := false
		for j := len(ids) - 1; j >= 0; j-- {
			ri := ids[j]
			r := right[ri]
			equal := l.null != 3 && r.null != 3 && l.n == r.n
			if shape == "pair" {
				equal = equal && l.null != 4 && r.null != 4 && l.u == r.u
			}
			if shape == "dictionary" {
				equal = l.null != 1 && r.null != 1 && l.s == r.s
			}
			if !equal {
				continue
			}
			found = true
			matched[j] = true
			if typ == join.Anti {
				continue
			}
			pair := [2]int64{int64(li), int64(ri)}
			if typ == join.Semi {
				pair[1] = -1
			}
			pairs = append(pairs, pair)
			if typ == join.Semi {
				break
			}
		}
		if !found && (typ == join.Left || typ == join.Full || typ == join.Anti) {
			pairs = append(pairs, [2]int64{int64(li), -1})
		}
	}
	if typ == join.Right || typ == join.Full {
		for j, ri := range ids {
			if !matched[j] {
				pairs = append(pairs, [2]int64{-1, int64(ri)})
			}
		}
	}
	return pairs
}

func upperBoundJoinWrapper(t *testing.T, src source.Source, rows []genericJoinRow, kind string) (source.Source, []int) {
	t.Helper()
	ids := make([]int, len(rows))
	for i := range ids {
		ids[i] = i
	}
	var err error
	if strings.Contains(kind, "filter") {
		src, err = NewFilterSource(src, expr.Not(expr.EQ(expr.Col("row"), expr.I64(1))))
		if err != nil {
			t.Fatal(err)
		}
		ids = nil
		for i := range rows {
			if i != 1 {
				ids = append(ids, i)
			}
		}
	}
	if strings.Contains(kind, "limit") {
		n := 3
		if kind == "limit0" {
			n = 0
		}
		src, err = NewLimitSource(src, int64(n))
		if err != nil {
			t.Fatal(err)
		}
		if len(ids) > n {
			ids = ids[:n]
		}
	}
	if kind == "filter-none" {
		src, err = NewFilterSource(src, expr.Bool(false))
		if err != nil {
			t.Fatal(err)
		}
		ids = nil
	}
	return src, ids
}

func TestJoinBuildUpperBoundModes(t *testing.T) {
	ctx := context.Background()
	k := kernel.New(kernel.ModeScalar)
	rows := []genericJoinRow{{s: "a", n: 1, u: 7}, {s: "removed", n: 8, u: 8}, {s: "a", n: 1, u: 7}, {s: "a", n: 1, u: 9}, {s: "null", n: 1, u: 7, null: 3}, {s: "null", n: 4, u: 7, null: 1}, {s: "right", n: 5, u: 7}, {s: "a", n: 1, u: 7, null: 4}}
	leftRows := []genericJoinRow{{s: "a", n: 1, u: 7}, {s: "left", n: 9, u: 9}, {s: "a", n: 1, u: 7, null: 3}, {s: "null", n: 4, u: 7, null: 1}, {s: "a", n: 1, u: 7, null: 4}}
	for _, shape := range []string{"fixed", "pair", "radix", "dictionary"} {
		keys := []JoinKey{{"n", "n"}}
		if shape == "pair" {
			keys = append(keys, JoinKey{"u", "u"})
		}
		if shape == "dictionary" {
			keys = []JoinKey{{"s", "s"}}
		}
		for _, chunk := range []int{1, 3, 16} {
			for _, kind := range []string{"filter", "limit", "filter-limit", "filter-none", "limit0"} {
				for _, emptyLeft := range []bool{false, true} {
					for typ := join.Inner; typ <= join.Full; typ++ {
						// The preexisting explicitly forced radix probe supports four modes.
						if shape == "radix" && (typ == join.Right || typ == join.Full) {
							continue
						}
						t.Run(fmt.Sprintf("%s/chunk%d/%s/empty%v/%d", shape, chunk, kind, emptyLeft, typ), func(t *testing.T) {
							lr := leftRows
							if emptyLeft {
								lr = nil
							}
							left := upperBoundJoinInput(t, lr, shape == "dictionary", chunk, "left-domain")
							base := upperBoundJoinInput(t, rows, shape == "dictionary", chunk, "right-domain")
							right, ids := upperBoundJoinWrapper(t, base, rows, kind)
							m := memory.NewBudgetManager(8 << 20)
							opts := JoinSourceOptions{Kernels: k}
							if shape == "radix" {
								opts.RadixBits = 2
							}
							owner, err := NewJoinSourceWithOptions(ctx, left, right, typ, keys, "", m, opts)
							if err != nil {
								t.Fatal(err)
							}
							if shape == "dictionary" && owner.shape != joinDictionary {
								t.Fatalf("did not exercise dictionary path: %d", owner.shape)
							}
							if shape != "dictionary" && owner.shape != joinFixed {
								t.Fatalf("did not exercise fixed path: %d", owner.shape)
							}
							want := upperBoundJoinReference(lr, rows, ids, shape, typ)
							for repeat := 0; repeat < 2; repeat++ {
								got, batches := collectGenericJoin(t, owner)
								for _, b := range batches {
									b.ReleaseOwned()
								}
								if shape == "radix" {
									less := func(p [][2]int64) func(int, int) bool {
										return func(i, j int) bool {
											if p[i][0] != p[j][0] {
												return p[i][0] < p[j][0]
											}
											return p[i][1] < p[j][1]
										}
									}
									sort.Slice(got, less(got))
									sort.Slice(want, less(want))
								}
								if !reflect.DeepEqual(got, want) {
									t.Fatalf("repeat%d got%v want%v", repeat, got, want)
								}
							}
							if err = owner.Close(); err != nil {
								t.Fatal(err)
							}
							if m.Used() != 0 {
								t.Fatalf("resident budget leak: %d", m.Used())
							}
						})
					}
				}
			}
		}
	}
}

type upperBoundJoinSource struct {
	source.Source
	stats                       *source.Stats
	parts                       []source.Partition
	statsErr, nextErr, closeErr error
	failAt                      int
	opens, nexts, closes        int
	cancel                      context.CancelFunc
}

func (s *upperBoundJoinSource) Stats(ctx context.Context) (source.Stats, error) {
	if s.statsErr != nil {
		return source.Stats{}, s.statsErr
	}
	if s.stats != nil {
		return *s.stats, nil
	}
	return s.Source.Stats(ctx)
}
func (s *upperBoundJoinSource) Partitions(ctx context.Context) ([]source.Partition, error) {
	if s.parts != nil {
		return s.parts, nil
	}
	return s.Source.Partitions(ctx)
}
func (s *upperBoundJoinSource) NewBatchStream(ctx context.Context, opts source.ReadOptions) (source.BatchStream, error) {
	r, err := source.OpenBatchStream(ctx, s.Source, opts)
	if err != nil {
		return nil, err
	}
	s.opens++
	return &upperBoundJoinStream{source: r, owner: s}, nil
}

type upperBoundJoinStream struct {
	source source.BatchStream
	owner  *upperBoundJoinSource
	closed bool
}

func (r *upperBoundJoinStream) Next(ctx context.Context, b *memory.Batch) (bool, error) {
	r.owner.nexts++
	if r.owner.nexts == r.owner.failAt {
		if r.owner.cancel != nil {
			r.owner.cancel()
			return false, ctx.Err()
		}
		return false, r.owner.nextErr
	}
	return r.source.Next(ctx, b)
}
func (r *upperBoundJoinStream) Close() error {
	if r.closed {
		return nil
	}
	r.closed = true
	r.owner.closes++
	return errors.Join(r.source.Close(), r.owner.closeErr)
}

func TestJoinBuildUpperBoundContractsAndCleanup(t *testing.T) {
	k := kernel.New(kernel.ModeScalar)
	rows := []genericJoinRow{{n: 1}, {n: 2}, {n: 3}}
	for _, dict := range []bool{false, true} {
		for _, mode := range []string{"stats-error", "exact-short", "bound-over", "partition-over", "unknown-upper", "empty-source", "next-error", "cancel", "close-error", "budget", "limit-cap"} {
			t.Run(fmt.Sprintf("dict%v/%s", dict, mode), func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				base := genericJoinSource(t, rows, dict, 1)
				left := genericJoinSource(t, rows, dict, 1)
				sentinel := errors.New("injected build error")
				closeErr := errors.New("injected close error")
				r := &upperBoundJoinSource{Source: base, stats: &source.Stats{Rows: 5, RowsUpperBound: true}, parts: []source.Partition{{ID: 0, Rows: 5}}}
				var right source.Source = r
				var want error
				wantText := ""
				hard := int64(1 << 20)
				switch mode {
				case "stats-error":
					r.statsErr = sentinel
					want = sentinel
				case "exact-short":
					r.stats.RowsUpperBound = false
					wantText = "partition contract declared"
				case "bound-over":
					r.stats.Rows = 2
					wantText = "more rows"
					r.closeErr = closeErr
					want = closeErr
				case "partition-over":
					r.parts[0].Rows = 2
					wantText = "more rows"
				case "unknown-upper":
					r.stats.Rows = -1
				case "empty-source":
					r.Source = genericJoinSource(t, nil, dict, 1)
				case "next-error":
					r.failAt = 2
					r.nextErr = sentinel
					r.closeErr = closeErr
					want = sentinel
				case "cancel":
					r.failAt = 2
					r.cancel = cancel
					r.closeErr = closeErr
					want = context.Canceled
				case "close-error":
					r.closeErr = closeErr
					want = closeErr
				case "budget":
					hard = 1
					want = memory.ErrBudgetExceeded
				case "limit-cap":
					r.stats.Rows = 1000000
					r.parts[0].Rows = 1000000
					var err error
					var input source.Source = r
					if dict {
						input = &upperBoundDictionarySource{upperBoundJoinSource: r}
					}
					right, err = NewLimitSource(input, 1)
					if err != nil {
						t.Fatal(err)
					}
				}
				keys := []JoinKey{{"n", "n"}}
				if dict {
					keys = []JoinKey{{"s", "s"}}
					if mode != "limit-cap" {
						right = &upperBoundDictionarySource{upperBoundJoinSource: r}
					}
				}
				m := memory.NewBudgetManager(hard)
				owner, err := NewJoinSource(ctx, left, right, join.Full, keys, "", m, k)
				if want != nil && !errors.Is(err, want) {
					t.Fatalf("got %v want %v", err, want)
				}
				if wantText != "" && (err == nil || !strings.Contains(err.Error(), wantText)) {
					t.Fatalf("missing %q: %v", wantText, err)
				}
				if want == nil && wantText == "" && err != nil {
					t.Fatal(err)
				}
				if r.closeErr != nil && !errors.Is(err, r.closeErr) {
					t.Fatalf("lost cleanup failure: %v", err)
				}
				if owner != nil {
					owner.Close()
				}
				if m.Used() != 0 {
					t.Fatalf("failed build retained %d bytes", m.Used())
				}
				if r.opens != r.closes {
					t.Fatalf("opens=%d closes=%d", r.opens, r.closes)
				}
				if (mode == "stats-error" || mode == "budget") && r.opens != 0 {
					t.Fatalf("opened source before failed admission: %d", r.opens)
				}
			})
		}
	}
}

type upperBoundDictionarySource struct{ *upperBoundJoinSource }

func (s *upperBoundDictionarySource) Dictionary(ctx context.Context, name string) (source.Dictionary, error) {
	return s.Source.(source.DictionaryProvider).Dictionary(ctx, name)
}

func TestJoinBuildUpperBoundBoundedBroadcast(t *testing.T) {
	ctx := context.Background()
	k := kernel.New(kernel.ModeScalar)
	rows := []genericJoinRow{{s: "a", n: 1}, {s: "b", n: 2}, {s: "a", n: 1}, {s: "null", n: 1, null: 3}}
	for _, dict := range []bool{false, true} {
		for _, limited := range []bool{false, true} {
			for typ := join.Inner; typ <= join.Full; typ++ {
				t.Run(fmt.Sprintf("dict%v/limit%v/%d", dict, limited, typ), func(t *testing.T) {
					columns := []string{"n", "u", "row"}
					keys := []JoinKey{{"n", "n"}}
					shape := "fixed"
					if dict {
						columns = append(columns, "s")
						keys = []JoinKey{{"s", "s"}}
						shape = "dictionary"
					}
					raw := upperBoundJoinInput(t, rows, dict, 2, "shared-domain")
					left, err := NewFilterProjectSource(raw, nil, columns)
					if err != nil {
						t.Fatal(err)
					}
					right, err := NewFilterProjectSource(raw, expr.Not(expr.EQ(expr.Col("row"), expr.I64(1))), columns)
					if err != nil {
						t.Fatal(err)
					}
					ids := []int{0, 2, 3}
					if limited {
						right, err = NewLimitSource(right, 1)
						if err != nil {
							t.Fatal(err)
						}
						ids = ids[:1]
					}
					const hard = 16 << 20
					manager := memory.NewBudgetManager(hard)
					owner, err := NewJoinSourceWithOptions(ctx, left, right, typ, keys, "", manager, JoinSourceOptions{Kernels: k, Spillable: true, RequireBounded: true, SpillConfigured: true, MemoryLimitBytes: hard})
					if err != nil {
						t.Fatal(err)
					}
					if !owner.boundedBuild || owner.spillable {
						t.Fatalf("expected admitted bounded broadcast, strategy=%s", owner.Strategy())
					}
					got, batches := collectGenericJoin(t, owner)
					for _, b := range batches {
						b.ReleaseOwned()
					}
					want := upperBoundJoinReference(rows, rows, ids, shape, typ)
					if !reflect.DeepEqual(got, want) {
						t.Fatalf("got%v want%v", got, want)
					}
					owner.Close()
					if manager.Used() != 0 {
						t.Fatalf("retained %d", manager.Used())
					}
				})
			}
		}
	}
}
