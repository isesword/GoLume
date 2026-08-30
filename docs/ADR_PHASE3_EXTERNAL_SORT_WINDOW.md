# ADR: Phase 3.5 External Sort、Top-K 与 Window 能力分级

状态：已实现并通过 Phase 3.5 全量验收。

## 决策

Phase 3.5 不增加第二套 eager 执行器。`Sort` 和 `Window` 仍是同一棵物理 Source/BatchStream
图中的节点，并共享 query budget、Spill store、取消、Explain 和 runtime diagnostics。

### Sort

`ExecutionBounded` 选择稳定外排；配置了有限内存与 Spill 的 `ExecutionAuto` 根据输入统计和
四倍 resident headroom 在内存排序与稳定外排之间选择：

1. 按 query budget 的一小部分生成有界 sorted run；每行附加内部 `uint64 RowOrdinal`。
2. run 使用 `ColumnarRunCodec` 写入 query-owned Spill store。
3. 固定 fan-in 4；run 多于 fan-in 时先做分层 merge，避免同时打开任意数量的文件和 decode buffer。
4. 最终 merge 按 batch 输出，并用用户 sort key 加 `RowOrdinal` 保证跨 run 稳定性。
5. 每个 run 的 metadata、编码/恢复 buffer、merge workspace 和输出 batch 都有独立 reservation。

`ExecutionMemoryOnly`、未配置 Spill 的普通 Auto，以及能安全放入 headroom 的小 Auto 查询
保留原 resident stable sort。严格 bounded
模式没有 Spill 配额时在 Prepare 返回 `ErrBoundedSpillRequired`，不会静默物化。

### Top-K

Top-K 不经过 External Sort。候选 heap 仍为 O(K)，但有限预算下同时追踪：

- 候选固定状态；
- 实际复制的字符串/二进制 key 字节；
- winning payload 的实际列式字节和物化过渡空间。

增长先按差额预留，再复制；超预算返回 `ErrBudgetExceeded`。全部 reservation 由 reader 的幂等
`Close` 释放。

### Window

Window lowering 分为四个可见能力层：

| 能力 | 物理策略 | bounded 行为 |
|---|---|---|
| 输入已按 partition/order 排序 | `ordered-state` | 直接跨 Batch 流水 |
| `CURRENT ROW` 结尾的有限 preceding frame | `ordered-state` | 只保留 frame 状态 |
| 输入未排序但 frame 可 bounded | `external-sort-restore` | 外排后计算，再按内部行号外排恢复输入顺序 |
| 需要 following/look-ahead | `unsupported` | Prepare 明确失败 |

排序窗口的物理链为：

```text
input -> append private ordinal -> External Sort(partition, order)
      -> ordered window state -> External Sort(private ordinal)
      -> drop private ordinal
```

因此 `WithWindows` 继续保持原输入行序。ranking 只保留前一 partition/order key；rolling
count/sum/avg 使用固定 ring state；rolling min/max 使用固定 ring 加单调队列；
`UNBOUNDED PRECEDING .. CURRENT ROW` 使用常量累计状态。字符串排序状态按实际 key 字节追加
reservation，超大 `PRECEDING` 在分配 ring 前先预留。

本阶段不假装支持 FOLLOWING。它需要 look-ahead、分区缓冲或双向外部算法，必须在独立阶段
定义延迟和磁盘契约。

## 易用性

- 用户 API 不变：小结果仍是 `Collect(ctx)`；选择 bounded 后只改变资源保证，不改变结果。
- Auto 在已经有排序证明时直接使用 bounded window；有限预算且显式配置 Spill 时自动外排。
- Explain 显示 `BLOCKING`、`BOUNDED` 或 `SPILLABLE` 及具体 strategy，避免把“运行在流式图”
  误解成“内存一定有界”。
- 错误指出是缺内存、缺 Spill 配额还是 frame 暂不支持，不做隐藏 fallback。

## SIMD 决策

External merge、Top-K heap 和 rolling 状态机是跨行依赖、分支密集的控制流，不新增伪 SIMD
路径。它们仍复用上游表达式、filter、hash 等已经过 gate 的 SIMD kernel。新的 Auto SIMD
候选只有在特定架构微基准和 Lazy 端到端基准同时证明收益后才允许加入；本阶段没有候选达到
这个前提。

## 验收证据

- 稳定多 key、ASC/DESC、NULL placement、NaN、重复值、多 run/多级 merge。
- 取消、磁盘配额、损坏 run 均清理 query Spill 文件和 reservation。
- 3 MiB 固定列输入在 256 KiB memory budget 下（12 倍）经多 Batch 外排完成。
- bounded Window 覆盖跨 Batch partition、ranking、有限 rolling 和原输入顺序恢复。
- Top-K 实际 variable-width key 超预算返回明确错误且 Close 后用量为零。
- Apple M4 Pro、darwin/arm64、10 个 1 秒样本：resident Sort 中位数回退 0.45%，Top-K
  中位数改善 0.58%，均通过不超过 3% 的回归门禁。

机器可读数据见 [`validation/phase35-sort-window-gate.json`](../validation/phase35-sort-window-gate.json)。
