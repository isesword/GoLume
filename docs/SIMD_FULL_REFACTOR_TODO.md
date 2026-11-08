# GoLume SIMD 全面重构迭代 TODO

> **历史阶段清单；Auto 策略已更新。** 下文的测量、勾选状态及“forced-only / Auto
> 待准入”记录属于当时阶段，不是当前默认策略。现在 Auto 在运行时保护允许时优先
> 使用已有原生 SIMD；被删除或尚未实现的后端不因此恢复。当前状态见
> [SIMD dispatch](SIMD_KERNELS.md) 与 [操作矩阵](SIMD_OPERATION_MATRIX.md)。
> planner 策略的独立测量规则不变；本轮未新增原生 arm64 执行证据，也不声称普遍提速。

## 目标与原则

目标是在 Go 1.27 下，把 SIMD 从少数独立 kernel 扩展到原生位宽数据、表达式、过滤压缩、聚合、Join 及适用的字符串扫描热点，完成 arm64 与 amd64 的原生验收。

- [ ] 原生支持 Int8/16/32/64、Uint8/16/32/64、Float32/64 的物理存储和批处理。
- [ ] 统一数据布局、内核注册、表达式编译和物理算子调用路径。
- [ ] 删除被替代的接口、扩宽存储分支、重复执行器和兼容适配；一次性迁移所有调用方、示例和测试。
- [ ] 每阶段都有单测、真实调用链验证、基准和交付证据。
- [ ] 保留规范 scalar 后端，用于差分测试和不适合 SIMD 的计算；它不是兼容层。
- [ ] 不通过改变 NULL、溢出、浮点或字符串语义换取性能；不引入 Rust、C 或 CGO。
- [ ] 不用文件数、候选函数数或未经测量的百分比宣称覆盖度。

统一完成定义：实现、单测、集成、性能证据和旧路径删除全部完成后才勾选阶段。候选必须有“准入”或“有证据拒绝”的结论，未测试不算拒绝。

## P0：基线、调用盘点和 Polars 对照

- [x] 固定基线提交、Go 1.27、CPU/指令集、系统和构建参数。
- [x] 固定一个 Polars 提交，逐项记录数值比较、算术、bitmap、filter/take、归约、字符串和扫描实现源码链接；见 `docs/SIMD_POLARS_REFERENCE.md`。
- [x] 区分显式 SIMD、编译器自动向量化、依赖库优化、批处理和多线程；见 `docs/SIMD_POLARS_REFERENCE.md`。
- [ ] 盘点所有绕开 `kernel.Set` 的逐行生产路径。
- [x] 建立操作矩阵：逻辑/物理类型、操作、输入形态、NULL、编码、架构、后端、调用方、测试和性能证据；见 `docs/SIMD_OPERATION_MATRIX.md`，待完成项已保留在文档末尾。
- [ ] 复跑基线，并采集数值投影、过滤、聚合、Join、排序、字符串和 Parquet 扫描的 CPU profile、分配和峰值内存。
- [ ] 检查内核清单、能力报告和矩阵一致；能力报告必须区分硬件 SIMD、标量展开和 scalar。
- [x] 基线执行模式结果逐项一致；核心 kernel/memory/expr/engine/lazy/physical 测试已分别通过普通、`GOEXPERIMENT=simd` 和 `GODEBUG=simd=0` 构建；profile 和完整 P0 盘点仍未完成。

## P1：原生位宽数据布局

- [x] 已完成第一步：新增 Int8/16、Uint8/16、Float32 原生 vector、Batch 绑定、访问器、清理和基础内存核算；下游 Arrow/Parquet/spill 迁移仍未完成。
- [x] 已完成 Arrow RecordBatch 输入的第一步迁移：Int8/16、Uint8/16、Float32 不再在 `source/arrowrecord` cache/read 路径中扩宽为 Int32/Uint32/Float64；原生绑定单测和现有 typed-source 测试通过。
- [x] 已补齐 Float32 原生列在谓词执行中的正式 `kernel.Set` scalar 路径及 NULL/特殊值单测；Float32 SIMD kernel 和投影链路仍未完成，当前未宣称 SIMD 覆盖。
- [x] 十种数值向量统一由 `internal/cmd/gensimd` 生成 `memory/zz_generated_numeric.go`，删除分散手写定义；`go run ./internal/cmd/gensimd -check` 与 `go test ./memory` 已通过。下游全链路迁移仍未完成。
- [ ] 统一长度、validity、切片、复制、清理、内存核算和所有权规则。
- [ ] 一次性迁移 Arrow、Parquet、内存源、spill、结果物化、Filter、Project、Gather、Sort、Join、DataFrame/Series 和嵌套字段。
- [ ] 删除“窄 schema + 宽容器”分支和重复存储；表达式类型提升必须显式完成。
- [x] 更新内部 spill 格式，不读取旧内部格式；Int8/16、Uint8/16、Float32 使用原生字节宽度并有 columnar round-trip 单测；完整执行链迁移仍未完成。
- [x] 已补齐 native Int8/16、Uint8/16、Float32 在 in-memory reader、Project passthrough/alias、Filter compact 和 Take gather 中的调用链，并以 Float32 Filter→Project 测试验证；Join/Sort/Parquet 仍未完成。
- [x] Parquet lazy reader 已增加物理 Float32 和 IntLogicalType(8/16 signed/unsigned) 原生读取、NULL、边界和特殊值测试；其他 Parquet 物理类型仍未完成。
- [x] Join payload clone/materialize 已支持 native Int8/16、Uint8/16、Float32 vectors；fixed-width integer key build/probe 也已支持 native maps，并以 Int8 重复 key/multiplicity 测试验证；Float32 key、spill join 和完整 pair/nullable 矩阵仍未完成。
- [x] Sort/Top-K 的 `snapshotCell`、排序结果 gather 已支持 native Int8/16、Uint8/16、Float32，并以 native Int8/Float32 排序测试验证；Window 输出和复杂 spill merge 仍未完成。
- [x] Window numeric readers now consume native Int8/16、Uint8/16、Float32 inputs without widening source vectors, with direct reader tests; aggregate output allocation remains governed by existing type-promotion contracts.
- [ ] 单测覆盖十种类型的极值、NULL、空列、非零偏移切片、释放、Arrow/Parquet/spill 往返、内存字节数和 Filter→Project→Join→Sort→输出链路。
- [x] Float32 已明确验证 NaN、无穷、负零、列列比较和类型提升；类型提升仍遵循表达式契约。

## P2：统一注册、形态分派和生成

- [x] `cmd/kernel-info -json` 现在同时输出实际后端、可用强制后端和 `auto_admitted`，并有 CLI 单测；完整内部变体分派仍未完成。
- [x] 已增加 Int8/16、Uint8/16、Float32 独立比较 `KernelID`/`kernel.Set` scalar 入口，并以差分单测接入表达式；这些类型仍未获得 SIMD/Auto 准入。
- [ ] 增加内部变体：操作、输入/输出类型、列列/列常量、无 NULL/nullable、连续/选中访问。
- [ ] 编译阶段绑定操作和类型；批次入口选择 NULL/访问形态，循环内不得重复分派。
- [ ] 扩展 `internal/cmd/gensimd` 生成专用函数和注册表，禁止手工维护大批重复实现。
- [x] 生产路径只由 `engine.Engine` 创建并持有共享 `kernel.Set`，下游通过 source/physical ReadOptions 传播；生产代码未发现算子重建 kernel 的路径（测试中的显式构造仅用于隔离测试）。
- [x] 能力报告增加 JSON，区分后端可用、Auto 准入和本次实际选择；`cmd/kernel-info -json` CLI 单测已覆盖。
- [x] 结构检查、普通构建、SIMD 构建和 `GODEBUG=simd=0` 后端选择均已通过验证；不支持的后端如实回落 scalar。
- [ ] 单测覆盖完整注册、非法 ID、模式、CPU 能力、NULL/非 NULL、列列/列常量、生成结果和不可用指令保护。

## P3：数值、bitmap 和压缩 kernel

- [x] 十种物理类型均有五种列常量比较入口；列列比较也已全部接入 typed scalar，Float32/Int32/Uint32 另有 forced SIMD 候选。
- [x] Float32 五种列常量比较已有 Go 1.27 portable SIMD 强制候选；Auto 仍待架构基准准入。
- [x] Float32 加减乘除已加入独立 `KernelID`、scalar 规范后端并完成差分测试；portable SIMD 候选因 M4 基准回退而删除，拒绝证据已记录。
- [x] Int8 列常量比较已接入原生宽度 `kernel.Set` scalar 路径，覆盖五种操作、NULL 和尾部；尚无架构专用实现或性能门证据，未宣称 SIMD。
- [ ] 扩展浮点加减乘除、整数加减乘法、cast 和时间单位缩放。
- [ ] 整数加减用向量结果和溢出掩码，异常块内定位首个错误行；禁止静默回绕。
- [ ] 按架构评估整数乘法；无合适指令时保留正确 scalar，不强行拆分。
- [x] 完成现有 bitmap/压缩候选的第一轮决策：Boolean AND、Hash64、UTF-8、Hash128 和 checked-integer 候选的已知拒绝证据见 `docs/SIMD_CANDIDATE_DECISIONS.md`；新的 bitmap/compact 实现仍需独立 gate。
- [ ] 定长压缩同时处理值、NULL 和稳定行序；候选按 arm64/amd64 分别准入。
- [x] 已提供 native 五种窄类型的正式 `kernel.Set` scalar compact 入口，并纳入统一 dispatch、KernelID 能力报告；Filter/Take 复用同一入口并有顺序/尾部单测；尚无 SIMD 或 Auto 准入证据。
- [x] `TestNativeCompactKernelIDsHaveScalarCapabilities` 逐一验证十种 compact KernelID 的 scalar 能力标志、无硬件 SIMD 和零向量宽度。
- [ ] 单测覆盖尾部、非对齐、哨兵、边界溢出、NULL 溢出、浮点特殊值、三值逻辑和零分配稳态。

## P4：表达式批执行和融合

- [ ] 在现有表达式编译体系上统一谓词和数值表达式的类型化执行表示，不增加平行执行框架。
- [ ] 工作区按执行者隔离复用并纳入内存预算。
- [ ] 为比较与 AND/OR/NOT 生成直接输出选择 bitmap 的融合路径。
- [ ] 为现有加减乘除/转换链生成分块融合路径，避免整列中间物化。
- [ ] 不支持融合的节点也经过同一执行表示和正式 kernel，不保留旧解释器兜底。
- [ ] 保持表达式顺序、错误定位、NULL 传播和浮点求值顺序；Filter、Project、聚合输入、Join key 全部接入。
- [ ] ExplainAnalyze 可按需报告实际后端、行数和未选原因，默认不在热循环计数。
- [x] resident/spill aggregate 的 cold-path diagnostics 已报告实际 `kernel_mode` 与 `kernel_arch`，并保留行数、组数、spill 和峰值内存字段；逐操作 backend/未选原因仍未完成。
- [ ] 单测覆盖共享子表达式、列列、常量广播、融合/非融合/scalar 三方差分、批次变化、并发、取消和工作区复用。
- [x] direct、resident、sorted、radix fallback 和 spill aggregate 的 `EvalWorkspace` 均通过 `SetReserveHook` 申请动态 dictionary-cache 预算，预算失败会传播为执行错误；各路径均在 workspace/query/stream 生命周期结束时释放 reservation，`TestDictionaryPredicateReserveHookPropagatesFailure` 已通过。完整融合预算仍未完成。
- [x] 已完成 native 五种窄类型的表达式列绑定、类型保持和 alias/cast 正确性测试；通用融合、工作区预算和列列执行仍未完成。
- [x] Float32 列列比较已接入同一 predicate program、双 validity 合并和 kernel scalar/forced SIMD 入口；其他类型列列及通用融合仍未完成。
- [x] Int32 列列比较已接入同一 predicate program、双 validity 合并和 kernel scalar/forced SIMD 入口；其他类型列列及通用融合仍未完成。
- [x] Uint32 列列比较已接入同一 predicate program，使用无符号边界语义并覆盖双 validity；其他类型列列及通用融合仍未完成。
- [x] Int64 列列比较已接入同一 predicate program 和双 validity scalar 路径；portable SIMD 三组基准回退，候选已删除并记录拒绝证据。
- [x] Float64 列列比较已接入同一 predicate program 和 IEEE scalar 路径，覆盖 NaN/无穷/负零；portable SIMD 三组基准回退，候选已删除。
- [x] Int8/Int16/Uint8/Uint16/Uint64 列列比较已接入同一 predicate program 的 typed scalar 路径，并覆盖窄类型/无符号边界测试。
- [x] 数值表达式的 Int64/Uint64/Float64 同类型列列算术继续通过同一 `kernel.Set`，保留溢出和 validity 契约；混合类型仍按显式提升走 scalar 逐行路径。
- [x] `EvalWorkspace` 为编译后的字符串字面量保留按指令索引的 `[]byte` 缓存，重复批次执行不再在 `StringMatch` 和 `FIND` 热路径重复构造字节切片；缓存键随指令变化失效，并通过表达式/内核测试验证。
- [x] `EvalWorkspace.MemoryBytes` 汇总四类容器 backing array 与 bitmap、literal、dictionary match 的自有容量，遍历完整 capacity，包含 `Ensure` 缩短后隐藏槽位；排除借用字符串/regex、workspace 本体和分配器开销，尚未全面接入预算扣账。
- [x] `TestEvalWorkspaceMemoryBytesTracksRetainedCapacity` 验证容器和嵌套容量、借用字符串长度变化、`Ensure(0, 0)` 后保留容量、重置归零与零分配；2026-09-09 修复此前漏算容器及隐藏槽位的问题，不能再用原先仅断言 32 字节的测试作为完整计费证据。
- [x] 查询 workspace 创建阶段按编译指令字面量长度预留每个 worker 的 literal byte cache，避免首次执行时产生未计费的缓存增长；dictionary match 容量仍按运行时数据形态处理。

## P5：聚合、Hash 和 Join

- [ ] 分离 key 规范化、hash、probe 和 aggregate update，使用预算约束的块工作区。
- [x] `Hash64Batch` 的 canonical hash 契约、Scalar/Auto/SIMD 差分和 in-place 输入输出语义已有 kernel 单测；probe/update 分阶段重构仍未完成。
- [x] `Hash128Batch` 已覆盖 pair key、nullable mask、Scalar/SIMD 差分及左右输入原地输出单测；Join probe 和 aggregate update 仍未接入统一批工作区。
- [x] 现有 `hashagg` 已有 COUNT/SUM/MIN/MAX/AVG 的 nullable、冲突、rehash 和 state merge scalar 契约单测；这些更新仍未改造成独立 SIMD batch kernel。
- [x] `PairTable.AggregateSelectedNullableValues` 已接入 direct `GroupShapePairFixed`，统一处理选择 bitmap、双 key NULL mask 和 value validity；差分/边界测试已通过。Pair key/validity/value scratch 现由 worker workspace 复用并在创建时预留预算；probe/update SIMD 与完整端到端证据仍未完成。
- [x] 已补充 `BenchmarkPairAggregateSelectedNullableValues`（Apple M4 Pro，最终代码版本）记录批聚合基线；该基准包含表初始化成本，不能作为端到端 SIMD 准入证据。
- [ ] 单 key、pair key、nullable key 统一 hash 契约，不改变相等性和 NULL 分组。
- [ ] 删除只为 hash SIMD 构造整列临时数组的路径。
- [ ] 将 COUNT、MIN、MAX、SUM、AVG 的规则访问部分纳入批 kernel；连续归约和 state merge 优先向量化，随机槽位更新保持有序。
- [ ] Join build/probe 复用 key kernel，保持重复 key、NULL、左右连接和输出 multiplicity。
- [ ] resident、partitioned、spill 共用规则；不扩张未支持聚合或修改溢出语义。
- [ ] 单测覆盖全同/全异/强冲突/倾斜/NULL key/value、同批重复槽位、分区合并、重复匹配、空侧、spill、预算错误和取消。
- [ ] 端到端基准必须计入 hash 暂存、probe、state 更新和结果物化。

## P6：字符串、编码和扫描

- [x] dictionary predicate 已先计算字典项再处理索引，并保持稳定域缓存和零分配复用；offsets byte-length 仍是 scalar kernel。
- [x] equality、prefix、suffix、contains 已统一进入 `kernel.Set.CompareStringMatch` 的正式 scalar 入口，并保留标准库字节语义；SIMD 候选和性能准入仍待评估。
- [x] 普通 StringView 的 equality predicate 已从 typed comparison 路径调用同一 `kernel.Set` 入口；dictionary equality 继续使用字典域缓存。
- [x] equality/prefix/suffix/contains 现在各自有独立 `KernelID`，能力报告可逐操作显示 scalar 后端。
- [x] `kernel/string_match_benchmark_test.go` 已建立 Apple M4 Pro scalar 基线，覆盖 ASCII equality/prefix/suffix、混合 Unicode contains、NULL 和长短字符串；三组 1 秒样本均为稳态零分配，尚无 SIMD 准入。
- [x] 已生成最终代码版本的 10 组 1 秒字符串基准和 CPU profile：`validation/simd-string-match-arm64.pprof`；命令、架构和样本结果已留在本轮交付记录。
- [x] 已补齐真实 predicate 表达式的 10 组 1 秒 ASCII/混合 Unicode contains 基准；Scalar/Auto 约 `311–321 µs`、`8 B/op`，当前 Auto 与 scalar 一致且无 SIMD 候选。
- [x] ASCII/Unicode equality/prefix/suffix/contains 共用 `CompareStringMatch` 正式入口；该入口按原始字节语义工作，不增加 ASCII 之外的额外全量扫描。
- [x] UTF-8 长度继续使用既有 rune 解码路径；ASCII、中文、emoji 和畸形输入由 `TestUTF8LengthASCIIUnicodeAndMalformedInputs` 直接覆盖，未以 continuation byte 计数替代语义。
- [x] `TestStringMatchKernelSharedBufferAndNonZeroOffsets` 覆盖共享 data buffer、非零 offsets、空值尾部和字节匹配结果；完整字符串矩阵仍待补齐。
- [ ] 评估 Arrow/Parquet 交界的批量转换、时间缩放和 validity 构造，复用依赖库已有优化。
- [x] Parquet Float32 与 IntLogicalType 窄整数读取已验证原生宽度、定义级别 NULL、边界和溢出拒绝；其他物理类型转换仍待评估。
- [ ] 删除失败候选并保留拒绝证据，不把失败实现藏在 forced 模式。
- [x] 字符串单测矩阵已覆盖空串、NULL、ASCII、中文、emoji、组合字符、畸形 UTF-8、非零 offsets、共享 buffer、字典域变化和长短混合；`TestUTF8LengthASCIIUnicodeAndMalformedInputs`、`TestStringMatchKernelSharedBufferAndNonZeroOffsets` 及 dictionary workspace 测试通过。该项只证明语义覆盖，不证明 SIMD 性能准入。

## P7：排序、窗口和嵌套数据

- [ ] Sort/Top-K key 表达式统一使用批执行器，批量处理可规则化 key。
- [ ] 评估有序段边界、相邻 key、窗口分组边界和前缀累计的向量化阶段。
- [ ] 保持浮点求值顺序和整数溢出契约。
- [ ] List/Struct 只复用连续子数组和 validity kernel，不强行处理不规则遍历。
- [ ] 比较器、堆、随机 gather、复杂窗口无收益时保留正式 scalar 并记录证据。
- [ ] 单测覆盖多 key、NULL 排序、特殊浮点、tie、批边界、spill 分段、空/全 NULL 分组和父子容器 NULL。
- [x] 已补齐 native 数值排序 key 读取与结果物化的 scalar 路径和 Int8/Float32 调用链测试；尚未实现排序/窗口 SIMD 或完整边界矩阵。
- [x] `TestSortFloatSpecialValuesAndStableZeroTie` 已补充 Float64 NaN、负零/正零稳定 tie 的排序语义回归；完整窗口/Spill 分段矩阵仍未完成。
- [x] Window reader native input coverage is tested for signed, unsigned, and Float32 values; full window output/partition-boundary matrix remains pending.

## P8：双架构验收和清理

- [ ] arm64 在原生 Apple Silicon 验收；amd64 在原生 Linux AVX2 验收；AVX-512 需要原生机器。
- [ ] 不支持的宽度标记不适用；缺少硬件证据保持未完成，不用交叉编译或模拟代替。
- [ ] 更新 `validation/simd-auto-gates.json`，关联正确性、原始样本、统计和调用链证据。
- [ ] 删除旧接口、兼容 wrapper、重复执行器、失效生成模板和测试捷径，更新 README、扩展指南、脚本和示例。
- [ ] 输出覆盖报告：候选数、硬件 SIMD 数、Auto 数、实际调用数和拒绝原因分开统计；不把 profile 占比当作指令占比。

## 接口和语义约束

- `memory.Batch` 使用十种真实位宽向量；同步更新内存核算、序列化和消费者，禁止旧宽容器适配层。
- 扩展现有 `KernelID`、`kernel.Set` 和能力报告；SIMD 类型不得进入公共 API。
- 统一表达式编译和工作区，复用原生 `BatchStream`；被替代接口直接删除，不维护新旧两套执行器。
- 标准 Arrow/Parquet 继续支持；删除内部兼容代码不等于破坏标准格式。
- 原生存储不自动改变类型提升、整数溢出、AVG/SUM、浮点或字符串语义。
- Scalar/Auto/SIMD 继续存在；强制模式只执行正式后端，无 SIMD 时如实报告 scalar。

## 强制单测、全链路和性能门槛

- [ ] 每个操作测试长度 0、1、lane−1、lane、lane+1、63/64/65、多向量和尾部。
- [ ] NULL 密度测试 0%、1%、50%、99%、100%，覆盖 nil 和显式全有效 bitmap。
- [ ] 选择率测试 0%、1%、10%、50%、90%、100%，覆盖连续、随机、聚集选择。
- [ ] 覆盖列列、列常量、切片、字典及输出复用；关键整数使用独立精确参考，布尔使用真值表，fuzz 固化失败样本。
- [ ] 无 SIMD、SIMD、`GODEBUG=simd=0`、支持的 128/256/512 位路径全测；完成 race、vet、生成检查和百万行端到端链路。
- [ ] 测试小内存 spill、OOM、取消、I/O 错误、重复 Close 和借用数据生命周期。
- [ ] 同一最终构建、同一数据，每组至少 10 次、每次 1 秒样本。
- [ ] Auto 候选相对规范 scalar 至少改善 10%，`p < 0.05`；相关端到端工作负载不得退化超过 3%。
- [ ] 分别记录 ns/op、rows/s、B/op、allocs/op、峰值内存；不得用未计入的暂存换局部加速。
- [ ] 每项完成记录实现提交、单测入口、架构、命令、基准文件和准入/拒绝结论。

## 最终交付检查

## 本轮增量交付证据（2026-09-07）

- Native vectors/storage: `memory/native_numeric_test.go`, `source/arrowrecord/source_test.go`, `memory/spill_columnar_test.go`；验证命令为 `go test ./memory ./source/arrowrecord`。
- Native expression and physical chain: `expr/native_numeric_test.go`, `expr/program_test.go`, `physical/native_numeric_test.go`；验证命令为 `go test ./expr ./physical`。
- Parquet Float32: `source/parquet/float32_test.go`；验证物理 4 字节、nullable definition levels、NaN/负零，命令为 `go test ./source/parquet -run NativeFloat32`。
- Parquet logical narrow integers: `source/parquet/narrow_test.go`；验证 IntLogicalType 8/16 signed/unsigned、原生容器、NULL 和边界，命令为 `go test ./source/parquet -run NativeParquet`。
- Join native payload: `physical/native_numeric_test.go`；验证 clone 深拷贝和五种 native payload map，命令为 `go test ./physical -run Native`；key build/probe 尚未迁移。
- Native scalar comparison/compact entries: `kernel/int8_compare_test.go`, `kernel/kernel_test.go`；验证命令为 `go test ./kernel`。
- Float32 column-column predicate: `expr/program_test.go`, `kernel/float32_columns_simd.go`；验证命令为 `GOEXPERIMENT=simd go test ./kernel ./expr`，覆盖双列 NULL、尾部和 forced mode；列列 SIMD 尚未进入 Auto。
- Float32 column-column 三组 1 秒基准（Apple M4 Pro）约 `1.20 ms` SIMD 对 `1.44 ms` scalar，仍需十组统计及端到端门槛后才能考虑 Auto。
- Int32 column-column 三组 1 秒基准（Apple M4 Pro）约 `1.20 ms` SIMD 对 `1.43 ms` scalar，仍需十组统计及端到端门槛后才能考虑 Auto。
- Uint32 column-column 三组 1 秒基准（Apple M4 Pro）约 `1.20 ms` SIMD 对 `1.41 ms` scalar，仍需十组统计及端到端门槛后才能考虑 Auto。
- Int64 column-column 三组 1 秒基准（Apple M4 Pro）约 `1.43 ms` SIMD 对 `1.41 ms` scalar；portable SIMD 回退，保留正式 scalar。
- Float64 column-column 三组 1 秒基准（Apple M4 Pro）约 `1.44 ms` SIMD 对 `1.40 ms` scalar；portable SIMD 回退，保留正式 scalar。
- `kernel/float32_columns_fuzz_test.go` 增加长度、截断输入、validity mask 和五种比较操作的 scalar/SIMD 差分 fuzz 种子。
- Float32 forced SIMD candidate: `kernel/float32_simd_test.go`, `kernel/float32_compare_benchmark_test.go`；`GOEXPERIMENT=simd go test ./kernel -run TestFloat32ForcedSIMDRegistrationAndDifferential` 通过，Apple M4 Pro 三组 1 秒基准约 0.64 ms vs scalar 1.18 ms；尚未达到十组统计和端到端准入门槛，Auto 保持 scalar。
- Float32 arithmetic: `kernel/float32_arithmetic_test.go`, `kernel/float32_compare_benchmark_test.go`；scalar 语义和无效值处理通过，portable SIMD 三组基准回退后删除候选，保留 scalar。
- Cross-mode checks: `GOEXPERIMENT=simd go test ./kernel ./expr ./physical ./memory ./source/inmemory ./source/arrowrecord`、`GODEBUG=simd=0` 同命令、`go vet`、`go run ./internal/cmd/gensimd -check` 和 `./scripts/check_simd_structure.sh`。
- 本轮没有 SIMD/Auto 性能准入；拒绝/保留 scalar 的理由记录在 `docs/SIMD_CANDIDATE_DECISIONS.md`。

- [ ] 所有阶段有可核验交付记录。
- [ ] 原生位宽贯穿输入、执行、spill 和输出。
- [ ] 所有生产数值路径使用统一正式执行体系。
- [ ] 新 SIMD 后端通过语义差分、边界和架构测试。
- [ ] 性能结论来自最终代码版本。
- [ ] 没有以兼容、临时分支或 forced-only 失败实现掩盖未完成工作。
- [ ] 最终报告明确哪些真正加速、哪些保持 scalar、原因和仍缺失的硬件验收。


## 2026-11-08 contiguous numeric stage

Implemented Float64 Add and ordered literal/column comparisons, native Float32
Add/Sub/Mul/Div kernels, and all five literal comparisons for the existing
Int8/Int32/Int64/Uint32/Uint64 dispatch families. Portable native selection preserves
runtime emulation guards and existing arm64 overrides. See
[validation/numeric-native](../validation/numeric-native/README.md) for the exact
correctness, native execution, cross-compilation, and measurement scope.

Remaining: Int16/Uint8/Uint16 literal dispatch; wider integer column comparisons;
Float32 expression arithmetic currently promotes to Float64 and is not a native
Float32 kernel call. Keep this public type behavior until a separate semantics
change is requested. Next optimize mask packing/null handling with measured query
and memory effects, then profile aggregation/compaction before selecting more work.
