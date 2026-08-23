# Keys and encodings

## Why encoding is a physical property

逻辑类型 `string` 不等于唯一物理表示。Planner/Executor 必须看到 Encoding：

```text
String + Dictionary -> uint32 code hot path
String + View       -> offsets + bytes hot path
```

提前把 dictionary 展开为 Go `string` 会失去 code hash/group-by 优势并增加 GC 指针。

## DictionaryDomain contract

两个 batch 都有 code=7，并不意味着它们表示同一个字符串。只有 domain 相同时才允许直接 compare/group/join code：

```text
batch A: domain="country-v3", code 7 -> SG
batch B: domain="country-v3", code 7 -> SG    OK

batch C: domain="rowgroup-19", code 7 -> US   NOT OK
```

因此 `memory.Field.DictionaryDomain` 和 `DictionaryStringVector.DictionaryDomain` 是 correctness contract，不是 metadata decoration。

## Plain string ownership

Source batch 的 Data buffer 只保证当前 `ReadPartition` 生命周期。长期 hash table state 不能引用它。当前 `hashagg.StringTable` 的 arena 只复制**新 group key**，不会为每一行创建 string。

## Composite key

两列 fixed key 编译为 128-bit，避免：

- `fmt.Sprintf`；
- delimiter/string concat；
- heap tuple object；
- generic reflection equality。

后续超过 16 bytes 的 multi-column key 应引入 row-key encoder / fingerprint + payload equality，而不是无限扩 struct specialization。

## NULL identity in Phase 2.2

Physical key payload and validity are separate semantics. Single-key tables keep one dedicated NULL aggregate state; fixed composite keys include a validity mask in hash/equality identity. Invalid component payload is canonicalized before hashing, so arbitrary bytes under NULL cannot create multiple logical NULL groups.

Dictionary NULL rows do not require a reserved code: validity controls NULL independently of the dictionary index buffer.
