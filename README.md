# 分片键值存储：本地副本同步与恢复（教学/模拟实现）

本工程用**本地文件 + 内存状态机**模拟一个多分片、多副本的键值存储，重点演示并可验证以下主题：

- 按键（CRC32 哈希）分片，**分片区间与副本归属可动态调整（再平衡）**；
- 写入在**多数派副本确认**后才对外可见；
- **重复提交**幂等、不重复应用、不版本回退；
- **写入失败后用新请求标识安全重提**：失败值被物理中止，重启/追赶/再平衡后都不会复活；
- **不声明批次，普通写入接口也能对同键并发决胜**：胜出值只由本批提交内容决定，
  每个客户端拿到可区分的结论（COMMITTED / SUPERSEDED）；
- 重启后**日志重放**，落后副本**追赶**；
- 截断 / 缺号 / 乱序 / 校验和损坏的日志**不会被静默跳过**，恢复停在最后一个一致位点；
- 同一键的并发写入，最终版本**确定且可复现**；
- 上述结论在**整集群重启、副本追赶、分片归属重新划分（含中途失败）之后依然成立**。

全程只依赖 JDK（`java.util`、`java.util.concurrent`、CRC32、文件 IO）与 Spring Boot
测试设施，**不需要任何外部服务或真实凭据**。

---

## 1. 目录结构与磁盘布局

核心代码位于 `src/main/java/com/github/h3yow/shardedreplicatedstorejava/store/`：

| 文件 | 职责 |
| --- | --- |
| `KeyHasher` | `key -> CRC32 mod 2^20`，稳定映射到哈希空间 |
| `ShardRange` / `Topology` / `TopologyValidator` / `TopologyException` | 分片区间、世代拓扑、合法性校验（重叠/空洞/越界/非法副本集等可区分原因） |
| `LogEntry` / `EntryType` | 追加日志条目（PUT / MIGRATE），带行内 CRC32 校验和，制表符分隔、Base64 编码 |
| `PersistentLog` | 本地追加日志；严格扫描与“容错扫描（返回到损坏前的一致前缀）” |
| `LogCorruptionReason` / `LogCorruptionException` | 截断 / 缺号 / 乱序 / 校验和 / 格式错误等可区分原因 |
| `KvStateMachine` / `VersionedValue` | 键值状态机：顺序应用、版本单调、拒绝回退 |
| `Replica` | 单个分片副本：日志 + 状态机 + 提交位点 + 可用性模拟 + 重启恢复 |
| `ReplicaCatchup` | 在线同步（只追加缺失后缀）与恢复性追赶（截断分叉/损坏后对齐） |
| `ShardReplicaSet` | 一个分片的多数派写入协调、requestId 幂等、提交传播、失败尾部安全中止 |
| `IdempotencyTable` | requestId -> 首次结论的持久化去重表（含成功/失败/落败，随键再平衡迁移） |
| `ContentionGate` | 普通写入路径的同键并发决胜门（无需声明批大小，两段式自适应安静窗口） |
| `DeterministicLock` | 互斥 + 批量严格全序（按 requestId 排序放行），供显式批次接口使用 |
| `Cluster` | 门面：路由写入/读取、再平衡编排、整集群重启、副本追赶 |
| `TopologyManager` | 拓扑文件的解析、校验、临时文件 + 原子替换 |

磁盘布局（运行时由测试的临时目录承载）：

```
<dataDir>/
  topology.txt                 当前世代拓扑（原子替换）
  <shardId>/
    idempotency.txt            requestId 首次结案表
    <replicaId>/log.txt        追加日志（每行带 CRC32）
    <replicaId>/commit.txt     提交位点（一个数字）
```

---

## 2. 一致性模型

### 2.1 分片与世代（epoch）

- 哈希空间固定为 `[0, 2^20)`。每个分片是一个左闭右开区间。
- 一份 `Topology` 属于一个 **epoch（世代）**，包含全部分片区间与每个分片的副本列表。
- 拓扑生效前由 `TopologyValidator` 强校验，任一项不满足都拒绝生效，并给出**可区分原因**：
  - `EMPTY_TOPOLOGY`：没有任何分片；
  - `DUPLICATE_SHARD_ID`：分片 ID 重复；
  - `RANGE_OUT_OF_BOUNDS`：区间越出 `[0, 2^20]`；
  - `RANGE_INVALID_BOUNDARY`：空区间 / 起点不小于终点；
  - `OVERLAPPING_RANGES`：区间两两重叠；
  - `UNCOVERED_HASH_GAP`：区间首尾不相接或未覆盖到空间上界，存在**空洞 / 未覆盖键**；
  - `REPLICA_SET_INVALID`：副本为空或含重复副本 ID。

### 2.2 多数派规则（quorum）

- 每个分片 `N` 个副本，多数派 `quorum = N/2 + 1`。
- 写入流程（`ShardReplicaSet.write`）：
  1. 整个写入在分片级临界区内串行（普通路径互斥；并发测试可走“批量严格全序”）；
  2. 先查幂等表，`requestId` 已有结论则直接返回首次结论（`DUPLICATE`）；
  3. leader 先把条目追加到本地日志（分配连续 `index` 与单调 `version`）；
  4. 对每个 UP 的 follower，原子地“**在线同步缺失的连续后缀 + 追加本轮条目**”；
  5. 只有 UP 副本成功落盘才计入 ack；
  6. `acks >= quorum` 才推进提交位点并应用到状态机，返回 `COMMITTED`。
- 三类失败**明确区分**：
  - `COMMITTED`：多数派确认，对外可见；
  - `REPLICA_UNAVAILABLE`：leader 不可用，或集群中 UP 副本数本身凑不够多数派；
  - `INSUFFICIENT_ACKS`：副本在线但本轮确认数不足多数派（条目未提交、不可见）；
  - `SUPERSEDED`：同键并发决胜落败（见 2.5），不复制、不占版本、不可见；
  - `DUPLICATE`：同一 requestId 重复提交，返回首次结论。
- **只有提交位点之前的日志会被应用到状态机并对外可见**；未达多数派的条目即使在
  leader 落盘，也不会被读取到。

### 2.3 幂等、版本与“失败后安全重提”

- `requestId -> 首次 WriteResult` 持久化在每分片的 `idempotency.txt`，结论记录还
  携带该请求写入的 `key`（供再平衡时随键迁移）。
- 重复提交（无论首次是成功、失败还是并发落败、无论重试时携带什么 value）都返回
  首次结论（`DUPLICATE` 包装），**不产生第二条日志、不重复应用、不回退版本**。
- 每个键的 `version` 在状态机内单调递增：
  - 更大 version 才覆盖；
  - 相同 version（重复迁移）幂等忽略；
  - 更小 version 直接抛错，杜绝回退。
- **安全重提规则（重点新增）**：一次写入因副本掉线 / 确认数不足失败后，
  在下一次提交尝试之前，协调者会先执行 **失败尾部安全中止**：
  1. leader 把自己日志里“提交位点之后”的未决条目**物理截断**；
  2. 每个在线 follower 走一次以 leader 为准的逐 index 对齐，提交点之后若持有
     上一轮失败条目（与截断后的 leader 分叉）则截断并补齐，**已确认前缀不动**；
  3. 仍 DOWN/SLOW 的副本此刻无法处理，等其恢复后由在线同步的分叉截断或恢复性
     追赶清掉同一条失败尾部。

  于是：**换一个新 requestId 重提同一 key 时，成功值严格以更大版本对外可见；
  之前失败提交的值，无论副本之后怎么同步、集群怎么重启、分片怎么调整，都不会再被
  任何读取看到**（它从不进入提交位点，且日志尾部被物理移除）。旧的失败 requestId
  本身继续保留第一次的失败结论，重提它只得到 `DUPLICATE`，不会被当成新的成功提交，
  也不会重复应用。

### 2.4 同键并发写入的决胜（两条路径）

现在提供两条互不冲突的路径：

**（A）普通写入接口即可决胜（新增，无需声明批次）**——直接调用
`Cluster.write(requestId, key, value)`。分片内的 `ContentionGate` 会自动把
“同一时刻挤进来的同键写请求”识别为一批：

- 封轮后，本批 **requestId 字典序最大者获胜**，胜出值只由这批提交的内容决定，
  与线程调度、请求到达先后、副本处理顺序无关；重复运行结果一致；
- 成员按 requestId 升序逐个进入分片临界区分配连续 index / 单调 version，因此
  不会出现版本重复、跳号、回退，也不会有 index 冲突；
- 每个客户端结论可区分：恰好 1 个 `COMMITTED`，其余全部 `SUPERSEDED`
  （不复制、不占版本、不可见）。`COMMITTED` 客户端随后读到的一定是自己的值；
  落败者用自己的 requestId 重提只会得到 `DUPLICATE`（首次落败结论被持久化）。

轮次识别采用**两段式自适应安静窗口**（没有外部屏障时，“同一批”本质上只能按时间
上的并发关系识别）：

1. *探测窗口*：轮次里第一个请求先等一个很短的探测窗口（默认 2ms，
   `-Dcontention.probe.millis`），窗口内没有后来者即判定为无竞争单独写，立即放行
   ——顺序写几乎不付额外延迟；
2. *批次窗口*：一旦出现竞争者就升级为批次模式，此后每来一个新竞争者都把安静窗口
   重新计时，必须连续一个批次窗口（默认 200ms，`-Dcontention.cohort.grace.millis`）
   没有新竞争者才封轮。该窗口按“覆盖十几路线程同时被唤醒时的操作系统调度抖动”
   取值，单 JVM 压测（同 key 十几路并发）下不会把一批拆成多批、不会死锁或超时。

> 局限（如实说明）：纯时间窗口是单 JVM 教学模拟里的启发式归组，不是形式化屏障。
> 极端过载（调度抖动超过批次窗口）理论上仍可能拆批；需要零时间依赖的严格全序时，
> 请用下面的显式批次接口。

**（B）显式批次严格全序（原有，保留兼容）**——
`Cluster.writeInBatch(requestId, key, value, batchSize)`：调用方声明本批竞争者数，
`DeterministicLock` 在竞争者到齐前只排队，到齐后严格按 requestId 升序逐个提交，
不做 SUPERSEDED 判定（同批每个请求都成为一个顺序版本）。这与（A）的差异是：
（B）由调用方给出精确批次边界，结论零时间依赖；（A）无需任何调用方信息，由系统按
并发关系自动归组并只让最大者胜出。

### 2.5 胜出值的归属标识

每个可见值额外记录 `winnerRequestId`（产生该值的原始请求标识）：普通 PUT 等于
本条 requestId；再平衡 MIGRATE 沿用迁移前的胜者。因此“最终胜出值属于哪个请求”
跨世代、跨重启保持不变，可用于断言“某客户端拿到成功且最终读到的确实是自己的值”。

---

## 3. 分片归属变更（再平衡）

`Cluster.reshard(newRanges, newReplicas)` 的编排：

1. 用新区间/副本构造 `Topology(epoch+1)` 并先做合法性校验：重叠、空洞、越界、
   非法副本集一律拒绝，旧拓扑与旧数据保持不变；世代号必须递增，防止旧拓扑回滚。
2. 在切换 `this.topology` **之前**枚举旧拓扑各分片 leader 状态机中的**已确认**快照
   （只含已提交数据，未提交条目不迁移）。
3. 打开新分片副本集；按新拓扑把每个键路由到新分片，以 `MIGRATE` 日志条目追加，
   并要求**新分片多数派确认**；迁移沿用原值与原 version（不重生版本、不回退）。
   键按字典序迁移，保证崩溃后重放顺序一致；同一逻辑键只迁移一次。
4. 全部键迁移成功后，**再把每个旧分片幂等表中的 requestId 首次结论（成功 / 失败 /
   并发落败）按其写入 key 的新归属登记到新分片去重表**——保证调整归属后，用旧
   requestId（包括失败请求）重提仍然是 `DUPLICATE`，不会被当成新提交；
5. 数据与去重结论都迁移成功后，新拓扑写入 `*.staged` 临时文件，再 `ATOMIC_MOVE`
   替换 `topology.txt`，对外原子切换世代。中途任一步失败（含非法目标拓扑）都不会
   替换拓扑文件，旧拓扑与旧的已确认数据原样保留；
6. 旧分片目录保留不删，迁移过程可追溯、可重放。

由此保证：调整过程中**已确认数据不丢失、不重复、不会落到错误分片；requestId 的
首次结论（含失败值的结论）不改变；失败提交的值不会借再平衡重新可见**。

---

## 4. 恢复流程

### 4.1 日志格式与校验

每行一条，制表符分隔，键/值/requestId 做 Base64 编码，行首是**整行载荷的 CRC32**：

```
crc=<校验和>  idx=<日志index>  term=<世代>  req=<base64 requestId>  type=PUT|MIGRATE  ver=<键版本>  key=<b64>  val=<b64>  win=<b64 胜者requestId>
```

无 `win` 字段的旧格式日志仍可解析（胜者取该条目自身 requestId）；幂等表
`idempotency.txt` 同样在 `req` 之后增加了 `key` 字段，旧 8 字段行也能载入。

### 4.2 重启重放

`Replica.recover()`（以及构造时）调用 `PersistentLog.scanWithReport()` 顺序扫描，
逐条验证“行可解析 → 校验和一致 → index 从 1 起严格连续”，并**在解析坏行之前**
先判断最后一行是否缺少换行终止符：

| 现象 | 原因 `LogCorruptionReason` | 恢复行为 |
| --- | --- | --- |
| 末行无换行符（写入中途结束） | `TRUNCATED_ENTRY` | 停在该行之前 |
| index 跳跃（如 1,2,4） | `MISSING_INDEX` | 停在缺口前一位点 |
| index 倒退/重复（如 1,2,2,4） | `INDEX_OUT_OF_ORDER` | 停在冲突前一位点 |
| CRC 不匹配（位翻转/篡改） | `CHECKSUM_MISMATCH` | 该行及之后不应用 |
| 行无法解析 | `MALFORMED_LINE` | 停在该行之前 |

恢复产出 `RecoveryReport(shardId, replicaId, lastConsistentIndex, appliedCount,
clean, stopReason)`。状态机只重放至 `min(最后一致位点, 提交位点)`，因此：

- **损坏/缺失/乱序不会被静默跳过**；
- 恢复**停在最后一个一致位点**，对外可见状态以该位点与提交位点的较小者为准。

### 4.3 落后副本追赶

`ReplicaCatchup` 区分两条路径：

- **在线同步 `alignCommittedSuffix`（写前）**：follower 只可能落后、不可能在已提交
  区间分叉，因此只顺序追加 leader 上缺失的连续后缀，**不截断、不回退、不擅自推进
  提交位点**。提交位点只能由协调者在多数派达成后统一推进（单调不降）。
- **恢复性追赶 `catchUp`（宕机/损坏后）**：以 follower 实际扫描到的一致前缀为基准，
  先截断损坏/分叉后缀，找到与 leader 的公共前缀，补齐缺失条目，再按 leader 的提交
  位点推进并重放为可见状态。

---

## 5. 本地验证方法

需要 JDK 21+、Maven 3.9+（仓库在更新的 JDK 上也可编译）。

```bash
# 编译
mvn -q compile

# 运行全部单测
mvn test

# 只跑某一类场景
mvn test -Dtest=TopologyChangeTest               # 分片变更：重叠/空洞/越界拒绝 + 合法再平衡不丢数据
mvn test -Dtest=WriteConsensusTest               # 多数派、确认不足/副本不可用区分、重复提交幂等
mvn test -Dtest=LogRecoveryTest                  # 截断/缺号/乱序/校验和损坏、停点、追赶
mvn test -Dtest=ConcurrentWriteTest              # 显式批次：同键并发最终版本确定可复现
mvn test -Dtest=SafeRetryAfterFailureTest        # 失败后换新 requestId 安全重提、失败值不复活
mvn test -Dtest=ContentionArbitrationTest        # 普通接口同键并发决胜（15 路×20 轮等）
mvn test -Dtest=ReshardRestartConsistencyTest    # 重启+再平衡+追赶后的最终一致性
```

测试位于 `src/test/java/.../tests/`，共 7 类 42 个用例，使用 JUnit 5 临时目录，
不落任何外部状态。**单测日志会打印位点（index/commit）、版本（version）与判定依据**，
例如：

```
[s1] 写入提交 requestId=req-2 key=alpha index=2 version=2 acks=2/3（判定依据: 多数派落盘）
同键决胜封轮 key=hot 批次大小=15 升序=[req-001, ..., req-015] 胜者=req-015（判定依据: requestId 字典序最大）
[s1][r1] 在线同步发现提交点之后的分叉，截断到 index=1（丢弃失败提交残留 1 条，不影响已确认数据 commit=1）
[判定依据] 再平衡+重启后重提 req-fail 仍为 DUPLICATE，可见值保持 retry-wins
```

并发决胜的归组安静窗口可用系统属性临时调大/调小复现边界行为：

```bash
mvn test -Dtest=ContentionArbitrationTest -DargLine="-Dcontention.cohort.grace.millis=300"
```

### 5.1 场景与结论对照

| 需求场景 | 对应用例 | 期望结论 |
| --- | --- | --- |
| 区间重叠 | `rejectsOverlappingRangesWithDistinctReason` | 抛 `OVERLAPPING_RANGES` |
| 区间空洞/未覆盖 | `rejectsUncoveredGapWithDistinctReason` | 抛 `UNCOVERED_HASH_GAP` |
| 越界/空区间/重复ID/重复副本 | `rejectsOutOfBoundsAndInvalidBoundaryAndDuplicates` | 各自不同 reason |
| 合法再平衡 | `validReshardMovesCommittedDataWithoutLossOrDuplication` | 值/版本不变、归属更新、重载一致 |
| 多数派可见性 | `writeIsVisibleOnlyAfterQuorumAck` | 2/3 可见；1/3 不可见且原因可区分 |
| 重复提交 | `duplicateSubmitReturnsFirstOutcomeAndNeverAppliesTwice` | `DUPLICATE`，不重复应用、重启后仍幂等 |
| 失败请求重试 | `failedRequestIdIsAlsoDistinctAndNotConfusedWithDuplicateOrCommit` | 首次失败结论保持，新 requestId 才提交 |
| 截断日志 | `truncatedLastEntryStopsRecoveryAtLastConsistentIndex` | 停点=4，追赶后到 5 |
| 缺号/乱序 | `missingIndexAndOutOfOrderAreDetectedAtCorrectStopPoint` | 分别 `MISSING_INDEX` / `INDEX_OUT_OF_ORDER` |
| 校验和损坏 | `checksumMismatchIsDetectedAndNeverApplied` | `CHECKSUM_MISMATCH`，被篡改条目不应用 |
| 宕机后追赶 | `laggingReplicaCatchesUpAfterDowntime` | 补齐缺失并重新参与多数派 |
| 并发确定性（显式批次） | `concurrentWritesToSameKeyProduceDeterministicFinalVersion` 等 | 多次运行 index/version/最终值完全一致 |
| 失败后安全重提 | `failedValueNeverResurfacesAfterSafeRetryWithNewRequestId` | 新 requestId 以更大版本成功；失败值在追赶/重启后不复活；旧 requestId 永远 DUPLICATE |
| 确认不足后安全重提 | `insufficientAcksFailureIsAlsoAbortedBeforeSafeRetry` | 失败尾部被中止，重提位点从 1 连续无跳号 |
| 普通接口同键决胜 | `ordinaryWriteArbitratesSameKeyConcurrentlyWithoutBatchHint`（15 路×20 轮） | 恰好 1 个 COMMITTED，其余 SUPERSEDED，版本=1，不死锁/超时 |
| 决胜多次可复现 | `repeatedRunsAlwaysConvergeToSameWinnerAcrossIndependentDirectories` | 多次独立运行最终胜者/值/版本完全一致 |
| 决胜后顺序写单调 | `sequentialWritesAfterContentionKeepMonotonicVersions` | 版本继续 +1，不回退不跳号 |
| 重启+再平衡+追赶一致 | `winnerValueVersionOwnerAndWinnerRequestIdSurviveRestartAndReshardAndCatchup` | 值/版本/胜者/归属调整前后一致，失败值不复活 |
| 再平衡中途失败 | `failedReshardKeepsCommittedDataIntact` | 非法拓扑被拒绝，epoch 与已确认数据原样保留 |

---

## 6. 边界与简化说明

这是用于演示一致性结论的本地模拟，有意做了如下简化：

- 单 leader、无真实选举；副本“可用性”通过 `ReplicaStatus`（UP/SLOW/DOWN）注入。
- 复制为同步派发模型，不模拟网络延迟/分区/真实并发日志协商；在线同步利用“同 term
  单 leader 不会在已提交区间分叉”的不变量，提交点之后的分叉只可能来自失败提交，
  安全中止 / 在线同步会将其截断。
- 每次追加后直接关闭文件以模拟落盘（fsync 语义），追求结论清晰而非吞吐。
- 普通写入路径的同键决胜通过“两段式自适应安静窗口”自动归组，不要求调用方声明批
  大小；这是单 JVM 的启发式（默认探测 2ms / 批次 200ms，可调），极端过载下不保证
  形式化的批次边界。需要零时间依赖的严格全序时使用显式批次接口
  `writeInBatch`（与原有用法完全兼容，同批每个请求仍各占一个顺序版本，不做
  SUPERSEDED 判定）。
