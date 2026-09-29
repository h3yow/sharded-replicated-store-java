# 分片键值存储：本地副本同步与恢复（教学/模拟实现）

本工程用**本地文件 + 内存状态机**模拟一个多分片、多副本的键值存储，重点演示并可验证以下主题：

- 按键（CRC32 哈希）分片，**分片区间与副本归属可动态调整（再平衡）**；
- 写入在**多数派副本确认**后才对外可见；
- **重复提交**幂等、不重复应用、不版本回退；
- **写失败后可用新请求标识安全重提**：失败值永不复活，重提版本严格更大；失败的旧
  requestId 永远保留首次结论，重提它不会变成新提交、也不会重复应用；
- **普通写入路径（无需声明批大小）同键并发决胜**：同批并发最终胜出值只由请求内容
  （requestId）决定，与线程调度/到达先后/副本处理顺序无关；赢家 COMMITTED、落败者
  SUPERSEDED，结论可区分；
- 重启后**日志重放**，落后副本**追赶**；
- 截断 / 缺号 / 乱序 / 校验和损坏的日志**不会被静默跳过**，恢复停在最后一个一致位点；
- 同一键的并发写入，最终版本**确定且可复现**，并能扛住整集群重启、落后追赶与再平衡。

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
| `ShardReplicaSet` | 一个分片的多数派写入协调、requestId 幂等、提交传播、安全重提清场、同键决胜 |
| `IdempotencyTable` | requestId -> 首次结论的持久化去重表（含 COMMITTED/SUPERSEDED/失败） |
| `DeterministicLock` | 互斥 + 批量严格全序（按 requestId 排序放行），供显式批次接口使用 |
| `KeyContestGate` | 普通写入路径的同键并发决胜闸门（**无需声明批大小**，探测+收集两段式自动成批） |
| `Cluster` | 门面：路由写入/读取、再平衡编排（含中途失败回滚）、整集群重启、副本追赶 |
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
  - `COMMITTED`：多数派确认，对外可见（并发决胜路径上还表示它就是同批赢家）；
  - `SUPERSEDED`：已多数派落盘但在同键决胜批中落败，不是最终可见值（见 2.4）；
  - `REPLICA_UNAVAILABLE`：leader 不可用，或集群中 UP 副本数本身凑不够多数派；
  - `INSUFFICIENT_ACKS`：副本在线但本轮确认数不足多数派（条目未提交、不可见）；
  - `DUPLICATE`：同一 requestId 重复提交，返回首次结论。
- **只有提交位点之前的日志会被应用到状态机并对外可见**；未达多数派的条目即使在
  leader 落盘，也不会被读取到。

#### 2.2.1 写失败后的安全重提（新 requestId）

未达多数派的失败条目可能已经物理落在 leader（甚至跨重启仍在），或落在部分 follower
本地。它们**永不提交**，并遵循“重提前清场”规则：

1. 普通写入进入时，协调者先把 **leader 上 `index > leader.commitIndex` 的未提交后缀
   物理截断**；
2. 派发给每个 follower 时，先丢弃该副本所有超过 leader 提交位点的残留（无论来自哪一
   轮失败、是否经历过重启），再只补齐 leader 的**已提交**后缀（严格止于其 commit
   位点，绝不捎带在途条目），最后追加本轮条目；
3. 于是新版本号从已提交位点连续分配，严格等于“当前已确认版本 + 1”，**严格大于此前
   所有已确认版本**；失败条目不占 index、不被任何同步/追赶/重放路径带走，状态机
   永远看不到它，杜绝“失败值复活、新值被同版本幂等忽略”。

安全重提规则：**失败后必须换一个新 requestId 重写同一 key**。旧的失败 requestId 已在
幂等表结案，重提它只会得到 `DUPLICATE(首次失败结论)`——副本恢复不会让它悄悄变成成功
提交，也不会产生第二条日志或重复应用。

> 失败条目与已确认条目的唯一分界是多数派提交位点；截断只动 `index > commitIndex` 的
> 部分，已确认数据不丢、不重。

### 2.3 幂等与版本

- `requestId -> 首次 WriteResult` 持久化在每分片的 `idempotency.txt`。
- 重复提交（无论首次是成功还是失败、无论重试时携带什么 value）都返回首次结论，
  **不产生第二条日志、不重复应用、不回退版本**。
- 每个键的 `version` 在状态机内单调递增：
  - 更大 version 才覆盖；
  - 相同 version（重复迁移）幂等忽略；
  - 更小 version 直接抛错，杜绝回退。

### 2.4 同键并发写入的确定性决胜

系统提供两条并发路径，都保证日志 index 连续、版本不重复/不跳号/不回退、不死锁：

#### 2.4.1 普通写入路径：无需声明批大小（`Cluster.write`，默认）

普通 `write` 路径内置 `KeyContestGate` **同键决胜闸门**，调用方无需知道“这一批有几个
竞争者”。它采用**探测 + 收集两段式**自动成批：

1. 每个请求先经历一个很短的**探测窗口**（默认 2ms，`store.contest.probeNanos` 可调）。
   窗口内始终没有同伴则判定无竞争，立即单走——**串行写几乎不付额外延迟**，happens-before
   顺序保持（上一轮结束后才发起的请求与上一批隔着整轮处理时间，探测窗口看不到同伴）。
2. 一旦探测窗口内出现第二个请求，本轮升级为**竞争轮**，进入更长的**收集窗口**
   （默认 40ms，`store.contest.collectNanos` 可调），每来一个新成员窗口就顺延，直到
   连续一个收集窗口没有新成员才关轮。真正并发的请求在同一起跑线后、做磁盘 I/O 之前
   就完成纯内存登记（微秒级），收集窗口远大于线程池调度抖动，因此一批并发会被稳定收全。
3. 关轮后严格按 **requestId 升序**逐个进入分片临界区提交。于是同一批并发提交的
   index、version、最终值**只由这批请求的内容决定**，与谁先到达、哪个线程先被调度、
   副本以什么顺序处理完全无关，反复运行逐次一致。
4. **结论可区分**：requestId 最大者拿到 `COMMITTED`（它写的值即最终可见值）；其余
   n-1 个已多数派落盘者拿到 `SUPERSEDED`，结果里带 `contestPosition`（0 起的决胜位）、
   `contestSize`（批大小）、`winnerRequestId`（赢家）。这样不会出现“客户端拿到成功、
   最终读到的却是别人的值”而无法区分的情况。
5. 同一决胜轮内的重复 requestId 不重复参与、不分配第二个版本，等待先到者结案后复用
   其首次结论（由幂等表最终定性）。

> 边界（教学/模拟的诚实说明）：在完全不声明批次、也没有任何外部屏障时，从原理上无法
> 100% 预知“一个永远不会到来的请求是否属于本批”。这里用足够大的收集窗口覆盖真实线程
> 调度抖动来自动识别“同一批并发”，对真正同时发起的并发是确定且可复现的；它**不会**
> 把有明确先后（串行）的写错误合并。若需要数学上严格、与时序彻底无关的全序，请用
> 2.4.2 的显式批次接口。

#### 2.4.2 显式批次严格全序：`Cluster.writeInBatch(..., batchSize)`（保留，兼容旧用法）

需要可复现并发结论且**能**在调用侧知道竞争者数量时，可继续使用批量严格全序接口：
本批 `batchSize` 个竞争者到齐前只排队，到齐后严格按 requestId 升序逐个提交，与任何
时序因素都无关。该接口语义、签名与旧版本完全一致，既有调用与用例不受影响。

两条路径的版本/index 规则一致，幂等、安全重提、重启重放、追赶、再平衡对两条路径
同等适用。

---

## 3. 分片归属变更（再平衡）

`Cluster.reshard(newRanges, newReplicas)` 的编排：

1. 用新区间/副本构造 `Topology(epoch+1)` 并先做合法性校验：重叠、空洞、越界、
   非法副本集一律拒绝，旧拓扑与旧数据保持不变；世代号必须递增，防止旧拓扑回滚。
2. 在切换 `this.topology` **之前**枚举旧拓扑各分片 leader 状态机中的**已确认**快照
   （只含已提交数据，未提交条目不迁移）。
3. 打开新分片副本集；对每个新分片先 `purgeAllUncommitted()` 清掉可能复用的目录里
   上一次失败迁移/失败写留下的未提交后缀，再按新拓扑把每个键路由到新分片，通过
   `ShardReplicaSet.migrateCommitted(...)` 以 `MIGRATE` 日志条目追加，并要求
   **新分片多数派确认**；迁移沿用原值与原 version（不重生版本、不回退）。
   键按字典序迁移，保证崩溃后重放顺序一致；同一逻辑键只迁移一次。
4. 全部键迁移成功后，新拓扑写入 `*.staged` 临时文件，再 `ATOMIC_MOVE` 替换
   `topology.txt`，对外原子切换世代。
5. **中途任一步失败都不会替换拓扑文件**，并且内存路由会**回滚到旧世代**：对外仍按
   旧拓扑读写，旧分片的已确认数据原样保留（不丢、不重），失败的 `MIGRATE` 条目停留
   在未提交状态、不可见；副本恢复后用相同参数重新 `reshard` 即可成功。旧分片目录
   保留不删，迁移过程可追溯、可重放。

由此保证：调整过程中或调整压力下**已确认数据不丢失、不重复、不会落到错误分片**；
失败迁移与失败普通写入一样，其值不会在任何后续同步/重启/追赶中复活。

> 测试支持：`Cluster.setBeforeMigrateHook(Consumer<Cluster>)` 在“新分片已激活、
> 正式迁移之前”回调，用于在调整中途注入副本故障/压力（生产默认为空操作）。

---

## 4. 恢复流程

### 4.1 日志格式与校验

每行一条，制表符分隔，键/值/requestId 做 Base64 编码，行首是**整行载荷的 CRC32**：

```
crc=<校验和>  idx=<日志index>  term=<世代>  req=<base64 requestId>  type=PUT|MIGRATE  ver=<键版本>  key=<b64>  val=<b64>
```

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
mvn test -Dtest=TopologyChangeTest                 # 分片变更：重叠/空洞/越界拒绝 + 合法再平衡不丢数据
mvn test -Dtest=WriteConsensusTest                 # 多数派、确认不足/副本不可用区分、重复提交幂等
mvn test -Dtest=LogRecoveryTest                    # 截断/缺号/乱序/校验和损坏、停点、追赶
mvn test -Dtest=ConcurrentWriteTest                # 【显式批次】同键并发写入最终版本确定可复现
mvn test -Dtest=SafeRetryAfterFailureTest          # 【新】失败后换新 requestId 安全重提
mvn test -Dtest=OrdinaryPathContentionTest         # 【新】普通接口（无需批次）同键并发决胜
mvn test -Dtest=RestartReshardConsistencyTest      # 【新】重启+再平衡+追赶后最终一致、失败再平衡不毁数据
```

测试位于 `src/test/java/.../tests/`，共 7 类 22 个用例，使用 JUnit 5 临时目录，
不落任何外部状态。**单测日志会打印位点（index/commit）、版本（version）、决胜位
（position）与判定依据**，例如：

```
[s1] 写入提交 requestId=req-2 key=alpha index=2 version=2 acks=2/3（判定依据: 多数派落盘）
[判定依据] 3 次 16-路并发，requestId -> (index,version,position,status) 完全一致: {...}
[位点/版本] index 集合=1..16 无重复/跳号，version 集合=1..16 无重复/跳号/回退
[判定依据] 唯一赢家=req-16（15 个落败者 SUPERSEDED 可区分，不存在'拿成功却读到别人值'）
[s1][r1] 日志损坏，恢复停在 index=4，原因=TRUNCATED_ENTRY ...
[判定依据] 失败 requestId 重提 -> DUPLICATE(首次=REPLICA_UNAVAILABLE) ...，无新日志、不重复应用
```

普通路径决胜窗口可用系统属性在压测/复现时调整（默认探测 2ms、收集 40ms）：

```bash
mvn test -Dtest=OrdinaryPathContentionTest \
  -Dstore.contest.probeNanos=2000000 -Dstore.contest.collectNanos=40000000
```

可通过调整日志级别观察更细的复制/追赶/决胜过程（默认 INFO 已含关键结论）。

### 5.1 场景与结论对照

| 需求场景 | 对应用例 | 期望结论 |
| --- | --- | --- |
| 区间重叠 | `TopologyChangeTest.rejectsOverlappingRangesWithDistinctReason` | 抛 `OVERLAPPING_RANGES` |
| 区间空洞/未覆盖 | `rejectsUncoveredGapWithDistinctReason` | 抛 `UNCOVERED_HASH_GAP` |
| 越界/空区间/重复ID/重复副本 | `rejectsOutOfBoundsAndInvalidBoundaryAndDuplicates` | 各自不同 reason |
| 合法再平衡 | `validReshardMovesCommittedDataWithoutLossOrDuplication` | 值/版本不变、归属更新、重载一致 |
| 多数派可见性 | `WriteConsensusTest.writeIsVisibleOnlyAfterQuorumAck` | 2/3 可见；1/3 不可见且原因可区分 |
| 重复提交 | `duplicateSubmitReturnsFirstOutcomeAndNeverAppliesTwice` | `DUPLICATE`，不重复应用、重启后仍幂等 |
| 失败请求重试（旧结论） | `failedRequestIdIsAlsoDistinctAndNotConfusedWithDuplicateOrCommit` | 首次失败结论保持，新 requestId 才提交 |
| **失败后安全重提** | `SafeRetryAfterFailureTest.failedWriteThenNewRequestIdRetryWinsAndFailedValueNeverResurfaces` | 新 requestId 提交且版本严格更大；失败值重启后不复活；旧 requestId 仍 `DUPLICATE` |
| **确认不足后追赶再重提** | `SafeRetryAfterFailureTest.insufficientAcksFailureThenRetryAfterCatchupAlsoWins` | 追赶只补已提交前缀；重提成功，失败值在所有副本消失 |
| **普通接口并发决胜** | `OrdinaryPathContentionTest.ordinaryPathConcurrentWinnersAreDeterministicAcrossRepeatedRuns` | 3 次 16 路并发 index/version/position/赢家逐次一致；1 COMMITTED + 15 SUPERSEDED |
| **决胜结果扛重启** | `OrdinaryPathContentionTest.finalValueVersionAndShardSurviveRestart` | 重启后最终值/版本/分片不变；落败 requestId 重提仍 `DUPLICATE` |
| **重启+再平衡+追赶最终一致** | `RestartReshardConsistencyTest.winnerVersionAndShardRemainStableAcrossRestartReshardAndCatchup` | 赢家值/版本/归属跨流程一致，失败值不复活，落后副本追赶到赢家 |
| **再平衡中途失败不毁数据** | `RestartReshardConsistencyTest.failedReshardKeepsCommittedDataUntouchedAndCanRetrySuccessfully` | 拓扑保持旧世代、已确认数据原样；恢复后重提再平衡成功 |
| 截断日志 | `LogRecoveryTest.truncatedLastEntryStopsRecoveryAtLastConsistentIndex` | 停点=4，追赶后到 5 |
| 缺号/乱序 | `missingIndexAndOutOfOrderAreDetectedAtCorrectStopPoint` | 分别 `MISSING_INDEX` / `INDEX_OUT_OF_ORDER` |
| 校验和损坏 | `checksumMismatchIsDetectedAndNeverApplied` | `CHECKSUM_MISMATCH`，被篡改条目不应用 |
| 宕机后追赶 | `laggingReplicaCatchesUpAfterDowntime` | 补齐缺失并重新参与多数派 |
| 【显式批次】并发确定性 | `ConcurrentWriteTest` 2 例 | 多次运行 index/version/最终值完全一致 |

---

## 6. 边界与简化说明

这是用于演示一致性结论的本地模拟，有意做了如下简化：

- 单 leader、无真实选举；副本“可用性”通过 `ReplicaStatus`（UP/SLOW/DOWN）注入。
- 复制为同步派发模型，不模拟网络延迟/分区/真实并发日志协商；在线同步利用“同 term
  单 leader 不会在已提交区间分叉”的不变量。
- 每次追加后直接关闭文件以模拟落盘（fsync 语义），追求结论清晰而非吞吐。
- 同键并发决胜提供两条路径：普通 `write` 经 `KeyContestGate` 用“探测+收集”静默窗口
  自动识别同一批并发（无需调用方声明批大小），对真正同时发起的并发确定、可复现，且
  不串行化有明确先后的写；需要数学上与时序彻底无关的全序时，仍可用显式批次接口
  `writeInBatch`（旧用法完全兼容）。窗口默认 2ms/40ms，可用 `-Dstore.contest.*` 调整。
- “失败值绝不复活”通过“提交位点之上的未提交后缀在每次写入/迁移/对齐前物理截断”实现；
  这是单 leader、同 term 不在已提交区间分叉这一不变量下的简化，真实 Raft 类系统还会
  结合 term/Leader 变更与日志一致性检查。
