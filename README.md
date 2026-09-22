# 分片键值存储：本地副本同步与恢复（Sharded Replicated KV Store）

用**本地文件 + 内存对象**模拟一个多分片、多副本键值存储的写入确认、重启重放、
落后副本追赶与分片归属变更。不依赖任何外部服务或真实凭据，仅使用 Spring Boot
工程已有能力与 JDK 标准库。

- 键经 SHA-256 稳定映射到固定整数键空间 `[0, ringSize)`，按键区间分片；
- 每个分片有 N 个副本（默认测试用 3 副本），写入需**多数派**确认才对外可见；
- 每副本每分片一份**仅追加、带 CRC32 校验和、位点严格连续**的本地日志；
- 支持代际化的拓扑重配（分片范围/副本归属动态调整）与确定性的崩溃恢复。

---

## 1. 一致性模型

| 维度 | 规则 |
| --- | --- |
| 可见性 | 一条写入只有在多数派副本落盘、协调者推进 `commitIndex` 后才应用到状态机，`read` 只返回已提交状态。未确认数据对外不可见。 |
| 副本模型 | 类 Raft：固定领导（副本列表首节点）负责分配分片内连续位点并复制；跟随者缺失的前缀由领导顺序补齐。 |
| 并发写决胜 | 同一 key 的最终版本 = **`requestId` 字典序最大的已提交条目**。该规则不依赖线程调度顺序、也不依赖位点先后，因此同组已提交条目在任何时刻、任何副本、任何重启后结论都相同、可复现。 |
| 幂等 | 客户端为每次写生成唯一 `requestId`。同 `requestId` 同内容的重提（含崩溃后续提）复用首次结论，**不重复分配位点、不重复应用、不回退版本**；同 `requestId` 不同内容按 `DUPLICATE_REQUEST_CONFLICT` 拒绝。 |
| 失败原子性 | 确认数不足时写入保持“未确认”：条目可能留在部分副本上，但不可见；用相同 `requestId` 重试即可安全完成（追加本身幂等）。 |
| 分片归属 | 拓扑按代（generation）组织，代际号只增。重配只搬迁**已确认**数据；搬迁后逐条核对 requestId 集合，保证不丢失、不重复、不落错分片。 |

### 多数派规则

```
quorum = floor(N / 2) + 1
```

写入路径（`ShardCoordinator.write`，分片级串行）：

1. 查去重表：已提交的 `requestId` → 直接返回首次结论（`duplicate=true`）；
2. 统计可用副本：`available < quorum` → **拒绝**，`REPLICAS_UNAVAILABLE`（发起前就不够）；
3. 领导日志分配下一位点（若领导已有同 `requestId` 条目则复用，支持崩溃后续提）；
4. 向所有可用副本顺序复制（含缺失前缀补齐）并统计确认；
5. `acks < quorum` → **拒绝**，`INSUFFICIENT_ACKS`（发起时够用、实际确认不够），不推进提交位点；
6. `acks >= quorum` → 推进 `commitIndex`，驱动可用副本把状态机应用到提交位点，登记去重表。

> “副本不可用”与“确认不足”是两类不同结论：前者节点被标记 down、不参与复制；
> 后者节点在线（可用数达标）但落盘确认失败。测试通过故障注入分别覆盖。

### 失败原因码（对外结论可区分）

| reason | 触发场景 |
| --- | --- |
| `INVALID_SHARD_CONFIG` | ringSize 非法、分片 id 缺失/重复、副本集合为空或含重复、区间越界/空区间 |
| `SHARD_RANGE_OVERLAP` | 两个分片键区间重叠 |
| `SHARD_RANGE_HOLE` | 排序后相邻分片之间存在内部空洞 |
| `KEYSPACE_NOT_FULLY_COVERED` | 区间没有恰好覆盖整个 `[0, ringSize)`（头部或尾部未覆盖） |
| `REPLICAS_UNAVAILABLE` | 写入发起前可用副本数低于多数派 |
| `INSUFFICIENT_ACKS` | 可用副本数达标，但实际确认数低于多数派 |
| `DUPLICATE_REQUEST_CONFLICT` | 同一 `requestId` 携带了不同的写内容 |
| `LOG_TRUNCATED` / `LOG_MALFORMED` | 日志行被截断、字段不全或无法解析 |
| `LOG_CHECKSUM_MISMATCH` | 条目 CRC32 校验和不符（位翻转等） |
| `LOG_INDEX_GAP` / `LOG_INDEX_OUT_OF_ORDER` | 位点缺号 / 乱序 |
| `INVALID_GENERATION_ORDER` / `MIGRATION_*` | 代际号未递增，或重配搬迁中发现未覆盖键/落错分片/条数不符 |

---

## 2. 本地存储布局

```
<rootDir>/
├── CURRENT                         # 单行：当前代际号（原子替换）
└── gen-<generation>/
    ├── topology.properties         # 该代分片区间与副本归属
    ├── <shardId>__<replicaId>.log  # 每个副本每个分片一份日志
    └── topology.properties.tmp     # （提交流程中的临时文件）
```

日志行（Tab 分隔，value 使用 Base64，末列为 CRC32）：

```
v1	<index>	<term>	<PUT|DELETE>	<requestId>	<key>	<valueB64|->	<checksum>
```

- `index` 在每个分片内从 1 开始、严格连续；
- term 取写入时的拓扑代际号；
- 所有写入即时 flush 落盘（append 模式），无内存写缓冲。

---

## 3. 恢复与追赶流程

### 3.1 日志打开校验（`FileReplicaLog`）

从第 1 行起逐条解析，维护“期望下一位点”，遇到以下任一情况**立即停止**，
绝不静默跳过，并把文件物理截断到最后一个一致位点：

1. 行字段不全 / 无法解析 → `LOG_TRUNCATED` / `LOG_MALFORMED`；
2. CRC32 不符 → `LOG_CHECKSUM_MISMATCH`；
3. 位点跳号 → `LOG_INDEX_GAP`；
4. 位点倒退 → `LOG_INDEX_OUT_OF_ORDER`。

### 3.2 重启恢复（`Cluster.restart` / `ShardCoordinator.rebuildCommitFromQuorum`）

1. 读 `CURRENT` 指针确定代际，加载拓扑；
2. 打开每个副本分片日志 → 各自校验、停在最后一致位点、截断损坏尾部；
3. 每个分片选出**日志最长的副本作为权威来源**（位点并列取 id 字典序最小，选择确定）；
4. 从位点 1 起找出“存在于多数派副本”的最大连续位点 X，重建 `commitIndex = X`；
   不足多数派的尾部一律截断，不会重放；
5. 落后副本通过 `catchUp` 从权威副本重新复制缺失前缀（先截断、后重拷贝，而非跳过坏数据）；
6. 各副本把状态机重放到 X，重建已提交 requestId 去重表。

因此：**恢复停在最后一个一致位点；多数派丢失时提交位点回退**（见
`LogRecoveryTest.quorumLossRollsBackCommit`：5 条中后 3 条只剩 1 个副本时提交位点回退到 2）。

### 3.3 落后副本追赶（`Replica.catchUp` / `ShardCoordinator.catchUpReplica`）

拉取 `(本地lastIndex, min(leaderCommit, 领导日志末尾)]` 区间条目，顺序、连续、
幂等地追加并重放。重复追赶为空操作；追赶只应用不超过领导提交位点的内容。

### 3.4 分片重配（`Cluster.reconfigure`）

1. 校验新拓扑（重叠/空洞/未覆盖/非法配置分别拒绝）；要求代际号递增；
2. 收集旧拓扑各分片**已提交**条目，按（旧分片起点、旧位点）确定性排序；
3. 用新拓扑重新计算每个 key 的 hash 与归属，分组到新分片；
4. 为新代构建全新日志，位点在新分片内从 1 连续重排，校验和重算；
5. 核对迁移总条数与 requestId 集合与旧代完全一致（零丢失、零重复）；
6. 全部成功后才原子写新拓扑并替换 `CURRENT` 指针与内存协调者。旧代目录保留以便审计。

---

## 4. 本地验证方法

要求 JDK 21+、Maven 3.9+（仓库已带 Maven Wrapper 之外的标准 `pom.xml`）：

```bash
# 编译
mvn compile

# 全量测试（24 个用例）
mvn test

# 只跑某一类场景
mvn test -Dtest=TopologyChangeTest     # 分片变更：重叠/空洞/未覆盖拒绝 + 搬迁不丢不重
mvn test -Dtest=WriteQuorumTest        # 多数派 / 副本不可用 / 确认不足 / 重复提交
mvn test -Dtest=LogRecoveryTest        # 截断 / 校验和 / 缺号 / 多数派丢失回退
mvn test -Dtest=CatchUpTest            # 宕机落后、部分落后、追赶后重启
mvn test -Dtest=ConcurrentWriteTest    # 同键并发决胜可复现、并发重复提交
```

测试使用 JUnit 5 的 `@TempDir` 风格临时目录（`Files.createTempDirectory`），
不产生残留文件。日志中会打印**位点（index / commitIndex / appliedIndex）、
版本（requestId）与判定依据**（多数派确认、重复复用、决胜版本比较、
损坏停止位点、截断与追赶区间等），便于逐步对照结论。

### 测试覆盖矩阵

| 场景 | 用例 | 断言的可区分结论 |
| --- | --- | --- |
| 区间重叠 | `TopologyChangeTest.overlapRejected` | `SHARD_RANGE_OVERLAP` |
| 内部空洞 | `holeRejected` | `SHARD_RANGE_HOLE` |
| 头/尾部未覆盖 | `uncoveredHead/TailRejected` | `KEYSPACE_NOT_FULLY_COVERED` |
| 非法配置 | `emptyReplicasRejected` | `INVALID_SHARD_CONFIG` |
| 4→8 分片重配 + 重启 | `reconfigurePreservesCommittedData` | 条数守恒、值一致、位点连续 |
| 重配到非法拓扑 | `reconfigureToInvalidKeepsOldGeneration` | 拒绝且旧代际/数据不受影响 |
| 多数派可见性 | `WriteQuorumTest.quorumCommitVisible` | commitIndex 推进后才可见 |
| 副本不可用 | `twoDownUnavailable` | `REPLICAS_UNAVAILABLE` |
| 确认不足 + 续提 | `acceptFailuresInsufficientAcks` | `INSUFFICIENT_ACKS`，重试幂等成功 |
| 重复提交 | `duplicateSubmissionReusesDecision` | `duplicate=true`、位点不变 |
| 重复冲突 | `duplicateWithDifferentPayloadRejected` | `DUPLICATE_REQUEST_CONFLICT` |
| 校验和损坏 | `LogRecoveryTest.checksumCorruptionStopsAtConsistentPoint` | 停位点 3，`LOG_CHECKSUM_MISMATCH` |
| 物理截断 | `truncationStopsAtConsistentPoint` | 领导落后，多数派保持提交位点 |
| 缺号 | `indexGapDetected` | `LOG_INDEX_GAP`，停位点 2，尾部先截断再重拷贝 |
| 多数派丢失 | `quorumLossRollsBackCommit` | commitIndex 5→2，版本回退到 rec-2 |
| 宕机/部分落后追赶 | `CatchUpTest.*` | 位点连续补齐、重复追赶为空操作 |
| 同键并发（5 轮 ×12 线程） | `ConcurrentWriteTest.concurrentWinnersDeterministic` | 恒为字典序最大 requestId |
| 并发重复提交（10 线程） | `concurrentDuplicateSubmission` | 仅一次有效应用，commitIndex=1 |

---

## 5. 代码结构

```
model/      KeyRange / ShardSpec / Topology / LogEntry / WriteResult / RecoveryReport
exception/  StoreException 及 6 类带 reason 的业务异常
util/       Hashing(SHA-256→ring) / Checksums(CRC32)
log/        LogFormat(行编解码) / ReplicaLog(接口) / FileReplicaLog(文件实现+损坏校验)
replica/    StateMachine(确定性决胜) / Replica(落盘/应用/追赶)
cluster/    TopologyValidator / TopologyStore / ShardCoordinator(多数派) / Cluster(编排)
recovery/   RecoveryManager(扫描汇总恢复报告)
tests/      5 个测试类 + support/TestKit(临时目录与日志损坏工具)
```
