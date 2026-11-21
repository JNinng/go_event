# ADR 0014: 合并投递——同键最新一次消费

- 状态：已接受
- 日期：2026-11-21

## 背景

合并投递：同键短期事件取最新一次消费——知识库重建类任务在短窗内多次
触发，当前所有事件共用一条按 `position` FIFO 的队列，每次触发都会完整
消费一趟，仅消费最新一次即可省去冗余处理。

前序相关口径：需求条件下推（ADR 0004——过滤必须发生在读取/认领时）、
能力分级框架（ADR 0005——核心语义须原生或模拟，全后端同形实现）、延迟
事件先例（ADR 0013——信封可选字段 + 全后端同形 + 下推过滤 + 延迟行照常
占 position 的水位边界）、事务性入队（ADR 0012——AppendTx 不返回落库事
实、不发射观测）、认领轮计数（ADR 0011——计数与落库事实一致，供观测面
计量）。

## 决策

### 1. 信封可选字段 `CoalesceKey`：作用域 (type, key)，与事件 ID 正交

- **承载形态**：core 不新增接口面——合并投递是既有 Append / Fetch /
  Claim 语义上的一个可选字段（与延迟事件同形：无独立能力面、全后端同形
  实现，ADR 0013 §1 先例）。`RawEnvelope` / `Envelope` 增加可选字段
  `CoalesceKey`；发布侧经 `WithCoalesceKey(key)` 设置，事务性入队复用
  `RawEnvelope.CoalesceKey`——Publish 与 AppendTx 均可设置。空串 =
  未设置（后端落库 NULL），不参与合并。
- **作用域 = (Type, CoalesceKey)**：取代只在同一事件类型内生效，跨类型
  同键互不影响；键是业务键（如 `kb_rebuild:<docID>`），不是事件主键。
- **与事件 ID 正交（写死）**：同 ID 幂等 Append 语义不变——重复 Append
  同 ID 视为成功且**不触发取代**（取代谓词含 `NOT EXISTS(dup.id)` 守卫）。
  理由：若重复 Append 可以触发取代，重发一个已被取代的旧事件会把当前
  赢家标死且自身又插入失败，键从此无人消费——幂等语义反转成正确性缺陷。
  守卫的推论：去抖配方必须**每次发布新 ID**（见 usage.md §4.3），稳定 ID
  重发只会是纯 no-op。

### 2. Append 单条 CTE：原子「置同键待消费前驱 superseded + 插入新行」

- **写路径**：Append 由单条 INSERT 改为单条多 CTE 语句（pgxc / postgrec
  同构，普通 Append 与事务性 AppendTx 仍共用同一份语句定义，ADR 0012
  先例）——`sup`（UPDATE：同 `(type, coalesce_key)`、未被取代、且无任何
  认领行的事件行置 `superseded_at`）→ `ins`（INSERT … ON CONFLICT (id)
  DO NOTHING）。数据修改 CTE 按文档保证恰好执行一次，取代与插入同语句
  原子；同键并发发布不互斥（各自语句快照下可能并存）——合并是优化不是
  正确性保证，多跑一趟对幂等任务无害。
- **取代范围（v1 保守口径，写死）**：仅**无任何认领行**（任意消费者身份）
  的待消费前驱——处理中 / 等退避 / 租约过期待接管 / done / failed 的行
  一律不取代，新事件照常排队。理由：引入 superseded 认领行终态会破坏
  既有四态状态机与接管语义（ADR 0006）；旧事件多跑一趟对幂等任务无害；
  活跃租约保护在途处理。被取代的延迟行照常被取代（待消费即取代），且
  **永不到期投递**——下推排除先于 deliver_at 判定。
- **inmem 同形**：锁内原子取代（同键、未取代、无认领行），Fetch / Claim
  候选排除被取代行；事务性入队以「调用时解析候选、提交时原子应用、回滚
  丢弃」模拟 SQL 语句执行时刻语义（模拟档边界，注释写明）。

### 3. Fetch / Claim 下推排除：一种机制服务两种消费模式

- 被取代行不满足下推条件（`superseded_at IS NULL`），Fetch 与 Claim 拾取
  窗口同一下推——广播消费水位直接越过被取代行，认领消费不发放被取代行；
  superseded 是**事件行属性**，对新消费者身份（无任何认领行历史）同样
  生效——「新身份重放」不会捞回被取代行。
- **HeadPosition / position 不变**（ADR 0013 先例）：superseded 行照常
  占 position、可进基线锚点；被取代行仅在下推处被跳过，水位一次性越过，
  无新边界。
- **窗口语义不加糖**：纯「待消费即取代」，不内建时间窗参数。去抖经与
  既有 `WithDeliverAt` 组合表达：合并键 + `deliver_at = now + 静默期`，
  每次新发布自带基线（前驱被取代、新行在静默期后到期）——经典去抖，零
  新增时间机制。已否决的备选：内建静默窗参数——与 WithDeliverAt 完全
  重叠，徒增一套时间机制与口径。

### 4. schema v4：expand-only 加列 + 同键查找索引

- 版本计划追加 v4（沿 v2/v3 先例，一条语句恰好构造一个期望对象）：
  `event` 表加 `coalesce_key text` 与 `superseded_at timestamptz`，均可
  空无默认（NULL = 未设置 / 未取代）——旧二进制不写该列即 NULL，行为
  不变，无需数据回填；另建 `(type, coalesce_key)` **部分索引**（`WHERE
  coalesce_key IS NOT NULL`，仅覆盖参与合并的行）。快照比对只看名称/
  表/键列，部分谓词不参与比对口径。
- **混合版本边界（写进文档）**：混合版本部署下旧二进制照常消费被取代
  行（不识别该列）——**合并是优化不是正确性保证**，多实例应在同版本
  部署。

### 5. 观测与契约：AppendResult + supersede 计数 + Inspector 分档

- **`Driver.Append` 签名变更（破坏性，v0.x 允许）**：返回
  `(AppendResult, error)`，`AppendResult.Superseded` 为本次取代的待消费
  前驱行数——沿 ADR 0011 ClaimResult 先例（计数与落库事实一致，供观测
  面计量），是 core 侧计数的唯一事实来源。错误时返回零值。
- **指标**：core 新增 `<scope>.append.supersede` 计数器（event 维度，
  非零才打点，与 claim.granted 口径一致）；Publish 在 Append 成功后按
  `AppendResult` 发射。**AppendTx 不发射观测、不返回计数**（ADR 0012
  §3 边界不变：取代随事务提交才生效，提交前发射与回滚后的事实可能矛盾）。
  **Observer 不扩展**（未要求；PublishEvent 保持既有字段）。
- **Inspector**：`StateCounts` 增 `Superseded` 档——被取代行从 Pending
  移入该档（superseded 行无认领行，原口径计入 Pending），Pending 收窄为
  「无认领行且未取代」；六档互斥且完备、合计等于事件总数，口径经
  `AppendResult.Superseded` 可对账。已否决的备选：`superseded boolean`
  列——无取代时刻事实，与 `failed_at` 口径不一致（ADR 0009）。

### 6. 契约钉死

- eventtest 新增 `RunCoalescingSuite`（latest-wins 计数与双模式排除、
  水位越过与 HeadPosition 锚定、新身份跳过、活跃租约 / 退避 / 过期租约 /
  终态前驱不取代、作用域 (type, key)、空键不取代、ID 正交（重发旧 ID
  纯 no-op 不杀新赢家）、同 ID 幂等、被取代延迟行永不到期投递、信封
  往返、AppendTx 提交前不可见 / 提交后取代 / 回滚零事实、Inspector
  分档、保留期照常清理）。
- 迁移契约：后端集成增 v3 基线升级用例（Migrate 补齐 v4 对象、幂等、
  Verify 报告缺失、首次 DML 收敛）。

## 后果

- core 变更：`RawEnvelope` / `Envelope` 增 `CoalesceKey`、
  `WithCoalesceKey` 选项、`Driver.Append` 返回 `AppendResult`（破坏性）、
  `StateCounts.Superseded`（Pending 口径收窄）、
  `publisherMetrics.append.supersede`；Runner 无变更（水位天然越过）。
- pgxc / postgrec 同步 v4 计划与单 CTE 写路径 + 下推排除；inmem 同形
  模拟；pgxc 唤醒 NOTIFY 保持「仅实际插入行时发出」语义不变。
- eventtest 套件扩展见 §6；`RunInspectorSuite` 既有分档用例随六档口径
  回归。
- 文档同步：design.md §5.2/§6/§9.2、api.md §2.2/§2.3/§2.4/§2.10/§2.12/
  §4、usage.md §4.3/§10/§11、根与后端 README 能力边界表。
- 已知限制（写进 usage）：同键并发发布不互斥（短窗可能并存，各自语句
  快照下两行均待消费）；混合版本旧二进制照常消费被取代行；稳定 ID 重发
  不触发取代（去抖配方须每次新 ID）；superseded 行照常占 position（Head
  锚点、水位与基线口径不变）。
