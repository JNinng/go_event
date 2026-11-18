# ADR 0010: 管理面写操作的承载形态与 failed 手工重放

- 状态：已接受
- 日期：2026-11-18

## 背景

认领消费的 `failed` 终态此前只能等保留窗口清理：没有把失败事件重新置为
可认领的入口。TODO 把「failed 事件手工重放」定为 P1 项，写操作由管理面
`Admin` 承载；[ADR 0009](0009-error-summary-persistence.md) 已定下重放与
错误摘要的交集口径（清 `failed_at`、保留 `last_error`）。

设计口径（design.md §6）是三面分离：运行面 `Driver` 已定型，查询面
`Inspector`（可选接口，类型断言探测）已落地，管理面写操作的承载形态与
重放语义需要定稿。

## 决策

### 1. 写操作由独立 `Admin` 可选接口承载，不经 Driver 类型暴露

- core 定义 `event.Admin` 可选接口（当前仅 `ReplayFailed` 一个操作），各
  后端提供 `NewAdmin(...)` 构造，与 `Driver` 共享连接与命名空间选项。
- **Admin 不进入 Driver 契约，也不挂在后端 Driver 类型上**：与 Inspector
  不同（后端 Driver 同时实现 Inspector、可类型断言探测），管理写路径只在
  `NewAdmin` 构造的对象上——「非管理面路径无法调用管理写操作」由类型形态
  保证，而非约定。鉴权由使用方负责（管理面对象只授予运维角色）。
- 构造复用 Driver 的迁移模式语义（首次操作按模式处理持久结构），与
  Inspector 同构。

### 2. 重放语义：仅 failed、单条 CAS、计数归零

- **仅 `status='failed'` 的认领行可重放**，粒度为事件 × 消费者身份（与
  认领键一致，不做全身份重放）。`done` 幂等不可逆、不重放；`active` /
  `retrying` 行不重放（自动重试路径仍在正常运转，重放只会干扰退避节奏）。
- 重放由**单条条件更新（CAS，`WHERE status='failed'`）**完成，全部字段
  一次置入：
  - `status='retrying'`、`claim_count=0`——重试计数归零，此后 Fail 的
    计数从合法范围重新开始（若保留计数，重放行已达上限、第一次 Fail 即
    重新进入终态，重放无意义）；
  - 清 `lease_token`（置空串）、`lease_until` 置为重放时刻——原租约凭证
    失效，行不再被任何属主持有；
  - `next_eligible_at` 置为重放时刻——立即可认领，不做退避（重放是人工
    决策，不继承机器退避）；
  - `failed_at=NULL`、`last_error` 保留（ADR 0009：重放行与失败行经
    `failed_at` 区分，摘要留作最近一次失败的排障历史）。
- 并发防护即 CAS 本身：并发重放 / 认领 / Fail 竞态下不改写非 failed 行
  ——并发重放恰好一次生效；被重新认领（接管）或已完成的行不受迟到的
  重放请求影响。

### 3. 无效果统一 `ok=false`，与 Inspector 判别风格一致

- `ReplayFailed` 返回 `(ok, error)`：`ok=true` 表示行原为 failed 且已
  重放；**行不存在（事件未知、超保留窗口被清理、该身份尚无认领行）与
  非 failed 行（done / active / retrying）一律 `ok=false` 且无错误**，
  判别方式统一。需要区分原因时先经 `Inspector.ClaimState` 查询。
- 执行故障（连接、迁移）经 error 返回，不得折叠为 `ok=false`。

### 4. 重放窗口 = 保留窗口

- 超窗且无活跃租约的行已被惰性清理删除（design.md §5.2），重放该键与
  「行不存在」同判别——保留期是重放语义的硬边界，不另设重放台账或延长
  窗口。

### 5. 加固：与迁移执行共用命名空间互斥锁

- TODO 契约要求「所有写操作必须持相应锁、并发调用与迁移互斥」。重放在
  事务内先取得**与迁移执行同一把** advisory 锁
  （`postgresschema.LockKey(schema)`，键含 schema——不同命名空间互不
  阻塞），再执行 CAS 更新：管理写与迁移串行化（迁移的 `ADD COLUMN` 与
  行更新不再竞争表锁），双方等待均有界（try-lock + 退避，与迁移执行器
  共用 `postgresschema.AcquireExclusiveLock` 同一实现），管理侧超时映射
  为新哨兵 `event.ErrAdminLockTimeout`（瞬时错误）。锁随重放事务提交/
  回滚释放。
- inmem 无迁移执行，以进程内容量 1 信号量为契约等价形态（对齐
  simmigrate 执行器与 advisory 键的进程内形态），供契约套件注入验证。
- 互斥是串行化而非拒绝：锁释放后重放照常生效。

### 6. 加固：操作来源记录（观测旁路）

- TODO 契约要求「记录操作来源」。沿 [ADR 0008](0008-observability.md)
  口径补管理面观测：otel 指标 `<scope>.admin.replay`（Counter，`result`=
  ok/no_effect/canceled/lock_timeout/error 与 `schema` 属性，发射点单一
  在 Admin 写方法）、slog 审计日志（操作、事件 × 身份、结果；不含事件
  payload 与 last_error 摘要——标识符 `event_id` / `consumer_type` 允许
  入日志，失败结果附 `error.type` / `error` 分类属性）与类型化 Observer
  事件 `AdminReplayEvent`（新增 `ObserverV3` 扩展接口——嵌入
  `ObserverV2` 非破坏性演进，分发点类型断言；后端选项新增 `WithObserver`）。
- 结果枚举 `no_effect` 单列：无效果与生效对运维是不同的处置信号（无效果
  通常意味着该键已不是 failed，需先经 Inspector 查询）；观测是纯旁路，
  失败不影响业务结果。
- 指标属性不含事件 ID（业务标识禁入属性值），日志可含（排查需要）。

### 7. 加固：错误可判别

- 执行故障一律经 error 返回，不得折叠为 `ok=false`（既有决策 §3 的边界
  强化）。错误分类与排查入口：
  - `event.ErrAdminLockTimeout`：命名空间互斥锁等待超时，瞬时（退避后
    重试）；检查是否有迁移执行或其他管理写操作持锁。
  - 迁移面哨兵（`ErrInvalidSchema` / `ErrMigrationPermissionDenied` /
    `*event.VerifyError` 等）：经首次操作的迁移模式处理产生，原样上抛，
    处置口径与迁移一致（ADR 0007）。
  - 连接 / 驱动故障：原样包装（`error.type` / SQLSTATE 判别），不吞掉。
- 无效果（`ok=false` 且无错误）保持判别统一，不扩错误面（§3 决策不变）。

## 理由

- **写面独立于 Driver 类型**：运行面是高频自动路径，管理面是低频人工
  路径；把写操作挂上 Driver 会让一切持有 Driver 的代码（Runner、使用方
  业务代码）都触达管理写路径。三面分离在类型层面固化（Inspector 之所以
  可挂 Driver，是它只读、不改变任何状态）。
- **`ok=false` 而非类型化错误**：done / active / retrying / 不存在对
  重放而言是同一个事实——「该键当前不可重放」；Inspector 已能查询原因，
  错误面扩到四类哨兵只会让调用方被迫穷举。返回风格与 Inspector 的
  `(值, ok, error)` 判别一致。
- **CAS 单语句而非锁表 / 事务编排**：重放是单行单写操作，`WHERE
  status='failed'` 的条件更新天然原子；任何「读-判-写」多步形态都会在
  与认领 / Fail 的并发下产生竞态窗口。
- **`claim_count` 归零而非保留**：重放的前提是人工确认要再给完整重试
  预算；保留计数会让重放行一次 Fail 就回到终态，语义自相矛盾。

## 被否决的备选

- **ReplayFailed 挂在 Driver 接口或后端 Driver 类型上**：运行面暴露管理
  写路径，违背三面分离（design.md §6），否决。
- **区分 done / active / 不存在的类型化错误**：判别价值可由 Inspector
  承担，四类哨兵错误徒增调用方负担，否决。
- **重放保留 `claim_count` 或不清租约凭证**：前者使重放行一次 Fail 即回
  终态；后者让失效凭证残留（旧属主的迟到 Complete/Fail 语义变得含糊），
  均否决。
- **批量重放 / 全身份重放**：当前无场景；粒度与认领键一致是最小面，未来
  需要时以新操作扩展 Admin，否决一次性做宽。
- **重放前强校验事件存在**：「事件不存在但认领行存在」不是合法状态
  （认领行经 FK 随事件清理）；多一次查询不增加防护，仅增加竞态窗口，
  否决。
- **依赖 DDL 表锁天然串行化、不显式取锁**：`ALTER TABLE` 的
  `ACCESS EXCLUSIVE` 与行更新在存储层确实互斥，但等待无界（迁移侧
  `lock_timeout` 会先超时回滚）、管理侧也无瞬时错误口径；显式同键
  advisory 锁让双方等待有界、错误可判别（§5），否决。

## 后果

- core 新增 `event.Admin` 可选接口；pgxc / postgrec 提供 `NewAdmin`，
  inmem 提供 `NewAdmin`（与 Driver 共享内存状态）。后端 Driver 类型上
  不存在管理写路径。
- eventtest 新增 `RunAdminReplaySuite`：端到端重放（重放 → 认领 →
  完成）、计数归零与循环重放、done / active / retrying / 未知键的无效果
  与判别统一、`last_error` 保留与 `failed_at` 清空、并发重放恰好一次
  生效、被接管后迟到重放不污染新属主；加固后追加互斥用例（锁占用期间
  锁超时且行不变、释放后重放生效，经 `AdminExclusionLock` 注入）与观测
  用例（ok/no_effect/lock_timeout 结果与事件 × 身份一致，经
  `AdminObserverSetter` 注入）——互斥与操作来源记录是契约约束，注入
  接口缺失即 Fatal。
- Postgres 后端重放为「事务内取锁 → CAS 更新 → 提交」三步，同一事务
  保证锁与写原子；管理写锁等待上限独立于迁移锁等待上限
  （`SetAdminLockWaitForTest` 供测试缩短）。管理写路径的 ensure 先于取锁
  ——未预热实例在迁移持锁期间的首次调用可能先收到迁移面锁超时
  （`ErrMigrationLockTimeout`），预热后落在管理面
  `ErrAdminLockTimeout`；两者均为同键瞬时错误。
- 加固（§5–§7）随本决策一并落地：互斥经 `postgresschema.
  AcquireExclusiveLock` 与迁移执行器共用实现（单一锁键来源）；观测沿
  ADR 0008 口径扩展（`ObserverV3`、`<scope>.admin.replay`）；哨兵
  `ErrAdminLockTimeout` 进 core errors.go。鉴权仍由使用方负责；无自动
  down；迁移侧无台账、无需 reset/stamp 入口（口径不变）。
