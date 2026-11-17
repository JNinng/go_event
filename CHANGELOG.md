# Changelog

格式基于 [Keep a Changelog](https://keepachangelog.com/zh-CN/1.1.0/)，版本遵循[语义化版本](https://semver.org/lang/zh-CN/)。

## [Unreleased]

### 新特性

- **outbox 事务性入队 `TransactionalAppender`（可选面，不进 `Driver` 契约）**：调用方在自己的业务事务内追加事件，事件 INSERT 与业务写共享同一事务，随事务原子提交/回滚；事务回滚不产生事件行，提交前对外不可见（Fetch / Claim / HeadPosition 只锚已提交行），提交后经轮询/认领可见。**core 不定义接口**——事务载体是后端具体类型（pgxc 为 `pgx.Tx`、postgrec 为 `*sql.Tx`、inmem 为最小缓冲事务模拟），各后端提供 `NewTransactionalAppender` 构造（与 `Driver` 共享连接与命名空间选项）或经组合对象 `Store` 聚合（`Store.TransactionalAppender`），不经运行面类型获得。语义边界（ADR 0012）：幂等与普通 Append 同口径（同 ID 首写为准）；提交与回滚归调用方（库不 begin/commit）；不触发惰性清理（清理 DELETE 不落入调用方事务）；不发射观测（库侧发射点在提交前，与回滚后的落库事实可能矛盾）；position 同事务内按追加序分配、不承诺跨事务连续，回滚不回收（Postgres 序列不回收，缺口不进消费契约）。
  - **广播位置语义重审（ADR 0006 定稿修订，ADR 0012）**：事务内发布不发就近唤醒信号（提交后由轮询兜底）；并发提交的更大 position 会把广播实例的内存水位推过晚提交的行，该行对在途广播实例**可能永久漏投**——「Start 后发布的事件必然可投递」收窄为普通 Publish 路径（单语句独立提交，理论残余窗口可忽略、非绝对保证）；认领消费不受影响（Claim 直接查库）。已否决的修复备选（已广播集合/缺口水位重排、同事务 NOTIFY）与理由见 ADR 0012。
  - `eventtest` 新增 `RunTransactionalAppendSuite` 契约套件（经 `TxSession` 适配注入事务载体，三后端统一跑、无门控）：回滚零事件、中途回滚连带先前 Append 一起丢弃、提交前对外不可见、提交后广播/认领可见、同事务多事件原子可见、事务内与跨事务幂等（首写为准）、回滚 position 缺口不破坏消费、乱序边界（并发业务事务交错提交下广播漏投、认领可见）。
- **后端组合对象 `Store`（便利构造，非能力面）**：pgxc / postgrec / inmem 均提供 `NewStore`，一个对象同时持有 `Driver` / `Inspector` / `Admin` 三面（组合而非合并接口，核心仍保持独立能力，core 不定义 `event.Store`）。选项解析一次、经同一份配置传导三面（观测三面共享同一 scope 与 logger / Observer）；内部只建一个 Driver 实例——Inspector 视图与 Admin 复用同一实例（共享 ensure 状态与失败负缓存），各面行为与独立构造完全等价，`eventtest.RunStoreSuite` 契约钉住三面同源。持有 Admin 是容器聚合而非把管理写路径挂到 Driver 类型上（对 Driver 做 `event.Admin` 类型断言仍不成立，ADR 0010 口径不变）；构造不触发迁移（沿 `NewDriver` 现状，首次操作按迁移模式处理持久结构）。inmem 的 `Store.Admin` 为缺省构造（无 Observer），需要观测注入时可经 `inmem.NewAdmin(st.Driver, ...)` 构造等价 Admin 替换。
- **认领轮事实汇总（接管与终态计数）**：`Driver.Claim` 返回 `ClaimResult{Leases, Takeovers, TerminalFailed}`——本轮发放的租约、接管过期租约而发放的数量（含接管后直接置 failed 的终态接管）、认领动作直接置 failed 的数量。计数取自后端落库事实（SQL 后端为终态 UPDATE 影响行数与认领接管标记，inmem 为内存计数），与 `Inspector.ClaimState` / `claim_count` 可对账；接管计数覆盖发放与终态两条路径。当前认领语义下置终态的下推条件要求租约已过期，认领侧终态必经接管路径（恒有 `TerminalFailed ≤ Takeovers`），一次终态接管同时计入两者；无主首认与 Fail 退避期满的重认领不计接管。决策记录见 [ADR 0011](docs/adr/0011-claim-result-and-takeover-counting.md)。
  - **观测面**：新增指标 `<scope>.claim.takeover`（接管发放数）与 `<scope>.claim.terminal_failed`（认领侧直接置 failed 数，属性 `event` + `identity`，与 `<scope>.claim.granted` 同风格，仅计数非零时打点）；`Observer` 非破坏性演进至 `ObserverV4`（嵌入 V3），新增 `OnClaim(ClaimEvent)` 认领轮计数事件（仅成功轮次分发）。`Attempts > 1` 混合 Fail 重试与过期接管，不能代替接管计数（usage.md §6.7/§6.8）。
- **失败错误摘要落库**：认领消费进入 `failed` 终态时，`event_claim` 行留存错误摘要（`last_error`）与终态时刻（`failed_at`），直接查库即可定位失败原因，无需回源应用日志。
  - Postgres 后端由版本计划 v2 自动加列，升级随部署隐式发生；不变式：`failed_at` 非空 ⟺ 行当前处于 `failed` 终态（接管终态只写 `failed_at`）。
- **`FailRequest` 新增 `Error` 字段**：记录失败时可携带错误文本，core 的 `Runner` 自动下推 handler 错误；文本按不可信输入清洗为摘要（去除 Unicode 控制字符、截断 4 KiB、不含堆栈，口径见 `event.SanitizeErrorSummary`），仅终态写入。
- **`Inspector` 只读查询面（可选接口，按能力探测，不加入 `Driver`）**：三类查询——某（事件 × 消费者身份）的当前状态与重试计数；按消费者身份的状态分档计数（「待处理」= 该身份无认领行，retrying / processing / failed / done 按认领行 `status` 分档，done 为归档档）；failed 事件偏移分页列表（错误摘要与 `failed_at` 随行返回，按 `failed_at` 降序、同刻按事件 ID 升序）。
  - pgxc / postgrec / inmem 均已提供：`NewInspector` 构造（与 `Driver` 共享连接与命名空间选项），或对 `Driver` 做 `event.Inspector` 类型断言探测。
  - 只读边界：不产生簿记、不参与认领/完成路径、不做租约过期推断；结果仅在保留窗口内可见（硬边界，超窗行被清理后自然不可见）。`eventtest` 新增 `RunInspectorSuite` 契约套件（三类查询与 Claim/Fail/Complete 落库事实比对）。
- **`Admin` 管理面写接口（可选接口，不加入 `Driver`、不挂后端 `Driver` 类型）**：`ReplayFailed` 把（事件 × 消费者身份）的 `failed` 终态行重新置为可认领——仅 `failed` 可重放（`done` 不可逆；`active` / `retrying` 拒绝且不改动），单条 CAS 条件更新保证并发重放 / 认领 / Fail 竞态下不改写非 failed 行。重放置 `retrying`、`claim_count=0`（此后 Fail 的重试计数从合法范围重新开始）、清 `lease_token` / `lease_until`、`next_eligible_at=now`（立即可认领）、`failed_at` 清空、`last_error` 保留（ADR 0009 口径）；无效果（行不存在或非 failed）统一返回 `ok=false`；**重放窗口 = 保留窗口**。
  - pgxc / postgrec / inmem 均已提供：`NewAdmin` 构造（与 `Driver` 共享连接与命名空间选项；管理写路径不经 `Driver` 类型暴露、不经运行面类型获得——经 `NewAdmin` 构造或经组合对象 `Store` 聚合（同一内部构造路径），鉴权由使用方负责）。
  - **写路径加固**：重放在事务内持与迁移执行共用的命名空间互斥锁（键含 schema，不同命名空间互不阻塞；等待有界，超时返回新哨兵 `event.ErrAdminLockTimeout`——瞬时错误），与迁移执行串行化；行归属校验即 CAS 本身（事件 × 身份主键 + `status='failed'` 条件 + 构造期 schema 命名空间），已写进契约注释与文档；执行故障一律经 `error` 返回、不得折叠为 `ok=false`（错误分类：锁超时 / 迁移面哨兵 / 连接驱动故障）。
  - **新增公开 API**：`postgresschema.AcquireExclusiveLock`（命名空间互斥锁的有界等待实现，迁移执行器与管理面写操作共用）；core Admin 重放结果枚举常量 `AdminReplayOK` / `AdminReplayNoEffect` / `AdminReplayCanceled` / `AdminReplayLockTimeout` / `AdminReplayError`（`AdminReplayEvent.Result` 与指标 result 属性取值）；inmem Admin 构造选项 `AdminOption` / `WithAdminObserver` / `WithAdminLockWait`。
  - **操作来源记录**：每次重放发射 otel 指标 `<scope>.admin.replay`（`result`=ok/no_effect/canceled/lock_timeout/error 与 `schema` 属性）、slog 审计日志（操作、事件 × 身份、结果；不含事件 payload 与 last_error 摘要，失败结果附 error.type / error 分类属性）与类型化 Observer 事件 `AdminReplayEvent`（新增 `ObserverV3` 扩展接口，非破坏性演进；后端选项新增 `WithObserver`）。
  - `eventtest` 新增 `RunAdminReplaySuite` 契约套件（端到端重放、计数归零与循环重放、非 failed 行无效果判别统一、摘要保留事实、并发恰好一次生效、被接管后迟到重放不污染新属主；互斥经 `AdminExclusionLock` 注入断言——锁占用期间锁超时且行不变、释放后重放生效；操作来源经 `AdminObserverSetter` 注入断言——ok/no_effect/lock_timeout 结果与事件 × 身份一致）。

### 破坏性变更与兼容期

- **`Driver.Claim` 返回类型由 `[]Lease` 变更为 `ClaimResult`**（v0.x 允许破坏性变更）：调用方取 `ClaimResult.Leases` 承载原返回形态；全部后端（pgxc / postgrec）、`eventtest`、`eventtest/inmem` 与 `eventtest/simmigrate` 已同步。`Lease` 结构不变、不承载接管标记——接管与终态计数随 `ClaimResult` 按轮汇总（[ADR 0011](docs/adr/0011-claim-result-and-takeover-counting.md)）。
- `postgresschema.Diff` 对缺失基线列的报告类别由 `missing` 改为 `outdated`（迁移不修改既有表，须人工处置）；版本加列缺失仍为 `missing`，可由迁移自动补齐。计划升级至 v2（`event_claim` 加 `last_error` / `failed_at`）。
- `eventtest.RunSuite` 要求 Driver 实现 `eventtest.ClaimStateReader`（套件据它断言错误摘要落库事实）；`eventtest/inmem` 已原生实现。

### 文档

- 新增 [ADR 0009](docs/adr/0009-error-summary-persistence.md)：错误摘要落库口径与 `Fail` 接口扩展的决策记录。
- 新增 [ADR 0010](docs/adr/0010-admin-write-plane-and-replay.md)：管理面写操作的承载形态（独立 `Admin`、不经 `Driver` 类型暴露）与 failed 手工重放语义的决策记录，含加固口径（与迁移的命名空间互斥、操作来源观测、错误分类）。
- 新增 [ADR 0011](docs/adr/0011-claim-result-and-takeover-counting.md)：`Claim` 返回认领轮事实（`ClaimResult`）与接管/终态计数口径的决策记录；[ADR 0008](docs/adr/0008-observability.md) 补充接管指标与 `ObserverV4`（解除「无接管专属指标」的已知口径限制）。
- 新增 [ADR 0012](docs/adr/0012-outbox-transactional-append-and-broadcast-ordering.md)：outbox 事务性入队的承载形态（core 不定义接口、各后端 `TransactionalAppender`）与 ADR 0006 广播位置语义重审定稿（事务内发布下广播投递为尽力而为、认领不受影响；观测不发射、清理不落调用方事务、position 回滚不回收）的决策记录。

[Unreleased]: https://github.com/JNinng/go_event/compare/v0.1.1...HEAD
