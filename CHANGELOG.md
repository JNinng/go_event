# Changelog

格式基于 [Keep a Changelog](https://keepachangelog.com/zh-CN/1.1.0/)，版本遵循[语义化版本](https://semver.org/lang/zh-CN/)。

## [Unreleased]

### 新特性

- **失败错误摘要落库**：认领消费进入 `failed` 终态时，`event_claim` 行留存错误摘要（`last_error`）与终态时刻（`failed_at`），直接查库即可定位失败原因，无需回源应用日志。
  - Postgres 后端由版本计划 v2 自动加列，升级随部署隐式发生；不变式：`failed_at` 非空 ⟺ 行当前处于 `failed` 终态（接管终态只写 `failed_at`）。
- **`FailRequest` 新增 `Error` 字段**：记录失败时可携带错误文本，core 的 `Runner` 自动下推 handler 错误；文本按不可信输入清洗为摘要（去除 Unicode 控制字符、截断 4 KiB、不含堆栈，口径见 `event.SanitizeErrorSummary`），仅终态写入。
- **`Inspector` 只读查询面（可选接口，按能力探测，不加入 `Driver`）**：三类查询——某（事件 × 消费者身份）的当前状态与重试计数；按消费者身份的状态分档计数（「待处理」= 该身份无认领行，retrying / processing / failed / done 按认领行 `status` 分档，done 为归档档）；failed 事件偏移分页列表（错误摘要与 `failed_at` 随行返回，按 `failed_at` 降序、同刻按事件 ID 升序）。
  - pgxc / postgrec / inmem 均已提供：`NewInspector` 构造（与 `Driver` 共享连接与命名空间选项），或对 `Driver` 做 `event.Inspector` 类型断言探测。
  - 只读边界：不产生簿记、不参与认领/完成路径、不做租约过期推断；结果仅在保留窗口内可见（硬边界，超窗行被清理后自然不可见）。`eventtest` 新增 `RunInspectorSuite` 契约套件（三类查询与 Claim/Fail/Complete 落库事实比对）。
- **`Admin` 管理面写接口（可选接口，不加入 `Driver`、不挂后端 `Driver` 类型）**：`ReplayFailed` 把（事件 × 消费者身份）的 `failed` 终态行重新置为可认领——仅 `failed` 可重放（`done` 不可逆；`active` / `retrying` 拒绝且不改动），单条 CAS 条件更新保证并发重放 / 认领 / Fail 竞态下不改写非 failed 行。重放置 `retrying`、`claim_count=0`（此后 Fail 的重试计数从合法范围重新开始）、清 `lease_token` / `lease_until`、`next_eligible_at=now`（立即可认领）、`failed_at` 清空、`last_error` 保留（ADR 0009 口径）；无效果（行不存在或非 failed）统一返回 `ok=false`；**重放窗口 = 保留窗口**。
  - pgxc / postgrec / inmem 均已提供：`NewAdmin` 构造（与 `Driver` 共享连接与命名空间选项；管理写路径不经 `Driver` 类型暴露，只经本构造获得，鉴权由使用方负责）。
  - **写路径加固**：重放在事务内持与迁移执行共用的命名空间互斥锁（键含 schema，不同命名空间互不阻塞；等待有界，超时返回新哨兵 `event.ErrAdminLockTimeout`——瞬时错误），与迁移执行串行化；行归属校验即 CAS 本身（事件 × 身份主键 + `status='failed'` 条件 + 构造期 schema 命名空间），已写进契约注释与文档；执行故障一律经 `error` 返回、不得折叠为 `ok=false`（错误分类：锁超时 / 迁移面哨兵 / 连接驱动故障）。
  - **新增公开 API**：`postgresschema.AcquireExclusiveLock`（命名空间互斥锁的有界等待实现，迁移执行器与管理面写操作共用）；core Admin 重放结果枚举常量 `AdminReplayOK` / `AdminReplayNoEffect` / `AdminReplayCanceled` / `AdminReplayLockTimeout` / `AdminReplayError`（`AdminReplayEvent.Result` 与指标 result 属性取值）；inmem Admin 构造选项 `AdminOption` / `WithAdminObserver` / `WithAdminLockWait`。
  - **操作来源记录**：每次重放发射 otel 指标 `<scope>.admin.replay`（`result`=ok/no_effect/canceled/lock_timeout/error 与 `schema` 属性）、slog 审计日志（操作、事件 × 身份、结果；不含事件 payload 与 last_error 摘要，失败结果附 error.type / error 分类属性）与类型化 Observer 事件 `AdminReplayEvent`（新增 `ObserverV3` 扩展接口，非破坏性演进；后端选项新增 `WithObserver`）。
  - `eventtest` 新增 `RunAdminReplaySuite` 契约套件（端到端重放、计数归零与循环重放、非 failed 行无效果判别统一、摘要保留事实、并发恰好一次生效、被接管后迟到重放不污染新属主；互斥经 `AdminExclusionLock` 注入断言——锁占用期间锁超时且行不变、释放后重放生效；操作来源经 `AdminObserverSetter` 注入断言——ok/no_effect/lock_timeout 结果与事件 × 身份一致）。

### 破坏性变更与兼容期

- `postgresschema.Diff` 对缺失基线列的报告类别由 `missing` 改为 `outdated`（迁移不修改既有表，须人工处置）；版本加列缺失仍为 `missing`，可由迁移自动补齐。计划升级至 v2（`event_claim` 加 `last_error` / `failed_at`）。
- `eventtest.RunSuite` 要求 Driver 实现 `eventtest.ClaimStateReader`（套件据它断言错误摘要落库事实）；`eventtest/inmem` 已原生实现。

### 文档

- 新增 [ADR 0009](docs/adr/0009-error-summary-persistence.md)：错误摘要落库口径与 `Fail` 接口扩展的决策记录。
- 新增 [ADR 0010](docs/adr/0010-admin-write-plane-and-replay.md)：管理面写操作的承载形态（独立 `Admin`、不经 `Driver` 类型暴露）与 failed 手工重放语义的决策记录，含加固口径（与迁移的命名空间互斥、操作来源观测、错误分类）。

[Unreleased]: https://github.com/JNinng/go_event/compare/v0.1.1...HEAD
