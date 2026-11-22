# Changelog

格式基于 [Keep a Changelog](https://keepachangelog.com/zh-CN/1.1.0/)，版本遵循[语义化版本](https://semver.org/lang/zh-CN/)。

## [Unreleased]

## [v0.4.0] - 2026-11-22

### 新特性

- **启动后动态注册订阅（[ADR 0015](docs/adr/0015-dynamic-subscription-registration.md)）**：订阅注册不再要求在 `Runner.Start` 之前完成——`SubscribeBroadcast` / `SubscribeClaim` 在 Runner 生命周期内（`Stop` 前）任意时刻可用，运行期新增的订阅注册返回即生效，既有订阅与停机语义不受影响。
  - 认领订阅：注册即参与认领，起点与新增消费者身份一致——从保留窗口内的全部历史（含其他身份已 `done`/`failed` 的事件）开始认领，后续事件照常。
  - 广播订阅：基线在注册调用内锚定（`Driver.HeadPosition`），注册返回即基线确立，只处理注册之后发布的事件；锚定查询失败注册返回错误、订阅不生效，修复后可重试。
  - 新增 `<scope>.subscribe` 指标（`event` / `mode` / `result` 维度，仅计动态注册），动态注册可在指标与日志中直接观测。
  - `Driver` 契约与内置后端零改动：三个后端经契约套件自动执行六个新动态注册用例。
- 新增哨兵错误 `event.ErrRunnerStopped`：`Runner.Stop` 之后注册订阅（Runner 一次性，不支持重启）返回该错误；Start ctx 已取消但未 `Stop` 的窗口同样拒绝，避免注册出永不消费的订阅。

### 破坏性变更与兼容期

- **移除 `event.ErrRunnerStarted`**（v0.x 允许破坏性变更）：动态注册生效后「Start 后再订阅」不再是拒绝路径，该哨兵永不返回；`Stop` 后注册的拒绝由新增的 `event.ErrRunnerStopped` 承接。以 `ErrRunnerStarted` 判别「订阅须在 Start 前」的代码可直接移除该判别分支。

### 文档

- 新增 [ADR 0015](docs/adr/0015-dynamic-subscription-registration.md)：动态订阅注册的语义口径决策记录（两种消费模式的起点语义、错误面、Runner 一次性与已否决备选）。
- usage.md 新增 §3.1 启动后动态注册配方；design.md §5 消费语义、api.md 订阅注册契约与 usage.md 排障口径同步为「订阅生效时刻」表述。

## [v0.3.0] - 2026-11-21

### 新特性

- **合并投递（`WithCoalesceKey`）**：同键短期多次发布只消费最新一次——知识库重建类任务在短窗内多次触发时，不再全量重跑。合并键作用域为 `(Type, key)`、与事件 ID 正交：每次发布原子将同键的待消费前驱置为 superseded 并插入新行，广播与认领两种消费模式均不再返回被取代行（广播水位直接越过，`HeadPosition` / 基线锚点不变）。
  - 取代范围为保守口径：仅无任何认领行的待消费前驱——处理中 / 等退避 / 租约过期待接管 / 终态的前驱一律不动，新事件照常排队；被取代的延迟行永不到期投递，被取代行照常参与保留期清理。
  - 不内建时间窗：去抖经与 `WithDeliverAt` 组合表达（合并键 + `deliver_at = now + 静默期`，每次新发布自带基线）。须每次发布新 ID——稳定 ID 重复发布不触发取代（与 ID 幂等语义一致），配方见 usage.md §4.3。
  - 已知边界：同键并发发布不互斥（短窗可能并存）；混合版本部署下旧二进制照常消费被取代行——合并是优化不是正确性保证，需要「恰好一次」仍靠 handler 幂等。
  - Postgres 后端由版本计划 v4 自动补齐（`event.coalesce_key` / `event.superseded_at` 可空加列与同键查找索引，升级随部署隐式发生）；inmem 同形模拟。契约套件 `eventtest.RunCoalescingSuite` 三后端全过。
- **合并投递可观测**：`<scope>.append.supersede` 计数（`event` 维度）记录每次发布的取代行数；`Inspector.CountStates` 新增 `Superseded` 档，取代事实可直接对账与排障。

### 破坏性变更与兼容期

- **`Driver.Append` 返回类型由 `error` 变更为 `(AppendResult, error)`**（v0.x 允许破坏性变更）：调用方经 `AppendResult.Superseded` 读取本次取代行数（未设合并键与重复发布恒为 0）；库内全部后端与契约套件已同步。`AppendTx` 签名不变（取代随事务提交才生效，不返回计数）。
- **`StateCounts` 新增 `Superseded` 档、Pending 口径收窄**：被合并取代的行从 Pending 移入 Superseded——「待处理」= 该身份无认领行且未被取代；六档互斥且完备，合计仍等于该类型事件总数。
- `postgresschema` 版本计划升级至 v4（`Plan` / `ExpectedObjects` / `SQL` 随之扩展）：存量 schema 由迁移自动补齐，无手工升级步骤；`Create` 需 DDL 权限的口径不变。

### 文档

- 新增 [ADR 0014](docs/adr/0014-coalescing-delivery.md)：合并投递的语义口径决策记录（取代范围、双模式排除、去抖组合、schema v4 边界、观测口径与已否决备选）。
- design.md 增「合并投递」能力行与广播水位边界；api.md 同步 `WithCoalesceKey` / `AppendResult` / 六档 `StateCounts` 签名；usage.md §4.3 增合并投递与去抖配方、§11 增运维注意事项；根与后端 README 能力边界表同步。

## [v0.2.0] - 2026-11-19

### 新特性

- **版本计划与共享迁移执行器（取代基线步骤模型）**：postgresschema 新增版本计划（`Plan` / `Version` / `PlannedStatements`，迁移语句与期望对象 1:1 配对），pgxc / postgrec 共享同一执行器——迁移锁有界等待 → 接管校验（仅缺对象放行，冲突即拒）→ 整版单事务执行，失败整体回滚、重跑收敛。**无台账模型**：版本号只存在于计划中，升级随部署隐式发生（错误摘要 v2 加列与延迟事件 v3 加列均由此自动补齐）；手工 SQL 脚本改由同一计划派生，整段可重放。决策记录见 [ADR 0007](docs/adr/0007-migration-mode-and-protocol.md)（重写）。
- **可观测面（otel 指标 + slog + 类型化 Observer）**：core 新增构造选项 `WithMeterProvider` / `WithScopeName` / `WithObserver`（构造期快照；缺省跟随 otel 全局 Provider，未配置即 noop 静默）。运行面指标：publish / fetch / claim / heartbeat / complete / fail 计数与耗时、`claim.granted`、`lease.abandoned`、`consume.duration` / `handle.duration`、wake `sent` / `dropped`（补齐本地唤醒丢弃不可见）；runner 启动/停止生命周期日志；既有日志属性键对齐 OTel（`error.type` + `error`，消息文本不变）。类型化 Observer 旁路非破坏性演进（`Observer` → `ObserverV4` 嵌入链，回调同步执行、库内 recover）；handler panic 类型化为 `handlerPanicError` 供观测分类，业务行为不变。后端（pgxc / postgrec）仅迁移生命周期埋点：result 七档分类（ok / canceled / lock_timeout / permission_denied / verify_conflict / permanent / transient），成功打 Info `migration applied`。core 首次引入 `go.opentelemetry.io/otel` 依赖（未配置即 noop）。决策记录见 [ADR 0008](docs/adr/0008-observability.md)。
- **延迟事件（deliver\_at 定时投递）**：事件信封新增可选投递时刻字段 `DeliverAt`（`RawEnvelope` / `Envelope`，零值 = 立即可投递）；发布侧经新选项 `WithDeliverAt(t)` 设置，事务性入队复用 `RawEnvelope.DeliverAt`（Publish 与 AppendTx 均可设置）。未到期行不满足 Fetch / Claim 的下推条件（`deliver_at <= now`，判据时钟由 core 下推——`FetchQuery` 新增 `Now` 字段，Claim 沿用 `ClaimQuery.Now`；过滤在单条操作内完成）；过去时刻等价立即，不报错。**保留窗口自可投递时刻起算**（清理判据 `COALESCE(deliver_at, occurred_at)`）：延迟行不因「等待投递」被清理，零值行语义不变、无需数据回填。**广播组合边界（写死）**：延迟行可被内存水位越过而到期后不再广播投递（与 ADR 0012 乱序边界同源），需要定时必达用认领消费。Postgres 后端由版本计划 v3 自动加列（`event.deliver_at timestamptz` 可空，升级随部署隐式发生）；inmem 同形模拟。HeadPosition / position 语义不变。决策记录见 [ADR 0013](docs/adr/0013-delayed-delivery-and-cross-instance-wakeup.md)。
  - `eventtest` 新增 `RunDelayedDeliverySuite` 契约套件（零值/过去时刻立即、到期边界含到时刻、Fetch / Claim 到期前过滤、信封透传、水位边界用例），`RunRetentionSuite` 增保留期组合用例，`RunTransactionalAppendSuite` 增 AppendTx 延迟用例，`RunSuite` 增 Publish 路径用例。
- **跨实例低延迟唤醒（pgxc LISTEN/NOTIFY）**：pgxc 新增 `Wake`（`NewWake` 构造即后台监听、`Bind(*event.LocalWake)` 桥接、`Close()` 释放）：发布侧事件 INSERT 经 CTE 发 `pg_notify`（通道 `go_event_wake_<schema>`，通道与 payload 只带 schema 名、不带事件内容；仅实际插入行时发出，幂等冲突不重发；`pg_notify` 随事务提交可见——AppendTx 的信号在提交时刻发出）；订阅侧专用连接 LISTEN（经连接池 Acquire 持有一条不归还），断线按有界退避重连（100ms 起、封顶 30s）并重新 LISTEN。收到通知后向绑定的 `LocalWake` 注入 `NotifyAll` 信号（core 新增导出桥接方法，向本实例全部已订阅类型发信号）——跨实例信号触发本地已挂起的 Fetch / 唤醒等待立即拉取/认领，读取与认领仍走后端存储；**仅信号、不承载投递保证**（ADR 0003 边界不变），错过的信号（断连窗口、通道满、无订阅）由周期 poll 兜底。延迟行的信号在插入时发出、不判断 deliver\_at（消费侧空拉一趟由轮询兜底）。非法 schema / 通道名超 63 字节 / 连接池为 nil 时监听不启动（信号退化由轮询兜底）。**观测保持最小**：无新指标（效果由既有 fetch / claim 轮次指标承载），连接生命周期走 `WithLogger` 日志。postgrec 不做实现、退化轮询（database/sql 无驱动无关的 LISTEN 接入，ADR 0013 §4）；inmem 无跨实例面（同进程 LocalWake 已覆盖）。决策记录见 [ADR 0013](docs/adr/0013-delayed-delivery-and-cross-instance-wakeup.md)。
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
- **Admin 加固续作（P1.5 收尾）**：pgxc / postgrec 新增公开选项 `WithAdminLockWait(d)`——管理写互斥锁的有界等待上限按实例配置（缺省同迁移锁 `postgresschema.LockWaitTimeout`，原测试钩子 `SetAdminLockWaitForTest` 移除、集成测试改用本选项）；未预热 Admin 实例在迁移持锁期间**首次**调用时，`ensure` 撞上的迁移锁超时与写事务内的锁等待同键同判，统一改判 `ErrAdminLockTimeout`（此前该窗口先收到 `ErrMigrationLockTimeout`，ADR 0010 后果口径收窄）；管理面观测补 `<scope>.admin.replay.duration` 直方图（`result` 维度，操作入口到返回全程耗时——含 ensure、锁等待与事务执行）。
- **跨实例唤醒通道名适配超长 schema（pgxc，ADR 0013 §2 修订）**：前缀 + 全名超 Postgres 标识上限 63 字节的 schema 不再退化轮询——以确定性哈希通道名适配（前缀 + schema 前 32 字节 + 全名 FNV-1a 64 位哈希十六进制，恰 63 字节；LISTEN 与 NOTIFY 两侧经同一函数派生必然一致），任意合法 schema 都有唤醒通道；短 schema 通道名不变，无信号 INSERT 变体移除。

### 破坏性变更与兼容期

- **`Driver.Claim` 返回类型由 `[]Lease` 变更为 `ClaimResult`**（v0.x 允许破坏性变更）：调用方取 `ClaimResult.Leases` 承载原返回形态；全部后端（pgxc / postgrec）、`eventtest`、`eventtest/inmem` 与 `eventtest/simmigrate` 已同步。`Lease` 结构不变、不承载接管标记——接管与终态计数随 `ClaimResult` 按轮汇总（[ADR 0011](docs/adr/0011-claim-result-and-takeover-counting.md)）。
- `postgresschema.Diff` 对缺失基线列的报告类别由 `missing` 改为 `outdated`（迁移不修改既有表，须人工处置）；版本加列缺失仍为 `missing`，可由迁移自动补齐。计划升级至 v2（`event_claim` 加 `last_error` / `failed_at`）。
- `eventtest.RunSuite` 要求 Driver 实现 `eventtest.ClaimStateReader`（套件据它断言错误摘要落库事实）；`eventtest/inmem` 已原生实现。
- **`FetchQuery` 新增 `Now` 字段**（v0.x 允许）：延迟事件到期判据的时钟，由 core 下推；直接构造 `FetchQuery` 的调用方未设置 `Now`（零值）时，非零 `DeliverAt` 的行不可见（判据时钟未推进），零值 `DeliverAt` 行不受影响。`eventtest` 全部套件与三后端已同步。
- `postgresschema` 删除 `Baseline` / `Step` / `MigrationPath`（P1 台账与 fast/step 双路径否决），`FaultPoint` 粒度改为语句边界；迁移模型由版本计划与共享执行器取代（上文首条），v0.x 允许破坏性变更。

### 文档

- usage.md §13.2 补充显式说明：`lease.abandoned`（停机放弃意图）与 `claim.takeover`（接管完成事实）事件源不同，不可相加对账；api.md §2.12 补充提示：`AppendTx` 仅执行单条幂等 INSERT、沿用调用方事务隔离级别（可重复读下 INSERT 语义不变，无需提升隔离级别）。
- 新增 [ADR 0013](docs/adr/0013-delayed-delivery-and-cross-instance-wakeup.md)：延迟事件（信封字段、下推判据、保留期自可投递时刻起算、广播水位边界）与跨实例低延迟唤醒（pgxc LISTEN/NOTIFY 形态、专用连接生命周期、payload 边界、postgrec 退化口径、观测保持最小）的决策记录。
- 新增 [ADR 0009](docs/adr/0009-error-summary-persistence.md)：错误摘要落库口径与 `Fail` 接口扩展的决策记录。
- 新增 [ADR 0010](docs/adr/0010-admin-write-plane-and-replay.md)：管理面写操作的承载形态（独立 `Admin`、不经 `Driver` 类型暴露）与 failed 手工重放语义的决策记录，含加固口径（与迁移的命名空间互斥、操作来源观测、错误分类）。
- 新增 [ADR 0011](docs/adr/0011-claim-result-and-takeover-counting.md)：`Claim` 返回认领轮事实（`ClaimResult`）与接管/终态计数口径的决策记录；[ADR 0008](docs/adr/0008-observability.md) 补充接管指标与 `ObserverV4`（解除「无接管专属指标」的已知口径限制）。
- 新增 [ADR 0012](docs/adr/0012-outbox-transactional-append-and-broadcast-ordering.md)：outbox 事务性入队的承载形态（core 不定义接口、各后端 `TransactionalAppender`）与 ADR 0006 广播位置语义重审定稿（事务内发布下广播投递为尽力而为、认领不受影响；观测不发射、清理不落调用方事务、position 回滚不回收）的决策记录。
- usage.md 新增 §13 可观测（装配示例、指标口径、Observer 旁路、测试断言）与 §10 选项速查行；api.md 新增 §2.9 可观测（选项 / Observer / 指标清单）。
- 新增 LICENSE（MIT），README 增许可证一节并同步状态与路线。

[v0.2.0]: https://github.com/JNinng/go_event/compare/v0.1.1...v0.2.0
[v0.3.0]: https://github.com/JNinng/go_event/compare/v0.2.0...v0.3.0
[v0.4.0]: https://github.com/JNinng/go_event/compare/v0.3.0...v0.4.0
[Unreleased]: https://github.com/JNinng/go_event/compare/v0.4.0...HEAD
