# go\_event 设计文档

后端无关的事件生产/消费库：提供**广播消费**（发完即忘，上游无感）与**认领
消费**（key + 有效期 + 心跳 + 完成标记）两种消费方式，与有参（强类型）/
无参事件正交；支持分布式多实例与进程内就近消费。

本文档是项目的单一设计事实源；关键决策的背景、备选与后果见
[docs/adr/](adr/)。具体签名（结构体字段、方法、选项）不在本文档定稿，
随实施规划确定——本文只约定能力面与语义。

## 1. 定位

**非高并发、非实时**。面向低频事件的任务协作场景（长尾副作用异步化、完成
状态追踪），不追求高吞吐流水线与毫秒级投递——DB 轮询兜底、租约表簿记等选型
以此为前提；高吞吐流式场景不是目标（Kafka 类后端被排除的部分原因，见
[ADR 0005](adr/0005-backend-capability-boundary.md)）。

工程形态：

- **多 Go module 布局**：core 与各后端独立 go.mod，使用方按需引入后端依赖
  （见 [ADR 0001](adr/0001-multi-module-layout.md)）。
- **嵌套 tag 独立发版**：如 `postgrec/v0.1.0`。
- **契约测试套件约束语义**：接口的语义就是这套测试，新后端必须全过。
- **ADR 记录关键决策**。

## 2. 背景与动机

业务中的长尾副作用（如文档摄取向量化、通知派发）需要以事件形式异步化：主流程
落库后立即返回，副作用失败可观测、可自动重试，而不是把失败抛回调用方或阻塞
主流程。

自建而非引入现成库的理由：

- **River**（Postgres 作业队列）是"一条作业一次投递"的模型，缺少"一个事件按
  消费者类型扇出、每类型恰好一次"的订阅语义，也没有消费租约/心跳（挂死而未
  崩溃的消费者无法被接管）；schema 由自身迁移管理，不便纳入使用方的存储治理。
- **Watermill** 提供事件总线抽象，但没有租约认领层，引入后仍需自建租约语义，
  抽象收益有限。
- **asynq** 等 Redis 系方案要求额外基础设施；核心目标环境是仅 PostgreSQL 可用。

## 3. 设计目标

- 两种消费方式（广播/认领）× 两种载荷形态（有参/无参）**正交组合**，2×2 均
  合法。
- 广播方式发完即忘（见 §5.2）；认领方式内置失败自动重试且**有上限**。
- **后端可插拔**，Postgres 后端先行；新后端只需实现 Driver 并通过 eventtest
  全套契约测试。
- 认领消费底线为 at-least-once 投递，handler 幂等是消费方责任（写进文档与
  契约测试）；广播消费不承诺投递，失败即跳过（见 §5.2）。

## 4. 架构总览

分层与缝合点：

```
使用方代码
   │  Declare[T] / Publisher / Runner（core 提供的契约与编排）
   ▼
core：编排层（poll 循环、心跳、重试上限、就近唤醒、并发度控制、优雅停机）
   │  Driver 接口（单次原子操作）
   ▼
后端 Driver（postgrec / pgxc）：存储与投递实现，共享 postgresschema 方言 DDL
```

- **Driver 是最小缝合点**：只做单次原子操作
  （Append / Fetch / HeadPosition / Claim / Heartbeat / Complete / Fail），全部
  编排逻辑在 core 的 Runner 统一完成。需求条件（订阅类型集 + 租约过期判据 +
  可认领/水位条件）在单条 Fetch/Claim 操作内下推，保证原子性
  （见 [ADR 0004](adr/0004-demand-driven-pull.md)）；广播消费的已读水位仅存
  于实例内存，由 core 在 Fetch 时作为条件传入，不在存储侧簿记。
- **依赖注入友好**：注入面只有三个（Publisher / Runner / Driver），必选
  Go 接口仅 Driver 一个（查询面 Inspector 与管理面 Admin 为可选接口，按
  能力探测接入——Admin 不经运行面类型获得（经后端 NewAdmin 构造或经组合
  对象 Store 聚合，同一内部构造路径）、不挂 Driver 类型；
  Publisher 与 Runner 为具体类型，见 docs/api.md §1）；全部
  构造器 `NewX(依赖, opts...)` 显式注入，无包级可变状态、无 init 魔法；
  接口定义遵循 Go 惯例放在消费侧。core 不引 DI 框架依赖（wire 用普通构造
  函数即可装配），必要时消费方自建 ProviderSet。
- **事件 ID 与元数据**：每个事件在发布侧生成或通过 `WithEventID` 显式指定
  string 唯一 ID（封装可选 ID 生成器，Publisher 构造级注入，缺省用内置
  实现）；ID 是幂等去重与全链路追踪
  的主键，后端必须原样落库，并按事件 ID 施加唯一约束——重复 Append 同 ID 视为
  幂等成功：首写为准，不覆盖既有行、不触发重投，不校验 payload 一致性
  （同 ID 即同一事件，写进契约）。Envelope 携带元数据块（string 键值），后端原样存储透传、不解释
  语义；暂用于携带 trace\_id（使用方可注入 otel TraceID 等追踪上下文）做事件流
  与调用链的关联。
- **依赖策略**：core 依赖 stdlib（slog）与 otel metric API，不携带任何数据库
  驱动，也不依赖具体方言；方言 DDL（版本计划、期望对象、锁语句与共享执行器）
  放在独立 `postgresschema` 模块，只做构造、不 import 驱动；各后端 require
  该模块并做连接适配与错误分类。core 的初始化面只定义契约类型（迁移模式、
  校验报告、能力声明），不内置任何后端 DDL。
- **观测策略**：日志走 stdlib `slog`，指标走
  `go.opentelemetry.io/otel/metric` 并跟随全局装配（未配置时 otel 全局
  provider 即 noop，热路径零开销）；类型化 `Observer` 为纯旁路。库直接
  持有 otel 依赖，不设 `Metrics` 适配接口（见 ADR 0008、§9.1）。
- **强类型与无参事件并存**：泛型声明式事件（`Declare[T](name)`）提供强类型
  生产/消费，payload 编解码默认 JSON、可插拔 codec；无参事件是
  `Declare[NoPayload]` 的快捷形态。Type 名是认领与路由的稳定标识，改名即
  破坏性变更（写进文档）。

## 5. 消费语义

两种方式统一约定：订阅注册在 Runner 生命周期内任意时刻可用（Stop 前）；
Start 前注册的订阅由 Start 统一装配并锚定基线，启动后注册即时生效
（起点语义见 §5.1 / §5.2，[ADR 0015](adr/0015-dynamic-subscription-registration.md)）。

### 5.1 消费者身份（认领消费）

消费者身份是**认领消费**的概念，作 claim 键（consumer\_type）；广播消费
不使用身份——无簿记、无跨实例状态，每个实例独立全收，订阅仅按事件类型集
声明（见 §5.2）。

- Runner 构造级设默认身份，订阅级可覆盖；一个进程可承载多个身份，各身份
  独立认领、心跳。
- 身份是消费者的事实身份而非可热切换的旋钮：新增或变更身份即新消费者从头
  认领（历史事件重投，仅限后端保留窗口内），不是断点续跑；存量身份
  的重命名迁移归管理面（生产化能力）。
- 启动后动态注册的认领订阅对齐同一口径（[ADR 0015](adr/0015-dynamic-subscription-registration.md)）：
  注册即时生效，从当前可认领行（保留窗口内，含其他身份已 done/failed 的）
  开始认领，后续事件照常——认领订阅无簿记，不引入任何新状态。

### 5.2 广播消费（发完即忘）

- 消费端不在存储侧留下任何簿记（无游标、无认领）：消费进度以实例内存中的
  已读水位记录，由 core 在 Fetch 时作为条件下推
  （见 [ADR 0004](adr/0004-demand-driven-pull.md)）。
- 起点：只处理订阅生效后发布的事件；基线锚定于订阅生效时刻
  （[ADR 0015](adr/0015-dynamic-subscription-registration.md)）——Start 前
  注册的订阅在 `Runner.Start` 内同步锚定：对每订阅取 `Driver.HeadPosition`
  （该类型已提交的最大位置，单条语句快照）为初始水位，Start 返回即基线
  确立，此后发布的事件必然可投递，锚点查询失败则 Start 返回错误、整体
  不启动；启动后动态注册的订阅在注册调用内同步锚定，注册返回即基线确立，
  锚定失败则注册以错误返回、订阅不生效、修复后可重试。停机与重启期间
  发布的事件不补投，重启从当前位置开始（对齐 Redis Pub/Sub 的发完即忘
  语义）。需要不丢、可重试、可追踪的场景应使用认领消费。基线锚点为
  单条语句快照，只锚已提交行（事务性入队未提交的行不进基线）；Postgres
  上 position 分配序（bigserial）与提交可见序在并发发布下存在理论上的
  乱序窗口（早分配的行晚提交会被水位越过）——普通 Publish 保持单语句
  独立提交使该窗口可忽略，「订阅生效后发布的事件必然可投递」仅在此路径
  成立。
  事务性入队（outbox）把窗口放大为常态：并发提交的更大 position 会把
  内存水位推过晚提交的行，该行对所有在途广播实例永久不可见（重启实例
  的基线锚定同样不含）——事务内发布下的广播投递为尽力而为，认领消费
  不受影响（Claim 直接查库，晚提交行最迟在下一次认领轮次可见；重审定稿
  见 [ADR 0012](adr/0012-outbox-transactional-append-and-broadcast-ordering.md)
  与 §6 表格「事务入队（可选）」行）。同源边界还有**延迟行被水位越过**
  （§9.2「延迟事件」）：延迟行占用 position，其后的即时行会把水位推过它
  ——延迟行到期后不再有广播可见性，需要定时必达用认领消费
  （[ADR 0013](adr/0013-delayed-delivery-and-cross-instance-wakeup.md)）。
  同源边界还有**被取代行**（§9.2「合并投递」）：superseded 行照常占
  position，下推条件直接排除——水位一次性越过，被取代行不再有广播可见性
  （HeadPosition / 基线锚点口径不变）。
- 投递语义为尽力而为：handler 失败仅记日志并跳过，不重试、不阻塞、不设
  死信；不承诺 at-least-once，也不承诺保序（轮询批次并发处理下顺序天然
  不保）——均写进契约。
- 广播：每个活跃实例各自独立拉取并处理全部事件，实例间互不感知、互不
  影响，实例数不影响语义；无单活跃实例约束。
- 适合通知、广播类副作用。上游发布时无需感知消费方存在。
- **惰性清理资格（统一规则）**：超过保留窗口且无活跃租约的行（无论终态与
  否）均可被清理。保留窗口对认领消费是重投、重试与完成状态查询语义的硬
  边界（`done` 行过窗即删，「完成状态上游可查」仅在窗口内成立；窗口须
  覆盖最坏重试路径，见 §5.3）；对广播消费仅是存储清理边界，无额外语义。

### 5.3 认领消费

- 认领即抢锁：`event_claim(event_id, consumer_type)` 唯一，只认领无主或租约
  过期行（`FOR UPDATE SKIP LOCKED`，Postgres 后端；语义由契约测试约束）。
- 租约 `lease_until` 即有效期；dispatcher 在订阅级**最大处理时长**内自动
  心跳续租，超时停止续租，租约到期由其他实例接管——挂死 handler 不被无限
  续保。
- 完成置 `done`（幂等、不可逆）；失败递增 `claim_count` 并按递增退避置为
  再次可认领（防热循环），达上限置 `failed` 终态。重试计数以「一次处理尝试
  未完成」为单位：显式 Fail 计入；租约过期被接管（前任未 Fail 而失去租约）
  在认领时同样计入，否则每轮挂死的事件会被无限接管、永不进入终态
  （细节见 [ADR 0006](adr/0006-broadcast-fire-and-forget-and-claim-retry.md)）。
  接管与 Fail 重试在重试计数中不可区分（`Attempts` 混合两者），接管专属
  的事实由 `Claim` 随轮返回：`ClaimResult.Takeovers` / `.TerminalFailed`
  与落库事实一致（§9.2、[ADR 0011](adr/0011-claim-result-and-takeover-counting.md)）。
- 一个事件可被 N 种 consumer\_type 各消费一次；同类型**同一时刻至多一个活跃
  租约**。租约过期被接管后，若旧实例仍存活（如慢 handler 被误接管），可能
  出现并发重复处理——整体投递语义仍是 at-least-once，不承诺恰好一次。
- **租约互斥不是消费去重**：它只保证"同一时刻至多一个实例在处理"，接管等
  场景下同一事件仍会被处理多次，库不在消费路径做去重簿记（广播消费无簿记，
  认领消费的 event\_claim 只做互斥不做已处理记录）。handler 幂等
  是消费方责任，推荐以事件 ID（见 §4，ID 即幂等去重主键）为幂等键——
  去重表、upsert/条件更新、天然幂等的状态机写法均可，只须保证重复投递
  不产生二次副作用。
- Complete/Fail 以**持有租约为前提**（对租约属主做条件更新）：租约过期或
  已被接管后，旧实例迟到的 Complete/Fail 不生效，不得递增 claim\_count 或
  覆盖新属主的认领状态（写进契约测试）。
- **失败摘要落库**（[ADR 0009](adr/0009-error-summary-persistence.md)）：
  `event_claim` 留存 `last_error`（错误摘要）与 `failed_at`（终态时刻）。
  错误文本按不可信输入处理，口径为：移除全部 Unicode 控制字符（Cc 类，
  含换行与制表符）、截断 4 KiB 字节（不撕裂 UTF-8 码点）、不落堆栈；清洗函数在 core
  （`event.SanitizeErrorSummary`）供各后端共享。仅终态写入：本次 Fail 使行
  进入 `failed` 终态时写摘要与时刻，重试中不写；接管终态只写 `failed_at`
  （无本次处理错误）；迟到的 Fail 不写。不变式：`failed_at` 非空 ⟺ 行当前
  处于 `failed` 终态；`last_error` 是最近一次终态失败的摘要（重放清
  `failed_at`、保留 `last_error`，直到下一次终态失败覆盖）。Inspector 与
  重放以该两列为数据源。
- **failed 手工重放**（[ADR 0010](adr/0010-admin-write-plane-and-replay.md)）：
  管理面写操作 `Admin.ReplayFailed` 把（事件 × 消费者身份）的 failed 终态
  行重新置为可认领——仅 failed 可重放（done 幂等不可逆、不重放；active /
  retrying 拒绝且不改动）。重放为单条条件更新（CAS，仅命中
  `status='failed'`）：并发重放 / 认领 / Fail 竞态下不改写非 failed 行，
  已被重新认领（接管）或已完成的行不受迟到重放影响。重放写入：置
  `status='retrying'`、`claim_count=0`（此后 Fail 的重试计数从合法范围
  重新开始）、清 `lease_token`、`lease_until` 置为重放时刻（原租约凭证
  失效）、`next_eligible_at=now`（立即可认领，不做退避）、`failed_at`
  清空、`last_error` 保留（见上文 ADR 0009 口径）。**重放窗口 = 保留
  窗口**：超窗行已被惰性清理删除，重放该键与「行不存在」同判别
  （ok=false）。无效果（行不存在或非 failed 终态）统一返回 ok=false，
  需要区分原因先经 Inspector 查询。写操作由独立 Admin 可选接口承载，
  不进 Driver、不挂后端 Driver 类型，不经运行面类型获得——经后端
  `NewAdmin` 构造或经组合对象 Store 聚合（同一内部构造路径）。写路径
  加固（ADR 0010）：**持锁**——重放在事务内持有与迁移执行相同的命名
  空间互斥锁（键含 schema，与迁移串行化，等待有界且上限经后端选项
  `WithAdminLockWait` 配置，超时返回 `ErrAdminLockTimeout`——未预热实例
  首次操作时 ensure 撞上的迁移锁等待与写事务内的锁等待同键同判）；**行
  归属校验即 CAS 本身**——（事件 × 身份）
  主键 + status 条件 + 构造期固定的命名空间，不命中即不归属；**记录操作
  来源**——otel 指标（`<scope>.admin.replay` 与
  `<scope>.admin.replay.duration`）+ slog 审计日志 + 类型化
  Observer 事件（`ObserverV3.OnAdminReplay`），均为旁路；**错误可判别**
  ——锁超时（瞬时）、迁移面哨兵（ensure 原样上抛）与连接故障分列，执行
  故障不折叠为 ok=false。
- 终态是（事件 × consumer\_type）粒度：`done`/`failed` 仅对该身份自身生效，
  终态事件不参与该身份的常规认领（done 幂等不可逆；failed 经管理面手工
  重放，见上文与 §9.2）；新增 consumer\_type 重放全部窗口内历史——含其他身份
  已 done/failed 的事件，各身份消费彼此独立。
- 完成状态上游可查。适合任务类副作用（需要完成追踪与失败重试）。
- 保留窗口是认领语义的硬边界：滞留超窗且无活跃租约的行会被清理（§5.2），
  事件随之丢失——窗口须配置为覆盖最坏重试路径（约重试上限 × (最大退避 +
  最大处理时长)，另留心跳间隔与时钟偏差余量）。

### 5.4 需求驱动的拉取

见 [ADR 0004](adr/0004-demand-driven-pull.md)。要点：

- 订阅注册表（订阅 → 事件类型集；认领订阅绑定消费者身份）是需求的单一
  事实源，承担三重职责：把"我要什么"编译成查询条件下推到后端、就近唤醒
  的定向路由、事件的分发。
- 延迟投递同属下推条件：`deliver_at <= now`（判据时钟由 core 下推，
  [ADR 0013](adr/0013-delayed-delivery-and-cross-instance-wakeup.md)），
  未到期行在单条读取/认领操作内被过滤，不进拾取窗口。
- 过滤必须发生在认领/读取时而非取回之后：先取后滤会把无法处理的行反复捞出，
  并破坏跨实例的认领语义。
- 广播消费的已读水位由 core 按订阅类型在内存中维护并在 Fetch 时下推，只进
  不退，不在存储侧簿记。
- 单类型洪泛会饿死其他类型：编排按类型轮询限额取数。认领侧每轮有效批
  大小为 `min(WithFetchLimit, 空闲并发槽位)`；无空闲槽位时不发起认领。
- 通配订阅（消费全量）基线不允许，订阅必须显式列出类型。

### 5.5 就近消费

见 [ADR 0003](adr/0003-local-delivery-as-wakeup-only.md)。要点：

- 同进程直投只是"唤醒信号"（内存 channel 触发本实例 poller 立即拉取/认领），
  不是投递本身；读取与认领仍走后端存储。
- 跨实例与直投失败由周期 poll 兜底。
- 绝不引入"本地直投跳过存储"的快路径，避免投递语义分裂。

## 6. 核心接口能力面

| 能力    | 接口面                           | 语义要点                                                                                                                                                                 |
| ----- | ----------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 事件声明  | 泛型 `Declare[T]` / `NoPayload` | Type 名是认领与路由的稳定标识；payload 默认 JSON codec、可插拔                                                                                                                          |
| 事件信封  | Envelope                      | 发布侧唯一 ID、Type、Payload、元数据块（透传）、OccurredAt、DeliverAt（最早投递时刻，零值=立即，ADR 0013）、CoalesceKey（合并投递键，空串=未设置，ADR 0014）；Attempts/LastAttempt 为 core 运行时注入（认领消费按租约尝试序号与 RetryLimit 计算，广播恒 0/false）                                                                                                                          |
| 生产    | Publisher                     | Publish 发布；ID 与元数据缺省自动填充；`WithDeliverAt` 声明延迟投递（ADR 0013）；`WithCoalesceKey` 声明合并投递键（ADR 0014）；ID 生成器等构造级注入；outbox 拓展见生产化能力                                                                                                                |
| 消费编排  | Runner + Subscribe            | 订阅级声明消费方式（广播/认领命名式入口，同一 Runner 可混合承载）与消费者身份（仅认领消费，构造默认 + 订阅级覆盖）、并发、租约、轮询间隔、重试上限与退避（仅认领消费；广播消费失败即跳过，见 §5.2）；两种方式同一订阅签名（强类型糖）；订阅全生命周期可注册（Stop 前），启动后动态注册即时生效（ADR 0015）；签名见 docs/api.md                                              |
| 后端缝合点 | Driver                        | 单次原子操作，需求条件（订阅类型集 + 租约过期判据 + 可认领/水位条件）在单条 Fetch/Claim 内下推：Append / Fetch、HeadPosition（广播消费，水位条件与基线锚点由 core 传入）/ Claim、Heartbeat、Complete、Fail（认领消费）；语义见 §5 与 ADR |
| 查询面（可选） | Inspector                 | 只读查询，按能力探测接入（ADR 0005）：（事件 × 消费者身份）状态与重试计数；按身份的状态分档计数——pending（该身份无认领行且未被取代）/ superseded（无认领行且已被合并取代，事件行属性档，ADR 0014）/ retrying / processing / failed / done（done 为归档档，六档互斥且完备）；failed 事件偏移分页列表（错误摘要与 failed_at 随行返回，ADR 0009）。与 Driver 共享连接与命名空间选项；只读、不参与认领/完成路径；结果仅在保留窗口内可见（硬边界，§5.2） |
| 管理面（可选） | Admin                     | 写操作与运行面分离、不进 Driver、不挂后端 Driver 类型（ADR 0010）：`ReplayFailed` 把（事件 × 消费者身份）的 failed 终态行重置为可认领——仅 failed 可重放（done 不可逆），单条 CAS 条件更新防竞态；重放置 retrying、claim_count=0、清租约、next_eligible_at=now、failed_at 清空、last_error 保留；不经运行面类型获得——经后端 `NewAdmin` 构造或经组合对象 Store 聚合（同一内部构造路径；与 Driver 共享连接与命名空间选项，鉴权归使用方）；重放窗口 = 保留窗口（§5.3）。写路径持与迁移执行共用的命名空间互斥锁（等待有界）、归属校验即 CAS、发射操作来源观测（指标/日志/Observer 事件）、错误可判别（`ErrAdminLockTimeout` / 迁移面哨兵 / 连接故障）；无自动 down，迁移侧无台账无需 reset/stamp 入口 |
| 事务入队（可选） | TransactionalAppender    | outbox（[ADR 0012](adr/0012-outbox-transactional-append-and-broadcast-ordering.md)）：调用方在自己的业务事务内追加事件并随事务原子提交/回滚；core 不定义接口——事务载体是后端具体类型（pgxc 为 pgx.Tx、postgrec 为 *sql.Tx、inmem 为缓冲事务模拟），各后端提供 `NewTransactionalAppender` 构造（与 Driver 共享连接与命名空间选项）或经组合对象 Store 聚合，不经运行面类型获得。语义边界：提交与回滚归调用方（库不 begin/commit）；幂等与普通 Append 同口径（同 ID 首写为准）；提交前对外不可见（Fetch / Claim / HeadPosition 只锚已提交行）；就近唤醒信号不覆盖事务内发布，广播消费下可能被并发提交的更大 position 越过水位而漏投（§5.2），认领消费不受影响；不触发惰性清理（清理 DELETE 不落入调用方事务）；不发射观测（库侧发射点在提交前，与回滚后的落库事实可能矛盾）；position 同事务内按 INSERT 序分配、不承诺跨事务连续，回滚不回收（Postgres 序列不回收，缺口不进消费契约） |
| 可观测    | otel 指标 + slog + Observer     | 跟随全局装配（未配置 noop 静默，热路径只打点不打日志）；core 计量 driver 操作与消费编排，后端计量迁移生命周期与管理面写操作（`<scope>.migrate` 与 `<scope>.admin.replay`，发射点各自单一，无重复计量）；类型化 Observer 纯旁路（同步回调、库内 recover）；见 ADR 0008 与 api.md §2.9 |
| 延迟投递    | RawEnvelope.DeliverAt          | 定时投递（[ADR 0013](adr/0013-delayed-delivery-and-cross-instance-wakeup.md)）：最早投递时刻随信封落库（零值=立即），Fetch / Claim 下推 `deliver_at <= now`（判据时钟由 core 随 FetchQuery.Now / ClaimQuery.Now 下推，过滤在单条操作内完成，ADR 0004）；保留窗口自可投递时刻起算（COALESCE(deliver_at, occurred_at)）——延迟行不因「等待投递」被清理；HeadPosition / position 语义不变；**广播组合边界**：延迟行可被水位越过而不再投递（§5.2），需要定时必达用认领消费；经 Publish 的 `WithDeliverAt` 与事务性入队的 `RawEnvelope.DeliverAt` 均可设置 |
| 合并投递    | RawEnvelope.CoalesceKey        | 同键最新一次消费（[ADR 0014](adr/0014-coalescing-delivery.md)）：合并键随信封落库（空串=NULL，未设置），作用域为 (Type, key)、与事件 ID 正交（重复 Append 同 ID 不触发取代——幂等与合并正交）；Append 单条 CTE 原子「置同键待消费前驱 superseded + 插入新行」（v1 保守取代范围：仅无任何认领行的待消费前驱，处理中 / 等退避 / 租约过期待接管 / 终态行不动，新事件照常排队）；Fetch / Claim 下推排除被取代行——广播水位直接越过、认领不发放、新消费者身份同样跳过（superseded 是事件行属性）；HeadPosition / position 语义不变（被取代行照常占 position）；被取代的延迟行永不到期投递；superseded 行照常参与保留期清理；窗口语义不内建——去抖经与 `WithDeliverAt` 组合表达（合并键 + deliver_at = now + 静默期，每次新发布自带基线，须每次发布新 ID）；取代行数随 `Driver.Append` 的 `AppendResult` 返回（与落库事实一致），core 发射 `<scope>.append.supersede` 计数；`AppendTx` 的取代与插入同事务（不返回计数，ADR 0012 边界不变）；Postgres 后端为版本计划 v4 加列 `event.coalesce_key` / `event.superseded_at`（expand-only，混合版本下旧二进制照常消费被取代行——合并是优化不是正确性保证） |
| 跨实例唤醒（可选） | pgxc Wake                    | 低延迟唤醒（[ADR 0013](adr/0013-delayed-delivery-and-cross-instance-wakeup.md)）：仅信号、不承载投递保证（ADR 0003 边界不变，错过的信号由周期 poll 兜底）。pgxc 原生 LISTEN/NOTIFY——发布侧 INSERT 经 CTE 发 `pg_notify`（仅实际插入行时；通道与 payload 只带 schema 名，不带事件内容；随事务提交可见，事务性入队的信号在提交时刻发出），订阅侧 `Wake` 专用连接 LISTEN `go_event_wake_<schema>`（前缀 + 全名超 63 字节的 schema 走确定性哈希通道名，两侧同源派生必然一致，任意合法 schema 都有通道）、断线有界退避重连，`Bind(*event.LocalWake)` 桥接为 `NotifyAll` 信号（跨实例信号触发本地已挂起的 Fetch/唤醒等待）；postgrec 退化轮询（database/sql 无驱动无关的 LISTEN 接入）；inmem 无跨实例面（同进程 LocalWake 已覆盖） |

## 7. 仓库布局

module path 为 `github.com/jninng/go_event`，子 module 为
`github.com/jninng/go_event/postgrec`、`github.com/jninng/go_event/pgxc`、
`github.com/jninng/go_event/postgresschema`、`github.com/jninng/go_event/eventtest`。 

```
go_event/
├── doc.go / event.go / publisher.go / runner.go / driver.go /
│   admin.go / inspector.go / errorsummary.go / codec.go /
│   errors.go / localwake.go / migrate.go / observability.go
│                                                          # core：契约与编排、迁移契约类型
├── postgrec/        # Postgres 后端（标准库）：event/event_claim 表、SKIP LOCKED、迁移执行
├── pgxc/            # Postgres 后端（pgx v5）：event/event_claim 表、SKIP LOCKED、迁移执行
├── postgresschema/  # Postgres 方言 DDL 单一事实源：版本计划、期望对象、共享执行器（无驱动依赖）
├── eventtest/       # 契约测试套件：RunSuite/RunRetentionSuite/RunDelayedDeliverySuite/RunCoalescingSuite/RunMigrationSuite/RunInspectorSuite/RunAdminReplaySuite/RunStoreSuite/RunTransactionalAppendSuite
│   ├── inmem/       # 进程内 Driver（契约测试与使用方本地测试）
│   └── simmigrate/  # 迁移契约测试的进程内模拟 backend（语句边界故障注入）
└── docs/
    ├── api.md       # API 文档
    ├── design.md    # 本文
    └── adr/         # 关键决策记录
```

## 8. 后端能力边界

核心语义在各后端必须达到「原生或模拟」档——「不支持」即无法通过 eventtest
全套契约测试，核心语义不存在该档位；各后端文档必须给出能力边界表，注明每项
语义的实现档位。非核心的可选能力（延迟事件、管理面查询、跨实例推送唤醒等）
按可选拓展面接入（探测或后端具体构造，与 outbox 同模式；延迟事件已落地为
信封字段 + 全后端同形实现，跨实例唤醒由 pgxc 提供、postgrec 退化轮询，
[ADR 0013](adr/0013-delayed-delivery-and-cross-instance-wakeup.md)）。

**持久结构初始化按迁移模式处理**（[ADR 0007](adr/0007-migration-mode-and-protocol.md)）：

- `Create`（缺省）：首次操作时以构造 Driver 的连接持迁移锁幂等应用版本
  计划全量语句（基线与各版本；schema、表、约束、索引）；升级随部署隐式
  发生，无独立 `Upgrade` 模式，需要 DDL 权限。
- `Verify`：只读校验对象集合与计划期望（全量版本）一致，不执行 DDL；
  缺失表示待升级（处置为执行迁移），定义冲突时首次操作返回 `VerifyError`。
  表须由部署流程创建，运行账号可只授 DML（另需系统目录读）。
- `None`：不建表也不校验，完全交给调用方。

迁移执行（单一执行路径，无台账）：

- **版本计划**：迁移单元是幂等 DDL 单语句（expand-only：建表/加列/建索引），
  由版本 `Version{Number, Statements}` 构成计划，版本号自基线 1 起严格
  递增；版本号只存在于计划中，库内不落版本记录——对象集合即事实。一条
  语句恰好构造期望对象集合中的一个对象；约束不在建表语句中内联，统一用
  探测后条件执行的 DO 块创建（`ADD CONSTRAINT` 无 `IF NOT EXISTS`）。
  后续版本以追加方式进入计划，不修改既有版本语句。
- **统一锁**：基线与升级共用同一锁键（含 schema 命名空间）与同一份语句
  定义；执行任何 DDL 前先取锁（并发 `IF NOT EXISTS` 类 DDL 存在元数据
  竞态，不能依赖语句幂等替代锁）。事务级 advisory 锁，随提交/回滚释放；
  等待有界（try-lock + 退避，DDL 自身锁等待由 `lock_timeout` 限定），超时
  映射为瞬时错误，不得无限阻塞。锁能力为无（`LockNone`）时，`Create`
  返回 `ErrMigrationUnsupported`，只能 `Verify`/`None`；DDL 原子性为无
  （`DDLAtomicNone`）时同样拒绝。
- **整版单事务**：取锁 → 接管校验（对象集合与计划期望比对：仅缺对象 →
  放行执行；任一定义冲突 → 差异错误，不修复）→ 依次执行计划内全部语句，
  任何失败整体回滚；中断后重跑即收敛。不做逐对象探测、不写执行状态、
  无恢复矩阵。
- **基线接管校验**：表已存在（旧二进制、手工脚本或其他工具创建）时校验
  对象集合（名称、列与类型、主键/唯一/外键引用/索引列）。已有对象与期望
  定义一致且仅缺对象 → 补齐并收敛；任一已有对象定义冲突或无法判定归属 →
  返回差异错误，不自动修复。
- **不支持先查后建**：不用 `Verify` 作为 `Migrate` 的前置门——检查与创建
  无法组成原子操作；原子性由锁 + 整版事务保证。
- **失败处理**：迁移失败直接返回错误（不持久化）。`ensure` 失败按 Driver
  实例负缓存：瞬时错误（数据库不可用、锁等待超时）有界指数退避（100ms
  起、封顶 30s）；永久配置错误（权限、定义冲突、非法 schema、后端不支持）
  fail-fast。`Verify` 首次通过后在 Driver 内缓存「已检查」，部署补建或
  定义修复后需重启/重建 Driver。
- **导出物**：`Statements` 为计划内全部语句清单（不含锁）；`SQL` 由版本
  计划派生：以 `BEGIN` 开始，随后设 `lock_timeout` 并取迁移锁（锁语句位于
  全部 DDL 之前），随后依次输出计划内全部语句（基线 + 各版本，全部幂等），
  末尾 `COMMIT`，整段须在同一事务执行、整段可重放。脚本与执行器共用同一
  份语句定义，不维护第二份拷贝；人工执行必须先取锁、整段执行，不得拆分
  跳过。

**能力矩阵**（值以各后端 `Capabilities()` 声明为准）：

| 能力项 | postgrec | pgxc | inmem |
| --- | --- | --- | --- |
| DDL 原子性 | 跨语句事务 | 跨语句事务 | 无（`DDLAtomicNone`，零值；迁移为 no-op） |
| `IF NOT EXISTS` 覆盖 | schema/table/column/index 原生；constraint 无原生语法（探测 + DO 块） | 同 postgrec | 无（nil，零值） |
| 对象探测 | schema 仅存在性；表/列/索引/约束为存在性 + 定义 | 同 postgrec | 无（nil，零值） |
| 错误分类 | 已存在 / 权限 / 瞬时全支持 | 同 postgrec | 无（全 false） |
| 锁原语 | 事务级 advisory 锁 | 事务级 advisory 锁 | `LockNone` |
| 锁释放 | 提交/回滚自动释放 | 提交/回滚自动释放 | 零值（无锁，无实际语义） |
| DDL 完成语义 | 同步 | 同步 | 零值（无 DDL） |
| 事务外语句 | 计划可包含事务外语句（如并发建索引） | 同 postgrec | false（零值） |
| DDL 是否隐式提交 | 否（事务内可整体回滚） | 同 postgrec | false（零值） |
| 自动迁移 | 支持（版本计划走整版单事务） | 支持（版本计划走整版单事务） | no-op，不参与 DDL |

inmem 无持久结构，零值能力声明不触发「锁能力为无」的拒绝分支。

**连接权限矩阵**：

| 模式 | 所需权限 | 推荐场景 |
| --- | --- | --- |
| `Create` | DDL + DML | 部署/启动期高权限连接初始化空库或幂等补齐版本计划全量对象（升级随部署隐式发生） |
| `Verify` | DML + 系统目录只读 | 运行期自检；运行账号仅 DML，表由部署期建好 |
| `None` | 仅 DML | schema 完全由部署流程管理，库不做检查 |

自动迁移（`Create`）需要 DDL 权限；无 DDL 权限的运行连接不能自动
迁移，只能 `Verify`/`None`。库不负责账号或授权管理。

Driver 保持**拉形状**（Claim/Fetch 返回批次），推送型后端经其拉式 API 适配
（JetStream Fetch、XREADGROUP 等），真推送投递留作可选 DeliverySource 拓展，
不动基线。

各后端的具体映射与取舍（NATS JetStream、RocketMQ 5.x、Redis Stream、
RabbitMQ、Kafka 排除）见 [ADR 0005](adr/0005-backend-capability-boundary.md)。

## 9. 能力路线

按能力组织。

### 9.1 基线能力（P0）

- **事件生产**：强类型（`Declare[T]`）/无参（`NoPayload`）两种载荷形态发布；
  发布侧唯一事件 ID（可插拔生成器；可显式指定 ID，优先于生成器），ID 全局
  唯一、业务键须按类型加前缀后才能当 ID 用；元数据块透传（trace\_id）。
- **广播消费**：发完即忘、上游无感——无存储侧簿记，已读水位存于实例内存；
  只处理订阅启动后发布的事件，停机期间事件不补投；handler 失败仅记日志
  跳过，不重试、不保序；每个活跃实例各自独立收取全部事件（广播）。
- **认领消费**：按消费者身份（构造默认 + 订阅覆盖）抢锁认领（无主或租约过期
  行）、租约有效期、自动心跳续租（最大处理时长内）、done/failed 终态、失败
  重试有上限且按退避重认领（租约过期被接管同样计入）；每事件 × 每类型同一
  时刻至多一个活跃租约（整体 at-least-once，不承诺恰好一次）；迟到的
  Complete/Fail 不生效；终态为（事件 × 消费者身份）粒度，新增身份重放全部
  窗口内历史（含其他身份已终态的事件）；handler 可从信封读到尝试序号与
  最后一次尝试标记（广播恒为 0/false，接管即置 failed 的路径不投递最后一次）。
- **消费编排**：认领消费多身份承载、需求驱动拉取（按类型轮询限额，防单类型
  洪泛饿死其他类型）、并发度控制、优雅停机（停领在途跑完）；认领每轮有效
  批大小为 `min(WithFetchLimit, 空闲并发槽位)`，无空闲槽位时不认领；派发前
  由 core 按租约尝试序号与订阅 RetryLimit 注入 Attempts/LastAttempt；订阅
  全生命周期可注册（Stop 前），启动后动态注册即时生效——广播基线锚定于
  注册调用、认领对齐 §5.1 新身份口径（[ADR 0015](adr/0015-dynamic-subscription-registration.md)）。
- **就近消费**：同进程发布唤醒本实例立即拉取/认领（仅信号，不跳过存储）。
- **可观测**：日志走 stdlib `slog`，指标走 `go.opentelemetry.io/otel/metric`
  并跟随全局装配（未配置时 otel 全局 provider 即 noop，热路径零开销）；
  core 计量 driver 操作与消费编排（含认领侧接管与终态计数，
  `<scope>.claim.takeover` / `<scope>.claim.terminal_failed`），后端计量
  迁移生命周期（单一发射点）；类型化 `Observer` 为纯旁路（同步回调、
  库内 recover）。见 ADR 0008、
  [ADR 0011](adr/0011-claim-result-and-takeover-counting.md) 与
  [api.md §2.9](api.md)。
- **Postgres 后端**：event / event\_claim 两表、幂等版本计划 DDL、SKIP LOCKED
  认领、惰性清理（资格：超窗且无活跃租约，无论状态；保留窗口可配，历史
  重放仅限窗口内，认领消费窗口须覆盖最坏重试路径）。
- **持久结构初始化与迁移三模式**：`Create`（缺省，首次操作持迁移锁幂等
  应用版本计划全量语句；升级随部署隐式发生）/ `Verify`（只读校验）/
  `None`（不建不校验）；`Create` 做基线接管校验，定义冲突返回差异错误、
  不自动修复；`Statements`/`SQL` 导出计划全量语句与含锁手工脚本；
  能力声明（`Capabilities`）用于契约测试与文档判定；运行期无 DDL 权限
  只能 `Verify`/`None`。迁移失败按 Driver 负缓存：瞬时错误有界退避，永久
  配置错误 fail-fast。迁移协议见 [ADR 0007](adr/0007-migration-mode-and-protocol.md)
  （幂等语句、单一执行路径、无台账）。
- **契约测试**：inmem Driver + eventtest 套件，覆盖上述全部语义（广播消费
  基线锚定与起点语义/失败跳过且不阻塞/广播各自全收/水位单调；无参事件
  的广播与认领；类型间不互相饿死；认领消费互斥（含并发认领）/接管/
  接管计入重试/心跳/超时停续/退避重试/重试耗尽终态/迟到 Complete 与
  Fail 不生效/新身份重放全部窗口内历史/超限行不占限额槽位；接管与终态
  计数随 ClaimResult 与落库事实一致——普通发放零计数、接管发放
  Takeovers=1、终态接管双计、Fail 重试重认领与 retrying 行重认领不计
  接管且不认领侧置终态、混合批次只计接管行并可与 Attempts/落库状态
  对账；公共的
  ID 唯一与幂等 Append 首写为准、元数据透传、HeadPosition 基线锚点；
  错误摘要与终态时刻仅终态写入（含接管终态仅写 failed\_at）、重试中与
  迟到 Fail 不写、摘要截断与去控制字符；
  持久结构初始化的幂等可重复执行；迁移契约 `RunMigrationSuite`（空库/
  完整/部分/冲突、三模式 Driver 首启、导出物、并发首启、锁释放、
  Lock=none、语句边界故障注入与回滚/收敛）；保留窗口清理资格
  （RunRetentionSuite）；只读查询面 Inspector（`RunInspectorSuite`：
  三类查询与 Claim/Fail/Complete 落库事实比对——状态与重试计数、分档
  口径含 done 归档、failed 分页边界与摘要事实、只读稳定性）；管理面
  重放（`RunAdminReplaySuite`：重放后可再认领并完成、重试计数归零与
  循环重放、done/active/retrying/未知键无效果且判别统一、last_error
  保留与 failed_at 清空、并发重放恰好一次生效、被接管后迟到重放不
  污染新属主、互斥锁占用期间锁超时且行不变/释放后重放生效、观测事件
  ok/no_effect/lock_timeout 结果与事件 × 身份一致））；延迟事件
  （`RunDelayedDeliverySuite`：零值立即、到期边界含到时刻、Fetch / Claim
  到期前过滤、信封透传、延迟行与广播水位的组合边界；`RunRetentionSuite`
  增保留期组合用例；`RunTransactionalAppendSuite` 增 AppendTx 延迟用例；
  `RunSuite` 增 Publish 路径用例）。

### 9.2 生产化能力（P1）

- **outbox（事务性入队）**：已落地——见 §6 表格「事务入队（可选）」行与
  [ADR 0012](adr/0012-outbox-transactional-append-and-broadcast-ordering.md)。
  core 不定义接口（事务载体是后端具体类型），pgxc / postgrec / inmem 各
  提供 `TransactionalAppender`（`NewTransactionalAppender` 构造或经组合
  对象 Store 聚合）：业务事务与事件入队原子提交/回滚，不需要事务内发布
  的场景可跳过本面。广播位置语义已随本项重审定稿（§5.2）：事务内发布下
  广播投递为尽力而为（可能被并发提交的更大 position 越过水位而漏投），
  认领消费不受影响；契约套件 `RunTransactionalAppendSuite` 钉死回滚零
  事件、提交前不可见、提交后可见、事务内幂等、回滚缺口与乱序边界。
- **观测（接管计数）**：已落地——`Claim` 返回
  `ClaimResult{Leases, Takeovers, TerminalFailed}`：`Takeovers` 统计本次
  认领中「接管过期租约」而发放的数量（含终态接管；无主首认与 Fail 退避
  期满的重认领不计入），`TerminalFailed` 独立统计认领动作直接置 failed
  的数量（当前认领语义下置终态的下推条件要求租约已过期，故认领侧终态
  必经接管路径，恒有 TerminalFailed ≤ Takeovers）；一次终态接管同时计入
  两者。计数取自后端落库事实（SQL 后端为终态 UPDATE 影响行数与认领标记，
  inmem 为内存计数），与 Inspector.ClaimState / claim\_count 可对账；
  `Lease` 不承载接管标记（v0.x 允许破坏性变更）。观测面：<scope>.claim.takeover
  与 <scope>.claim.terminal_failed 指标 + `ObserverV4.OnClaim` 事件（ADR 0008
  与 [ADR 0011](adr/0011-claim-result-and-takeover-counting.md)）。
- **管理面**：查询面 `Inspector` 已落地（§6 表格「查询面（可选）」行）：
  只读可选接口、按能力探测接入，提供（事件 × 消费者身份）状态与重试
  计数、按身份的状态分档计数（「待处理」= 该身份无认领行）、failed 事件
  偏移分页列表；结果仅在保留窗口内可见（硬边界）。failed 手工重放与
  Admin 加固已落地（§6 表格「管理面（可选）」行）：写操作由独立可选接口
  `Admin` 承载、不进 Driver、不挂后端 Driver 类型，不经运行面类型获得
  ——经后端 `NewAdmin` 构造或经组合对象 Store 聚合（同一内部构造路径）；
  `ReplayFailed` 仅对 `failed` 终态重新置为可认领（`done` 不可逆、
  不重放），语义与并发防护见 §5.3；加固约束（ADR 0010）：写路径持与
  迁移执行共用的命名空间互斥锁（等待有界，上限经后端选项
  `WithAdminLockWait` 配置）、行归属校验即 CAS、操作
  来源观测（`<scope>.admin.replay` 与 `<scope>.admin.replay.duration`
  指标 + slog 审计日志 + Observer 事件，
  均为旁路）、错误可判别（`ErrAdminLockTimeout` / 迁移面哨兵 / 连接
  故障分列，执行故障不折叠为 ok=false）。剩余项仍在规划：dead-letter
  处置钩子、存量消费者身份的重命名迁移（§5.1）。失败信息留存
  （最后错误摘要与终态时刻）已落地（§5.3、
  [ADR 0009](adr/0009-error-summary-persistence.md)），是 Inspector 查询
  与重放的数据源。迁移侧无台账，不需要 reset/stamp 入口；schema 异常经
  迁移重跑或人工修复。
- **延迟事件**：已落地——见 §6 表格「延迟投递」行与
  [ADR 0013](adr/0013-delayed-delivery-and-cross-instance-wakeup.md)。
  信封可选字段 `DeliverAt`（零值=立即），Publish（`WithDeliverAt`）与
  事务性入队（`RawEnvelope.DeliverAt`）均可设置；未到期行不满足
  Fetch / Claim 的下推条件（判据时钟由 core 下推，过滤在单条操作内
  完成）；保留窗口自可投递时刻起算（Postgres 后端为版本计划 v3 加列
  `event.deliver_at timestamptz` 可空，零值行语义不变、无需回填）；
  与广播消费组合的边界写进文档（§5.2）：延迟行被内存水位越过即不再
  投递，需要定时必达用认领消费。
- **低延迟唤醒**：跨实例推送已落地（pgxc）——见 §6 表格「跨实例唤醒
  （可选）」行与 ADR 0013：仅信号、不承载投递保证（ADR 0003 边界不变）；
  pgxc 走 LISTEN/NOTIFY（专用连接、断线重连、payload 只带 schema 名），
  postgrec 退化轮询（不做实现，能力边界表注明），inmem 无跨实例面
  （同进程 `LocalWake` 已覆盖）。
- **合并投递**：已落地——见 §6 表格「合并投递」行与
  [ADR 0014](adr/0014-coalescing-delivery.md)。信封可选字段
  `CoalesceKey`（作用域 (Type, key)，与事件 ID 正交），Publish
  （`WithCoalesceKey`）与事务性入队（`RawEnvelope.CoalesceKey`）均可
  设置；Append 单条 CTE 原子「置同键待消费前驱 superseded + 插入新行」，
  v1 保守取代范围仅覆盖无认领行的待消费前驱（活跃租约 / 退避 / 待接管 /
  终态行不动）；Fetch / Claim 下推排除被取代行，一种机制同时服务广播与
  认领两种消费模式（水位直接越过，HeadPosition / 基线锚点不变）；窗口
  语义不内建——去抖经与 `WithDeliverAt` 组合表达；观测面：取代行数随
  `AppendResult` 返回，core 发射 `<scope>.append.supersede` 计数，
  Inspector 增 superseded 分档（pending 口径收窄为「无认领行且未取代」，
  六档互斥且完备）；Postgres 后端为版本计划 v4 加列（expand-only），
  混合版本下旧二进制照常消费被取代行——合并是优化不是正确性保证。

### 9.3 扩展能力（P2）

- **新后端**（均须过 eventtest 全套契约测试并给出能力边界表）：NATS
  JetStream（首选）、RocketMQ（候选，限 5.x）、Redis Stream、RabbitMQ；
  Kafka 排除。映射与边界详见
  [ADR 0005](adr/0005-backend-capability-boundary.md)。
- **可选拓展接口**（能力探测或后端具体构造，能力边界表标明各后端支持
  度）：延迟事件与跨实例推送唤醒已提前落地（§9.2，形态见 §6 表格——
  延迟事件为信封字段、唤醒为 pgxc 具体构造 `Wake`）；管理面查询、真
  推送投递（DeliverySource）仍为规划项；outbox
  （TransactionalAppender）已提前落地（§9.2）。
- **事件链**：correlation\_id / causation\_id 贯穿，追踪一条业务链上的事件族。
- **批量认领**：一次抢多条，降低高吞吐下的 DB 往返。
- **限流/背压**：按类型限速，防止下游过载。

### 9.4 优先级依据

- **基线（P0）**：定契约、定语义、跑通首个后端与契约测试。契约测试先于第二个
  后端存在，语义即测试。
- **生产化（P1）**：不破坏核心契约的生产化缺口（outbox、观测、管理面），每项
  对应明确场景。
- **扩展（P2）**：新后端与高级语义，涉及一致性论证（Redis 模拟租约、Kafka
  排除、RocketMQ 版本与顺序边界）或面扩张（事件链），放契约测试成熟之后。
