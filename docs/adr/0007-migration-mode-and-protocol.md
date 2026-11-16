# ADR 0007: 迁移模式与迁移协议

- 状态：已接受
- 日期：2026-11-16

## 背景

持久结构初始化原先只有一条隐含约定：首次使用 Driver 时自动执行幂等 DDL。
该约定在两类场景失效：

1. 运行账号通常只有 DML 权限。以运行连接执行 DDL 会在首次 Append/Claim
   时报权限错误，而调用方无从区分「部署漏建表」与「凭据过期」。
2. 版本化演进需要区分「只建基线」「只校验」「自动升级」，并需要台账、
   锁与崩溃恢复协议；把所有行为压进一个 `Migrate` 会让语义不可判定。

同时，不同后端的 DDL 能力差异大：是否有跨语句事务、是否支持
`IF NOT EXISTS`、能否探测对象定义、有哪些锁原语。协议必须按能力声明选择
执行路径，而不是假设所有后端都能走同一条实现。消费与投递语义见
[ADR 0002](0002-two-consumption-modes-and-lease.md)、
[ADR 0004](0004-demand-driven-pull.md)、
[ADR 0006](0006-broadcast-fire-and-forget-and-claim-retry.md)；后端能力分级
见 [ADR 0005](0005-backend-capability-boundary.md)。

## 决策

### 1. 迁移模式与向后兼容政策

`MigrateMode` 定义在 core，选项函数由各后端提供：

| 模式 | 语义 | 现状 |
| --- | --- | --- |
| `Create`（缺省） | 首次操作时持迁移锁执行基线；只建基线，不升级存量表 | P0 |
| `Verify` | 只读校验 schema/表/列/约束/索引与基线一致；不执行 DDL | P0 |
| `None` | 不建表也不校验，完全交给调用方 | P0 |
| `Upgrade` | 在基线上应用待升级版本 | P1 计划 |

- 未设置选项即 `Create`，既有调用方行为不变。
- `WithMigrateMode` 只对 `NewDriver` 有意义；显式传给 `Migrate` 返回
  `ErrMigrateModeUnsupported`，避免 `Migrate(..., WithMigrateMode(None))`
  的语义歧义。
- P1 引入版本台账后：`Create` 只建基线；若台账显示库内版本低于二进制要求，
  `Create` fail-fast 并提示改用 `Upgrade`，不静默运行在旧 schema 上。库代码
  不得读取仅在 `Upgrade` 后存在的列，除非当前模式已确认版本。
- `Verify` 只报告、不修复。`missing` 可由基线补齐，`outdated` 必须人工对齐
  或走版本化迁移。

### 2. DDL 原子性档位与执行路径

DDL 原子性档位有：无 / 单语句 / 跨语句事务（后端能力边界框架见
[ADR 0005](0005-backend-capability-boundary.md)；DDL 原子性档位为本协议
新增维度）。单版本的执行路径另有两个取值：

- `fast`：后端支持跨语句事务且该版本全部步骤可事务化。锁、基线、版本 DDL
  与台账放在同一事务，全成或全败；失败回滚即恢复，不写 `applying`、不逐
  对象探测（基线接管校验除外）。P0 的基线执行即此路径；版本 DDL 与台账写入
  属 P1。
- `step`：其余情况。写 `applying`、逐 step 探测/执行、按状态机恢复；即使
  后端支持事务 DDL，含不可事务化步骤的版本也走此路径。P1 落到后端执行器；
  P0 由模拟 backend 覆盖协议与收敛。

约束：

- 事务档后端在全可事务化版本上必须走 `fast`，不得静默降级为探测/状态机
  路径。
- `step` 要求至少单语句原子。不满足该前提（DDL 原子性为「无」）的后端不
  支持自动迁移，只能 `Verify`/`None`。
- 两条路径共用同一锁键、同一状态记录语义（P1 台账）、同一份步骤定义与
  同一套验收。
- DDL 原子性档位与执行路径是两个维度，不混用。

### 3. 结构化步骤

迁移单元不是裸 SQL 字符串，而是结构化 step：对象类型（schema/table/index/
column/constraint）、对象标识、动作、语句、是否可事务化、探测谓词、是否
可验证。

- `fast` 路径把可事务化 step 批在同一事务执行，不逐对象探测；基线接管
  校验是唯一例外。
- `step` 路径由执行器对每步探测：已存在且定义一致则跳过，缺失则执行，
  定义不一致报差异错误（P1 后端执行器；P0 由模拟 backend 验证）。
- 每步必须可安全重入：崩溃后重跑整版依赖探测收敛，不依赖上一步的台账
  （P1）。
- 不可探测或不可验证的步骤不得进入 `step` 路径，除非提供独立校验语句
  （P1）。
- 没有 `IF NOT EXISTS` 的后端不依赖该语法；探测不可用时，按能力声明把
  「对象已存在/重复定义」类错误视为该步已应用，其余错误不吞。
- `Statements`/`SQL` 是 step 的派生视图：`fast` 可导出整段脚本并单事务
  执行；`step` 只导出步骤清单与探测方式，不提供可重放脚本（P1）。

### 4. 迁移锁

- 优先级：事务级锁（随提交/回滚释放）→ 原生会话锁（固定连接、断连释放）
  → 锁行租约（P1；条件写取得所有权，TTL、续约与过期回收）。
- 等待必须有界：try-lock + 退避，或设置锁等待超时并把超时映射为瞬时错误；
  不得无限阻塞。
- 锁键含命名空间（如 schema），不同 schema 或应用不互相阻塞。
- 每步执行前与每次台账写入前校验所有权（P1 step 路径）；失去所有权立即
  停止执行新步骤。
- 锁行租约的锁表按幂等步骤引导（探测 + 容忍已存在），引导完成后才取
  租约；并发引导由条件写保证单所有者（P1）。
- 锁能力为无（`LockNone`）时：`Create` 返回 `ErrMigrationUnsupported`，
  只能 `Verify`/`None`；`Upgrade` 同属 P1 且同样拒绝。DDL 原子性为无
  （`DDLAtomicNone`）时 `Create` 同样返回 `ErrMigrationUnsupported`（P0
  无此档后端，P1 执行器按声明返回）。

### 5. P1 台账状态机与恢复

P0 无台账。P1 台账按版本一行：版本号、状态（pending / applying / applied /
failed）、尝试次数、最后错误摘要、更新时间；`step` 路径另记录 owner/epoch
用于所有权校验。失败摘要按不可信输入处理：截断到固定上限、去控制字符、
不落堆栈。

引导顺序：取锁 → 确保台账存在（探测 + 容忍已存在）→ 读目标版本行，无行
则插入 pending → 进入对应路径。`fast` 把引导、基线、版本 DDL、台账读写
放在同一事务；`step` 的台账写入独立提交。

`fast`：`applied` 直接结束；任何语句失败整体回滚，版本与对象均不变，不写
`applying`、不产生待恢复状态、不逐对象探测。

`step` 状态机（锁下执行，每步前校验锁所有权）：

1. `applied`：结束。
2. `failed`：fail-fast，返回可处置错误；人工修复后经 `Admin` 重置回
   pending，不提供自动 down。
3. `pending`：写 `applying` 并提交，进入执行。
4. `applying`：逐 step 探测，已存在且一致则跳过，否则执行；冲突类错误按
   能力声明容忍；全部完成后整体校验对象集合，写 `applied` 并提交。

崩溃恢复矩阵（`step`）：

- 写 `applying` 前崩溃：无 DDL，重试从 pending 开始。
- 写 `applying` 后、首步前崩溃：重试探测并执行。
- 步骤之间崩溃：已执行步骤由探测跳过，未执行者补做。
- 末步后、写 `applied` 前崩溃：全量探测通过后直接写 `applied`。
- 写 `applied` 后崩溃：幂等结束。
- 异步 DDL job：提交后/运行中崩溃，重入先按 job 标识与对象状态查询，未
  完成则等待或安全重提；不得盲目重复提交。job 完成但台账未写，探测通过后
  补写；job 运行中锁租约过期，续约失败即停止，接管方先查 job 状态再决定。
- DDL 与台账不共享事务，「步骤已生效、台账未记」是预期状态，由探测收敛；
  step 路径下不得把 DDL 与台账写入包进同一事务。

永久失败（约束冲突、数据不满足非空等）写 `failed` + 摘要；后续实例
fail-fast，不进入重试风暴。`Admin` 的 reset/stamp 只能由管理面调用，且必须
在锁下与对象校验后执行。

### 6. 基线接管统一判定

已有事件表但无台账（P0）或台账无该版本记录（P1）时，统一按对象集合判定：

- 已有对象与期望定义一致、仅缺对象 → 补齐并收敛（P1 补记基线版本）。
- 任一已有对象定义冲突，或无法判定归属 → 返回差异错误，不自动修复，也不
  自动 stamp。

判定依据对象级快照（表/列类型、主键、唯一、外键、索引列）。不做「先
`Verify` 再 `Migrate`」的前置门：检查与创建无法组成原子操作，原子性由锁 +
事务（`fast`）或执行器探测 + 单语句原子（`step`）保证。

### 7. 中间态兼容（P1）

`step` 路径迁移期间 schema 对应用可见。要求停服迁移，或每一步保持向后
兼容（先加后删的 expand/contract）。P1 的 `Verify` 在启动时报告版本不匹配
与 `failed` 状态；运行期 `None` 模式由调用方承担一致性责任。

### 8. 不可事务化步骤（P1）

计划可声明某些步骤必须在事务外执行（如 `CREATE INDEX CONCURRENTLY`）。
含此类步骤的版本退出 `fast`，其余可事务化步骤仍尽量批在事务内，事务外
步骤按 `step` 路径执行并复用同一台账。事务档不得因为个别步骤把整版降级
为逐对象探测。

### 9. 前向兼容与版本过新（P1）

`Verify` 报告 `too_new`（库内版本高于二进制要求）与 `failed`（版本处于
failed 状态）。`Create` 只处理「库内版本落后」，并 fail-fast 提示
`Upgrade`；不自动降级，也不读取未确认版本的列。版本过新时调用方应升级
二进制或使用匹配版本的部署流程。P0 的 `Create` 不做版本判断。

### 10. Claim 接口变更（P1 计划）

观测指标需要区分接管与终态置 failed。计划把 `Claim` 返回值改为
`ClaimResult{Leases, Takeovers, TerminalFailed}`：`Takeovers` 统计本次认领中
的租约接管（含终态接管），`TerminalFailed` 单独统计认领时直接置 failed 的
数量；`Lease` 不再承担接管标记。该变更同步覆盖所有后端、`eventtest` 与
inmem；v0.x 允许破坏性变更。

### 11. 权限边界

- 自动迁移（P0 `Create`；P1 `Upgrade`）与显式 `Migrate` 同义：都以调用方
  连接执行 DDL，需要 DDL 权限。
- 运行期仅 DML 的连接不能自动迁移，只能 `Verify`（另需系统目录读）或
  `None`；表必须由部署期的高权限连接建好。
- 库不负责账号或授权管理，导出物不含账号与授权。

### 12. P0 实现范围与 P1 计划

P0 已实现：

- `MigrateMode`（Create/Verify/None）、`VerifyReport`/`VerifyIssue`/
  `VerifyProblem`/`VerifyError`、`MigrationCapabilities`、`MigrationPath`、
  `FaultPoint` 与迁移哨兵；`MigrationPath`/`FaultPoint` 由迁移契约测试的
  `ForcePath`/`InjectFault` 使用，P1 状态机复用。
- `postgresschema` 方言包：结构化基线步骤、期望对象、系统目录快照、差异
  判定、锁键与锁语句；不依赖驱动。
- 两个 Postgres 后端：`Migrate`（fast 单事务）、`Verify`、`Statements`、
  `SQL`、`Capabilities`、`WithMigrateMode`；`ensure` 按模式分派并做有界
  负缓存（瞬时错误退避、永久错误 fail-fast、`Verify` 成功缓存）。
- `eventtest.RunMigrationSuite` + `eventtest/simmigrate` 模拟 backend
  （可强制 step、可注入故障）；真实 Postgres 集成由 `TEST_POSTGRES_DSN`
  门控。P0 两个 Postgres 后端的基线全部可事务化，执行器只走 fast；step
  路径由模拟 backend 覆盖协议与崩溃收敛。
- inmem 显式声明迁移为 no-op。

P1 计划：

- `Upgrade` 模式与版本台账、`step` 路径在后端执行器落地（含异步 DDL 恢复）。
- `Admin` 的版本 reset/stamp 与 failed 事件重放。
- `ClaimResult` 观测接口变更。
- 版本过新/failed 的完整报告与处置流程。

## 理由

- 模式显式化让「谁在什么权限下建表」成为可判定的契约，而不是隐含在
  `NewDriver` 的副作用里；运行账号只授 DML 是常见部署约束，必须有官方
  路径。
- 能力声明用于契约测试与文档判定；P1 执行器按声明选择路径，避免把
  Postgres 的事务与目录探测假设强加给其他后端；
  也避免事务档被悄悄降级成低保障的探测路径。
- 基线接管采用统一判定而非自动修复，防止库把「同名但定义不同」的对象
  误认为自己的基线，造成不可逆的 schema 污染。
- 锁先行、有界等待与所有权校验，解决并发首启与长事务占锁两类现场问题。

## 备选

- **保留单一自动迁移、要求运行账号具备 DDL 权限**：与最小权限部署冲突，
  且无法区分部署漏建表与凭据问题，否决。
- **只提供显式 `Migrate`、取消 Driver 构造期迁移**：破坏既有调用方，且
  多实例首启仍需自行编排；保留 `Create` 缺省更平滑。
- **所有后端统一走逐对象探测**：牺牲事务档的原子性与性能，且无法保证
  探测与执行之间的竞态，否决。
- **用 `Verify` 作为 `Migrate` 的前置门**：检查与创建非原子，仍会出现
  并发竞态，否决。

## 后果

- 文档需同时给出能力矩阵与连接权限矩阵；能力矩阵的值以各后端
  `Capabilities()` 为准，权限矩阵覆盖 Create/Verify/None 与 P1 Upgrade。
- 新后端接入必须实现 `MigrationBackend` 并通过 `RunMigrationSuite`；
  step 路径的能力必须如实声明，不能默认支持。
- 运行期仅 DML 的部署必须显式选择 `Verify` 或 `None`；缺省 `Create` 在
  该场景下会 fail-fast，这是预期行为而非缺陷。
- P1 台账引入前，`Create` 不做版本判断；引入后 `Create` 对旧库 fail-fast
  并提示 `Upgrade`，不会静默改表。
