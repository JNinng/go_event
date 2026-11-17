# postgrec

go_event 的 Postgres 后端，基于标准库 `database/sql`（连接由使用方建立并
注册驱动，如 pgx/v5/stdlib）。能力与选项语义见
[docs/api.md](../docs/api.md)，语义事实源见 [docs/design.md](../docs/design.md)。

```go
db, _ := sql.Open("pgx", dsn)
_ = postgrec.Migrate(ctx, db)                      // 部署/启动期可选：与缺省 Create 同义，需 DDL 权限
drv := postgrec.NewDriver(db, postgrec.WithRetention(7*24*time.Hour))
// 运行账号仅 DML（+ 系统目录读）、表已由部署流程建好时：
// drv := postgrec.NewDriver(db, postgrec.WithMigrateMode(event.MigrateVerify))
```

缺省迁移模式 `Create` 在首次操作时持迁移锁幂等应用版本计划全量语句
（基线与各版本；升级随部署隐式发生）；`Verify` 只读校验；`None` 不建表
也不校验。无 DDL 权限的运行连接不能自动迁移，只能用后两者。

## 能力边界表（ADR 0005）

核心语义全部为「原生」实现；eventtest 全套契约测试通过。

| 语义 | 档位 | 实现 |
| --- | --- | --- |
| 幂等 Append（首写为准） | 原生 | `event` 表 PRIMARY KEY (id) + `ON CONFLICT DO NOTHING` |
| 元数据透传 | 原生 | `jsonb` 列原样存取 |
| 广播 Fetch（类型过滤 + 水位 + 升序 + 限额） | 原生 | `WHERE type = $1 AND position > $2 ORDER BY position LIMIT $3`（bigserial 全序位置） |
| 广播基线锚点（HeadPosition） | 原生 | `SELECT COALESCE(MAX(position), 0) WHERE type = $1`（(type, position) 索引即时求值，单条语句快照；无该类型事件时返回空串） |
| 认领互斥 / 接管 / SKIP LOCKED | 原生 | `FOR UPDATE SKIP LOCKED` + `event_claim` 唯一约束 + `ON CONFLICT DO UPDATE`（CAS 谓词） |
| 接管计入重试、达上限置 failed | 原生 | 认领事务内终态标记 + 认领递增计数 |
| 心跳 / 迟到 Complete / Fail 不生效（CAS） | 原生 | `lease_token` 条件更新，不命中静默返回 |
| 递增退避 | 原生 | Fail 事务内按 `Backoff.Next` 计算 `next_eligible_at` |
| 惰性清理 | 原生 | 超窗且无活跃租约（无论终态）行删除，`event_claim` 经 FK ON DELETE CASCADE 级联；墙钟节流，随 Append 机会性执行 |

## 迁移能力

迁移相关能力与 [ADR 0007](../docs/adr/0007-migration-mode-and-protocol.md) 的协议一致；核心语义档位见上表（[ADR 0005](../docs/adr/0005-backend-capability-boundary.md)）。

| 项 | 值 | 说明 |
| --- | --- | --- |
| 迁移模式 | `Create` / `Verify` / `None` | `Create` 缺省，首次操作持锁幂等应用版本计划全量语句（升级随部署隐式发生）；`Verify` 只读校验；`None` 不建不校验 |
| DDL 原子性 | 跨语句事务 | 版本计划在单事务内执行；失败整体回滚 |
| 执行路径 | 单一路径 | 锁 + 接管校验（对象集合与计划期望比对）+ 计划语句同事务；不逐对象探测、无台账 |
| 基线接管校验 | 系统目录快照 | 定义一致且仅缺对象则补齐；冲突返回差异错误、不自动修复 |
| `Verify` 报告 | `VerifyReport` / `*VerifyError` | 报告 `missing`（待升级）/ `outdated`（不自动修复）；`too_new` / `failed` 为预留 |
| 导出物 | `Statements` / `SQL` | 两者由同一版本计划派生；`Statements` 不含锁；`SQL` 以 `BEGIN` 开始，随后设 `lock_timeout` 并取迁移锁（锁语句位于全部 DDL 之前），随后输出计划内全部语句，末尾 `COMMIT`；整段可重放 |
| 锁原语 | 事务级 advisory 锁 | `pg_try_advisory_xact_lock(hashtext('go_event:<schema>'))`，有界等待；提交/回滚自动释放 |
| 能力声明 | `Capabilities()` | 跨语句事务；constraint 无原生 `IF NOT EXISTS`（探测 + DO 块）；schema 仅存在性，表/列/索引/约束为存在性 + 定义；错误分类全支持；同步完成；计划可包含事务外语句；不隐式提交 |
| 权限边界 | 按模式 | `Create`/`Migrate` 需 DDL；`Verify`/`None` 只需 DML（+ 系统目录读）；锁能力为 `LockNone` 时 `Create` 返回 `ErrMigrationUnsupported` |

导出 `SQL` 使用阻塞式 `pg_advisory_xact_lock` + `lock_timeout`；执行器使用
`pg_try_advisory_xact_lock` + 有界退避，两者不是同一形式。

完整能力矩阵与连接权限矩阵见 [design.md §8](../docs/design.md#8-后端能力边界)。

## 可选能力

| 能力 | 支持度 |
| --- | --- |
| 延迟事件（deliver_at） | P1 可选拓展，Postgres 原生可实现 |
| LISTEN/NOTIFY 推送唤醒 | 不支持（P1 由 pgxc 提供；本后端退化回轮询） |
| outbox（TransactionalAppender） | P1 可选拓展 |
