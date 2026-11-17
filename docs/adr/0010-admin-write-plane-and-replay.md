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

## 后果

- core 新增 `event.Admin` 可选接口；pgxc / postgrec 提供 `NewAdmin`，
  inmem 提供 `NewAdmin`（与 Driver 共享内存状态）。后端 Driver 类型上
  不存在管理写路径。
- eventtest 新增 `RunAdminReplaySuite`：端到端重放（重放 → 认领 →
  完成）、计数归零与循环重放、done / active / retrying / 未知键的无效果
  与判别统一、`last_error` 保留与 `failed_at` 清空、并发重放恰好一次
  生效、被接管后迟到重放不污染新属主。
- 文档同步：design.md §5.3 重放语义与 §6 能力面表 Admin 行；重放窗口 =
  保留窗口写入 usage.md 运维注意事项。
- 观测埋点（操作来源记录）、与迁移的并发互斥（advisory lock）、错误可
  诊断性强化属 Admin 加固的后续项，不在本决策范围内。
