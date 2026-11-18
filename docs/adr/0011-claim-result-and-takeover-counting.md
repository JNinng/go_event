# ADR 0011: Claim 返回认领轮事实（接管与终态计数）

- 状态：已接受
- 日期：2026-11-18
- 关联：[ADR 0006](0006-broadcast-fire-and-forget-and-claim-retry.md)（接管计入
  重试）、[ADR 0008](0008-observability.md)（观测口径分界）

## 背景

认领消费的接管（前任失去租约）与「达重试上限直接置 failed」是排障与
SLA 观测的关键事实。落地本项之前没有接管专属的观测面：`Attempts` 混合
显式 Fail 重试与过期租约接管两种来源，`Attempts > 1` 不能当作接管计数；
接管仅在停机放弃路径有间接前兆（`<scope>.lease.abandoned`）。设计口径
（design.md §9.2）早已定为 `Claim` 返回
`ClaimResult{Leases, Takeovers, TerminalFailed}`，本 ADR 记录形态与计数
口径的定稿。

## 决策

### 1. 接口形态（v0.x 破坏性变更）

`Driver.Claim` 返回 `ClaimResult`：

```go
type ClaimResult struct {
    Leases         []Lease // 本次发放的租约（至多 q.Limit 条）
    Takeovers      int     // 接管发放数量（含终态接管）
    TerminalFailed int     // 认领动作直接置 failed 的数量
}
```

- **计数必须是后端落库事实**，由各后端在 Claim 内汇总（SQL 后端取终态
  UPDATE 影响行数与认领 UPDATE 的接管标记，inmem 取内存计数）；core 不
  从 `Leases` 推断接管——core 看不到「本轮哪些行曾被接管」，推算必然与
  落库状态漂移。
- **`Lease` 不承载接管标记**：接管是「认领轮」的事实而非「单条租约」的
  属性，按轮汇总即可满足观测诉求，避免为标记扩张租约结构或后端透传面。
- 错误时返回零值 `ClaimResult`；`len(Leases) == 0` 表示本轮无发放
  （nil 性不作承诺——后端空轮次可能返回 nil 或非 nil 空切片，调用方以
  len 判断）。

### 2. 计数口径

- **Takeovers** = 本轮「接管过期租约」而发放的数量，**含**接管后直接置
  failed 的终态接管。无主首次认领、Fail 退避期满的重认领（retrying——
  Fail 时已计入一次失败，重认领不接管任何租约）均不算接管。
- **TerminalFailed** = 本轮认领动作因 `claim_count` 达 `RetryLimit` 直接
  置 failed 的数量，独立统计。一次终态接管（既接管又置 failed）同时计入
  两者。
- **现状边界（写进契约并钉住）**：认领侧置终态的下推条件要求租约已过期
  （`status='active' AND lease_until <= now`），故认领侧终态必经接管路径，
  恒有 `TerminalFailed ≤ Takeovers`。retrying 行不因 `RetryLimit` 下调而
  在认领时置 failed（重认领发放，`Attempts` 可能超出下调后的上限——订阅
  配置差异边界）。口径由 eventtest 用例钉住
  （`RetryingOverLimitRow_ClaimResult_NoTerminalOnClaim`）。
- **与落库事实对账**：计数可与 `Inspector.ClaimState` / `claim_count`
  互验（接管发放行 Attempts = 接管前计数 + 1；终态接管行
  `failed_at` 非空且无摘要）。

### 3. 观测面落点（沿 ADR 0008 口径）

core 在 Runner 计量（后端不重复埋同口径指标）：

- 指标 `<scope>.claim.takeover`（接管发放数，含终态接管）与
  `<scope>.claim.terminal_failed`（认领侧直接置 failed 数），属性
  `event` + `identity`，与 `<scope>.claim.granted` 同风格；只在计数非零
  时打点（无空轮次零值数据点）。
- 类型化 Observer 非破坏性演进：`ObserverV4`（嵌入 `ObserverV3`）新增
  `OnClaim(ClaimEvent)`，携带 `Event` / `Identity` / `Leases` /
  `Takeovers` / `TerminalFailed`，仅成功轮次分发（失败轮次经
  `<scope>.claim` 指标的 `result=error` 与日志可见）。
- 选择独立指标而非给 `<scope>.claim` 加维度：轮次计数的既有属性只有
  `result` 枚举，把「数量」塞进属性值违反「属性只允许枚举与标识符」的
  口径（ADR 0008 §3）。

## 理由

- 放弃「从 `Attempts` 推断接管」：Fail 重试与过期接管都递增计数且无法
  区分（重认领 count=1 与接管发放 count=1 相同），推算无事实依据。
- 放弃「接管标记加在 `Lease`/信封上」：接管判定发生在后端认领事务内，
  逐租约携带标记会把观测诉求变成每条租约的透传义务；轮级汇总一次返回
  即可，且与 core 的 `claim.granted`（逐租约）形成互补——租约侧只关心
  发放了什么，事实侧由 ClaimResult 承载。
- SQL 后端以「拾取窗口 + 双数据修改 CTE」实现：`picked` CTE 按 position
  升序拾取至多 Limit 条并锁定事件行（单窗口保持既有拾取顺序语义），
  `takeover` CTE 对拾取快照中状态为 `active` 的过期租约做接管 UPDATE
  （`RETURNING` 带接管标记），`reclaim` CTE 处理无主 INSERT 与 retrying
  重认领——接管标记来自拾取快照的旧行状态，不依赖任何「哨兵值」约定，
  认领原子性与 CAS 谓词不变。一个已知边界：快照路由为 `active`（只走
  takeover）的行若在 UPDATE 执行时被旧属主的迟到 Fail 翻成 `retrying`
  且退避未期满，guard 重查不命中则本轮不发放、下轮再认领——与旧单语句
  实现非逐点等价（旧实现在零退避配置下该行当轮按 retrying 发放），语义
  更安全（不发放刚被显式记为失败的行），at-least-once 不破坏，勿作
  等价性回归排查。
- 终态 UPDATE（先于认领执行、不占 Limit 槽位）的影响行数即
  `TerminalFailed`：置终态不发放，行随后离开可认领窗口。

## 备选

- **`Lease` 增加接管标记字段**：透传面扩张、core 与后端都要维护，且
  终态接管根本没有租约可标记（置 failed 行不发放），否决。
- **`<scope>.claim` 计数加 takeover 维度**：维度值须为枚举/标识符，计数
  不是；拆为独立指标口径干净，否决。
- **单条 `INSERT ... ON CONFLICT` 内以哨兵值区分接管**（如经 `EXCLUDED`
  在 `RETURNING` 携带标记）：依赖「接管行必与既有认领行冲突」的隐含
  不变量与魔术值约定，可读性与可维护性差，否决。
- **拆成两条语句（接管 UPDATE + 无主/retrying INSERT 各自限流）**：
  LIMIT 窗口分裂后跨类别的 position 拾取顺序无法保持，否决。

## 后果

- 破坏性公开 API 变更：`Driver.Claim` 签名、全部后端、inmem、eventtest
  契约套件与测试替身同步（v0.x 允许）；CHANGELOG 记录。
- 接管观测缺口闭合：发放与终态两条路径均有专属指标与 Observer 事件，
  ADR 0008 的「已知口径限制」解除（见其补充节）。
- 新后端实现 Claim 时必须按本口径汇总三个计数并过 eventtest 计数用例。
