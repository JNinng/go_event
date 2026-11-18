# ADR 0009: 错误摘要落库口径与 Fail 接口扩展

- 状态：已接受
- 日期：2026-11-17

## 背景

认领消费的 `failed` 终态此前只有状态位与计数：排障需要回源应用日志，
多实例、多消费者身份下难以定位某次失败的直接原因；管理面（Inspector 查询
与 Admin 手工重放，见 design.md §9.2）也需要落库事实作为数据源。

`event_claim` 因此需要留存最后错误摘要与终态时刻；错误文本经 `FailRequest`
进入 Driver（公开 API 变更）。该文本是**不可信输入**——handler 错误可含
控制字符、超长内容与堆栈文本，直接落库会污染查询面、放大存储并留下注入面。
消费与重试语义见 [ADR 0006](0006-broadcast-fire-and-forget-and-claim-retry.md)，
迁移协议见 [ADR 0007](0007-migration-mode-and-protocol.md)。

## 决策

### 1. 摘要按不可信输入处理：清洗在 core，落库在后端

- `FailRequest.Error` 是不可信输入。任何后端写 `last_error` 前必须经 core
  的 `event.SanitizeErrorSummary` 清洗：
  - **去控制字符**：移除全部 Unicode 控制字符（Unicode Cc 类：C0/C1/DEL，
    含换行与制表符——非控制字符如 U+2028/U+2029 分隔符原样保留），消除
    控制字符的日志伪造与查询面破坏；非法字节以 UTF-8 替换符（U+FFFD）
    保留，保证列内容始终是合法 UTF-8。
  - **截断到固定上限**：4 KiB 字节（`event.MaxErrorSummaryBytes`），与
    `text` 列的存储口径一致；按码点边界截断，放不下的尾部码点整体丢弃，
    不产生撕裂的 UTF-8。
  - **不落堆栈**：库不捕获堆栈、不展开错误链结构，摘要只承载错误文本
    （`err.Error()`）；handler 侧堆栈由其自身观测（slog/otel）承担。
- 清洗函数定义在 core、由各后端共享（Postgres 后端与 inmem 同一口径），
  避免口径在各后端漂移。清洗收敛在**落库边界**而非 Runner：直连 Driver 的
  调用方（不经 Runner 的使用方式同样受支持）写入的文本同样被清洗。

### 2. 仅终态写入，接管终态只写时刻

- `last_error` 与 `failed_at` 仅在**本次 Fail 使行进入 `failed` 终态**时
  写入；重试中（`retrying`）不写。不变式：**`failed_at` 非空 ⟺ 行当前
  处于 `failed` 终态**。
- `last_error` 的语义是「最近一次终态失败的摘要」：`Error` 为空串视为未
  提供，落 NULL。Admin 手工重放（后续项）按统一口径**清空 `failed_at`、
  保留 `last_error`**：重放行与失败行经 `failed_at` 区分，摘要留作最近
  一次失败的排障历史，直到下一次终态失败覆盖。
- 接管终态（认领时达 RetryLimit 直接置 `failed`）**只写 `failed_at`**
  （取认领时钟），不写摘要——该路径没有本次处理错误可记。
- 迟到的 Fail（CAS 不命中，租约已被新属主持有）沿用 ADR 0006 的防护：
  不递增、不写摘要与时刻、不污染新属主行。

### 3. 迁移：版本计划追加加列版本（v2）

- `event_claim` 新增 `last_error text`（可空、无默认值）与
  `failed_at timestamptz`（可空），作为**版本 2** 以两条
  `ALTER TABLE ... ADD COLUMN IF NOT EXISTS` 进入 `postgresschema.Plan`；
  一条语句恰好对应期望对象集合中的一个独立列对象（`KindColumn`），同步
  系统目录快照比对。
- 加列 expand-only、幂等，对旧二进制与既有行无害；升级随部署隐式发生
  （ADR 0007）。缺失的版本加列可由迁移自动补齐——`Repairable` 放开
  `KindColumn` 缺失档；缺失的基线列仍属定义偏差，`Diff` 报告类别由
  `missing` 修正为 `outdated`（迁移不修改既有表，须人工处置），使
  `missing` 与 api.md 的文档语义（「计划可补齐」）恢复一致。

## 理由

- **清洗放 core 而非 Runner**：Driver 是唯一的存储缝合点，直连 Driver 的
  调用方不受 Runner 编排约束；「不可信输入在落库边界清洗」使不变式对一切
  调用路径成立。Runner 侧不做二次清洗（幂等且代价可忽略，但口径只此一处）。
- **移除而非替换控制字符**：口径单一、可测、无歧义；摘要面向 Inspector
  展示与人工排障，换行与制表（属 Cc）随控制字符一并移除即可，空格替换
  会引入「空白归一化」这一无必要的第二口径。
- **字节上限 4 KiB**：与 Postgres `text` 列存储口径一致，截断边界确定，
  不受多字节字符影响；错误摘要的价值在头部（错误类型与关键上下文），
  截断不损失主要信息。
- **`failed_at` 由接管终态同样写入**：`failed_at` 非空 ⟺ `failed` 的
  不变式使 Inspector 的 failed 列表可以单条件过滤并按时刻排序；接管终态
  进入终态的时刻同样是事实。
- **重放保留 `last_error`**：重放意味着该失败已被人工看过并决定重试；
  摘要是这次判断的依据，清掉会丢失现场。清 `failed_at` 即可区分两态，
  不引入额外状态列。
- **加列版本而非修改基线**：版本计划以追加方式演进（ADR 0007），基线
  语句不可修改；独立列对象与 `ADD COLUMN IF NOT EXISTS` 语句一一配对，
  维持「一条语句恰好构造一个期望对象」的配对不变量。

## 被否决的备选

- **在 core Runner 清洗后下推、后端直存**：直连 Driver 的调用方绕过
  Runner，不变式对一切调用路径不成立，否决；清洗收敛在落库边界。
- **摘要按 rune/字符数截断（如 512 字符）**：字符上限与列存储口径脱节，
  且对多字节文本的存储放大不可控，否决。
- **控制字符替换为空格**：引入空白归一化第二口径，测试与文档成本高于
  收益，否决。
- **接管终态不写 `failed_at`**：破坏「`failed` ⟹ `failed_at` 非空」的
  不变式，Inspector 需要按时刻排序时出现空值行，否决。
- **重放同时清空 `last_error`**：丢失最近一次失败的现场（重放决策的
  依据），且「重放行」与「从未失败行」无法区分，否决。
- **v2 单条语句加两列**：违反 ADR 0007「一条语句恰好构造期望对象集合中
  的一个对象」的配对不变量，否决。

## 后果

- `FailRequest` 增加 `Error` 字段、core 导出 `SanitizeErrorSummary` 与
  `MaxErrorSummaryBytes`（公开 API 变更，v0.x 允许）；core 的 Runner 把
  handler 错误文本下推 `Fail`。
- 两个 Postgres 后端与 inmem 的 Fail/Claim 终态写路径各多两列写入；
  `postgresschema` 计划升至 v2（11 条语句、11 个期望对象），
  `Diff` 的基线缺列类别变更为 `outdated`。
- eventtest 契约套件新增落库事实用例，经 `ClaimStateReader`（inmem 原生
  实现，Postgres 后端在测试装配处以 SQL 适配）断言终态/非终态/迟到/
  接管终态/截断/控制字符口径。
- Inspector 的 failed 查询与 Admin 重放（后续项）以 `failed_at` /
  `last_error` 为数据源；重放按第 2 节口径清 `failed_at`、保留
  `last_error`。
- 错误摘要不替代应用日志：无堆栈、无结构化字段，深度排障仍以使用方的
  观测面为准。
