# Changelog

格式基于 [Keep a Changelog](https://keepachangelog.com/zh-CN/1.1.0/)，版本遵循[语义化版本](https://semver.org/lang/zh-CN/)。

## [Unreleased]

### 新特性

- **失败错误摘要落库**：认领消费进入 `failed` 终态时，`event_claim` 行留存错误摘要（`last_error`）与终态时刻（`failed_at`），直接查库即可定位失败原因，无需回源应用日志。
  - Postgres 后端由版本计划 v2 自动加列，升级随部署隐式发生；不变式：`failed_at` 非空 ⟺ 行当前处于 `failed` 终态（接管终态只写 `failed_at`）。
- **`FailRequest` 新增 `Error` 字段**：记录失败时可携带错误文本，core 的 `Runner` 自动下推 handler 错误；文本按不可信输入清洗为摘要（去除 Unicode 控制字符、截断 4 KiB、不含堆栈，口径见 `event.SanitizeErrorSummary`），仅终态写入。

### 破坏性变更与兼容期

- `postgresschema.Diff` 对缺失基线列的报告类别由 `missing` 改为 `outdated`（迁移不修改既有表，须人工处置）；版本加列缺失仍为 `missing`，可由迁移自动补齐。计划升级至 v2（`event_claim` 加 `last_error` / `failed_at`）。
- `eventtest.RunSuite` 要求 Driver 实现 `eventtest.ClaimStateReader`（套件据它断言错误摘要落库事实）；`eventtest/inmem` 已原生实现。

### 文档

- 新增 [ADR 0009](docs/adr/0009-error-summary-persistence.md)：错误摘要落库口径与 `Fail` 接口扩展的决策记录。

[Unreleased]: https://github.com/JNinng/go_event/compare/v0.1.1...HEAD
