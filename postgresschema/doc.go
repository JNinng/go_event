// Package postgresschema 是 Postgres 基线 DDL 与对象描述的单一事实源：
// 只构造 schema/表/索引/约束的结构化步骤、派生语句与期望对象，不 import
// 任何数据库驱动。
//
// 基线覆盖 event / event_claim 两表、主键/唯一/外键约束与两个索引；
// 不含账号与授权。基线只负责建表与补齐缺失对象，不升级既有表：列或索引
// 变更必须走版本化迁移，不能依赖基线的 IF NOT EXISTS 语义。
package postgresschema
