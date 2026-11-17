// Package postgresschema 是 Postgres 版本计划（幂等 DDL 语句）与对象描述
// 的单一事实源：构造版本计划、派生语句与手工脚本、描述期望对象集合，并
// 提供两个 Postgres 后端共享的迁移执行器；不 import 任何数据库驱动。
//
// 计划覆盖 event / event_claim 两表、主键/唯一/外键约束与两个索引；不含
// 账号与授权。迁移单元是幂等 DDL 单语句（expand-only：建表/加列/建索引），
// 一条语句恰好构造期望对象集合中的一个对象；约束不在建表语句中内联，
// 统一用探测后条件执行的 DO 块创建（ADD CONSTRAINT 无 IF NOT EXISTS）。
// 版本号只存在于计划中，库内不落版本记录——对象集合即事实。后续版本以
// 追加方式进入 Plan；升级随部署隐式发生（Create 幂等应用全计划）。
package postgresschema
