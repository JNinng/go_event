// Package event 是后端无关的事件生产/消费库（core）：提供广播消费
// （发完即忘，上游无感）与认领消费（key + 有效期 + 心跳 + 完成标记）
// 两种消费方式，与有参（强类型）/无参事件正交；支持分布式多实例与
// 进程内就近消费。
//
// 能力面与语义的单一事实源是 docs/design.md 与 docs/adr/；签名见
// docs/api.md。必选接口面只有 Driver 一个（单次原子操作，最小缝合点），
// 全部编排逻辑在本包的 Runner 完成；查询面 Inspector（只读）与管理面
// Admin（写操作）为可选接口，按能力探测接入（ADR 0005）——Inspector 可
// 对后端 Driver 类型断言探测，Admin 不经任何运行面类型获得——经后端
// NewAdmin 构造或经组合对象 Store 聚合（同一内部构造路径）；事务性入队
// （outbox）同为可选面但本包不定义接口——事务载体是后端具体类型，各后
// 端提供 TransactionalAppender（构造或经 Store 聚合，ADR 0012）；
// Publisher 与 Runner 为具体类型，依赖经构造函数显式注入，无包级可变
// 状态。
package event
