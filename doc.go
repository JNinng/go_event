// Package event 是后端无关的事件生产/消费库（core）：提供广播消费
// （发完即忘，上游无感）与认领消费（key + 有效期 + 心跳 + 完成标记）
// 两种消费方式，与有参（强类型）/无参事件正交；支持分布式多实例与
// 进程内就近消费。
//
// 能力面与语义的单一事实源是 docs/design.md 与 docs/adr/；签名见
// docs/api.md。接口面只有 Driver 一个（单次原子操作，最小缝合点），
// 全部编排逻辑在本包的 Runner 完成；Publisher 与 Runner 为具体类型，
// 依赖经构造函数显式注入，无包级可变状态。
package event
