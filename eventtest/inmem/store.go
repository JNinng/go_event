package inmem

import (
	"github.com/jninng/go_event"
)

// Store 后端组合对象（便利构造，非能力面）：一个对象同时持有运行面
// Driver、只读查询面 Inspector、管理面 Admin 与事务性入队
// TransactionalAppender，各面共享同一份内存状态与互斥锁。
//
// 组合而非合并接口（design.md §6 三面分离）：Store 不实现任何能力接口，
// core 也不定义 event.Store 接口，各面语义与独立构造完全等价（签名见
// docs/api.md §4 Store 块）；Admin 与 TransactionalAppender 为容器聚合，
// 不经运行面类型获得（ADR 0010 / 0012）。Admin 为缺省构造（无 Observer、
// 缺省锁等待），需观测注入或缩短锁等待可经 NewAdmin(st.Driver, ...)
// 构造等价 Admin 替换（共享同一份内存状态与互斥锁，进程内观测仅 Observer 旁路）。
type Store struct {
	// Driver 运行面：与 New 等价（Store 内唯一实例）。
	Driver event.Driver
	// Inspector 只读查询面：同一 Driver 实例的只读视图（*Driver 同时
	// 实现 event.Inspector）。
	Inspector event.Inspector
	// Admin 管理面写操作（当前仅 failed 手工重放，ADR 0010）。
	Admin event.Admin
	// TransactionalAppender 事务性入队（outbox 可选面，ADR 0012）：与
	// Driver 共享同一份内存状态；BeginTx / AppendTx 的调用与提交归调用方。
	TransactionalAppender *TransactionalAppender
}

// NewStore 构造组合对象：内部只建一个 Driver 实例——Inspector 视图、
// Admin 与 TransactionalAppender 复用同一实例（共享内存状态与互斥锁），
// 供契约套件与本地测试各面同源使用。选项与 New 一致。
func NewStore(opts ...Option) *Store {
	d := newDriver(opts...)
	return &Store{
		Driver:                d,
		Inspector:             d,
		Admin:                 &Admin{d: d, exclWait: adminLockWait},
		TransactionalAppender: &TransactionalAppender{d: d},
	}
}
