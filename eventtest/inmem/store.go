package inmem

import (
	"github.com/jninng/go_event"
)

// Store 后端组合对象（便利构造，非能力面）：一个对象同时持有运行面
// Driver、只读查询面 Inspector 与管理面 Admin，三面共享同一份内存状态
// 与互斥锁。
//
// 组合而非合并接口（design.md §6 三面分离）：Store 不实现任何能力接口，
// core 也不定义 event.Store 接口——NewPublisher / NewRunner 仍接收
// Store.Driver，各面语义与独立构造完全等价。Store 持有 Admin 是容器
// 聚合，不是把管理写路径挂到 Driver 类型上（ADR 0010）。
//
// Admin 为缺省构造（无 Observer、缺省锁等待）：需要观测注入或缩短锁
// 等待的测试可经 NewAdmin(st.Driver, ...) 构造等价 Admin 替换——与
// Store 的 Driver 共享同一份内存状态与互斥锁（进程内无 otel / slog，
// 观测仅 Observer 旁路）。
type Store struct {
	// Driver 运行面：与 New 等价（Store 内唯一实例）。
	Driver event.Driver
	// Inspector 只读查询面：同一 Driver 实例的只读视图（*Driver 同时
	// 实现 event.Inspector）。
	Inspector event.Inspector
	// Admin 管理面写操作（当前仅 failed 手工重放，ADR 0010）。
	Admin event.Admin
}

// NewStore 构造组合对象：内部只建一个 Driver 实例——Inspector 视图与
// Admin 复用同一实例（共享内存状态与互斥锁），供契约套件与本地测试
// 三面同源使用。选项与 New 一致。
func NewStore(opts ...Option) *Store {
	d := newDriver(opts...)
	return &Store{
		Driver:    d,
		Inspector: d,
		Admin:     &Admin{d: d, exclWait: adminLockWait},
	}
}
