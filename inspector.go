package event

import (
	"context"
	"time"
)

// Inspector 只读查询面（可选接口，不加入 Driver）：面向排障与观测的三类
// 查询——（事件 × 消费者身份）状态、按身份的状态分档计数、failed 事件
// 分页列表；与运行面 Driver、管理面 Admin 三面分离（design.md §6）。
//
// 能力探测（ADR 0005）：对后端 Driver 做 event.Inspector 类型断言探测
// （提供该能力的后端其 Driver 同时实现本接口），或经后端 NewInspector
// 显式构造——与各自 Driver 共享连接与命名空间选项。
//
// 硬边界：只读，不产生任何簿记、不做租约过期推断——查询返回落库事实；
// 结果仅在保留窗口内可见（design.md §5.2）。
type Inspector interface {
	// ClaimState 返回（事件 × 消费者身份）认领行的当前状态与重试计数。
	// ok 为 false 表示该身份对该事件无认领行——事件不存在、已超保留
	// 窗口被清理、或该身份尚未认领，三者即「待处理」态（该身份无认领
	// 行的行按 superseded 属性细分进 CountStates 的 Pending / Superseded
	// 两档）；查询故障经 error 返回，不得折叠为 ok=false。
	ClaimState(ctx context.Context, eventID, consumerType string) (ClaimState, bool, error)

	// CountStates 按消费者身份对某事件类型做状态分档计数：待处理 /
	// superseded / 重试中 / processing / failed / done 六档互斥且完备，
	// 合计等于该类型的事件总数，分档口径与 Claim/Fail/Complete/Append
	// 的落库事实严格对齐（docs/api.md §2.10）——Pending 即「该身份无认领
	// 行且未被取代」；Superseded 是事件行属性档（ADR 0014）；processing
	// 按落库 status='active' 分档，租约是否已过期不改变本档。
	CountStates(ctx context.Context, typ, consumerType string) (StateCounts, error)

	// FailedEvents 返回 failed 终态事件的分页列表（含错误摘要与终态
	// 时刻，ADR 0009），排序与分页边界见 FailedQuery。
	FailedEvents(ctx context.Context, q FailedQuery) ([]FailedEvent, error)
}

// ClaimStatus 认领行的查询面状态。processing 对应落库列值 active
// （持有租约）；其余取值与落库列值一致。
type ClaimStatus string

const (
	ClaimRetrying   ClaimStatus = "retrying"   // 已失败退避中，待重认领
	ClaimProcessing ClaimStatus = "processing" // 租约内处理中（落库 status='active'）
	ClaimDone       ClaimStatus = "done"       // 完成终态（幂等、不可逆）
	ClaimFailed     ClaimStatus = "failed"     // 失败终态（可经管理面 Admin 重放）
)

// ClaimState （事件 × 消费者身份）的当前查询面状态。
type ClaimState struct {
	Status ClaimStatus
	// Attempts 是该（事件 × 身份）已计入的重试计数（claim_count 落库
	// 事实，ADR 0006）：显式 Fail 计一次、接管认领计一次。与租约/
	// 信封的 Attempts（含本次认领，= 本值 + 1）相差一次当前投递：
	// processing / done 行的实际尝试次数为本值 + 1，retrying / failed
	// 行为本值。
	Attempts int
}

// StateCounts 某事件类型在某消费者身份下的状态分档计数（口径见
// Inspector.CountStates）。
type StateCounts struct {
	Pending    int // 待处理：该身份无认领行且未被取代
	Superseded int // superseded：该身份无认领行且已被合并取代（ADR 0014）
	Retrying   int // 重试中：Fail 未达终态
	Processing int // processing：落库持有租约（status='active'）
	Failed     int // failed 终态
	Done       int // done 终态（归档档）
}

// FailedQuery failed 事件分页列表条件。Type 与 ConsumerType 为空串表示
// 不限（跨类型 / 跨身份列出全部 failed 行）。
//
// 分页语义（偏移分页，边界写死）：Offset < 0 视为 0；Limit <= 0 返回空
// 列表（nil）且不发起查询；末页剩余不足 Limit 条时返回剩余条（可少于
// Limit）；越界（Offset 不小于结果总数）返回空列表（非错误）。排序固定
// 为 failed_at 降序（最新在前），同一时刻按事件 ID 升序保证确定性。
// 稳定性边界：以每次查询时刻的数据为准，无快照——跨页间隙发生的终态
// 变化（新增 failed、超窗清理、管理面重放）可能造成跨页重复或遗漏。
type FailedQuery struct {
	Type         string
	ConsumerType string
	Offset       int
	Limit        int
}

// FailedEvent failed 列表行：认领键（事件 × 消费者身份）+ 事件类型 +
// 失败摘要事实。
type FailedEvent struct {
	EventID      string
	Type         string
	ConsumerType string
	// LastError 是最近一次终态失败的错误摘要（清洗后文本）。空串表示
	// 未提供摘要——接管终态（只写 failed_at）与 Error 为空串的终态
	// Fail 均落 NULL（ADR 0009）。
	LastError string
	// FailedAt 是进入 failed 终态的时刻。不变式「failed_at 非空 ⟺ 行
	// 处于 failed 终态」仅对 v2 加列后的写路径成立（ADR 0009）：v2 是
	// ADD COLUMN、无回填，升级前已存在的 failed 行该列为 NULL，列表中
	// 映射为零值 time.Time（升级边界）。
	FailedAt time.Time
}
