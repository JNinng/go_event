package event

import (
	"context"
	"time"
)

// Inspector 只读查询面（可选接口，不加入 Driver）：面向排障与观测的
// 三类查询——（事件 × 消费者身份）状态、按身份的状态分档计数、failed
// 事件分页列表。设计口径见 design.md §6/§9.2：运行面 Driver、查询面
// Inspector 与管理面 Admin 是三个分离的面，只读与写操作分离；Inspector
// 不定义任何写路径。
//
// 能力探测（ADR 0005）：Inspector 不进入 Driver 契约，调用方对后端
// Driver 做 event.Inspector 类型断言探测（提供该能力的后端其 Driver
// 同时实现本接口），或经后端的 NewInspector 显式构造——与各自 Driver
// 共享连接与命名空间选项。
//
// 边界（硬边界，写进契约）：
//   - 只读：不产生任何簿记，不参与认领/完成/心跳路径，不做租约过期
//     推断——查询返回落库事实（认领行有无与 status 列）。
//   - 保留窗口：结果仅在保留窗口内可见（design.md §5.2）。超窗且无
//     活跃租约的行被惰性清理删除后，查询结果自然不再可见，Inspector
//     不做任何额外过滤。
type Inspector interface {
	// ClaimState 返回（事件 × 消费者身份）认领行的当前状态与重试计数。
	// ok 为 false 表示该身份对该事件无认领行——事件不存在、已超保留
	// 窗口被清理、或该身份尚未认领，三者即「待处理」态（与
	// CountStates 的 Pending 档同口径）；查询故障经 error 返回，不得
	// 折叠为 ok=false。
	ClaimState(ctx context.Context, eventID, consumerType string) (ClaimState, bool, error)

	// CountStates 按消费者身份对某事件类型做状态分档计数：待处理 /
	// 重试中 / processing / failed / done 五档互斥且完备，合计等于该
	// 类型的事件总数（每（事件 × 身份）恰处一档）。分档口径与
	// Claim/Fail/Complete 的落库事实严格对齐：
	//   - Pending：该类型事件中该身份无认领行的数量——「待处理」的
	//     口径即此，而非「退避中」；
	//   - Retrying：认领行 status='retrying'（Fail 未达终态，退避中）；
	//   - Processing：认领行 status='active'（该身份落库持有租约；
	//     租约是否已过期不改变本档——接管由认领路径发生）；
	//   - Failed / Done：认领行 status='failed' / 'done' 终态；done 为
	//     归档档，不计入其余四档。
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
	ClaimFailed     ClaimStatus = "failed"     // 失败终态（可经管理面重放，后续 Admin）
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
	Pending    int // 待处理：该身份无认领行
	Retrying   int // 重试中：Fail 未达终态
	Processing int // processing：落库持有租约（status='active'）
	Failed     int // failed 终态
	Done       int // done 终态（归档档）
}

// FailedQuery failed 事件分页列表条件。Type 与 ConsumerType 为空串表示
// 不限（跨类型 / 跨身份列出全部 failed 行）。
//
// 分页语义（偏移分页，边界写死）：
//   - Offset < 0 视为 0；Limit <= 0 返回空列表（nil）且不发起查询；
//   - 首页：Offset = 0 返回排序后的前 Limit 条；末页：剩余不足 Limit
//     条时返回剩余条（可少于 Limit）；越界：Offset 不小于结果总数时
//     返回空列表（非错误）；
//   - 排序固定为 failed_at 降序（最新在前），同一时刻按事件 ID 升序
//     保证确定性；
//   - 稳定性边界：偏移分页以每次查询时刻的数据为准，无快照——跨页
//     间隙发生的终态变化（新增 failed、超窗清理、管理面重放）可能
//     造成跨页重复或遗漏。
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
