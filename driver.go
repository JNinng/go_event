package event

import (
	"context"
	"time"
)

// Driver 单次原子操作（design.md §4、ADR 0004）：全部编排逻辑在 core 的
// Runner 完成。所有方法语义由 eventtest 契约测试约束。
//
// 错误约定（docs/api.md §2.4）：除迁移相关错误（ErrMigrationLockTimeout、
// ErrMigrationPermissionDenied、ErrMigrationUnsupported、ErrMigrateModeUnsupported、
// ErrInvalidSchema 等）外，Driver 不返回哨兵错误；其余错误一律视为该次操作
// 失败，core 记日志重试或跳过。迁移错误在操作入口返回，调用方应按错误性质
// 修正配置、退避重试或改用 Verify/None。Complete/Fail/Heartbeat 的 CAS
// 不命中（租约过期、已被接管）返回 nil 而非错误——「不生效」是接管场景的
// 正常结果。
type Driver interface {
	// Append 幂等：重复 Append 同 ID 视为成功（首写为准，不覆盖既有行、
	// 不触发重投，不校验 payload 一致性）。
	Append(ctx context.Context, e RawEnvelope) error

	// Fetch 广播消费读取：Type 匹配且 Position 严格大于 q.After 的行，
	// 按 Position 升序返回至多 q.Limit 条。只读，不产生任何簿记。
	// q.After 为空串表示从头；非空但无法解析为 Position 时返回错误。
	Fetch(ctx context.Context, q FetchQuery) ([]RawEnvelope, error)

	// HeadPosition 返回该类型当前已提交事件的最大 Position，作为广播
	// 消费的基线锚点：core 在 Runner.Start 时调用一次，水位锚定到该
	// 快照，Start 返回即基线确立（ADR 0006）。该类型无事件时返回空串。
	HeadPosition(ctx context.Context, typ string) (string, error)

	// Claim 认领消费：原子认领至多 q.Limit 条「无主、租约过期、或退避
	// 期满」的行（Postgres 实现为 FOR UPDATE SKIP LOCKED）。已 done /
	// failed 的行不可认领；接管（租约过期）认领递增重试计数（ADR 0006），
	// 达 q.RetryLimit 的行置 failed 终态、不再认领（详见 ClaimQuery）。
	// 返回 ClaimResult 汇总本轮落库事实：发放的租约与接管/终态计数——
	// 计数必须与落库状态一致，供观测面计量（design.md §9.2）。错误时返回
	// 零值 ClaimResult。
	Claim(ctx context.Context, q ClaimQuery) (ClaimResult, error)

	// Heartbeat 续租：仅当属主凭证匹配时生效；租约过期或已被接管后
	// 迟到的心跳不生效（CAS 不命中静默返回 nil）。
	Heartbeat(ctx context.Context, h HeartbeatRequest) error

	// Complete 置 done：幂等、不可逆；仅当属主凭证匹配时生效（CAS），
	// 迟到的 Complete 不生效、不覆盖新属主状态（ADR 0006）。
	Complete(ctx context.Context, c CompleteRequest) error

	// Fail 记失败：递增重试计数，按 f.Backoff 递增退避置为再次可认领；
	// 计数达 f.RetryLimit 时置 failed 终态——递增计数、退避调度与终态
	// 判定在单次 Fail 内原子完成（ADR 0006）；仅当属主凭证匹配时生效
	// （CAS），迟到的 Fail 不递增、不污染新属主状态。本次 Fail 使行进入
	// failed 终态时，后端把清洗后的 f.Error 摘要（SanitizeErrorSummary）
	// 与时刻写入 last_error / failed_at；重试中（未到终态）与迟到的 Fail
	// 均不写摘要与时间。认领侧的接管终态（达 RetryLimit 直接置 failed）
	// 只写 failed_at，不写摘要（无本次处理错误）。
	Fail(ctx context.Context, f FailRequest) error
}

// FetchQuery 广播消费读取条件。
type FetchQuery struct {
	Type  string // 单类型单次取数：编排按类型轮询限额，防单类型洪泛（ADR 0004）
	After string // 已读水位（Position），首次为空串；core 内存维护，只进不退
	Limit int
}

// ClaimQuery 认领条件。
//
// RetryLimit：重试上限，达此值的接管认领不再发放租约、直接置 failed
// （ADR 0006「claim_count 达重试上限后置 failed 终态，不再被认领」——
// 终态判定需要认领侧获知上限，故由 core 随条件下推；与 FailRequest.RetryLimit
// 同源，均来自订阅级 WithRetryLimit）。
type ClaimQuery struct {
	Type         string
	ConsumerType string        // 认领键（design.md §5.1）
	Lease        time.Duration // 本次认领的租约有效期
	Limit        int
	RetryLimit   int
	Now          time.Time // 租约过期与退避判据的时钟，由 core 下推
}

// ClaimResult 一次认领轮的结果与落库事实汇总（design.md §9.2）。三类
// 计数语义：
//   - Leases：本次成功发放的租约；
//   - Takeovers：本次认领中「接管过期租约」而发放的数量，含接管后直接
//     置 failed 的终态接管；无主首次认领与 Fail 退避期满的重认领
//     （retrying，Fail 时已计入）不算接管；
//   - TerminalFailed：本次认领中因 claim_count 达 RetryLimit 被认领动作
//     直接置 failed 的数量，独立于 Takeovers 统计；一次接管既发放又达
//     上限置 failed 时同时计入两者。
//
// 当前认领语义下，置 failed 的下推条件要求租约已过期（下推条件见
// ClaimQuery.RetryLimit），故认领侧终态必经接管路径，恒有
// TerminalFailed ≤ Takeovers。计数与落库事实严格一致（接管计数覆盖发放
// 与终态两条路径），可经 Inspector.ClaimState / claim_count 对账。
// len(Leases) == 0 表示本轮无发放（nil 性不作承诺——后端空轮次可能返回
// nil 或非 nil 空切片，调用方以 len 判断）。
type ClaimResult struct {
	Leases         []Lease // 本次发放的租约（至多 q.Limit 条；批次内顺序不承诺）
	Takeovers      int     // 接管发放数量（含终态接管）
	TerminalFailed int     // 认领动作直接置 failed 的数量
}

// Lease 认领结果：一次租约的属主凭证。租约不承载接管标记——接管与
// 终态计数随 ClaimResult 按轮汇总返回（design.md §9.2）；Attempts 表达
// 「一次处理尝试未完成」的累计（显式 Fail 与接管各计一次，ADR 0006），
// 不能当作接管计数（两者混合，见 usage.md §6.7）。
type Lease struct {
	Envelope     RawEnvelope
	ConsumerType string
	LeaseToken   string // 不透明凭证；Heartbeat/Complete/Fail 必须携带（CAS 依据）
	LeaseUntil   time.Time
	Attempts     int // 当前重试计数（含本次认领）：新认领为「已失败次数+1」，
	// 接管认领在先前基础上递增（ADR 0006）
}

// HeartbeatRequest 心跳续租请求。
type HeartbeatRequest struct {
	EventID      string
	ConsumerType string
	LeaseToken   string
	Extend       time.Duration // 续租时长
	Now          time.Time
}

// CompleteRequest 完成请求。
type CompleteRequest struct {
	EventID      string
	ConsumerType string
	LeaseToken   string
	Now          time.Time
}

// FailRequest 失败请求。
type FailRequest struct {
	EventID      string
	ConsumerType string
	LeaseToken   string
	// Error 是本次失败的错误文本，按不可信输入处理：后端以
	// SanitizeErrorSummary 清洗（截断至 MaxErrorSummaryBytes、去控制
	// 字符、不落堆栈）后仅在终态写入 last_error；空串视为未提供摘要
	// （last_error 置 NULL）。core 的 Runner 下推 handler 的 err.Error()。
	Error      string
	RetryLimit int     // 重试计数达此值置 failed（订阅级配置下推）
	Backoff    Backoff // 递增退避（订阅级配置下推）
	Now        time.Time
}

// Backoff 递增退避：依据已失败次数计算下次可认领前延时（ADR 0006，
// 防热循环）。attempt 从 1 开始（首次失败 = 1）。
type Backoff interface {
	Next(attempt int) time.Duration
}

// LinearBackoff 线性退避：base * attempt，封顶 max。
func LinearBackoff(base, max time.Duration) Backoff {
	return &linearBackoff{base: base, max: max}
}

type linearBackoff struct {
	base time.Duration
	max  time.Duration
}

func (b *linearBackoff) Next(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1 // attempt 约定从 1 开始；≤0 视同首次失败
	}
	d := time.Duration(attempt) * b.base
	if d > b.max || d <= 0 {
		return b.max
	}
	return d
}

// ExponentialBackoff 指数退避：base * 2^(attempt-1)，封顶 max。
func ExponentialBackoff(base, max time.Duration) Backoff {
	return &exponentialBackoff{base: base, max: max}
}

type exponentialBackoff struct {
	base time.Duration
	max  time.Duration
}

func (b *exponentialBackoff) Next(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1 // attempt 约定从 1 开始；≤0 视同首次失败
	}
	d := b.base
	for range attempt - 1 {
		d *= 2
		if d >= b.max {
			return b.max
		}
	}
	if d > b.max || d <= 0 {
		return b.max
	}
	return d
}
