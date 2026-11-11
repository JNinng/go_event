package event

import "errors"

// 哨兵错误（docs/api.md §2.7）。
var (
	// ErrRunnerStarted：Runner.Start 后再订阅。
	ErrRunnerStarted = errors.New("event: runner already started")

	// ErrNotClaimSubscription：广播订阅传入认领专属选项。
	ErrNotClaimSubscription = errors.New("event: claim-only option on broadcast subscription")

	// ErrIdentityMissing：认领订阅未设置身份（订阅级与 Runner 默认皆缺）。
	ErrIdentityMissing = errors.New("event: consumer identity missing")
)
