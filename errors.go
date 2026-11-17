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

	// ErrEmptyEventID：WithEventID 传入空串（不静默回退到 IDGenerator）。
	ErrEmptyEventID = errors.New("event: empty event ID")

	// ErrMigrateModeUnsupported：WithMigrateMode 显式传给 Migrate（该选项
	// 只对 NewDriver 有意义）。
	ErrMigrateModeUnsupported = errors.New("event: migrate mode option unsupported on Migrate")

	// ErrMigrationUnsupported：锁能力为无（LockNone）时 Create 不支持
	// 自动迁移（只能 Verify / None）。
	ErrMigrationUnsupported = errors.New("event: automatic migration unsupported by backend")

	// ErrMigrationLockTimeout：等待迁移锁超时，属瞬时错误（退避后重试，
	// 不得缓存为永久错误）。
	ErrMigrationLockTimeout = errors.New("event: migration lock timeout")

	// ErrAdminLockTimeout：管理面写操作等待命名空间互斥锁超时，属瞬时
	// 错误（退避后重试）。锁与迁移执行共用同一把（键含 schema）——等待
	// 对象是迁移执行或其他管理写操作；不同命名空间互不阻塞。
	ErrAdminLockTimeout = errors.New("event: admin lock timeout")

	// ErrMigrationPermissionDenied：DDL 权限不足，属永久配置错误（可能由
	// 底层驱动的权限错误包装）。
	ErrMigrationPermissionDenied = errors.New("event: migration permission denied")

	// ErrInvalidSchema：schema 名非法（只允许 ^[A-Za-z_][A-Za-z0-9_]*$）。
	// NewDriver 在所有模式下都会校验；非法名不会进入任何 DML/DDL。
	ErrInvalidSchema = errors.New("event: invalid schema name")
)
