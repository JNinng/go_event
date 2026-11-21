// Package postgrec 是 go_event 的 Postgres 后端，基于标准库 database/sql
// （连接由使用方建立并注册驱动，本包不携带数据库驱动）：event / event_claim
// 两表、FOR UPDATE SKIP LOCKED 认领、惰性清理。
//
// 可选能力面（Inspector / Admin / TransactionalAppender）均不经运行面类型
// 获得，经各自构造函数接入、与 Driver 共享连接与命名空间选项；延迟投递
// 与合并投递为 Driver 语义内字段（WithDeliverAt / RawEnvelope.DeliverAt、
// WithCoalesceKey / RawEnvelope.CoalesceKey，ADR 0014）；契约见本目录
// README.md「能力边界表」（跨实例推送唤醒不做实现，退化轮询）。
//
// 持久结构按迁移模式在首次使用时处理：缺省 Create 持迁移锁幂等应用版本
// 计划（需 DDL 权限）；Verify 只读校验、None 不建不校验——两者要求表已
// 由部署流程创建，运行账号只授 DML 权限。
package postgrec

import (
	"log/slog"
	"time"

	"github.com/jninng/go_event"
	"github.com/jninng/go_event/postgresschema"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
)

// Option 后端构造选项。
type Option func(*config)

type config struct {
	schema    string
	retention time.Duration
	mode      event.MigrateMode
	modeSet   bool

	// adminLockWait 是管理写互斥锁的有界等待上限（Admin 写路径专用，
	// 与迁移锁同一把锁；缺省同 postgresschema.LockWaitTimeout）。
	adminLockWait time.Duration

	logger *slog.Logger         // 迁移/管理面生命周期日志（缺省构造期快照 slog.Default()）
	mp     metric.MeterProvider // nil = 未注入（newConfig 快照全局，未装配时即 noop）
	scope  string
	obs    event.Observer // 管理面观测旁路（缺省无观察者）
}

// WithSchema 表所在 schema（缺省 public）。
func WithSchema(s string) Option {
	return func(c *config) { c.schema = s }
}

// WithRetention 保留窗口（缺省 7 天）：惰性清理资格为「超窗且无活跃
// 租约，无论终态与否」（design.md §5.2、ADR 0006）。认领消费须将窗口
// 配置为覆盖最坏重试路径：约 重试上限 × (最大退避 + 最大处理时长)，
// 另留心跳间隔与时钟偏差余量。
func WithRetention(d time.Duration) Option {
	return func(c *config) { c.retention = d }
}

// WithMigrateMode 设置运行面迁移模式（缺省 Create）：Create 首次使用时
// 幂等应用版本计划全量语句（基线与各版本）；Verify 只读校验、不执行 DDL；
// None 不建表也不校验。该选项对 NewDriver / NewStore（内部 Driver 构造期）
// 生效，显式传给 Migrate 返回 ErrMigrateModeUnsupported。
func WithMigrateMode(m event.MigrateMode) Option {
	return func(c *config) {
		c.mode = m
		c.modeSet = true
	}
}

func newConfig(opts []Option) *config {
	c := &config{
		schema:        "public",
		retention:     7 * 24 * time.Hour,
		mode:          event.MigrateCreate,
		adminLockWait: postgresschema.LockWaitTimeout,
		logger:        slog.Default(),
		mp:            otel.GetMeterProvider(), // 构造期快照；快照后替换全局不追溯
		scope:         defaultScope,
	}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

// WithLogger 注入 slog Logger（缺省构造期快照 slog.Default()）：仅迁移
// 完成这类低频生命周期事件使用，热路径不打日志。
func WithLogger(l *slog.Logger) Option {
	return func(c *config) { c.logger = l }
}

// WithMeterProvider 注入指标 MeterProvider；缺省构造期快照
// otel.GetMeterProvider()（未装配时即 noop provider，天然静默）。
func WithMeterProvider(mp metric.MeterProvider) Option {
	return func(c *config) { c.mp = mp }
}

// WithScopeName 覆写指标 scope 与指标名前缀（<scope>.<动作>）；缺省为
// 模块名 postgrec。部署期决策——改名会使仪表盘口径漂移。
func WithScopeName(scope string) Option {
	return func(c *config) { c.scope = scope }
}

// WithObserver 注入类型化观测旁路（缺省无观察者）：当前仅管理面 Admin
// 发射观测事件（AdminReplayEvent，经 ObserverV3 分发；未实现 V3 的观察者
// 不感知）。回调在调用方 goroutine 同步执行、须微秒级返回，库内 recover
// panic；观测是纯旁路，不改变业务语义。
func WithObserver(o event.Observer) Option {
	return func(c *config) { c.obs = o }
}

// WithAdminLockWait 覆盖管理写互斥锁的有界等待上限（缺省同迁移锁
// postgresschema.LockWaitTimeout，仅 Admin 写路径生效）：运维按实例的
// 锁等待预算配置。非正值不阻塞——保留一次 try-lock 后立即返回
// ErrAdminLockTimeout（瞬时）。
func WithAdminLockWait(d time.Duration) Option {
	return func(c *config) { c.adminLockWait = d }
}
