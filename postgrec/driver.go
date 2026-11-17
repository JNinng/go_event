package postgrec

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jninng/go_event"
	"github.com/jninng/go_event/postgresschema"
	"go.opentelemetry.io/otel/metric"
)

// Driver event.Driver 的 Postgres 实现（*sql.DB 载体）。
// 持久结构按迁移模式在首次使用时处理（design.md §8）：缺省 Create 幂等
// 应用版本计划全量语句，Verify 只读校验，None 直接放行。
type Driver struct {
	db        *sql.DB
	schema    string
	retention time.Duration
	mode      event.MigrateMode
	manager   schemaManager

	ensureMu     sync.Mutex
	ensured      bool
	ensureErr    error
	retryAt      time.Time
	retryBackoff time.Duration

	// now 可注入时钟（缺省 time.Now），用于失败负缓存的退避判定。
	now func() time.Time

	// initErr 是构造期错误（非法 schema）；所有模式都要在 ensure 首行返回，
	// 避免非法 schema 拼进任何 DML/DDL。
	initErr error

	cleanupMu   sync.Mutex
	lastCleanup time.Time
}

// NewDriver 基于 *sql.DB 构造 event.Driver。schema 名在构造期校验（所有
// 模式一致）：非法名记录为 initErr，首次操作直接返回 ErrInvalidSchema。
func NewDriver(db *sql.DB, opts ...Option) event.Driver {
	return newDriver(db, opts...)
}

// newDriver 构造具体 Driver（*Driver 同时实现 event.Inspector，供
// NewInspector 与调用方的类型断言探测共用）。
func newDriver(db *sql.DB, opts ...Option) *Driver {
	cfg := newConfig(opts)
	d := &Driver{
		db:        db,
		schema:    cfg.schema,
		retention: cfg.retention,
		mode:      cfg.mode,
		manager: &pgSchemaManager{db: db, schema: cfg.schema,
			logger: cfg.logger, mp: cfg.mp, scope: cfg.scope},
		now: time.Now,
	}
	if err := postgresschema.ValidateSchema(cfg.schema); err != nil {
		d.initErr = fmt.Errorf("postgrec: %w: %v", event.ErrInvalidSchema, err)
	}
	return d
}

// schemaManager 抽象初始化面的两种操作，便于在无 DB 的单测中注入替身。
type schemaManager interface {
	migrate(ctx context.Context) error
	verify(ctx context.Context) (event.VerifyReport, error)
}

type pgSchemaManager struct {
	db     *sql.DB
	schema string

	// 可观测注入：透传给包级 Migrate/Verify（迁移指标的单一发射点）。
	logger *slog.Logger
	mp     metric.MeterProvider
	scope  string
}

func (m *pgSchemaManager) migrate(ctx context.Context) error {
	return Migrate(ctx, m.db, WithSchema(m.schema),
		WithLogger(m.logger), WithMeterProvider(m.mp), WithScopeName(m.scope))
}

func (m *pgSchemaManager) verify(ctx context.Context) (event.VerifyReport, error) {
	return Verify(ctx, m.db, WithSchema(m.schema),
		WithLogger(m.logger), WithMeterProvider(m.mp), WithScopeName(m.scope))
}

const (
	ensureInitialBackoff = 100 * time.Millisecond
	ensureMaxBackoff     = 30 * time.Second
)

// ensure 按迁移模式处理持久结构，并对失败做并发安全的负缓存：
// 瞬时错误（DB 不可用、锁等待超时）在指数退避窗口内直接返回同一错误，
// 窗口过后重试；永久配置错误（权限、定义冲突、不支持的模式）fail-fast，
// 不再重试。Verify 成功后同样缓存「首次检查通过」。
func (d *Driver) ensure(ctx context.Context) error {
	if d.initErr != nil {
		return d.initErr
	}
	d.ensureMu.Lock()
	defer d.ensureMu.Unlock()
	if d.ensured {
		return nil
	}
	if d.ensureErr != nil {
		if event.IsPermanentMigrationError(d.ensureErr) {
			return d.ensureErr
		}
		if d.now().Before(d.retryAt) {
			return d.ensureErr
		}
	}
	err := d.runEnsure(ctx)
	if err == nil {
		d.ensured = true
		d.ensureErr = nil
		d.retryBackoff = 0
		return nil
	}
	// 调用方取消/超时不是迁移结论：ctx 已取消时（含 DDL 执行中取消被驱动
	// 映射为 57014/连接关闭）不写负缓存、不推进退避。
	if ctx.Err() != nil {
		return err
	}
	d.ensureErr = err
	if event.IsPermanentMigrationError(err) {
		return err
	}
	if d.retryBackoff == 0 {
		d.retryBackoff = ensureInitialBackoff
	} else {
		d.retryBackoff *= 2
		if d.retryBackoff > ensureMaxBackoff {
			d.retryBackoff = ensureMaxBackoff
		}
	}
	d.retryAt = d.now().Add(d.retryBackoff)
	return err
}

func (d *Driver) runEnsure(ctx context.Context) error {
	switch d.mode {
	case event.MigrateCreate:
		return d.manager.migrate(ctx)
	case event.MigrateVerify:
		report, err := d.manager.verify(ctx)
		if err != nil {
			return err
		}
		if !report.OK() {
			return &event.VerifyError{Report: report}
		}
		return nil
	case event.MigrateNone:
		return nil
	default:
		return fmt.Errorf("postgrec: unsupported migrate mode %d: %w",
			int(d.mode), event.ErrMigrateModeUnsupported)
	}
}

// HeadPosition 广播基线锚点：该类型已提交事件的最大 Position，无该类型
// 事件时返回空串。单条语句快照（ADR 0006）；(type, position) 索引支撑即时求值。
func (d *Driver) HeadPosition(ctx context.Context, typ string) (string, error) {
	if err := d.ensure(ctx); err != nil {
		return "", err
	}
	var pos int64
	if err := d.db.QueryRowContext(ctx, fmt.Sprintf(
		`SELECT COALESCE(MAX(position), 0) FROM %s.event WHERE type = $1`, d.schema),
		typ).Scan(&pos); err != nil {
		return "", fmt.Errorf("postgrec: head position: %w", err)
	}
	if pos == 0 {
		return "", nil
	}
	return strconv.FormatInt(pos, 10), nil
}

// Append 写入事件：同 ID 首写为准、不覆盖不重投（幂等）。
func (d *Driver) Append(ctx context.Context, e event.RawEnvelope) error {
	if err := d.ensure(ctx); err != nil {
		return err
	}
	meta, err := marshalMetadata(e.Metadata)
	if err != nil {
		return err
	}
	// 幂等 Append：同 ID 首写为准，不覆盖、不重投（design.md §4）。
	_, err = d.db.ExecContext(ctx, fmt.Sprintf(`
INSERT INTO %s.event (id, type, payload, metadata, occurred_at)
VALUES ($1, $2, $3, $4::jsonb, $5)
ON CONFLICT (id) DO NOTHING`, d.schema),
		e.ID, e.Type, e.Payload, meta, e.OccurredAt)
	if err != nil {
		return fmt.Errorf("postgrec: append: %w", err)
	}
	d.lazyCleanup(ctx, time.Now())
	return nil
}

// Fetch 返回 position 严格大于 After 的已提交事件（升序，按 Limit 截断）；Limit<=0 返回 nil。
func (d *Driver) Fetch(ctx context.Context, q event.FetchQuery) ([]event.RawEnvelope, error) {
	if err := d.ensure(ctx); err != nil {
		return nil, err
	}
	if q.Limit <= 0 {
		return nil, nil
	}
	after := int64(0)
	if q.After != "" {
		var err error
		if after, err = strconv.ParseInt(q.After, 10, 64); err != nil {
			return nil, fmt.Errorf("postgrec: invalid position %q: %w", q.After, err)
		}
	}
	rows, err := d.db.QueryContext(ctx, fmt.Sprintf(`
SELECT id, type, payload, metadata, occurred_at, position
FROM %s.event
WHERE type = $1 AND position > $2
ORDER BY position ASC
LIMIT $3`, d.schema), q.Type, after, q.Limit)
	if err != nil {
		return nil, fmt.Errorf("postgrec: fetch: %w", err)
	}
	defer rows.Close()
	return scanEnvelopes(rows)
}

type claimedRow struct {
	eventID string
	count   int // 认领后的 claim_count（已失败次数）
}

// Claim 原子认领至多 Limit 个租约：终态接管、CAS 抢租约与取回拼装在同一事务内完成。
func (d *Driver) Claim(ctx context.Context, q event.ClaimQuery) ([]event.Lease, error) {
	if err := d.ensure(ctx); err != nil {
		return nil, err
	}
	if q.Limit <= 0 {
		return nil, nil
	}
	leaseUntil := q.Now.Add(q.Lease)
	token := newToken()

	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("postgrec: claim tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	// 1) 接管达重试上限的行：置 failed 终态、不再认领（ADR 0006）。
	//    failed_at 记录进入终态的时刻；接管路径没有本次处理错误，不写摘要。
	if _, err = tx.ExecContext(ctx, fmt.Sprintf(`
UPDATE %s.event_claim c
SET status = 'failed', lease_until = $3, next_eligible_at = '-infinity'::timestamptz,
	failed_at = $3
FROM %s.event e
WHERE c.event_id = e.id AND e.type = $1 AND c.consumer_type = $2
  AND c.status = 'active'
  AND c.lease_until <= $3 AND c.next_eligible_at <= $3
  AND c.claim_count + 1 >= $4`, d.schema, d.schema),
		q.Type, q.ConsumerType, q.Now, q.RetryLimit); err != nil {
		return nil, fmt.Errorf("postgrec: claim terminal: %w", err)
	}

	// 2) 原子认领：无主（INSERT）或租约过期/退避期满（ON CONFLICT UPDATE，
	//    接管计入重试）。过滤在认领时单条操作内下推（ADR 0004），
	//    FOR UPDATE SKIP LOCKED 降低并发争用（design.md §5.3）。
	rows, err := tx.QueryContext(ctx, fmt.Sprintf(`
WITH picked AS (
	SELECT e.id
	FROM %s.event e
	LEFT JOIN %s.event_claim c
	       ON c.event_id = e.id AND c.consumer_type = $2
	WHERE e.type = $1
	  AND (
	        c.event_id IS NULL
	     OR (c.status = 'active' AND c.lease_until <= $3 AND c.next_eligible_at <= $3)
	     OR (c.status = 'retrying' AND c.next_eligible_at <= $3)
	  )
	ORDER BY e.position ASC
	LIMIT $4
	FOR UPDATE OF e SKIP LOCKED
)
INSERT INTO %s.event_claim
	(event_id, consumer_type, lease_token, lease_until, claim_count, status, next_eligible_at)
SELECT p.id, $2, $5, $6, 0, 'active', '-infinity'::timestamptz
FROM picked p
ON CONFLICT (event_id, consumer_type) DO UPDATE
	SET lease_token      = EXCLUDED.lease_token,
	    lease_until      = EXCLUDED.lease_until,
	    status           = 'active',
	    next_eligible_at = '-infinity'::timestamptz,
	    claim_count      = CASE
	        WHEN %s.event_claim.status = 'active'
	        THEN %s.event_claim.claim_count + 1  -- 接管：前任失去租约计一次失败
	        ELSE %s.event_claim.claim_count      -- retrying 重认领：Fail 时已计入
	    END
	WHERE ((%s.event_claim.status = 'active'
	       AND %s.event_claim.lease_until <= $3
	       AND %s.event_claim.next_eligible_at <= $3)
	   OR (%s.event_claim.status = 'retrying'
	       AND %s.event_claim.next_eligible_at <= $3))
RETURNING event_id, claim_count
`, d.schema, d.schema, d.schema, d.schema, d.schema, d.schema,
		d.schema, d.schema, d.schema, d.schema, d.schema),
		q.Type, q.ConsumerType, q.Now, q.Limit, token, leaseUntil)
	if err != nil {
		return nil, fmt.Errorf("postgrec: claim: %w", err)
	}
	var claimed []claimedRow
	for rows.Next() {
		var c claimedRow
		if err = rows.Scan(&c.eventID, &c.count); err != nil {
			rows.Close()
			return nil, fmt.Errorf("postgrec: claim scan: %w", err)
		}
		claimed = append(claimed, c)
	}
	rows.Close()
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("postgrec: claim rows: %w", err)
	}
	if len(claimed) == 0 {
		if err = tx.Commit(); err != nil {
			return nil, fmt.Errorf("postgrec: claim commit: %w", err)
		}
		return nil, nil
	}

	// 3) 取回信封拼回。批次内顺序不承诺（契约仅约束 Fetch 升序）：
	//    claimed 来自 INSERT ... RETURNING，顺序无保证。
	ids := make([]string, len(claimed))
	for i, c := range claimed {
		ids[i] = c.eventID
	}
	envs, err := fetchByIDInTx(ctx, tx, d.schema, ids)
	if err != nil {
		return nil, err
	}
	if err = tx.Commit(); err != nil {
		return nil, fmt.Errorf("postgrec: claim commit: %w", err)
	}

	leases := make([]event.Lease, 0, len(claimed))
	for _, c := range claimed {
		env, ok := envs[c.eventID]
		if !ok {
			return nil, fmt.Errorf("postgrec: claimed event %s not found", c.eventID)
		}
		leases = append(leases, event.Lease{
			Envelope:     env,
			ConsumerType: q.ConsumerType,
			LeaseToken:   token,
			LeaseUntil:   leaseUntil,
			Attempts:     c.count + 1, // 含本次认领
		})
	}
	return leases, nil
}

// fetchByIDInTx 按 ID 列表取回信封（结果以 map 返回，行序无意义）。
// database/sql 无数组绑定，用动态 IN 参数（driver 无关）。
func fetchByIDInTx(ctx context.Context, tx *sql.Tx, schema string, ids []string) (map[string]event.RawEnvelope, error) {
	var sb strings.Builder
	fmt.Fprintf(&sb, `SELECT id, type, payload, metadata, occurred_at, position FROM %s.event WHERE id IN (`, schema)
	args := make([]any, 0, len(ids))
	for i, id := range ids {
		if i > 0 {
			sb.WriteString(",")
		}
		fmt.Fprintf(&sb, "$%d", i+1)
		args = append(args, id)
	}
	sb.WriteString(`) ORDER BY position ASC`)
	rows, err := tx.QueryContext(ctx, sb.String(), args...)
	if err != nil {
		return nil, fmt.Errorf("postgrec: claim fetch: %w", err)
	}
	defer rows.Close()
	envs, err := scanEnvelopes(rows)
	if err != nil {
		return nil, err
	}
	out := make(map[string]event.RawEnvelope, len(envs))
	for _, e := range envs {
		out[e.ID] = e
	}
	return out, nil
}

// Heartbeat CAS 续租：仅属主凭证匹配且租约未过期时生效，不命中静默返回 nil。
func (d *Driver) Heartbeat(ctx context.Context, h event.HeartbeatRequest) error {
	if err := d.ensure(ctx); err != nil {
		return err
	}
	// CAS：仅属主凭证匹配且租约未过期时生效；不命中静默返回 nil。
	_, err := d.db.ExecContext(ctx, fmt.Sprintf(`
UPDATE %s.event_claim SET lease_until = $4
WHERE event_id = $1 AND consumer_type = $2 AND lease_token = $3
  AND status = 'active' AND lease_until > $5`, d.schema),
		h.EventID, h.ConsumerType, h.LeaseToken, h.Now.Add(h.Extend), h.Now)
	if err != nil {
		return fmt.Errorf("postgrec: heartbeat: %w", err)
	}
	return nil
}

// Complete CAS 置 done：幂等、不可逆，迟到的 Complete 静默不生效。
func (d *Driver) Complete(ctx context.Context, c event.CompleteRequest) error {
	if err := d.ensure(ctx); err != nil {
		return err
	}
	// CAS：幂等、不可逆；迟到的 Complete 不生效（ADR 0006）。
	_, err := d.db.ExecContext(ctx, fmt.Sprintf(`
UPDATE %s.event_claim SET status = 'done', lease_until = $4
WHERE event_id = $1 AND consumer_type = $2 AND lease_token = $3
  AND status = 'active'`, d.schema),
		c.EventID, c.ConsumerType, c.LeaseToken, c.Now)
	if err != nil {
		return fmt.Errorf("postgrec: complete: %w", err)
	}
	return nil
}

// Fail 记失败：递增计数、退避调度与终态判定在单事务内原子完成
// （ADR 0006）；行锁 + CAS 谓词保证仅属主凭证匹配时生效。仅当本次 Fail
// 使行进入 failed 终态时写入错误摘要与终态时刻：摘要经
// event.SanitizeErrorSummary 清洗（截断 4 KiB、去控制字符、不落堆栈），
// 空串视为未提供（NULL）；重试中（retrying）与迟到的 Fail 均不写。
func (d *Driver) Fail(ctx context.Context, f event.FailRequest) error {
	if err := d.ensure(ctx); err != nil {
		return err
	}
	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("postgrec: fail tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	// 属主 CAS 锁行并读取当前失败计数。
	var count int
	err = tx.QueryRowContext(ctx, fmt.Sprintf(`
SELECT claim_count FROM %s.event_claim
WHERE event_id = $1 AND consumer_type = $2 AND lease_token = $3
  AND status = 'active'
FOR UPDATE`, d.schema), f.EventID, f.ConsumerType, f.LeaseToken).Scan(&count)
	if errors.Is(err, sql.ErrNoRows) {
		return nil // 迟到的 Fail 不递增、不写摘要、不污染新属主状态（静默）
	}
	if err != nil {
		return fmt.Errorf("postgrec: fail scan: %w", err)
	}

	count++
	if count >= f.RetryLimit {
		var lastErr any
		if summary := event.SanitizeErrorSummary(f.Error); summary != "" {
			lastErr = summary
		}
		_, err = tx.ExecContext(ctx, fmt.Sprintf(`
UPDATE %s.event_claim SET claim_count = $4, status = 'failed',
	lease_until = $5, next_eligible_at = '-infinity'::timestamptz,
	last_error = $6, failed_at = $7
WHERE event_id = $1 AND consumer_type = $2 AND lease_token = $3`, d.schema),
			f.EventID, f.ConsumerType, f.LeaseToken, count, f.Now, lastErr, f.Now)
	} else {
		_, err = tx.ExecContext(ctx, fmt.Sprintf(`
UPDATE %s.event_claim SET claim_count = $4, status = 'retrying',
	lease_until = $5, next_eligible_at = $6
WHERE event_id = $1 AND consumer_type = $2 AND lease_token = $3`, d.schema),
			f.EventID, f.ConsumerType, f.LeaseToken, count,
			f.Now, f.Now.Add(f.Backoff.Next(count))) // 首次失败 = 1
	}
	if err != nil {
		return fmt.Errorf("postgrec: fail update: %w", err)
	}
	if err = tx.Commit(); err != nil {
		return fmt.Errorf("postgrec: fail commit: %w", err)
	}
	return nil
}

// lazyCleanup 惰性清理：超保留窗口且无活跃租约的行（无论终态与否）均可
// 清理；认领记录经 FK ON DELETE CASCADE 一并清理（design.md §5.2）。
// 内部簿记用墙钟，不参与契约语义。
func (d *Driver) lazyCleanup(ctx context.Context, now time.Time) {
	d.cleanupMu.Lock()
	defer d.cleanupMu.Unlock()
	if !d.lastCleanup.IsZero() && now.Sub(d.lastCleanup) < max(d.retention/4, 50*time.Millisecond) {
		return
	}
	d.lastCleanup = now
	cutoff := now.Add(-d.retention)
	_, _ = d.db.ExecContext(ctx, fmt.Sprintf(`
DELETE FROM %s.event e
WHERE e.occurred_at < $1
  AND NOT EXISTS (
	SELECT 1 FROM %s.event_claim c
	WHERE c.event_id = e.id AND c.status = 'active' AND c.lease_until > $2
)`, d.schema, d.schema), cutoff, now) // 失败静默：清理不影响主语义
}

// ---- helpers ----

func marshalMetadata(meta map[string]string) (string, error) {
	if meta == nil {
		return "{}", nil
	}
	b, err := json.Marshal(meta)
	if err != nil {
		return "", fmt.Errorf("postgrec: marshal metadata: %w", err)
	}
	return string(b), nil
}

func scanEnvelopes(rows *sql.Rows) ([]event.RawEnvelope, error) {
	var out []event.RawEnvelope
	for rows.Next() {
		var e event.RawEnvelope
		var meta []byte
		var pos int64
		if err := rows.Scan(&e.ID, &e.Type, &e.Payload, &meta, &e.OccurredAt, &pos); err != nil {
			return nil, fmt.Errorf("postgrec: scan envelope: %w", err)
		}
		e.Position = strconv.FormatInt(pos, 10)
		if len(meta) > 0 {
			if err := json.Unmarshal(meta, &e.Metadata); err != nil {
				return nil, fmt.Errorf("postgrec: unmarshal metadata: %w", err)
			}
		}
		out = append(out, e)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("postgrec: scan rows: %w", err)
	}
	return out, nil
}

func newToken() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}
