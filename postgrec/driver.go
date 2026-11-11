package postgrec

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jninng/go_event"
)

// Driver event.Driver 的 Postgres 实现（*sql.DB 载体）。
// 持久结构在首次使用时自动幂等初始化（design.md §8）。
type Driver struct {
	db        *sql.DB
	schema    string
	retention time.Duration

	ensureMu sync.Mutex
	ensured  bool

	cleanupMu   sync.Mutex
	lastCleanup time.Time
}

// NewDriver 基于 *sql.DB 构造 event.Driver。
func NewDriver(db *sql.DB, opts ...Option) event.Driver {
	cfg := newConfig(opts)
	return &Driver{
		db:        db,
		schema:    cfg.schema,
		retention: cfg.retention,
	}
}

// ensure 首次使用时自动执行幂等迁移（design.md §8）。迁移失败不缓存：
// DB 未就绪等瞬时故障只影响当次操作，下次操作重试迁移。
func (d *Driver) ensure(ctx context.Context) error {
	d.ensureMu.Lock()
	defer d.ensureMu.Unlock()
	if d.ensured {
		return nil
	}
	if err := Migrate(ctx, d.db, WithSchema(d.schema), WithRetention(d.retention)); err != nil {
		return err
	}
	d.ensured = true
	return nil
}

// HeadPosition 广播基线锚点：该类型已提交事件的最大 Position，无行返回
// 空串。单条语句快照（ADR 0006）；(type, position) 索引支撑即时求值。
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
	if _, err = tx.ExecContext(ctx, fmt.Sprintf(`
UPDATE %s.event_claim c
SET status = 'failed', lease_until = $3, next_eligible_at = '-infinity'::timestamptz
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
// （ADR 0006）；行锁 + CAS 谓词保证仅属主凭证匹配时生效。
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
		return nil // 迟到的 Fail 不递增、不污染新属主状态（静默）
	}
	if err != nil {
		return fmt.Errorf("postgrec: fail scan: %w", err)
	}

	count++ // 本次失败计入
	if count >= f.RetryLimit {
		_, err = tx.ExecContext(ctx, fmt.Sprintf(`
UPDATE %s.event_claim SET claim_count = $4, status = 'failed',
	lease_until = $5, next_eligible_at = '-infinity'::timestamptz
WHERE event_id = $1 AND consumer_type = $2 AND lease_token = $3`, d.schema),
			f.EventID, f.ConsumerType, f.LeaseToken, count, f.Now)
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
