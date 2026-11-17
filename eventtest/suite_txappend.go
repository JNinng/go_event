package eventtest

import (
	"testing"
	"time"

	"github.com/jninng/go_event"
)

// TxSession 事务性 Append 套件的事务载体适配（后端测试装配注入）：套件
// 不绑定后端事务类型（pgxc 为 pgx.Tx、postgrec 为 *sql.Tx、inmem 为缓冲
// 模拟），由装配函数把「事务内 Append / 提交 / 回滚」打包成同一形态。
// 同一 TxSession 的三次调用共享同一后端事务——装配处不得逐调用开新事务。
// 全部方法 Fatalf 风格：失败立即终止用例，不静默。
type TxSession interface {
	// AppendTx 在业务事务内追加事件（事件 INSERT 与业务写共享同一事务）。
	AppendTx(t *testing.T, e event.RawEnvelope)
	// Commit 提交业务事务（事件随事务原子可见）。
	Commit(t *testing.T)
	// Rollback 回滚业务事务（事务内已 Append 的事件全部丢弃）。
	Rollback(t *testing.T)
}

// RunTransactionalAppendSuite 运行 outbox 事务性入队契约测试（可选面，
// ADR 0012）：业务事务与事件入队原子提交/回滚、提交前对外不可见、提交
// 后广播/认领可见、乱序边界（并发业务事务交错提交下广播可能漏投、认领
// 不受影响——ADR 0006 广播位置语义重审的定稿口径）。
//
// d 与 beginTx 须同源（同一后端、同一命名空间——套件用 d 的 Fetch/Claim/
// HeadPosition 断言事务外可见性）。beginTx 为 nil、返回 nil TxSession
// 或装配缺失按先例（RunAdminReplaySuite）Fatal 终止——提供事务性入队的
// 后端必须全过本套件。
func RunTransactionalAppendSuite(t *testing.T, d event.Driver,
	beginTx func(t *testing.T) TxSession) {
	if beginTx == nil {
		t.Fatalf("beginTx is nil: the transactional-append suite requires a " +
			"transaction session factory wired by the backend test assembly")
	}
	if d == nil {
		t.Fatal("driver is nil: suite asserts out-of-tx visibility via the same backend")
	}
	// 探针事务：校验装配返回有效 TxSession（nil 即装配缺失，立即 Fatal）。
	// 空事务回滚不产生任何落库事实，不影响后续用例。
	probe := beginTx(t)
	if probe == nil {
		t.Fatalf("beginTx returned a nil TxSession: the transaction session " +
			"factory must yield a usable session (nil is a wiring fault)")
	}
	probe.Rollback(t)

	// ---- 原子性（回滚零事件 / 中途回滚连带先前 Append 一起丢弃）----
	t.Run("RolledBackTx_AppendTx_NoEventVisible", func(t *testing.T) { testTxRollbackNoEvent(t, d, beginTx) })
	t.Run("MidTxRollbackAfterFirstAppend_EarlierAppendAlsoDiscarded", func(t *testing.T) { testTxMidRollbackDiscardsAll(t, d, beginTx) })
	t.Run("RolledBackTx_SubsequentAppend_FetchSkipsPositionGap", func(t *testing.T) { testTxRollbackPositionGap(t, d, beginTx) })

	// ---- 隔离性（提交前对外不可见，HeadPosition 只锚已提交行）----
	t.Run("UncommittedTx_AppendTx_InvisibleToFetchClaimAndHeadPosition", func(t *testing.T) { testTxUncommittedInvisible(t, d, beginTx) })

	// ---- 提交后可见（广播 / 认领 / 基线锚点）----
	t.Run("CommittedTx_AppendTx_FetchClaimAndHeadPositionVisible", func(t *testing.T) { testTxCommitVisible(t, d, beginTx) })
	t.Run("CommittedTx_MultipleAppends_AllVisibleAtomically", func(t *testing.T) { testTxCommitMultipleVisible(t, d, beginTx) })

	// ---- 幂等（事务内、跨事务与跨路径同口径：首写为准）----
	t.Run("DuplicateID_AppendTx_FirstWriteWins", func(t *testing.T) { testTxDuplicateID(t, d, beginTx) })
	t.Run("CrossPathDuplicateID_AppendAndAppendTx_FirstWriteWins", func(t *testing.T) { testTxCrossPathIdempotent(t, d, beginTx) })

	// ---- 广播位置语义乱序边界（ADR 0012）----
	t.Run("OutOfOrderCommit_LaterPositionRow_BroadcastSkippedClaimVisible", func(t *testing.T) { testTxOutOfOrderCommit(t, d, beginTx) })
}

// txPayload 事务套件载荷编码。
func txPayload(t *testing.T, value string) []byte {
	t.Helper()
	b, err := event.NewJSONCodec().Encode(suitePayload{Value: value})
	if err != nil {
		t.Fatalf("encode payload: %v", err)
	}
	return b
}

// fetchAll 以空水位拉取该类型全部已提交事件（升序）。
func fetchAll(t *testing.T, d event.Driver, typ string) []event.RawEnvelope {
	t.Helper()
	envs, err := d.Fetch(t.Context(), event.FetchQuery{Type: typ, Limit: 100})
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	return envs
}

// fetchAfter 以指定水位拉取（乱序边界用例推进水位用）。
func fetchAfter(t *testing.T, d event.Driver, typ, after string) []event.RawEnvelope {
	t.Helper()
	envs, err := d.Fetch(t.Context(), event.FetchQuery{Type: typ, After: after, Limit: 100})
	if err != nil {
		t.Fatalf("fetch after %q: %v", after, err)
	}
	return envs
}

// requireVisible 断言恰一条 id 的可见事件（广播路径的事实断言）。
func requireVisible(t *testing.T, envs []event.RawEnvelope, id string) event.RawEnvelope {
	t.Helper()
	var found *event.RawEnvelope
	for i := range envs {
		if envs[i].ID == id {
			if found != nil {
				t.Fatalf("event %s appears more than once in fetch result", id)
			}
			found = &envs[i]
		}
	}
	if found == nil {
		t.Fatalf("event %s not visible in fetch result (len=%d)", id, len(envs))
	}
	return *found
}

// requireAbsent 断言 id 不可见。
func requireAbsent(t *testing.T, envs []event.RawEnvelope, id string) {
	t.Helper()
	for _, e := range envs {
		if e.ID == id {
			t.Fatalf("event %s must not be visible, got one (len=%d)", id, len(envs))
		}
	}
}

// 回滚零事件：事务内 Append 后回滚，事务外 Fetch / Claim / HeadPosition
// 均无该事件（ADR 0012：回滚不产生事件行）。
func testTxRollbackNoEvent(t *testing.T, d event.Driver, beginTx func(*testing.T) TxSession) {
	typ := t.Name()
	tx := beginTx(t)
	tx.AppendTx(t, event.RawEnvelope{ID: "tx-rb-1", Type: typ,
		Payload: txPayload(t, "1"), OccurredAt: baseNow})
	tx.Rollback(t)

	requireAbsent(t, fetchAll(t, d, typ), "tx-rb-1")
	res := claimResult(t, d, typ, "txrb", time.Minute, baseNow)
	if len(res.Leases) != 0 {
		t.Fatalf("claim after rollback = %+v, want no leases", res.Leases)
	}
	head, err := d.HeadPosition(t.Context(), typ)
	if err != nil {
		t.Fatalf("head position: %v", err)
	}
	if head != "" {
		t.Fatalf("head position after rollback = %q, want empty (no committed row)", head)
	}
}

// 中途回滚连带先前 Append 一起丢弃：同一事务两条 Append，第二条后回滚，
// 第一条也不可见——钉住「不逐条自治提交」（若实现错误地逐条提交，第一
// 条会残留）。业务事务与 Append 共用一个事务的原子性由此用例承载。
func testTxMidRollbackDiscardsAll(t *testing.T, d event.Driver, beginTx func(*testing.T) TxSession) {
	typ := t.Name()
	tx := beginTx(t)
	tx.AppendTx(t, event.RawEnvelope{ID: "tx-mid-1", Type: typ,
		Payload: txPayload(t, "1"), OccurredAt: baseNow})
	tx.AppendTx(t, event.RawEnvelope{ID: "tx-mid-2", Type: typ,
		Payload: txPayload(t, "2"), OccurredAt: baseNow})
	tx.Rollback(t)

	envs := fetchAll(t, d, typ)
	requireAbsent(t, envs, "tx-mid-1")
	requireAbsent(t, envs, "tx-mid-2")
}

// 回滚 position 缺口不破坏消费：回滚事务消耗的位置序列（实现细节，Postgres
// 序列不回收）之后，下一次提交的事件照常可 Fetch——消费契约（严格大于、
// 升序）不依赖 position 连续（ADR 0012 §5）。
func testTxRollbackPositionGap(t *testing.T, d event.Driver, beginTx func(*testing.T) TxSession) {
	typ := t.Name()
	tx := beginTx(t)
	tx.AppendTx(t, event.RawEnvelope{ID: "tx-gap-1", Type: typ,
		Payload: txPayload(t, "1"), OccurredAt: baseNow})
	tx.Rollback(t)

	tx2 := beginTx(t)
	tx2.AppendTx(t, event.RawEnvelope{ID: "tx-gap-2", Type: typ,
		Payload: txPayload(t, "2"), OccurredAt: baseNow})
	tx2.Commit(t)

	envs := fetchAll(t, d, typ)
	requireVisible(t, envs, "tx-gap-2")
	requireAbsent(t, envs, "tx-gap-1")
}

// 提交前对外不可见：未提交事务内 Append 的事件对事务外 Fetch / Claim /
// HeadPosition 不可见（隔离性；HeadPosition 基线锚点只锚已提交行）。
func testTxUncommittedInvisible(t *testing.T, d event.Driver, beginTx func(*testing.T) TxSession) {
	typ := t.Name()
	tx := beginTx(t)
	tx.AppendTx(t, event.RawEnvelope{ID: "tx-open-1", Type: typ,
		Payload: txPayload(t, "1"), OccurredAt: baseNow})
	defer tx.Rollback(t) // 用例结束丢弃，不影响其他用例

	requireAbsent(t, fetchAll(t, d, typ), "tx-open-1")
	res := claimResult(t, d, typ, "txopen", time.Minute, baseNow)
	if len(res.Leases) != 0 {
		t.Fatalf("claim over uncommitted tx = %+v, want no leases", res.Leases)
	}
	head, err := d.HeadPosition(t.Context(), typ)
	if err != nil {
		t.Fatalf("head position: %v", err)
	}
	if head != "" {
		t.Fatalf("head position over uncommitted tx = %q, want empty (anchor committed rows only)", head)
	}
}

// 提交后广播/认领可见：Commit 后 Fetch 可见（新水位之后）、Claim 可认领、
// HeadPosition 反映已提交行（ADR 0012 验收：提交后广播/认领可见）。
func testTxCommitVisible(t *testing.T, d event.Driver, beginTx func(*testing.T) TxSession) {
	typ := t.Name()
	tx := beginTx(t)
	tx.AppendTx(t, event.RawEnvelope{ID: "tx-ok-1", Type: typ,
		Payload: txPayload(t, "1"), OccurredAt: baseNow})
	tx.Commit(t)

	env := requireVisible(t, fetchAll(t, d, typ), "tx-ok-1")
	res := claimResult(t, d, typ, "txok", time.Minute, baseNow)
	if len(res.Leases) != 1 || res.Leases[0].Envelope.ID != "tx-ok-1" {
		t.Fatalf("claim after commit = %+v, want exactly tx-ok-1", res.Leases)
	}
	head, err := d.HeadPosition(t.Context(), typ)
	if err != nil {
		t.Fatalf("head position: %v", err)
	}
	if head == "" || comparePosition(head, env.Position) < 0 {
		t.Fatalf("head position = %q, want committed position %q", head, env.Position)
	}
}

// 同事务多条 Append 一次提交全可见：原子提交承载多条事件。
func testTxCommitMultipleVisible(t *testing.T, d event.Driver, beginTx func(*testing.T) TxSession) {
	typ := t.Name()
	tx := beginTx(t)
	tx.AppendTx(t, event.RawEnvelope{ID: "tx-multi-1", Type: typ,
		Payload: txPayload(t, "1"), OccurredAt: baseNow})
	tx.AppendTx(t, event.RawEnvelope{ID: "tx-multi-2", Type: typ,
		Payload: txPayload(t, "2"), OccurredAt: baseNow})
	tx.Commit(t)

	envs := fetchAll(t, d, typ)
	requireVisible(t, envs, "tx-multi-1")
	requireVisible(t, envs, "tx-multi-2")
	if len(envs) != 2 {
		t.Fatalf("fetch len = %d, want exactly 2 committed events", len(envs))
	}
	for i := 1; i < len(envs); i++ {
		if comparePosition(envs[i].Position, envs[i-1].Position) <= 0 {
			t.Fatalf("fetch order broken at %d: %q after %q",
				i, envs[i].Position, envs[i-1].Position)
		}
	}
}

// 事务内幂等（首写为准）：同事务重复 ID 只留一行首写 payload；已提交行
// 重复 AppendTx 不覆盖（与普通 Append 同口径，design.md §4）。
func testTxDuplicateID(t *testing.T, d event.Driver, beginTx func(*testing.T) TxSession) {
	typ := t.Name()

	// 已提交行：重复 AppendTx 不覆盖。
	tx0 := beginTx(t)
	tx0.AppendTx(t, event.RawEnvelope{ID: "tx-dup-1", Type: typ,
		Payload: txPayload(t, "first"), OccurredAt: baseNow})
	tx0.Commit(t)

	tx1 := beginTx(t)
	tx1.AppendTx(t, event.RawEnvelope{ID: "tx-dup-1", Type: typ,
		Payload: txPayload(t, "second"), OccurredAt: baseNow})
	// 同事务重复 ID：同口径首写为准。
	tx1.AppendTx(t, event.RawEnvelope{ID: "tx-dup-2", Type: typ,
		Payload: txPayload(t, "first"), OccurredAt: baseNow})
	tx1.AppendTx(t, event.RawEnvelope{ID: "tx-dup-2", Type: typ,
		Payload: txPayload(t, "second"), OccurredAt: baseNow})
	tx1.Commit(t)

	envs := fetchAll(t, d, typ)
	requireVisible(t, envs, "tx-dup-1")
	requireVisible(t, envs, "tx-dup-2")
	if len(envs) != 2 {
		t.Fatalf("fetch len = %d, want exactly 2 rows (duplicate ids discarded)", len(envs))
	}
	for _, e := range envs {
		p := decodeSuitePayload(t, e.Payload)
		if p.Value != "first" {
			t.Fatalf("event %s payload = %q, want first write %q", e.ID, p.Value, "first")
		}
	}
}

// decodeSuitePayload 解码套件载荷（首写为准断言用）。
func decodeSuitePayload(t *testing.T, raw []byte) suitePayload {
	t.Helper()
	var p suitePayload
	if err := event.NewJSONCodec().Decode(raw, &p); err != nil {
		t.Fatalf("decode payload: %v", err)
	}
	return p
}

// 跨路径幂等：事务性入队与普通发布对同一 ID 同口径（首写为准，无第二行）
// ——AppendTx 先提交后普通 Append、普通 Append 先提交后 AppendTx 两个
// 方向各断言一次。
func testTxCrossPathIdempotent(t *testing.T, d event.Driver, beginTx func(*testing.T) TxSession) {
	typ := t.Name()

	// 方向一：AppendTx 提交在前，普通 Append 同 ID 在后——首写保留。
	tx := beginTx(t)
	tx.AppendTx(t, event.RawEnvelope{ID: "tx-xp-1", Type: typ,
		Payload: txPayload(t, "first"), OccurredAt: baseNow})
	tx.Commit(t)
	if err := d.Append(t.Context(), event.RawEnvelope{ID: "tx-xp-1", Type: typ,
		Payload: txPayload(t, "second"), OccurredAt: baseNow}); err != nil {
		t.Fatalf("plain append over tx-committed id: %v", err)
	}
	envs := fetchAll(t, d, typ)
	requireVisible(t, envs, "tx-xp-1")
	if len(envs) != 1 {
		t.Fatalf("fetch len = %d, want exactly 1 row (cross-path duplicate discarded)", len(envs))
	}
	if p := decodeSuitePayload(t, envs[0].Payload); p.Value != "first" {
		t.Fatalf("tx-xp-1 payload = %q, want first write %q", p.Value, "first")
	}

	// 方向二：普通 Append 提交在前，AppendTx 同 ID 在后——首写保留。
	if err := d.Append(t.Context(), event.RawEnvelope{ID: "tx-xp-2", Type: typ,
		Payload: txPayload(t, "first"), OccurredAt: baseNow}); err != nil {
		t.Fatalf("plain append: %v", err)
	}
	tx2 := beginTx(t)
	tx2.AppendTx(t, event.RawEnvelope{ID: "tx-xp-2", Type: typ,
		Payload: txPayload(t, "second"), OccurredAt: baseNow})
	tx2.Commit(t)
	envs2 := fetchAll(t, d, typ)
	requireVisible(t, envs2, "tx-xp-2")
	if len(envs2) != 2 {
		t.Fatalf("fetch len = %d, want exactly 2 rows (tx-xp-1 + tx-xp-2)", len(envs2))
	}
	for _, e := range envs2 {
		if e.ID != "tx-xp-2" {
			continue
		}
		if p := decodeSuitePayload(t, e.Payload); p.Value != "first" {
			t.Fatalf("tx-xp-2 payload = %q, want first write %q", p.Value, "first")
		}
	}
}

// 广播位置语义乱序边界（ADR 0012 定稿口径）：并发业务事务交错提交下，
// 早分配晚提交的行会被已提交的更大 position 越过广播水位——广播不再可
// 见（写死的投递边界）；认领路径不受影响（直接查库，晚提交行可认领）。
// 构造为确定性次序：T1 分配小 position 不提交 → T2 分配大 position 提交
// → 广播水位推进到 T2 → T1 提交。
func testTxOutOfOrderCommit(t *testing.T, d event.Driver, beginTx func(*testing.T) TxSession) {
	typ := t.Name()

	tx1 := beginTx(t)
	tx1.AppendTx(t, event.RawEnvelope{ID: "tx-ooo-1", Type: typ,
		Payload: txPayload(t, "late-commit"), OccurredAt: baseNow}) // 早分配，未提交

	tx2 := beginTx(t)
	tx2.AppendTx(t, event.RawEnvelope{ID: "tx-ooo-2", Type: typ,
		Payload: txPayload(t, "first-commit"), OccurredAt: baseNow})
	tx2.Commit(t) // 后分配、先提交

	// 广播消费 T2 并把水位推进到其 position（模拟在途实例处理已提交批）。
	committed := fetchAll(t, d, typ)
	env2 := requireVisible(t, committed, "tx-ooo-2")
	requireAbsent(t, committed, "tx-ooo-1") // T1 未提交：不可见
	later := fetchAfter(t, d, typ, env2.Position)
	requireAbsent(t, later, "tx-ooo-1")

	// T1 提交：其 position 已被水位越过——广播不再可见（边界）；
	// 认领路径直接查库——照常可认领（正确性由认领消费承载）。Limit=1 按
	// position 升序拾取：恰好取回晚提交行（断言精确且单一）。
	tx1.Commit(t)

	requireAbsent(t, fetchAfter(t, d, typ, env2.Position), "tx-ooo-1")
	res, err := d.Claim(t.Context(), event.ClaimQuery{
		Type: typ, ConsumerType: "txooo", Lease: time.Minute,
		Limit: 1, RetryLimit: 5, Now: baseNow,
	})
	if err != nil {
		t.Fatalf("claim after out-of-order commit: %v", err)
	}
	if len(res.Leases) != 1 || res.Leases[0].Envelope.ID != "tx-ooo-1" {
		t.Fatalf("claim after out-of-order commit = %+v, want exactly tx-ooo-1 (claim path unaffected by watermark)", res.Leases)
	}
}
