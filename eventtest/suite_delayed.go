package eventtest

import (
	"testing"
	"time"

	"github.com/jninng/go_event"
)

// 延迟事件契约（deliver_at 定时投递，ADR 0013 §1）：零值立即可投递（既有
// 行为回归）、未到期行不满足 Fetch / Claim 下推条件、到期边界含到时刻
// 本身（时钟注入，过滤在单条操作内完成）、信封透传、与广播水位的组合
// 边界（延迟行被水位越过即不再投递，认领不受影响）。时钟一律注入判据
// 值；到期时刻取微秒截断（Postgres timestamptz 精度），使精确边界在
// SQL 后端确定性成立。
func RunDelayedDeliverySuite(t *testing.T, d event.Driver) {
	t.Run("ZeroDeliverAt_FetchAndClaim_ImmediatelyDeliverable", func(t *testing.T) { testDelayedZeroImmediate(t, d) })
	t.Run("PastDeliverAt_FetchAndClaim_EquivalentToImmediate", func(t *testing.T) { testDelayedPastImmediate(t, d) })
	t.Run("FutureDeliverAt_Fetch_BeforeDueFiltered", func(t *testing.T) { testDelayedFetchFiltered(t, d) })
	t.Run("FutureDeliverAt_Claim_BeforeDueNotClaimed", func(t *testing.T) { testDelayedClaimFiltered(t, d) })
	t.Run("DueDeliverAt_Fetch_AtDueInclusive", func(t *testing.T) { testDelayedFetchDueBoundary(t, d) })
	t.Run("BeforeDueDeliverAt_Claim_AtDueClaimed", func(t *testing.T) { testDelayedClaimDueBoundary(t, d) })
	t.Run("DelayedRow_Fetch_EnvelopeDeliverAtRoundTrips", func(t *testing.T) { testDelayedEnvelopeRoundTrip(t, d) })
	t.Run("DelayedRow_WatermarkAdvanced_BroadcastSkippedClaimVisible", func(t *testing.T) { testDelayedWatermarkBoundary(t, d) })
}

// microNow 契约时钟：截断到微秒（Postgres timestamptz 精度），使精确到期
// 边界经落库往返后仍确定性成立。
func microNow() time.Time {
	return time.Now().UTC().Truncate(time.Microsecond)
}

// appendDelayed 以指定 OccurredAt 与 DeliverAt 追加事件（延迟投递用例构造）。
func appendDelayed(t *testing.T, d event.Driver, id, typ string,
	occurredAt, deliverAt time.Time) {
	t.Helper()
	payload, err := event.NewJSONCodec().Encode(suitePayload{Value: id})
	if err != nil {
		t.Fatalf("encode payload: %v", err)
	}
	e := event.RawEnvelope{
		ID: id, Type: typ, Payload: payload,
		OccurredAt: occurredAt, DeliverAt: deliverAt,
	}
	if _, err := d.Append(t.Context(), e); err != nil {
		t.Fatalf("append delayed: %v", err)
	}
}

// fetchAt 以指定判据时钟读取该类型全部可投递事件（升序）。
func fetchAt(t *testing.T, d event.Driver, typ string, now time.Time) []event.RawEnvelope {
	t.Helper()
	envs, err := d.Fetch(t.Context(), event.FetchQuery{Type: typ, Limit: 100, Now: now})
	if err != nil {
		t.Fatalf("fetch at %v: %v", now, err)
	}
	return envs
}

// 零值 DeliverAt 立即可投递（既有行为回归）：Fetch 与 Claim 均不受延迟
// 判据影响。
func testDelayedZeroImmediate(t *testing.T, d event.Driver) {
	typ := t.Name()
	now := microNow()
	appendDelayed(t, d, "dz-1", typ, now, time.Time{})

	if envs := fetchAt(t, d, typ, now); len(envs) != 1 {
		t.Fatalf("fetch len = %d, want 1 (zero deliver_at is immediately deliverable)", len(envs))
	}
	leases := claim(t, d, typ, "dz", time.Minute, now)
	if len(leases) != 1 || leases[0].Envelope.ID != "dz-1" {
		t.Fatalf("claim = %+v, want exactly dz-1", leases)
	}
}

// 过去时刻等价立即可投递：不报错、照常可投递。
func testDelayedPastImmediate(t *testing.T, d event.Driver) {
	typ := t.Name()
	now := microNow()
	appendDelayed(t, d, "dp-1", typ, now, now.Add(-time.Hour))

	if envs := fetchAt(t, d, typ, now); len(envs) != 1 {
		t.Fatalf("fetch len = %d, want 1 (past deliver_at is immediate)", len(envs))
	}
	if leases := claim(t, d, typ, "dp", time.Minute, now); len(leases) != 1 {
		t.Fatalf("claim len = %d, want 1", len(leases))
	}
}

// 未到期行不满足 Fetch 下推条件（到期前读取为空）。
func testDelayedFetchFiltered(t *testing.T, d event.Driver) {
	typ := t.Name()
	now := microNow()
	appendDelayed(t, d, "df-1", typ, now, now.Add(time.Hour))

	if envs := fetchAt(t, d, typ, now); len(envs) != 0 {
		t.Fatalf("fetch before due = %+v, want none", envs)
	}
}

// 未到期行不满足 Claim 下推条件（到期前认领为空）。
func testDelayedClaimFiltered(t *testing.T, d event.Driver) {
	typ := t.Name()
	now := microNow()
	appendDelayed(t, d, "dc-1", typ, now, now.Add(time.Hour))

	res := claimResult(t, d, typ, "dc", time.Minute, now)
	if len(res.Leases) != 0 {
		t.Fatalf("claim before due = %+v, want no leases", res.Leases)
	}
}

// 恰好到期边界（Fetch，时钟注入）：deliver_at <= now 含到时刻本身，
// Now == deliver_at 即可见。
func testDelayedFetchDueBoundary(t *testing.T, d event.Driver) {
	typ := t.Name()
	now := microNow()
	due := now.Add(time.Minute)
	appendDelayed(t, d, "db-1", typ, now, due)

	if envs := fetchAt(t, d, typ, due.Add(-time.Microsecond)); len(envs) != 0 {
		t.Fatalf("fetch 1µs before due = %+v, want none", envs)
	}
	if envs := fetchAt(t, d, typ, due); len(envs) != 1 {
		t.Fatalf("fetch exactly at due len = %d, want 1 (deliver_at <= now is inclusive)", len(envs))
	}
}

// 恰好到期边界（Claim，时钟注入）：Now == deliver_at 即可认领；到期前
// 1µs 不可认领。
func testDelayedClaimDueBoundary(t *testing.T, d event.Driver) {
	typ := t.Name()
	now := microNow()
	due := now.Add(time.Minute)
	appendDelayed(t, d, "dc-2", typ, now, due)

	res := claimResult(t, d, typ, "dcb", time.Minute, due.Add(-time.Microsecond))
	if len(res.Leases) != 0 {
		t.Fatalf("claim 1µs before due = %+v, want no leases", res.Leases)
	}
	res = claimResult(t, d, typ, "dcb", time.Minute, due)
	if len(res.Leases) != 1 || res.Leases[0].Envelope.ID != "dc-2" {
		t.Fatalf("claim exactly at due = %+v, want exactly dc-2", res.Leases)
	}
}

// 信封透传：Fetch / Claim 返回的 DeliverAt 与发布侧声明一致（后端原样
// 落库透传）。
func testDelayedEnvelopeRoundTrip(t *testing.T, d event.Driver) {
	typ := t.Name()
	now := microNow()
	due := now.Add(2 * time.Minute)
	appendDelayed(t, d, "dr-1", typ, now, due)

	envs := fetchAt(t, d, typ, due)
	if len(envs) != 1 {
		t.Fatalf("fetch len = %d, want 1", len(envs))
	}
	if !envs[0].DeliverAt.Equal(due) {
		t.Fatalf("fetch deliver_at = %v, want %v (verbatim round trip)", envs[0].DeliverAt, due)
	}
	leases := claim(t, d, typ, "dr", time.Minute, due.Add(time.Minute))
	if len(leases) != 1 || !leases[0].Envelope.DeliverAt.Equal(due) {
		t.Fatalf("claim envelope deliver_at = %+v, want %v", leases, due)
	}
}

// 与广播水位的组合边界（ADR 0013 §1，与 ADR 0012 乱序边界同源）：延迟行
// position 靠前、即时行 position 靠后；即时行把广播水位推过延迟行后，
// 延迟行即使到期也不再广播可见——需要定时必达用认领消费（Claim 无水位
// 条件，到期后照常可认领）。确定性构造：先插延迟行再插即时行，水位以
// 即时行 position 模拟在途实例的推进。
func testDelayedWatermarkBoundary(t *testing.T, d event.Driver) {
	typ := t.Name()
	now := microNow()
	due := now.Add(time.Hour)
	appendDelayed(t, d, "wm-delayed", typ, now, due)  // position 靠前
	appendDelayed(t, d, "wm-now", typ, now, time.Time{}) // position 靠后

	// 到期前：延迟行被下推条件过滤，即时行可见并把水位推过延迟行。
	visible := fetchAt(t, d, typ, now)
	if len(visible) != 1 || visible[0].ID != "wm-now" {
		t.Fatalf("fetch before due = %+v, want exactly wm-now", visible)
	}
	watermark := visible[0].Position

	// 到期后：position <= 水位——广播不再可见（写死的投递边界）。
	envs, err := d.Fetch(t.Context(), event.FetchQuery{
		Type: typ, After: watermark, Limit: 100, Now: due,
	})
	if err != nil {
		t.Fatalf("fetch after due beyond watermark: %v", err)
	}
	for _, e := range envs {
		if e.ID == "wm-delayed" {
			t.Fatalf("delayed row %s crossed by the watermark must not broadcast", e.ID)
		}
	}

	// 认领路径不受影响：无水位条件，到期后照常可认领（替代用法）。
	res := claimResult(t, d, typ, "wm", time.Minute, due)
	var claimed bool
	for _, l := range res.Leases {
		if l.Envelope.ID == "wm-delayed" {
			claimed = true
		}
	}
	if !claimed {
		t.Fatalf("claim after due = %+v, want wm-delayed claimable (claim path unaffected)", res.Leases)
	}
}

// 发布路径（WithDeliverAt 经 Publish 落库，RunSuite 注册）：到期前
// Fetch / Claim 均不可见，到期后可见且 DeliverAt 原样透传。
func testPublishWithDeliverAt(t *testing.T, d event.Driver) {
	ctx := t.Context()
	typ := "delay-pub-" + t.Name()
	ev := event.Declare[suitePayload](typ)
	pub := event.NewPublisher(d)
	now := microNow()
	due := now.Add(time.Minute)

	mustPublish(t, ctx, pub, ev, suitePayload{"later"}, event.WithDeliverAt(due))

	if envs := fetchAt(t, d, typ, now); len(envs) != 0 {
		t.Fatalf("fetch before due = %+v, want none", envs)
	}
	res := claimResult(t, d, typ, "pub", time.Minute, now)
	if len(res.Leases) != 0 {
		t.Fatalf("claim before due = %+v, want no leases", res.Leases)
	}
	envs := fetchAt(t, d, typ, due)
	if len(envs) != 1 || !envs[0].DeliverAt.Equal(due) {
		t.Fatalf("fetch at due = %+v, want exactly one row with deliver_at %v", envs, due)
	}
}
