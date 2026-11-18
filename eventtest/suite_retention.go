package eventtest

import (
	"testing"
	"time"

	"github.com/jninng/go_event"
)

// 保留窗口惰性清理契约（design.md §5.2：清理资格为「超窗且无活跃租约，
// 无论终态与否」，以后端墙钟对 occurred_at 判定，超窗行早 2 小时构造）。
// 序列固定：追加新鲜行（触发首轮清理）→ 追加超窗行（被节流跳过）→
// 按用例认领/完成 → retentionSettle 等过节流窗 → 追加触发行评估超窗行。
// newDriver 契约：返回配置了保留窗口的全新 Driver（清理簿记按实例记忆），
// 窗口 ≤ 2s（600ms 等待越过 max(窗口/4, 50ms) 节流窗；推荐 2s）。
func retentionFixture(t *testing.T, newDriver func(*testing.T) event.Driver,
	typ string) (event.Driver, string) {
	t.Helper()
	d := newDriver(t)
	appendOccurredAt(t, d, "fresh-"+typ, typ, time.Now().UTC()) // 首轮清理：无可清理
	appendOccurredAt(t, d, "old-"+typ, typ, time.Now().UTC().Add(-2*time.Hour))
	return d, "old-" + typ
}

// retentionSettle 等过清理节流窗，使下一次 Append 触发新一轮清理。
func retentionSettle() {
	time.Sleep(600 * time.Millisecond)
}

// fetchIDs 取回该类型现存行 ID 集合。
func fetchIDs(t *testing.T, d event.Driver, typ string) map[string]bool {
	t.Helper()
	batch, err := d.Fetch(t.Context(), event.FetchQuery{Type: typ, Limit: 10})
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	ids := make(map[string]bool, len(batch))
	for _, e := range batch {
		ids[e.ID] = true
	}
	return ids
}

// fetchIDsAt 以指定判据时钟取回现存行 ID 集合（延迟行按到期口径过滤）。
func fetchIDsAt(t *testing.T, d event.Driver, typ string, now time.Time) map[string]bool {
	t.Helper()
	batch, err := d.Fetch(t.Context(), event.FetchQuery{Type: typ, Limit: 10, Now: now})
	if err != nil {
		t.Fatalf("fetch at %v: %v", now, err)
	}
	ids := make(map[string]bool, len(batch))
	for _, e := range batch {
		ids[e.ID] = true
	}
	return ids
}

// 超窗且无活跃租约的行被清理，不可再被认领。
func testRetentionExpiredDeleted(t *testing.T, newDriver func(*testing.T) event.Driver) {
	typ := t.Name()
	d, oldID := retentionFixture(t, newDriver, typ)

	retentionSettle()
	appendOccurredAt(t, d, "trigger-"+typ, typ, time.Now().UTC())

	if ids := fetchIDs(t, d, typ); ids[oldID] {
		t.Fatalf("expired row must be cleaned, still present: %s", oldID)
	}
	for _, l := range claim(t, d, typ, "late", time.Minute, baseNow) {
		if l.Envelope.ID == oldID {
			t.Fatalf("cleaned row must not be claimable, got lease %+v", l)
		}
	}
}

// 超窗但持有活跃租约的行不被清理（「无活跃租约」资格保护在途处理，
// design.md §5.2、ADR 0006）。
func testRetentionActiveLeaseRetained(t *testing.T, newDriver func(*testing.T) event.Driver) {
	typ := t.Name()
	d, oldID := retentionFixture(t, newDriver, typ)

	if ls := claim(t, d, typ, "worker", time.Hour, baseNow); len(ls) != 2 {
		t.Fatalf("claim: want both rows, got %+v", ls)
	}

	retentionSettle()
	appendOccurredAt(t, d, "trigger-"+typ, typ, time.Now().UTC())

	if ids := fetchIDs(t, d, typ); !ids[oldID] {
		t.Fatalf("expired row with active lease must be retained, missing %s", oldID)
	}
}

// 超窗的终态行被清理（「无论终态与否」：done 行过窗即删，「完成状态
// 上游可查」仅在窗口内成立，design.md §5.2）。
func testRetentionTerminalDeleted(t *testing.T, newDriver func(*testing.T) event.Driver) {
	typ := t.Name()
	d, oldID := retentionFixture(t, newDriver, typ)

	ls := claim(t, d, typ, "finisher", time.Hour, baseNow)
	var doneLease event.Lease
	for _, l := range ls {
		if l.Envelope.ID == oldID {
			doneLease = l
		}
	}
	if doneLease.Envelope.ID == "" {
		t.Fatalf("expired row must be claimable before completion, got %+v", ls)
	}
	if err := d.Complete(t.Context(), event.CompleteRequest{
		EventID: oldID, ConsumerType: "finisher",
		LeaseToken: doneLease.LeaseToken, Now: baseNow,
	}); err != nil {
		t.Fatalf("complete: %v", err)
	}

	retentionSettle()
	appendOccurredAt(t, d, "trigger-"+typ, typ, time.Now().UTC())

	if ids := fetchIDs(t, d, typ); ids[oldID] {
		t.Fatalf("expired done row must be cleaned, still present: %s", oldID)
	}
}

// 窗口内的行不被清理（清理资格以保留窗口为硬边界）。
func testRetentionWithinWindowRetained(t *testing.T, newDriver func(*testing.T) event.Driver) {
	typ := t.Name()
	d := newDriver(t)
	const n = 3
	for i := range n {
		appendOccurredAt(t, d, typ+string(rune('a'+i)), typ, time.Now().UTC())
	}

	retentionSettle() // 多轮清理均不触达窗口内行
	appendOccurredAt(t, d, "trigger-"+typ, typ, time.Now().UTC())

	if ids := fetchIDs(t, d, typ); len(ids) != n+1 {
		t.Fatalf("within-window rows must be retained, want %d got %d", n+1, len(ids))
	}
}

// 超窗但未到投递时刻的延迟行不被清理：保留窗口自可投递时刻起算
// （ADR 0013 §1）——延迟行不因「等待投递」被清理；到期后仍在保留窗口内。
func testRetentionDelayedUndeliveredRetained(t *testing.T, newDriver func(*testing.T) event.Driver) {
	typ := t.Name()
	d := newDriver(t)
	now := time.Now().UTC()
	due := now.Add(time.Hour)
	appendOccurredAt(t, d, "fresh-"+typ, typ, now) // 首轮清理：无可清理
	appendDelayed(t, d, "old-delayed-"+typ, typ,
		now.Add(-2*time.Hour), due) // occurred_at 已超窗、deliver_at 未到

	retentionSettle()
	appendOccurredAt(t, d, "trigger-"+typ, typ, now)

	if ids := fetchIDsAt(t, d, typ, due); !ids["old-delayed-"+typ] {
		t.Fatal("undelivered delayed row beyond occurred_at must be retained (retention starts at deliver_at)")
	}
}

// 可投递时刻已超窗的延迟行照常清理：判据是可投递时刻（deliver_at），
// 与即时行以 occurred_at 判定同口径。
func testRetentionDelayedOverdueDeleted(t *testing.T, newDriver func(*testing.T) event.Driver) {
	typ := t.Name()
	d := newDriver(t)
	now := time.Now().UTC()
	appendOccurredAt(t, d, "fresh-"+typ, typ, now)
	appendDelayed(t, d, "old-overdue-"+typ, typ,
		now.Add(-3*time.Hour), now.Add(-2*time.Hour)) // 可投递时刻已超窗

	retentionSettle()
	appendOccurredAt(t, d, "trigger-"+typ, typ, now)

	if ids := fetchIDsAt(t, d, typ, now); ids["old-overdue-"+typ] {
		t.Fatal("delayed row with deliver_at beyond the window must be cleaned")
	}
}
