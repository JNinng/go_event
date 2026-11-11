package eventtest

import (
	"testing"
	"time"

	"github.com/jninng/go_event"
)

// 保留窗口惰性清理契约（design.md §5.2：清理资格为「超窗且无活跃租约，
// 无论终态与否」）。清理资格以后端墙钟对 occurred_at 判定，故用例以
// 追加时刻的墙钟构造行：超窗行早 2 小时、窗口内行取当下。清理在
// Append 内节流触发，序列固定为：追加新鲜行（触发首轮清理）→ 追加
// 超窗行（被节流跳过，得以存活）→ 按用例认领/完成 → 等过节流窗 →
// 追加触发行（第二轮清理评估超窗行）。
//
// newDriver 契约：每用例返回配置了保留窗口的全新 Driver（清理簿记按
// Driver 实例记忆），窗口须 ≤ 2s（用例以 600ms 等待越过
// max(窗口/4, 50ms) 的清理节流窗）。推荐 2s。
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
