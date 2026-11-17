package eventtest

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/jninng/go_event"
)

// Fail 错误摘要落库口径用例（docs/design.md §5.3、ADR 0009）：
//   - last_error 仅在本次 Fail 使行进入 failed 终态时写入（清洗后文本，
//     截断 4 KiB、去控制字符、不落堆栈）；空串视为未提供（NULL）。
//   - failed_at 仅在 failed 终态有值；接管终态同样写入，接管路径无摘要。
//   - 迟到的 Fail（CAS 不命中）不写新属主行。
//
// 落库事实经 ClaimStateReader 读取；口径与后端实现解耦。

// readClaimState 读取落库事实；用例前提是行必须存在。
func readClaimState(t *testing.T, rd ClaimStateReader, eventID, consumerType string) ClaimState {
	t.Helper()
	st, ok := rd.ReadClaimState(t, eventID, consumerType)
	if !ok {
		t.Fatalf("claim row (%s, %s) not found", eventID, consumerType)
	}
	return st
}

// terminalClaim 认领并断言恰好一条租约（终态 Fail 用例的公共前提）。
func terminalClaim(t *testing.T, d event.Driver, typ, consumerType string) event.Lease {
	t.Helper()
	ls := claim(t, d, typ, consumerType, time.Minute, baseNow)
	if len(ls) != 1 {
		t.Fatalf("claim: %+v", ls)
	}
	return ls[0]
}

// terminalFail 以 RetryLimit=1 使本次 Fail 必然进入 failed 终态。
func terminalFail(t *testing.T, d event.Driver, ls event.Lease, errorText string, now time.Time) {
	t.Helper()
	if err := d.Fail(t.Context(), event.FailRequest{
		EventID: ls.Envelope.ID, ConsumerType: ls.ConsumerType, LeaseToken: ls.LeaseToken,
		Error: errorText, RetryLimit: 1, Backoff: linearBackoffMs(), Now: now,
	}); err != nil {
		t.Fatalf("fail: %v", err)
	}
}

// 终态 Fail 写入摘要：错误文本原样落为 last_error（普通文本不经清洗改变）。
func testFailSummaryTerminalStores(t *testing.T, d event.Driver, rd ClaimStateReader) {
	typ := t.Name()
	appendEvent(t, d, "es-fs1", typ, "1", nil)
	ls := terminalClaim(t, d, typ, "fs")
	terminalFail(t, d, ls, "boom: order rejected", baseNow)

	st := readClaimState(t, rd, "es-fs1", "fs")
	if st.Status != "failed" {
		t.Fatalf("status = %q, want failed", st.Status)
	}
	if st.LastError != "boom: order rejected" {
		t.Fatalf("last_error = %q, want %q", st.LastError, "boom: order rejected")
	}
}

// 终态 Fail 写入终态时刻：failed_at 取 FailRequest.Now（timestamptz 为
// 微秒精度，双侧截断到微秒比较）。
func testFailSummaryTerminalFailedAt(t *testing.T, d event.Driver, rd ClaimStateReader) {
	typ := t.Name()
	appendEvent(t, d, "es-ft1", typ, "1", nil)
	ls := terminalClaim(t, d, typ, "ft")
	failAt := baseNow.Add(30 * time.Second)
	terminalFail(t, d, ls, "boom", failAt)

	st := readClaimState(t, rd, "es-ft1", "ft")
	if !st.FailedAt.Truncate(time.Microsecond).Equal(failAt.Truncate(time.Microsecond)) {
		t.Fatalf("failed_at = %v, want %v", st.FailedAt, failAt)
	}
}

// 非终态 Fail（未达 RetryLimit，行转入 retrying）不写摘要与终态时刻。
func testFailSummaryRetryingUnwritten(t *testing.T, d event.Driver, rd ClaimStateReader) {
	typ := t.Name()
	appendEvent(t, d, "es-fn1", typ, "1", nil)
	ls := terminalClaim(t, d, typ, "fn")
	if err := d.Fail(t.Context(), event.FailRequest{
		EventID: ls.Envelope.ID, ConsumerType: ls.ConsumerType, LeaseToken: ls.LeaseToken,
		Error: "transient boom", RetryLimit: 5, Backoff: linearBackoffMs(), Now: baseNow,
	}); err != nil {
		t.Fatalf("fail: %v", err)
	}

	st := readClaimState(t, rd, "es-fn1", "fn")
	if st.Status != "retrying" {
		t.Fatalf("status = %q, want retrying", st.Status)
	}
	if st.LastError != "" {
		t.Fatalf("retrying row must not carry error summary, got %q", st.LastError)
	}
	if !st.FailedAt.IsZero() {
		t.Fatalf("retrying row must not carry failed_at, got %v", st.FailedAt)
	}
}

// 终态 Fail 未提供摘要（Error 为空串）：failed_at 照常写入，last_error
// 保持 NULL（空串视为未提供）。
func testFailSummaryEmptyError(t *testing.T, d event.Driver, rd ClaimStateReader) {
	typ := t.Name()
	appendEvent(t, d, "es-fe1", typ, "1", nil)
	ls := terminalClaim(t, d, typ, "fe")
	terminalFail(t, d, ls, "", baseNow)

	st := readClaimState(t, rd, "es-fe1", "fe")
	if st.Status != "failed" {
		t.Fatalf("status = %q, want failed", st.Status)
	}
	if st.LastError != "" {
		t.Fatalf("empty error must store NULL summary, got %q", st.LastError)
	}
	if st.FailedAt.IsZero() {
		t.Fatal("failed_at must be stored on terminal fail")
	}
}

// 迟到的 Fail（租约已被新属主持有）携带错误文本也不生效：不写入、不
// 污染新属主行（ADR 0006 CAS）。RetryLimit=2 使判别成立：若 CAS 防护
// 失效，本次迟到 Fail 会让新属主行计数 1+1=2 达上限、必然走终态分支写
// 摘要与终态时刻——断言不写即为终态路径的防护证据。
func testFailSummaryLateFailUnwritten(t *testing.T, d event.Driver, rd ClaimStateReader) {
	typ := t.Name()
	appendEvent(t, d, "es-fl1", typ, "1", nil)
	ls1 := terminalClaim(t, d, typ, "fl")
	// 接管：新属主持有租约，claim_count=1。
	ls2 := claim(t, d, typ, "fl", time.Minute, baseNow.Add(2*time.Minute))
	if len(ls2) != 1 {
		t.Fatalf("takeover: %+v", ls2)
	}
	// 旧实例迟到的 Fail：静默不生效，摘要与终态时刻不落库。
	if err := d.Fail(t.Context(), event.FailRequest{
		EventID: ls1.Envelope.ID, ConsumerType: ls1.ConsumerType, LeaseToken: ls1.LeaseToken,
		Error: "stale owner boom", RetryLimit: 2,
		Backoff: linearBackoffMs(), Now: baseNow.Add(2 * time.Minute),
	}); err != nil {
		t.Fatalf("late fail must return nil, got %v", err)
	}

	st := readClaimState(t, rd, "es-fl1", "fl")
	if st.Status != "active" {
		t.Fatalf("late fail must not touch the new owner row, status = %q, want active", st.Status)
	}
	if st.LastError != "" {
		t.Fatalf("late fail must not write summary to the new owner row, got %q", st.LastError)
	}
	if !st.FailedAt.IsZero() {
		t.Fatalf("late fail must not write failed_at, got %v", st.FailedAt)
	}
}

// 接管终态（认领时达 RetryLimit 直接置 failed）写 failed_at、不写摘要：
// 该路径没有本次处理错误可记。
func testFailSummaryTakeoverTerminal(t *testing.T, d event.Driver, rd ClaimStateReader) {
	typ := t.Name()
	appendEvent(t, d, "es-fv1", typ, "1", nil)
	// 接管计数达 RetryLimit=2 的认领直接置 failed：第 1 次认领（count=0）、
	// 第 2 次接管认领（count=1，发放）、第 3 次接管认领（count=2 达上限）。
	doClaim := func(now time.Time) []event.Lease {
		t.Helper()
		ls, err := d.Claim(t.Context(), event.ClaimQuery{
			Type: typ, ConsumerType: "fv", Lease: time.Minute, Limit: 10,
			RetryLimit: 2, Now: now,
		})
		if err != nil {
			t.Fatalf("claim: %v", err)
		}
		return ls
	}
	if ls := doClaim(baseNow); len(ls) != 1 {
		t.Fatalf("first claim: %+v", ls)
	}
	if ls := doClaim(baseNow.Add(2 * time.Minute)); len(ls) != 1 {
		t.Fatalf("second claim (within limit): %+v", ls)
	}
	terminalAt := baseNow.Add(4 * time.Minute)
	if ls := doClaim(terminalAt); len(ls) != 0 {
		t.Fatalf("over-limit takeover must not hand out lease, got %+v", ls)
	}

	st := readClaimState(t, rd, "es-fv1", "fv")
	if st.Status != "failed" {
		t.Fatalf("status = %q, want failed", st.Status)
	}
	// failed_at 取置终态的认领时钟（timestamptz 为微秒精度，双侧截断）。
	if !st.FailedAt.Truncate(time.Microsecond).Equal(terminalAt.Truncate(time.Microsecond)) {
		t.Fatalf("failed_at = %v, want %v (the terminal claim's clock)", st.FailedAt, terminalAt)
	}
	if st.LastError != "" {
		t.Fatalf("takeover terminal has no handler error, got summary %q", st.LastError)
	}
}

// 恰好 4 KiB 的摘要不截断。
func testFailSummaryExactlyLimit(t *testing.T, d event.Driver, rd ClaimStateReader) {
	typ := t.Name()
	appendEvent(t, d, "es-fx1", typ, "1", nil)
	ls := terminalClaim(t, d, typ, "fx")
	text := strings.Repeat("a", event.MaxErrorSummaryBytes)
	terminalFail(t, d, ls, text, baseNow)

	st := readClaimState(t, rd, "es-fx1", "fx")
	if st.LastError != text {
		t.Fatalf("boundary summary must be stored untruncated: got %d bytes, want %d",
			len(st.LastError), event.MaxErrorSummaryBytes)
	}
}

// 超限摘要截断到 4 KiB 字节（不撕裂 UTF-8 码点，保留前缀）。
func testFailSummaryTruncated(t *testing.T, d event.Driver, rd ClaimStateReader) {
	typ := t.Name()
	appendEvent(t, d, "es-fc1", typ, "1", nil)
	ls := terminalClaim(t, d, typ, "fc")
	text := strings.Repeat("a", event.MaxErrorSummaryBytes-1) + "错错错错"
	terminalFail(t, d, ls, text, baseNow)

	st := readClaimState(t, rd, "es-fc1", "fc")
	if !utf8.ValidString(st.LastError) {
		t.Fatalf("stored summary must stay valid UTF-8")
	}
	if len(st.LastError) > event.MaxErrorSummaryBytes {
		t.Fatalf("stored summary = %d bytes, want <= %d", len(st.LastError), event.MaxErrorSummaryBytes)
	}
	if !strings.HasPrefix(text, st.LastError) {
		t.Fatal("stored summary must be a prefix of the input")
	}
	if len(st.LastError) != event.MaxErrorSummaryBytes-1 {
		t.Fatalf("stored summary = %d bytes, want %d (tail rune that does not fit is dropped)",
			len(st.LastError), event.MaxErrorSummaryBytes-1)
	}
}

// 控制字符混合的错误文本：摘要移除全部 Unicode 控制字符（Cc，含换行/
// 制表），其余字符原样保留。
func testFailSummaryControlChars(t *testing.T, d event.Driver, rd ClaimStateReader) {
	typ := t.Name()
	appendEvent(t, d, "es-fk1", typ, "1", nil)
	ls := terminalClaim(t, d, typ, "fk")
	terminalFail(t, d, ls, "boom\x00\x1b[31m:\nhead\x7f\tdetail\u0085tail", baseNow)

	st := readClaimState(t, rd, "es-fk1", "fk")
	if st.LastError != "boom[31m:headdetailtail" {
		t.Fatalf("stored summary = %q, want control chars stripped", st.LastError)
	}
}

// 端到端：core Runner 把 handler 错误下推 Fail，重试耗尽进入终态后摘要
// 可读——落库事实与编排行为一致。
func testFailSummaryRunnerPushdown(t *testing.T, d event.Driver, rd ClaimStateReader) {
	ctx := t.Context()
	typ := "es-runner-" + t.Name()
	const handlerMsg = "runner handler boom"
	ev := event.Declare[suitePayload](typ)
	pub := event.NewPublisher(d)
	run := event.NewRunner(d, event.WithPollInterval(5*time.Millisecond),
		event.WithIdentity("es-runner"), event.WithLogger(discardLogger()))
	if err := ev.SubscribeClaim(run, func(context.Context, event.Envelope[suitePayload]) error {
		return errors.New(handlerMsg)
	}, event.WithRetryLimit(1), event.WithLease(time.Second),
		event.WithHeartbeat(200*time.Millisecond),
		event.WithBackoff(event.LinearBackoff(time.Millisecond, time.Millisecond)),
	); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	if err := run.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer func() { _ = run.Stop(context.WithoutCancel(ctx)) }()

	// 显式指定事件 ID：认领行的落库事实按（事件 × 身份）读取。
	mustPublish(t, ctx, pub, ev, suitePayload{"x"}, event.WithEventID("es-run-1"))
	waitFor(t, 5*time.Second, "terminal fail stored", func() bool {
		st, ok := rd.ReadClaimState(t, "es-run-1", "es-runner")
		return ok && st.Status == "failed"
	})
	st := readClaimState(t, rd, "es-run-1", "es-runner")
	if st.LastError != handlerMsg {
		t.Fatalf("stored summary = %q, want handler message %q", st.LastError, handlerMsg)
	}
}
