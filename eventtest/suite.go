// Package eventtest 契约测试套件：接口的语义就是这套测试，新后端必须
// 全过（design.md §1、§9.1）。RunSuite 覆盖公共语义（幂等 Append 首写为
// 准、元数据透传、HeadPosition 基线锚点）、广播消费读取（类型过滤/水位
// 严格大于且升序/限额/只读/无效水位报错）、认领消费（互斥/并发互斥/
// 接管计入重试/心跳/退避/终态/迟到 Complete 与 Fail 不生效/新身份重放/
// 超限行不占限额槽位）与 core 编排行为（广播基线在 Start 同步确立/失败
// 跳过且不阻塞/各自全收/水位单调/无参事件/类型间不互相饿死/端到端认领/
// 重试与耗尽/挂死接管/优雅停机/就近唤醒/订阅守卫）。RunRetentionSuite
// 覆盖保留窗口惰性清理资格（design.md §5.2）。
//
// 用例命名遵循三段式「条件_动作_预期结果」。
package eventtest

import (
	"context"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jninng/go_event"
)

// RunSuite 运行全套契约测试（design.md §9.1 列举的全部语义）。
// 新后端必须全过；语义即测试。
func RunSuite(t *testing.T, d event.Driver) {
	// ---- 公共语义 ----
	t.Run("DuplicateID_Append_FirstWriteWinsKeepsFirst", func(t *testing.T) { testAppendIdempotent(t, d) })
	t.Run("PublishMetadata_Append_StoredVerbatim", func(t *testing.T) { testMetadataPassthrough(t, d) })
	t.Run("PopulatedType_HeadPosition_ReturnsNewestPosition", func(t *testing.T) { testHeadPositionNewest(t, d) })
	t.Run("UnknownType_HeadPosition_ReturnsEmptyString", func(t *testing.T) { testHeadPositionEmpty(t, d) })

	// ---- 广播消费读取（Fetch）----
	t.Run("ForeignTypeRows_Fetch_Excluded", func(t *testing.T) { testFetchForeignType(t, d) })
	t.Run("AfterWatermark_Fetch_StrictGreaterAscending", func(t *testing.T) { testFetchWatermark(t, d) })
	t.Run("SmallLimit_Fetch_CapsBatch", func(t *testing.T) { testFetchLimit(t, d) })
	t.Run("MalformedAfter_Fetch_ReturnsError", func(t *testing.T) { testFetchMalformedAfter(t, d) })
	t.Run("RepeatedFetch_Fetch_ReadOnlyStable", func(t *testing.T) { testFetchReadOnly(t, d) })

	// ---- 认领消费（Claim/Heartbeat/Complete/Fail）----
	t.Run("ActiveLease_Claim_Rejected", func(t *testing.T) { testClaimActiveLeaseRejected(t, d) })
	t.Run("IndependentConsumerType_Claim_Succeeds", func(t *testing.T) { testClaimIndependentIdentity(t, d) })
	t.Run("ConcurrentClaimers_Claim_DisjointNoDoubleGrant", func(t *testing.T) { testClaimConcurrentDisjoint(t, d) })
	t.Run("ExpiredLease_Claim_TakeoverIncrementsAttempts", func(t *testing.T) { testClaimTakeover(t, d) })
	t.Run("OverLimitTakeover_Claim_TerminalNoLease", func(t *testing.T) { testClaimOverLimitTerminal(t, d) })
	t.Run("OverLimitRow_Claim_DoesNotConsumeLimitSlot", func(t *testing.T) { testClaimOverLimitSlot(t, d) })
	t.Run("TerminalRows_Claim_Skipped", func(t *testing.T) { testClaimSkipTerminal(t, d) })
	t.Run("NewConsumerType_Claim_ReplaysHistory", func(t *testing.T) { testNewConsumerReplays(t, d) })
	t.Run("MatchingToken_Heartbeat_ExtendsLease", func(t *testing.T) { testHeartbeatExtends(t, d) })
	t.Run("StaleToken_Heartbeat_SilentNoEffect", func(t *testing.T) { testHeartbeatStaleToken(t, d) })
	t.Run("ValidOwner_Complete_IdempotentIrreversible", func(t *testing.T) { testComplete(t, d) })
	t.Run("StaleToken_Complete_NoEffectNotDone", func(t *testing.T) { testLateComplete(t, d) })
	t.Run("StaleToken_Fail_NoIncrement", func(t *testing.T) { testLateFail(t, d) })
	t.Run("BackoffWindow_Claim_DelayedUntilEligible", func(t *testing.T) { testFailBackoff(t, d) })
	t.Run("RetryLimitFail_Fail_TerminalNoReclaim", func(t *testing.T) { testFailRetryLimit(t, d) })

	// ---- core 编排（Runner/Publisher 基于 Driver 的端到端语义）----
	t.Run("PreStartEvents_Broadcast_SkippedAndPostStartDelivered", func(t *testing.T) { testBroadcastOnlyAfterStart(t, d) })
	t.Run("ParallelInstances_Broadcast_EachReceivesAll", func(t *testing.T) { testBroadcastAllInstances(t, d) })
	t.Run("HandlerFailure_Broadcast_SkippedNoRetry", func(t *testing.T) { testBroadcastFailureSkipped(t, d) })
	t.Run("HandlerFailure_Broadcast_DoesNotBlockSubsequent", func(t *testing.T) { testBroadcastFailureNotBlocking(t, d) })
	t.Run("RepeatedBursts_Broadcast_MonotonicNoDuplicate", func(t *testing.T) { testBroadcastNoDuplicate(t, d) })
	t.Run("NoPayloadEvent_Broadcast_Delivered", func(t *testing.T) { testNoPayloadBroadcast(t, d) })
	t.Run("NoPayloadEvent_Claim_ProcessedOnceCompleted", func(t *testing.T) { testNoPayloadClaim(t, d) })
	t.Run("FloodedType_FetchLimit_DoesNotStarveOtherTypes", func(t *testing.T) { testFetchLimitNoStarvation(t, d) })
	t.Run("PublishedEnvelope_Claim_FieldsReachHandler", func(t *testing.T) { testEnvelopeFieldsReachHandler(t, d) })
	t.Run("ClaimSuccess_Complete_NoRedelivery", func(t *testing.T) { testClaimEndToEnd(t, d) })
	t.Run("TransientFailures_Claim_RetriedUntilSuccess", func(t *testing.T) { testClaimRetryUntilSuccess(t, d) })
	t.Run("RetryExhausted_Claim_TerminalNoMoreDelivery", func(t *testing.T) { testClaimRetryExhausted(t, d) })
	t.Run("ExceededMaxProcessing_Claim_TakenOverByOtherInstance", func(t *testing.T) { testTakeoverAfterMaxProcessing(t, d) })
	t.Run("InFlightHandler_Stop_WaitsUntilCompletion", func(t *testing.T) { testGracefulStop(t, d) })
	t.Run("AfterStop_Claim_NotResumed", func(t *testing.T) { testClaimNotResumedAfterStop(t, d) })
	t.Run("LocalWakePublish_Runner_ImmediateDelivery", func(t *testing.T) { testLocalWake(t, d) })
	t.Run("ClaimOnlyOption_BroadcastSubscribe_Rejected", func(t *testing.T) { testGuardClaimOnlyOnBroadcast(t, d) })
	t.Run("MissingIdentity_ClaimSubscribe_Rejected", func(t *testing.T) { testGuardMissingIdentity(t, d) })
	t.Run("PostStartSubscribe_RejectedErrRunnerStarted", func(t *testing.T) { testGuardSubscribeAfterStart(t, d) })
}

// RunRetentionSuite 运行保留窗口惰性清理契约（design.md §5.2：清理资格
// 为「超窗且无活跃租约，无论终态与否」）。newDriver 须为每个用例返回
// **已配置保留窗口**的全新 Driver 实例（清理簿记按 Driver 实例记忆，
// 共享实例会因节流跳过清理；窗口时长由构造方决定，用例以足够久远的
// OccurredAt 构造超窗行）。
func RunRetentionSuite(t *testing.T, newDriver func(*testing.T) event.Driver) {
	t.Run("ExpiredNoLease_Cleanup_Deleted", func(t *testing.T) { testRetentionExpiredDeleted(t, newDriver) })
	t.Run("ExpiredActiveLease_Cleanup_Retained", func(t *testing.T) { testRetentionActiveLeaseRetained(t, newDriver) })
	t.Run("ExpiredTerminal_Cleanup_Deleted", func(t *testing.T) { testRetentionTerminalDeleted(t, newDriver) })
	t.Run("WithinWindow_Cleanup_Retained", func(t *testing.T) { testRetentionWithinWindowRetained(t, newDriver) })
}

// ---- 公共测试设施 ----

// baseNow 契约测试的基准时钟：Driver 判据一律取入参 Now，不依赖宿主
// 时钟。取当前墙钟值——避免与后端保留窗口清理（以 occurred_at 对比墙钟）
// 相互作用而误删测试数据。
var baseNow = time.Now().UTC()

type suitePayload struct {
	Value string `json:"value"`
}

func appendEvent(t *testing.T, d event.Driver, id, typ string, value string,
	meta map[string]string) event.RawEnvelope {
	t.Helper()
	payload, err := event.NewJSONCodec().Encode(suitePayload{Value: value})
	if err != nil {
		t.Fatalf("encode payload: %v", err)
	}
	e := event.RawEnvelope{
		ID: id, Type: typ, Payload: payload,
		Metadata: meta, OccurredAt: baseNow,
	}
	if err := d.Append(t.Context(), e); err != nil {
		t.Fatalf("append: %v", err)
	}
	return e
}

// appendOccurredAt 以指定 OccurredAt 追加事件（保留窗口用例构造超窗行）。
func appendOccurredAt(t *testing.T, d event.Driver, id, typ string,
	occurredAt time.Time) {
	t.Helper()
	payload, err := event.NewJSONCodec().Encode(suitePayload{Value: id})
	if err != nil {
		t.Fatalf("encode payload: %v", err)
	}
	e := event.RawEnvelope{ID: id, Type: typ, Payload: payload, OccurredAt: occurredAt}
	if err := d.Append(t.Context(), e); err != nil {
		t.Fatalf("append: %v", err)
	}
}

func claim(t *testing.T, d event.Driver, typ, consumerType string,
	lease time.Duration, now time.Time) []event.Lease {
	t.Helper()
	ls, err := d.Claim(t.Context(), event.ClaimQuery{
		Type: typ, ConsumerType: consumerType, Lease: lease, Limit: 10,
		RetryLimit: 5, Now: now,
	})
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	return ls
}

// comparePosition 按数值比较 Position 字符串（后端 Position 为数值全序）。
func comparePosition(a, b string) int {
	ai, aerr := strconv.ParseUint(a, 10, 64)
	bi, berr := strconv.ParseUint(b, 10, 64)
	if aerr != nil || berr != nil {
		return strings.Compare(a, b)
	}
	switch {
	case ai < bi:
		return -1
	case ai > bi:
		return 1
	default:
		return 0
	}
}

// mustPublish 发布事件并显式断言成功（套件内禁止吞错：发布失败必须
// 立即失败于断言，而非表现为无关的超时）。
func mustPublish[T any](t *testing.T, ctx context.Context, pub *event.Publisher,
	ev event.Event[T], payload T, opts ...event.PublishOption) {
	t.Helper()
	if err := ev.Publish(ctx, pub, payload, opts...); err != nil {
		t.Fatalf("publish %s: %v", ev.Name, err)
	}
}

func waitFor(t *testing.T, timeout time.Duration, msg string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("timeout waiting: " + msg)
}
