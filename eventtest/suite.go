// Package eventtest 契约测试套件：接口的语义就是这套测试，新后端必须
// 全过（design.md §1、§9.1）。RunSuite 覆盖公共、广播读取、认领消费与
// core 编排语义；RunRetentionSuite 覆盖保留窗口惰性清理资格（design.md
// §5.2）；RunMigrationSuite 经 MigrationBackend 适配接口覆盖初始化面
// （Migrate/Verify、三种迁移模式、导出物、并发首启与锁语义、Lock=none
// 变体、语句边界故障注入）；RunInspectorSuite 覆盖只读查询面
// event.Inspector（可选能力，按类型断言探测，ADR 0005）。用例级清单以
// 各 Run 入口的注册表为准。
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

// RunSuite 运行公共、广播读取、认领消费与编排语义的契约测试；迁移与
// 保留窗口另经 RunMigrationSuite / RunRetentionSuite（design.md §9.1）。
// 新后端必须全过；语义即测试。
func RunSuite(t *testing.T, d event.Driver) {
	// ---- 公共语义 ----
	t.Run("DuplicateID_Append_FirstWriteWinsKeepsFirst", func(t *testing.T) { testAppendIdempotent(t, d) })
	t.Run("DuplicateID_SameType_Publish_SingleRowFirstPayload", func(t *testing.T) { testDuplicateIDPublishSameType(t, d) })
	t.Run("SameIDCrossType_Publish_FirstTypeOnlyNoError", func(t *testing.T) { testSameIDCrossTypePublish(t, d) })
	t.Run("EmptyEventID_EventPublish_RejectedNoRow", func(t *testing.T) { testEmptyEventIDEventPublish(t, d) })
	t.Run("EmptyEventID_PublishNoPayload_RejectedNoRow", func(t *testing.T) { testEmptyEventIDPublishNoPayload(t, d) })
	t.Run("PublishMetadata_Append_StoredVerbatim", func(t *testing.T) { testMetadataPassthrough(t, d) })
	t.Run("RuntimeFields_AppendFetchClaim_NotPersistedCoreComputesLast", func(t *testing.T) { testRuntimeFieldsNotPersisted(t, d) })
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
	t.Run("SmallLimit_Claim_CapsBatch", func(t *testing.T) { testClaimLimit(t, d) })
	t.Run("TerminalRows_Claim_Skipped", func(t *testing.T) { testClaimSkipTerminal(t, d) })
	t.Run("NewConsumerType_Claim_ReplaysHistory", func(t *testing.T) { testNewConsumerReplays(t, d) })
	t.Run("MatchingToken_Heartbeat_ExtendsLease", func(t *testing.T) { testHeartbeatExtends(t, d) })
	t.Run("StaleToken_Heartbeat_SilentNoEffect", func(t *testing.T) { testHeartbeatStaleToken(t, d) })
	t.Run("ValidOwner_Complete_IdempotentIrreversible", func(t *testing.T) { testComplete(t, d) })
	t.Run("StaleToken_Complete_NoEffectNotDone", func(t *testing.T) { testLateComplete(t, d) })
	t.Run("StaleToken_Fail_NoIncrement", func(t *testing.T) { testLateFail(t, d) })
	t.Run("BackoffWindow_Claim_DelayedUntilEligible", func(t *testing.T) { testFailBackoff(t, d) })
	t.Run("RetryLimitFail_Fail_TerminalNoReclaim", func(t *testing.T) { testFailRetryLimit(t, d) })

	// ---- Fail 错误摘要落库口径（docs/design.md §5.3；经 ClaimStateReader
	// ---- 断言落库事实）----
	reader, ok := d.(ClaimStateReader)
	if !ok {
		t.Fatalf("driver %T must implement ClaimStateReader: the error summary "+
			"persistence facts are part of the Driver contract", d)
	}
	t.Run("TerminalFailWithError_Fail_StoresSanitizedSummary", func(t *testing.T) { testFailSummaryTerminalStores(t, d, reader) })
	t.Run("TerminalFailWithError_Fail_StoresFailedAtAtFailNow", func(t *testing.T) { testFailSummaryTerminalFailedAt(t, d, reader) })
	t.Run("NonTerminalFail_Fail_LeavesSummaryAndFailedAtUnwritten", func(t *testing.T) { testFailSummaryRetryingUnwritten(t, d, reader) })
	t.Run("TerminalFailEmptyError_Fail_StoresNoSummary", func(t *testing.T) { testFailSummaryEmptyError(t, d, reader) })
	t.Run("StaleTokenWithError_Fail_DoesNotWriteSummaryOrFailedAt", func(t *testing.T) { testFailSummaryLateFailUnwritten(t, d, reader) })
	t.Run("OverLimitTakeover_Claim_StoresFailedAtWithoutSummary", func(t *testing.T) { testFailSummaryTakeoverTerminal(t, d, reader) })
	t.Run("ExactlyLimitError_Fail_StoresUntruncatedSummary", func(t *testing.T) { testFailSummaryExactlyLimit(t, d, reader) })
	t.Run("OverLimitError_Fail_TruncatesSummaryToLimitBytes", func(t *testing.T) { testFailSummaryTruncated(t, d, reader) })
	t.Run("ControlCharError_Fail_StoresStrippedSummary", func(t *testing.T) { testFailSummaryControlChars(t, d, reader) })
	t.Run("HandlerErrorViaRunner_TerminalFail_StoresHandlerMessage", func(t *testing.T) { testFailSummaryRunnerPushdown(t, d, reader) })

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
	t.Run("FirstDelivery_ClaimHandler_AttemptsOneLastFalse", func(t *testing.T) { testFirstDeliveryAttempts(t, d) })
	t.Run("RetryLimitTwo_FirstAttemptFails_SecondDeliveryLastTrue", func(t *testing.T) { testRetryLimitTwoLastAttempt(t, d) })
	t.Run("RetryLimitThree_SecondAttempt_LastAttemptFalse", func(t *testing.T) { testRetryLimitThreeNotLast(t, d) })
	t.Run("RetryLimitThree_ThirdAttempt_LastTrue", func(t *testing.T) { testRetryLimitThreeLast(t, d) })
	t.Run("RetryLimitOne_FirstDelivery_LastTrue", func(t *testing.T) { testRetryLimitOneFirstIsLast(t, d) })
	t.Run("ExpiredDirectLease_RunnerTakeover_HandlerAttemptsIncremented", func(t *testing.T) { testTakeoverAttemptsReachHandler(t, d) })
	t.Run("Broadcast_Handler_AttemptsZeroLastFalse", func(t *testing.T) { testBroadcastAttemptsZero(t, d) })
	t.Run("ClaimSuccess_Complete_NoRedelivery", func(t *testing.T) { testClaimEndToEnd(t, d) })
	t.Run("TransientFailures_Claim_RetriedUntilSuccess", func(t *testing.T) { testClaimRetryUntilSuccess(t, d) })
	t.Run("RetryExhausted_Claim_TerminalNoMoreDelivery", func(t *testing.T) { testClaimRetryExhausted(t, d) })
	t.Run("ExceededMaxProcessing_Claim_TakenOverByOtherInstance", func(t *testing.T) { testTakeoverAfterMaxProcessing(t, d) })
	t.Run("ActiveLeaseWithinMaxProcessing_SecondInstance_NoTakeover", func(t *testing.T) { testActiveLeaseNoTakeover(t, d) })
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

// ClaimState 认领行的落库事实快照（事件 × 消费者身份）：Fail 的错误摘要
// 与终态时刻写入口径以此断言（docs/design.md §5.3）。空串/零值表示对应
// 列为 NULL（未提供/未写入）。
type ClaimState struct {
	Status    string    // 行状态：active / retrying / done / failed
	LastError string    // 错误摘要（清洗后文本）
	FailedAt  time.Time // 行进入 failed 终态的时刻
}

// ClaimStateReader 是认领行落库事实的观察接口：RunSuite 用它断言 Fail
// 错误摘要与 failed_at 的写入时机与口径。Driver 必须实现（inmem 原生
// 实现；Postgres 后端在测试装配处以 SQL 适配 event_claim 行）。
type ClaimStateReader interface {
	// ReadClaimState 返回认领行的当前落库事实；行不存在时 ok 为 false。
	// 行缺失以外的查询失败属测试装配故障，经 t 报错终止（不得折叠为
	// 「行不存在」）。
	ReadClaimState(t *testing.T, eventID, consumerType string) (ClaimState, bool)
}

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
