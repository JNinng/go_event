package event_test

import (
	"context"
	"errors"
	"slices"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/jninng/go_event"
)

// stubDriver 单元测试用最小内存 Driver：不经过 eventtest 契约套件，
// 只支撑 core 单元测试的注入需求（错误注入 + 操作留痕）。
type stubDriver struct {
	mu       sync.Mutex
	events   map[string]event.RawEnvelope
	position uint64

	headFail  error        // HeadPosition 注入错误
	fetchErrs int          // Fetch 剩余注入错误次数
	appendErr error        // Append 注入错误
	appends   []event.RawEnvelope
}

func newStubDriver() *stubDriver {
	return &stubDriver{events: make(map[string]event.RawEnvelope)}
}

func (s *stubDriver) Append(_ context.Context, e event.RawEnvelope) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.appendErr != nil {
		return s.appendErr
	}
	if _, ok := s.events[e.ID]; ok {
		return nil // 幂等：首写为准
	}
	s.position++
	ev := e
	ev.Position = strconv.FormatUint(s.position, 10)
	s.events[e.ID] = ev
	s.appends = append(s.appends, ev)
	return nil
}

func (s *stubDriver) HeadPosition(_ context.Context, typ string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.headFail != nil {
		return "", s.headFail
	}
	var head uint64
	for _, ev := range s.events {
		if ev.Type == typ {
			if n, err := strconv.ParseUint(ev.Position, 10, 64); err == nil && n > head {
				head = n
			}
		}
	}
	if head == 0 {
		return "", nil
	}
	return strconv.FormatUint(head, 10), nil
}

func (s *stubDriver) Fetch(_ context.Context, q event.FetchQuery) ([]event.RawEnvelope, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.fetchErrs > 0 {
		s.fetchErrs--
		return nil, errors.New("stub: fetch injected failure")
	}
	var out []event.RawEnvelope
	after := uint64(0)
	if q.After != "" {
		n, err := strconv.ParseUint(q.After, 10, 64)
		if err != nil {
			return nil, errors.New("stub: invalid position")
		}
		after = n
	}
	for _, ev := range s.events {
		if ev.Type != q.Type {
			continue
		}
		if n, err := strconv.ParseUint(ev.Position, 10, 64); err != nil || n <= after {
			continue
		}
		out = append(out, ev)
	}
	slices.SortFunc(out, func(a, b event.RawEnvelope) int {
		an, _ := strconv.ParseUint(a.Position, 10, 64)
		bn, _ := strconv.ParseUint(b.Position, 10, 64)
		switch {
		case an < bn:
			return -1
		case an > bn:
			return 1
		default:
			return 0
		}
	})
	if len(out) > q.Limit {
		out = out[:q.Limit]
	}
	return out, nil
}

func (s *stubDriver) Claim(context.Context, event.ClaimQuery) ([]event.Lease, error) {
	return nil, nil
}

func (s *stubDriver) Heartbeat(context.Context, event.HeartbeatRequest) error { return nil }
func (s *stubDriver) Complete(context.Context, event.CompleteRequest) error   { return nil }
func (s *stubDriver) Fail(context.Context, event.FailRequest) error           { return nil }

// appended 快照已追加信封。
func (s *stubDriver) appended() []event.RawEnvelope {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]event.RawEnvelope(nil), s.appends...)
}

var _ event.Driver = (*stubDriver)(nil)

// waitForUnit 单元测试的轮询等待。
func waitForUnit(t *testing.T, timeout time.Duration, msg string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatal("timeout waiting: " + msg)
}

// errStub 注入用错误（errors.Is 可判别）。
var errStub = errors.New("stub: injected failure")
