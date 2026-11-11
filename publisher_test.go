package event_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/jninng/go_event"
)

type pubPayload struct {
	Value string `json:"value"`
}

// 缺省配置：ID 自动填充且唯一、OccurredAt 自动填 UTC 当前时间
// （design.md §6）。
func TestDefaultPublish_Fills_IDAndOccurredAt(t *testing.T) {
	d := newStubDriver()
	pub := event.NewPublisher(d)
	ev := event.Declare[pubPayload]("unit.pub")
	ctx := context.Background()
	testStart := time.Now().UTC()

	if err := ev.Publish(ctx, pub, pubPayload{"a"}); err != nil {
		t.Fatalf("publish 1: %v", err)
	}
	if err := ev.Publish(ctx, pub, pubPayload{"b"}); err != nil {
		t.Fatalf("publish 2: %v", err)
	}

	appends := d.appended()
	if len(appends) != 2 {
		t.Fatalf("want 2 appends, got %d", len(appends))
	}
	if appends[0].ID == "" || appends[0].ID == appends[1].ID {
		t.Fatalf("IDs must be filled and unique, got %q / %q", appends[0].ID, appends[1].ID)
	}
	for i, e := range appends {
		if e.OccurredAt.IsZero() || e.OccurredAt.Before(testStart) {
			t.Fatalf("append %d OccurredAt must be publish time, got %v", i, e.OccurredAt)
		}
		if e.OccurredAt.Location() != time.UTC {
			t.Fatalf("OccurredAt must be UTC, got %v", e.OccurredAt.Location())
		}
	}
}

// WithIDGenerator 注入的生成器替代缺省实现（design.md §6）。
func TestInjectedIDGenerator_Publish_UsesInjected(t *testing.T) {
	d := newStubDriver()
	pub := event.NewPublisher(d, event.WithIDGenerator(func() string { return "gen-id" }))
	ev := event.Declare[pubPayload]("unit.gen")

	if err := ev.Publish(context.Background(), pub, pubPayload{"x"}); err != nil {
		t.Fatalf("publish: %v", err)
	}
	appends := d.appended()
	if len(appends) != 1 || appends[0].ID != "gen-id" {
		t.Fatalf("injected ID generator must be used, got %+v", appends)
	}
}

// WithPublisherCodec 注入的 codec 替代缺省 JSON（design.md §6）。
func TestInjectedCodec_Publish_EncodesWithInjected(t *testing.T) {
	d := newStubDriver()
	pub := event.NewPublisher(d, event.WithPublisherCodec(&prefixCodec{}))
	ev := event.Declare[pubPayload]("unit.codec")

	if err := ev.Publish(context.Background(), pub, pubPayload{"x"}); err != nil {
		t.Fatalf("publish: %v", err)
	}
	appends := d.appended()
	if len(appends) != 1 || !bytes.HasPrefix(appends[0].Payload, []byte("prefixed:")) {
		t.Fatalf("injected codec must encode payload, got %q", appends[0].Payload)
	}
}

// WithMetadata 合并透传（多次选项合并；未提供时不携带元数据）。
func TestPublishMetadata_Merges_AndPassesThrough(t *testing.T) {
	d := newStubDriver()
	pub := event.NewPublisher(d)
	ev := event.Declare[pubPayload]("unit.meta")
	ctx := context.Background()

	if err := ev.Publish(ctx, pub, pubPayload{"a"},
		event.WithMetadata(map[string]string{"trace_id": "tr-1"}),
		event.WithMetadata(map[string]string{"k": "v"}),
	); err != nil {
		t.Fatalf("publish with metadata: %v", err)
	}
	if err := ev.Publish(ctx, pub, pubPayload{"b"}); err != nil {
		t.Fatalf("publish without metadata: %v", err)
	}

	appends := d.appended()
	if appends[0].Metadata["trace_id"] != "tr-1" || appends[0].Metadata["k"] != "v" {
		t.Fatalf("multiple WithMetadata must merge, got %+v", appends[0].Metadata)
	}
	if appends[1].Metadata != nil {
		t.Fatalf("publish without metadata must not carry metadata, got %+v", appends[1].Metadata)
	}
}

// 编码失败：Publish 返回错误且不落库（编码在 Append 前短路）。
func TestEncodeFailure_Publish_ReturnsErrorWithoutAppend(t *testing.T) {
	d := newStubDriver()
	pub := event.NewPublisher(d)
	ev := event.Declare[chan int]("unit.bad") // chan 无法 JSON 编组

	if err := ev.Publish(context.Background(), pub, make(chan int)); err == nil {
		t.Fatal("encode failure must return error")
	}
	if n := len(d.appended()); n != 0 {
		t.Fatalf("failed publish must not append, got %d appends", n)
	}
}

// Append 失败：Publish 透传错误。
func TestAppendFailure_Publish_ReturnsError(t *testing.T) {
	d := newStubDriver()
	d.mu.Lock()
	d.appendErr = errStub
	d.mu.Unlock()
	pub := event.NewPublisher(d)
	ev := event.Declare[pubPayload]("unit.appenderr")

	if err := ev.Publish(context.Background(), pub, pubPayload{"x"}); !errors.Is(err, errStub) {
		t.Fatalf("append failure must propagate, got %v", err)
	}
}

// 无参事件糖：PublishNoPayload 等价 Publish(NoPayload{})。
func TestPublishNoPayload_Appends(t *testing.T) {
	d := newStubDriver()
	pub := event.NewPublisher(d)
	ev := event.Declare[event.NoPayload]("unit.nop")

	if err := pub.PublishNoPayload(context.Background(), ev); err != nil {
		t.Fatalf("publish no-payload: %v", err)
	}
	appends := d.appended()
	if len(appends) != 1 || appends[0].Type != ev.Name {
		t.Fatalf("no-payload publish must append envelope, got %+v", appends)
	}
}

// ---- 测试用 codec ----

// prefixCodec 在 JSON 编码外加固定前缀，验证注入生效。
type prefixCodec struct{}

func (*prefixCodec) Encode(payload any) ([]byte, error) {
	b, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	return append([]byte("prefixed:"), b...), nil
}

func (*prefixCodec) Decode(data []byte, into any) error {
	return event.NewJSONCodec().Decode(bytes.TrimPrefix(data, []byte("prefixed:")), into)
}
