package event

import (
	"context"
	"errors"
	"testing"
)

// handler 正常返回：callHandler 透传返回值与 error。
func TestHandlerError_CallHandler_Propagates(t *testing.T) {
	want := errors.New("handler failed")
	err := callHandler(func(context.Context, RawEnvelope) error { return want },
		context.Background(), RawEnvelope{})
	if !errors.Is(err, want) {
		t.Fatalf("callHandler must propagate handler error, got %v", err)
	}
}

// handler panic：转为 error，不向调用方传播 panic。
func TestHandlerPanic_CallHandler_ConvertsToError(t *testing.T) {
	err := callHandler(func(context.Context, RawEnvelope) error {
		panic("boom")
	}, context.Background(), RawEnvelope{})
	if err == nil {
		t.Fatal("panic must be converted to error")
	}
	if got := err.Error(); got == "" {
		t.Fatal("converted error must carry context")
	}
}

// handler 正常完成：返回 nil。
func TestHandlerSuccess_CallHandler_ReturnsNil(t *testing.T) {
	err := callHandler(func(context.Context, RawEnvelope) error { return nil },
		context.Background(), RawEnvelope{})
	if err != nil {
		t.Fatalf("successful handler must return nil, got %v", err)
	}
}
