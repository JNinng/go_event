package event_test

import (
	"testing"

	"github.com/jninng/go_event"
)

type codecPayload struct {
	Value string `json:"value"`
}

// 正常载荷编解码往返一致（design.md §6）。
func TestRoundtripPayload_Codec_PreservesValue(t *testing.T) {
	c := event.NewJSONCodec()
	in := codecPayload{Value: "hello"}

	b, err := c.Encode(in)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	var out codecPayload
	if err := c.Decode(b, &out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if out != in {
		t.Fatalf("roundtrip mismatch: %+v vs %+v", out, in)
	}
}

// 不支持编组的载荷返回错误（异常路径显式断言）。
func TestUnsupportedPayload_Encode_ReturnsError(t *testing.T) {
	c := event.NewJSONCodec()

	if _, err := c.Encode(chan int(nil)); err == nil {
		t.Fatal("encoding unsupported payload must return error")
	}
}

// 非法 JSON 解码返回错误（异常路径显式断言）。
func TestMalformedJSON_Decode_ReturnsError(t *testing.T) {
	c := event.NewJSONCodec()

	var into codecPayload
	if err := c.Decode([]byte("{not json"), &into); err == nil {
		t.Fatal("decoding malformed JSON must return error")
	}
}

// 类型不匹配的解码返回错误（异常路径显式断言）。
func TestShapeMismatch_Decode_ReturnsError(t *testing.T) {
	c := event.NewJSONCodec()

	var into int
	if err := c.Decode([]byte(`{"value":"x"}`), &into); err == nil {
		t.Fatal("decoding into mismatched shape must return error")
	}
}
