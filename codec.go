package event

import (
	"encoding/json"
	"fmt"
)

// Codec payload 编解码，缺省 JSON，可插拔（design.md §6）。
type Codec interface {
	Encode(payload any) ([]byte, error)
	Decode(data []byte, into any) error
}

// NewJSONCodec 构造缺省 JSON codec。
func NewJSONCodec() *JSONCodec { return &JSONCodec{} }

// JSONCodec 基于 encoding/json 的缺省 codec。
type JSONCodec struct{}

func (*JSONCodec) Encode(payload any) ([]byte, error) {
	b, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("event: encode payload: %w", err)
	}
	return b, nil
}

func (*JSONCodec) Decode(data []byte, into any) error {
	if err := json.Unmarshal(data, into); err != nil {
		return fmt.Errorf("event: decode payload: %w", err)
	}
	return nil
}
