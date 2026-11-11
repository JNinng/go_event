package inmem_test

import (
	"testing"
	"time"

	"github.com/jninng/go_event"
	"github.com/jninng/go_event/eventtest"
	"github.com/jninng/go_event/eventtest/inmem"
)

// inmem Driver 必须全过契约套件（语义即测试，design.md §9.1）。
func TestInmemContractSuite(t *testing.T) {
	eventtest.RunSuite(t, inmem.New())
}

// 保留窗口惰性清理契约：每用例独立的短窗口全新 Driver（design.md §5.2）。
func TestInmemRetentionSuite(t *testing.T) {
	eventtest.RunRetentionSuite(t, func(t *testing.T) event.Driver {
		return inmem.New(inmem.WithRetention(2 * time.Second))
	})
}
