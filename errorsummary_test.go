package event_test

import (
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/jninng/go_event"
)

// SanitizeErrorSummary 清洗口径（docs/design.md §5.3）：去控制字符、
// 4 KiB 字节截断（不撕裂 UTF-8 码点）、空串透传。
func TestSanitizeErrorSummary(t *testing.T) {
	t.Run("EmptyText_Sanitize_ReturnsEmpty", func(t *testing.T) {
		if got := event.SanitizeErrorSummary(""); got != "" {
			t.Fatalf("SanitizeErrorSummary(\"\") = %q, want empty", got)
		}
	})

	t.Run("PlainText_Sanitize_ReturnsUnchanged", func(t *testing.T) {
		const text = "boom: delivery failed (order=42)"
		if got := event.SanitizeErrorSummary(text); got != text {
			t.Fatalf("SanitizeErrorSummary = %q, want unchanged %q", got, text)
		}
	})

	t.Run("ControlCharsMixed_Sanitize_StripsAllControlRunes", func(t *testing.T) {
		// C0（含 \n \t \r 与 ESC）、DEL 与 C1 一律移除；普通字符保留
		//（ESC 之后的转义序列残留属普通文本，只去控制字符本身）。
		const text = "a\x00b\x1b[31mc\nd\te\rf\x7fg\u0085h\u009bi"
		const want = "ab[31mcdefghi"
		if got := event.SanitizeErrorSummary(text); got != want {
			t.Fatalf("SanitizeErrorSummary = %q, want %q", got, want)
		}
	})

	t.Run("NewlineInput_Sanitize_StripsNewlines", func(t *testing.T) {
		const text = "line1\nline2"
		if got := event.SanitizeErrorSummary(text); strings.ContainsAny(got, "\n\r\t") {
			t.Fatalf("SanitizeErrorSummary = %q, want newlines stripped", got)
		}
	})

	t.Run("ExactlyLimitText_Sanitize_ReturnsUnchanged", func(t *testing.T) {
		text := strings.Repeat("a", event.MaxErrorSummaryBytes)
		if got := event.SanitizeErrorSummary(text); got != text {
			t.Fatalf("boundary text must be unchanged, got %d bytes", len(got))
		}
	})

	t.Run("OverLimitText_Sanitize_TruncatesToMaxBytes", func(t *testing.T) {
		text := strings.Repeat("a", event.MaxErrorSummaryBytes+1000)
		got := event.SanitizeErrorSummary(text)
		if len(got) != event.MaxErrorSummaryBytes {
			t.Fatalf("truncated length = %d, want %d", len(got), event.MaxErrorSummaryBytes)
		}
	})

	t.Run("OverLimitMultibyteText_Sanitize_TruncatesWithoutTearingRune", func(t *testing.T) {
		// 4095 字节 ASCII + 3 字节汉字（共 4098）：末尾码点放不下整体丢弃，
		// 结果仍是合法 UTF-8 且为保留前缀。
		text := strings.Repeat("a", event.MaxErrorSummaryBytes-1) + "错错错"
		got := event.SanitizeErrorSummary(text)
		if !utf8.ValidString(got) {
			t.Fatalf("truncated summary must stay valid UTF-8, got %q", got)
		}
		if len(got) != event.MaxErrorSummaryBytes-1 {
			t.Fatalf("truncated length = %d, want %d (rune that does not fit is dropped)",
				len(got), event.MaxErrorSummaryBytes-1)
		}
		if !strings.HasPrefix(text, got) {
			t.Fatalf("truncated summary must be a prefix of the input")
		}
	})

	t.Run("ControlCharsOnly_Sanitize_ReturnsEmpty", func(t *testing.T) {
		if got := event.SanitizeErrorSummary("\x00\x01\n\t\x7f"); got != "" {
			t.Fatalf("control-only input = %q, want empty", got)
		}
	})

	t.Run("LineSeparatorRunes_Sanitize_PreservedAsNonControl", func(t *testing.T) {
		// U+2028（行分隔）/U+2029（段分隔）属 Zl/Zp，不在 Cc 内，保留。
		const text = "a\u2028b\u2029c"
		if got := event.SanitizeErrorSummary(text); got != text {
			t.Fatalf("U+2028/U+2029 are not control chars and must be preserved, got %q", got)
		}
	})

	t.Run("InvalidUTF8Bytes_Sanitize_ReplacesWithRuneError", func(t *testing.T) {
		got := event.SanitizeErrorSummary("bad\xffbyte")
		if !utf8.ValidString(got) {
			t.Fatalf("summary must stay valid UTF-8, got %q", got)
		}
		if !strings.Contains(got, "bad\uFFFDbyte") {
			t.Fatalf("summary = %q, want invalid byte replaced by U+FFFD", got)
		}
	})

	t.Run("ControlCharsInOverLimitText_Sanitize_TruncatesAfterStripping", func(t *testing.T) {
		// 先去控制字符再截断：截断上限按清洗后的字节计。
		text := strings.Repeat("a\x00", event.MaxErrorSummaryBytes) // 清洗后 4096 字节
		got := event.SanitizeErrorSummary(text)
		if len(got) != event.MaxErrorSummaryBytes {
			t.Fatalf("stripped length = %d, want %d", len(got), event.MaxErrorSummaryBytes)
		}
		if strings.ContainsAny(got, "\x00") {
			t.Fatalf("summary must not contain control chars")
		}
	})
}
