package event

import (
	"unicode"
	"unicode/utf8"
)

// MaxErrorSummaryBytes 是错误摘要落库的字节上限（4 KiB）。摘要是不可信
// 输入的落库事实：超限截断（不撕裂 UTF-8 码点）、去控制字符、不含堆栈
// （docs/design.md §5.3）。
const MaxErrorSummaryBytes = 4 << 10 // 4096

// SanitizeErrorSummary 把不可信错误文本清洗为可落库摘要：移除全部
// Unicode 控制字符（Unicode Cc 类：C0/C1/DEL，含换行与制表符——非控制
// 字符如 U+2028/U+2029 分隔符原样保留），非法字节以 UTF-8 替换符
// （U+FFFD）保留，按字节上限截断且不撕裂 UTF-8 码点。清洗是落库前的
// 最后一步，各后端写入 last_error 前必须经此处理；堆栈不入库——摘要
// 只承载错误文本，handler 侧堆栈由其自身观测承担。空串输入返回空串。
func SanitizeErrorSummary(text string) string {
	cleaned := make([]byte, 0, min(len(text), MaxErrorSummaryBytes))
	for _, r := range text {
		if unicode.IsControl(r) {
			continue
		}
		size := utf8.RuneLen(r)
		if len(cleaned)+size > MaxErrorSummaryBytes {
			break
		}
		cleaned = utf8.AppendRune(cleaned, r)
	}
	return string(cleaned)
}
