package pipeline

import (
	"fmt"
	"strings"
	"time"
)

// entry 一条运行流水记录，报告的唯一数据来源。
// Seq 保证同一时间戳下顺序确定，报告可复现。
type entry struct {
	Seq  int
	At   time.Time
	Kind string // correlation / health / alert
	Msg  string
}

// Report 一次运行的判定报告，各段落均为已渲染文本，可直接打印。
type Report struct {
	// Correlation 关联标识走向（生成/继承/替换/派生/归还）。
	Correlation []string
	// Metrics 指标守恒结论与全局汇总。
	Metrics []string
	// Health 健康结论变化序列。
	Health []string
	// Alerts 告警触发与恢复序列。
	Alerts []string
	// Conserved 指标是否守恒。
	Conserved bool
}

// String 渲染为人可读的完整报告；无数据时各段落给出明确说明，不报错。
func (r Report) String() string {
	var b strings.Builder
	writeSection := func(title string, lines []string, emptyHint string) {
		fmt.Fprintf(&b, "== %s ==\n", title)
		if len(lines) == 0 {
			fmt.Fprintf(&b, "  %s\n", emptyHint)
			return
		}
		for _, l := range lines {
			fmt.Fprintf(&b, "  %s\n", l)
		}
	}
	writeSection("关联标识走向", r.Correlation, "无关联记录")
	writeSection("指标守恒", r.Metrics, "无指标记录")
	writeSection("健康结论变化", r.Health, "无健康判定记录")
	writeSection("告警触发与恢复", r.Alerts, "无告警判定记录")
	if r.Conserved {
		b.WriteString("== 总结 ==\n  指标守恒: 是\n")
	} else {
		b.WriteString("== 总结 ==\n  指标守恒: 否\n")
	}
	return b.String()
}

// Report 汇总运行流水，生成人可读的判定报告。
//
// 报告内容完全来自运行期间记录的流水（时间戳取自注入时钟），
// 同样输入重跑得到同样报告；没有任何记录时也能正常输出。
func (p *Pipeline) Report() Report {
	p.mu.Lock()
	journal := make([]entry, len(p.journal))
	copy(journal, p.journal)
	p.mu.Unlock()

	var r Report
	for _, e := range journal {
		line := fmt.Sprintf("[%s] %s", e.At.Format(time.RFC3339Nano), e.Msg)
		switch e.Kind {
		case "correlation":
			r.Correlation = append(r.Correlation, line)
		case "health":
			r.Health = append(r.Health, line)
		case "alert":
			r.Alerts = append(r.Alerts, line)
		}
	}

	// 健康段落：按 state/reason 归纳结论变化（详情随样本数变化，不参与比较）。
	r.Health = summarizeChanges(journal, "health", verdictKey)

	// 告警段落：按 firing 状态归纳触发/恢复变化。
	r.Alerts = summarizeChanges(journal, "alert", firingKey)

	// 指标段落：全局汇总 + 守恒校验，与 Summary 完全一致。
	s := p.Summary()
	r.Conserved = s.Conserved()
	r.Metrics = append(r.Metrics,
		fmt.Sprintf("started=%d finished=%d failed=%d background=%d",
			s.Started, s.Finished, s.Failed, s.Background),
		fmt.Sprintf("counters=%s", formatCounters(s.Counters)),
		fmt.Sprintf("latency: count=%d sum=%s min=%s max=%s mean=%s",
			s.Latency.Count, s.Latency.Sum, s.Latency.Min, s.Latency.Max, s.Latency.Mean()),
		fmt.Sprintf("守恒校验: started==finished 且账本与计数器一致 -> %v", s.Conserved()),
	)
	return r
}

// summarizeChanges 从流水中筛出某类记录；健康/告警判定相邻重复时
// 只保留结论变化的条目（首条必保留），让"什么时候变的"一眼可见。
func summarizeChanges(journal []entry, kind string, keyOf func(string) string) []string {
	var out []string
	prev := ""
	first := true
	for _, e := range journal {
		if e.Kind != kind {
			continue
		}
		key := keyOf(e.Msg)
		if first || key != prev {
			out = append(out, fmt.Sprintf("[%s] %s", e.At.Format(time.RFC3339Nano), e.Msg))
			prev = key
			first = false
		}
	}
	return out
}

// verdictKey 提取健康判定消息中的 state/reason 作为变化比较键。
func verdictKey(msg string) string {
	state, reason := "", ""
	for _, tok := range strings.Fields(msg) {
		if strings.HasPrefix(tok, "state=") {
			state = tok
		}
		if strings.HasPrefix(tok, "reason=") {
			reason = tok
		}
	}
	return state + " " + reason
}

// firingKey 提取告警判定消息中的 firing 状态作为变化比较键。
func firingKey(msg string) string {
	for _, tok := range strings.Fields(msg) {
		if strings.HasPrefix(tok, "firing=") {
			return tok
		}
	}
	return msg
}

// formatCounters 按名称排序渲染计数器，保证报告输出确定。
func formatCounters(c map[string]int64) string {
	if len(c) == 0 {
		return "(none)"
	}
	names := make([]string, 0, len(c))
	for n := range c {
		names = append(names, n)
	}
	// 简单插入排序，避免引入 sort 包外的顺序不确定性。
	for i := 1; i < len(names); i++ {
		for j := i; j > 0 && names[j] < names[j-1]; j-- {
			names[j], names[j-1] = names[j-1], names[j]
		}
	}
	var b strings.Builder
	for i, n := range names {
		if i > 0 {
			b.WriteString(" ")
		}
		fmt.Fprintf(&b, "%s=%d", n, c[n])
	}
	return b.String()
}
