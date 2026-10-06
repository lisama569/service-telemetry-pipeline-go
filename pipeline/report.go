package pipeline

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/lisama569/service-telemetry-pipeline-go/alert"
	"github.com/lisama569/service-telemetry-pipeline-go/correlation"
	"github.com/lisama569/service-telemetry-pipeline-go/health"
	"github.com/lisama569/service-telemetry-pipeline-go/metrics"
)

// CorrelationRecord 一次关联判定记录。
type CorrelationRecord struct {
	At       time.Time
	Kind     string // request / background
	Upstream string
	Context  correlation.Context
	Outcome  string // ok / error
}

// HealthRecord 一次健康结论变化。
type HealthRecord struct {
	At      time.Time
	Verdict health.Verdict
}

// AlertRecord 一次告警触发或恢复。
type AlertRecord struct {
	At       time.Time
	Decision alert.Decision
}

// Report 一次运行的判定报告。
//
// 报告内容全部由运行期记录与指标注册表快照生成：同样的输入（注入逻辑
// 时钟与确定性 ID 生成器）重跑得到相同报告；Counters/Latency 直接取自
// 全局指标注册表，与全局汇总严格一致。无数据时各节输出占位说明，不报错。
type Report struct {
	GeneratedAt       time.Time
	Correlation       []CorrelationRecord
	HealthTransitions []HealthRecord
	AlertTransitions  []AlertRecord
	Counters          map[string]int64
	Latency           metrics.Snapshot
}

// String 输出人可读的报告文本。
func (r Report) String() string {
	var b strings.Builder
	fmt.Fprintf(&b, "=== 运行报告 (generated_at=%s) ===\n", r.GeneratedAt.Format(time.RFC3339Nano))

	b.WriteString("\n-- 关联标识走向 --\n")
	if len(r.Correlation) == 0 {
		b.WriteString("(无数据)\n")
	}
	for _, c := range r.Correlation {
		upstream := c.Upstream
		if upstream == "" {
			upstream = "(缺失)"
		}
		fmt.Fprintf(&b, "[%s] kind=%s upstream=%s %s outcome=%s\n",
			c.At.Format(time.RFC3339Nano), c.Kind, upstream, c.Context, c.Outcome)
	}

	b.WriteString("\n-- 健康结论变化 --\n")
	if len(r.HealthTransitions) == 0 {
		b.WriteString("(无数据)\n")
	}
	for _, h := range r.HealthTransitions {
		fmt.Fprintf(&b, "[%s] %s\n", h.At.Format(time.RFC3339Nano), h.Verdict)
	}

	b.WriteString("\n-- 告警触发与恢复 --\n")
	if len(r.AlertTransitions) == 0 {
		b.WriteString("(无数据)\n")
	}
	for _, a := range r.AlertTransitions {
		phase := "触发"
		if !a.Decision.Firing {
			phase = "恢复"
		}
		fmt.Fprintf(&b, "[%s] %s %s\n", a.At.Format(time.RFC3339Nano), phase, a.Decision)
	}

	b.WriteString("\n-- 指标汇总(与全局一致) --\n")
	if len(r.Counters) == 0 {
		b.WriteString("(无数据)\n")
	}
	keys := make([]string, 0, len(r.Counters))
	for k := range r.Counters {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		fmt.Fprintf(&b, "%s=%d\n", k, r.Counters[k])
	}
	fmt.Fprintf(&b, "latency: count=%d sum=%s min=%s max=%s mean=%s\n",
		r.Latency.Count, r.Latency.Sum, r.Latency.Min, r.Latency.Max, r.Latency.Mean())
	return b.String()
}

// recorder 运行期事件记录器，并发安全。
type recorder struct {
	mu sync.Mutex

	correlations []CorrelationRecord

	healthInit        bool
	lastHealth        health.Verdict
	healthTransitions []HealthRecord

	alertFiring      map[string]bool // 按 规则名@版本 跟踪 firing 状态
	alertTransitions []AlertRecord
}

func newRecorder() *recorder {
	return &recorder{alertFiring: make(map[string]bool)}
}

func (r *recorder) recordCorrelation(c CorrelationRecord) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.correlations = append(r.correlations, c)
}

// recordHealth 仅在结论（状态或原因）变化时记录一次转变。
func (r *recorder) recordHealth(at time.Time, v health.Verdict) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.healthInit && r.lastHealth.State == v.State && r.lastHealth.Reason == v.Reason {
		return
	}
	r.healthInit = true
	r.lastHealth = v
	r.healthTransitions = append(r.healthTransitions, HealthRecord{At: at, Verdict: v})
}

// recordAlert 仅在 firing 状态翻转时记录一次触发/恢复。
func (r *recorder) recordAlert(d alert.Decision) {
	r.mu.Lock()
	defer r.mu.Unlock()
	key := fmt.Sprintf("%s@%d", d.RuleName, d.RuleVersion)
	if r.alertFiring[key] == d.Firing {
		return
	}
	r.alertFiring[key] = d.Firing
	r.alertTransitions = append(r.alertTransitions, AlertRecord{At: d.WindowEnd, Decision: d})
}

// report 生成报告快照；指标直接取自注册表，保证与全局汇总一致。
// 关联记录按 (时间, 类型, 上游标识, RequestID) 排序：并发下记录到达顺序
// 不定，排序后同样的输入重跑得到相同的报告输出。
func (r *recorder) report(now time.Time, reg *metrics.Registry) Report {
	r.mu.Lock()
	defer r.mu.Unlock()
	correlations := append([]CorrelationRecord(nil), r.correlations...)
	sort.Slice(correlations, func(i, j int) bool {
		a, b := correlations[i], correlations[j]
		if !a.At.Equal(b.At) {
			return a.At.Before(b.At)
		}
		if a.Kind != b.Kind {
			return a.Kind < b.Kind
		}
		if a.Upstream != b.Upstream {
			return a.Upstream < b.Upstream
		}
		return a.Context.RequestID < b.Context.RequestID
	})
	rep := Report{
		GeneratedAt:       now,
		Correlation:       correlations,
		HealthTransitions: append([]HealthRecord(nil), r.healthTransitions...),
		AlertTransitions:  append([]AlertRecord(nil), r.alertTransitions...),
		Counters:          reg.Counters(),
		Latency:           reg.Latency(MetricRequestLatency).Snapshot(),
	}
	return rep
}
