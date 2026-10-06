// Package pipeline 把 correlation、metrics、health、alert 四个子系统
// 拼装成一个可直接复用的请求处理流程：
//
//   - HandleRequest 处理一次同步/并发请求：建立关联上下文、记录指标、
//     上报探针、失败时摄入告警事件，结束时（无论成败）归还关联标识；
//   - SpawnBackground 从某个请求上下文派生后台任务：挂在同一条 TraceID
//     链路上，不另起链路，结束时同样归还标识；
//   - CheckHealth 做一次健康判定并记录结论变化；
//   - 全程把各环节判定写入日志流水（journal），跑完后用 Report 输出
//     人可读的判定报告。
//
// 时钟可注入（Options.Now），同样的输入序列重跑可得到可复现的报告。
package pipeline

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lisama569/service-telemetry-pipeline-go/alert"
	"github.com/lisama569/service-telemetry-pipeline-go/correlation"
	"github.com/lisama569/service-telemetry-pipeline-go/health"
	"github.com/lisama569/service-telemetry-pipeline-go/metrics"
)

// 指标名，报告的全局汇总与守恒校验均基于这些名称。
const (
	MetricRequestsTotal   = "requests_total"
	MetricRequestsFailed  = "requests_failed"
	MetricRequestLatency  = "request_latency"
	MetricBackgroundTotal = "background_total"
)

// Options Pipeline 配置。
type Options struct {
	// Now 时钟；nil 时使用 time.Now。注入固定步进时钟可让报告可复现。
	Now func() time.Time
	// AlertKey 失败事件摄入告警评估器时使用的规则 Key；为空则不摄入。
	AlertKey string
	// ProbeTimeout 单次请求耗时达到该值时，探针结果视为依赖超时。
	// <=0 时由 health.Checker 自身配置决定（此处仅用于生成探针耗时）。
	ProbeTimeout time.Duration
}

// Pipeline 可复用的可观测性处理流程，并发安全。
type Pipeline struct {
	Tracker *correlation.Tracker
	Metrics *metrics.Registry
	Health  *health.Checker
	Alerts  *alert.Evaluator

	opts Options

	// 内部账本，用于与 Registry 计数器做守恒校验。
	started  atomic.Int64
	finished atomic.Int64
	failed   atomic.Int64
	bgTotal  atomic.Int64

	mu      sync.Mutex
	journal []entry
}

// New 创建 Pipeline；各子系统为 nil 时使用默认实例。
func New(tr *correlation.Tracker, reg *metrics.Registry, hc *health.Checker, ev *alert.Evaluator, opts Options) *Pipeline {
	if tr == nil {
		tr = correlation.NewTracker(nil)
	}
	if reg == nil {
		reg = metrics.NewRegistry()
	}
	if hc == nil {
		hc = health.NewChecker(health.Config{})
	}
	if ev == nil {
		ev = alert.NewEvaluator()
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}
	return &Pipeline{Tracker: tr, Metrics: reg, Health: hc, Alerts: ev, opts: opts}
}

// HandleRequest 处理一次请求（同步或被并发调用均可）：
// 建立/继承关联上下文，执行 fn，记录指标与探针，失败时摄入告警事件；
// 无论 fn 成功或出错，结束时都归还 RequestID，标识可被后续请求重新继承。
//
// 返回携带追踪上下文的 ctx（可用于派生后台任务）、关联上下文与 fn 的错误。
func (p *Pipeline) HandleRequest(ctx context.Context, upstreamID string, fn func(ctx context.Context) error) (context.Context, correlation.Context, error) {
	ctx, tc := p.Tracker.Ensure(ctx, upstreamID)
	p.logf("correlation", "request start: %s upstream=%q", tc, upstreamID)
	p.started.Add(1)
	p.Metrics.Counter(MetricRequestsTotal).Inc(1)

	start := p.opts.Now()
	err := fn(ctx)
	dur := p.opts.Now().Sub(start)

	// 探针结果：出错时按错误耗时记录，供健康判定区分超时/非超时。
	probe := health.ProbeResult{At: p.opts.Now(), Err: err, Duration: dur}
	p.Health.Record(probe)
	p.Metrics.Latency(MetricRequestLatency).Observe(dur)
	if err != nil {
		p.failed.Add(1)
		p.Metrics.Counter(MetricRequestsFailed).Inc(1)
		p.ingestAlert(probe.At, 1)
		p.logf("correlation", "request done: %s err=%v", tc, err)
	} else {
		// 成功也摄入一条未命中事件，推动告警窗口前进，使恢复可被观测。
		p.ingestAlert(probe.At, 0)
		p.logf("correlation", "request done: %s ok", tc)
	}

	p.finished.Add(1)
	p.Tracker.Release(tc.RequestID)
	p.logf("correlation", "request released: request=%s active=%d", tc.RequestID, p.Tracker.Active())
	return ctx, tc, err
}

// SpawnBackground 从 reqCtx（通常由 HandleRequest 返回）派生一个后台任务：
// 通过 Tracker.Spawn 挂在派生它的请求的同一条 TraceID 上，不另起链路；
// 在 goroutine 中执行 fn，结束时归还标识并记录指标。
func (p *Pipeline) SpawnBackground(reqCtx context.Context, name string, fn func(ctx context.Context) error) {
	ctx, tc := p.Tracker.Spawn(reqCtx)
	parent, _ := correlation.FromContext(reqCtx)
	p.logf("correlation", "background %q spawned: %s parent_request=%s", name, tc, parent.RequestID)
	p.bgTotal.Add(1)
	p.started.Add(1)
	p.Metrics.Counter(MetricBackgroundTotal).Inc(1)
	go func() {
		start := p.opts.Now()
		err := fn(ctx)
		p.Metrics.Latency(MetricRequestLatency).Observe(p.opts.Now().Sub(start))
		if err != nil {
			p.failed.Add(1)
			p.Metrics.Counter(MetricRequestsFailed).Inc(1)
			p.Health.Record(health.ProbeResult{At: p.opts.Now(), Err: err})
			p.ingestAlert(p.opts.Now(), 1)
			p.logf("correlation", "background %q done: %s err=%v", name, tc, err)
		} else {
			p.ingestAlert(p.opts.Now(), 0)
			p.logf("correlation", "background %q done: %s ok", name, tc)
		}
		p.finished.Add(1)
		p.Tracker.Release(tc.RequestID)
	}()
}

// ingestAlert 向告警评估器摄入一条事件（value=1 表示失败命中，0 表示成功）
// 并记录判定；触发/恢复由报告按 firing 状态变化归纳。
func (p *Pipeline) ingestAlert(at time.Time, value float64) {
	if p.opts.AlertKey == "" {
		return
	}
	d, triggered := p.Alerts.Ingest(alert.Event{At: at, Key: p.opts.AlertKey, Value: value})
	p.logf("alert", "ingest: %s triggered=%v", d, triggered)
}

// CheckHealth 在 now 时刻做一次健康判定，记录结论供报告归纳变化。
func (p *Pipeline) CheckHealth(now time.Time) health.Verdict {
	v := p.Health.Evaluate(now)
	p.logf("health", "evaluate: %s", v)
	return v
}

// Summary 全局汇总：内部账本与 Registry 计数器的一致性快照。
type Summary struct {
	Started    int64
	Finished   int64
	Failed     int64
	Background int64
	Counters   map[string]int64
	Latency    metrics.Snapshot
}

// Conserved 校验指标守恒：账本与计数器一致，且 started==finished。
// started/finished 覆盖请求与后台任务；requests_total 只计同步/并发请求。
func (s Summary) Conserved() bool {
	if s.Started != s.Finished {
		return false
	}
	if s.Counters[MetricRequestsTotal] != s.Started-s.Background {
		return false
	}
	if s.Counters[MetricRequestsFailed] != s.Failed {
		return false
	}
	if s.Counters[MetricBackgroundTotal] != s.Background {
		return false
	}
	return s.Latency.Count == s.Started
}

// Summary 返回全局汇总快照。
func (p *Pipeline) Summary() Summary {
	return Summary{
		Started:    p.started.Load(),
		Finished:   p.finished.Load(),
		Failed:     p.failed.Load(),
		Background: p.bgTotal.Load(),
		Counters:   p.Metrics.Counters(),
		Latency:    p.Metrics.Latency(MetricRequestLatency).Snapshot(),
	}
}

// logf 追加一条带时钟时间戳的流水记录（报告的数据来源）。
func (p *Pipeline) logf(kind, format string, args ...any) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.journal = append(p.journal, entry{
		Seq:  len(p.journal),
		At:   p.opts.Now(),
		Kind: kind,
		Msg:  fmt.Sprintf(format, args...),
	})
}
