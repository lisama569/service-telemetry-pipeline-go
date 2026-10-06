// Package pipeline 将 correlation/metrics/health/alert 组装成可直接复用的
// 处理流程：调用方只需 New 一次，即可处理同步请求、并发任务与后台任务，
// 并在结束后输出可解释、可复现的运行报告。
//
// 语义：
//   - HandleRequest 处理同步/并发请求：建立关联上下文、记录指标、按结果
//     上报探针与告警事件，结束时（无论成功或出错）归还标识，标识可被后续
//     请求重新继承；
//   - SpawnBackground 从请求上下文派生后台任务：TraceID 挂在派生它的请求
//     链路上，不另起链路，RequestID 重新生成避免串号；
//   - 全程使用注入的时钟（默认 time.Now），注入逻辑时钟并配合确定性 ID
//     生成器（correlation.WithIDFunc）可让运行报告完全可复现。
package pipeline

import (
	"context"
	"time"

	"github.com/lisama569/service-telemetry-pipeline-go/alert"
	"github.com/lisama569/service-telemetry-pipeline-go/correlation"
	"github.com/lisama569/service-telemetry-pipeline-go/health"
	"github.com/lisama569/service-telemetry-pipeline-go/metrics"
)

// 流程内使用的指标名。
const (
	MetricRequestsTotal  = "requests_total"
	MetricRequestsFailed = "requests_failed"
	MetricRequestLatency = "request_latency"
)

// AlertKeyFailures 失败事件上报的告警键。
const AlertKeyFailures = "failures"

// Config 组装配置。
type Config struct {
	// Health 健康判定配置。
	Health health.Config
	// Rules 告警规则（失败事件按 AlertKeyFailures 键上报）。
	Rules []alert.Rule
	// Now 时钟；nil 使用 time.Now。注入逻辑时钟可让报告可复现。
	Now func() time.Time
	// TrackerOpts 关联标识选项（容量上限、确定性 ID 生成器等）。
	TrackerOpts []correlation.Option
}

// Pipeline 可直接复用的可观测性处理流程。
type Pipeline struct {
	Tracker *correlation.Tracker
	Metrics *metrics.Registry
	Health  *health.Checker
	Alerts  *alert.Evaluator

	now   func() time.Time
	rules []alert.Rule
	rec   *recorder
}

// New 组装一条处理流程。
func New(cfg Config) *Pipeline {
	now := cfg.Now
	if now == nil {
		now = time.Now
	}
	return &Pipeline{
		Tracker: correlation.NewTracker(nil, cfg.TrackerOpts...),
		Metrics: metrics.NewRegistry(),
		Health:  health.NewChecker(cfg.Health),
		Alerts:  alert.NewEvaluator(cfg.Rules...),
		now:     now,
		rules:   cfg.Rules,
		rec:     newRecorder(),
	}
}

// HandleRequest 处理一个同步/并发请求：建立关联上下文、记录指标、执行 fn、
// 按结果上报探针与告警事件。结束时（无论成功或出错）归还标识。
// 若 ctx 已有关联上下文（嵌套调用），直接沿用且不由本次调用归还。
func (p *Pipeline) HandleRequest(ctx context.Context, upstreamID string, fn func(ctx context.Context) error) (context.Context, error) {
	_, existed := correlation.FromContext(ctx)
	ctx, tc := p.Tracker.Ensure(ctx, upstreamID)
	if !existed {
		defer p.Tracker.Release(tc.RequestID)
	}
	return ctx, p.process(ctx, tc, "request", upstreamID, fn)
}

// SpawnBackground 从请求上下文派生后台任务：TraceID 挂在派生它的请求链路上，
// RequestID 重新生成。ctx 中无上下文时等价于 HandleRequest(ctx, "", fn)。
func (p *Pipeline) SpawnBackground(ctx context.Context, fn func(ctx context.Context) error) (context.Context, error) {
	ctx, tc := p.Tracker.Spawn(ctx)
	defer p.Tracker.Release(tc.RequestID)
	return ctx, p.process(ctx, tc, "background", "", fn)
}

// process 执行 fn 并完成指标、探针、告警与记录。
func (p *Pipeline) process(ctx context.Context, tc correlation.Context, kind, upstream string, fn func(ctx context.Context) error) error {
	p.Metrics.Counter(MetricRequestsTotal).Inc(1)
	start := p.now()
	err := fn(ctx)
	dur := p.now().Sub(start)
	p.Metrics.Latency(MetricRequestLatency).Observe(dur)

	outcome := "ok"
	if err != nil {
		outcome = "error"
		p.Metrics.Counter(MetricRequestsFailed).Inc(1)
		p.Health.Record(health.ProbeResult{At: p.now(), Err: err, Duration: dur})
		d, _ := p.Alerts.Ingest(alert.Event{At: p.now(), Key: AlertKeyFailures, Value: 1})
		p.rec.recordAlert(d)
	} else {
		p.Health.Record(health.ProbeResult{At: p.now(), Duration: dur})
	}

	// 每次请求结束都实时重估健康与告警：故障/恢复立即反映为结论变化，
	// 窗口滑过故障事件后告警恢复也能被记录。
	p.rec.recordHealth(p.now(), p.Health.Evaluate(p.now()))
	for _, rule := range p.rules {
		if d, ok := p.Alerts.Evaluate(rule.Name, rule.Version, p.now()); ok {
			p.rec.recordAlert(d)
		}
	}
	p.rec.recordCorrelation(CorrelationRecord{
		At: p.now(), Kind: kind, Upstream: upstream, Context: tc, Outcome: outcome,
	})
	return err
}

// Report 汇总本次运行的判定报告；指标数与全局汇总（Metrics 注册表）一致。
func (p *Pipeline) Report() Report {
	return p.rec.report(p.now(), p.Metrics)
}
