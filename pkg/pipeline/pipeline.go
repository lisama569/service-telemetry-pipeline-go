// Package pipeline 把关联标识、指标采集、健康判定、告警评估串到同一进程内
// 同一条可观测流程中，提供同步调用、并发扇出与后台任务三种执行模式。
package pipeline

import (
	"context"
	"sync"
	"time"

	"github.com/lisama569/service-telemetry-pipeline-go/pkg/alerting"
	"github.com/lisama569/service-telemetry-pipeline-go/pkg/correlation"
	"github.com/lisama569/service-telemetry-pipeline-go/pkg/health"
	"github.com/lisama569/service-telemetry-pipeline-go/pkg/metrics"
)

// DependencyFunc 是被观测的一次依赖调用。返回 (结果分类, 错误/耗时信息由上层包装)。
type DependencyFunc func(ctx context.Context) (metrics.Outcome, error)

// Config 构造 Pipeline 的配置。
type Config struct {
	HealthWindow     time.Duration
	FailureThreshold int
}

// Pipeline 聚合全部可观测性组件。
type Pipeline struct {
	Correlation *correlation.Store
	Metrics     *metrics.Registry
	Health      *health.Checker
	Alerts      *alerting.Evaluator

	nowMu sync.Mutex
	nowFn func() time.Time
}

// New 使用默认配置创建 Pipeline。
func New() *Pipeline {
	return NewWithConfig(Config{HealthWindow: 30 * time.Second, FailureThreshold: 3})
}

// NewWithConfig 使用指定配置创建 Pipeline。
func NewWithConfig(cfg Config) *Pipeline {
	if cfg.HealthWindow <= 0 {
		cfg.HealthWindow = 30 * time.Second
	}
	if cfg.FailureThreshold <= 0 {
		cfg.FailureThreshold = 3
	}
	return &Pipeline{
		Correlation: correlation.NewStore(),
		Metrics:     metrics.NewRegistry(),
		Health:      health.NewChecker(cfg.HealthWindow, cfg.FailureThreshold),
		Alerts:      alerting.NewEvaluator(),
		nowFn:       time.Now,
	}
}

// RunResult 一次被观测调用的可观测信息。
type RunResult struct {
	Corr     correlation.Context
	Outcome  metrics.Outcome
	Latency  time.Duration
	Verdict  health.Verdict
	NewAlert []alerting.Alert
}

// Begin 解析入口请求的关联标识并挂到 context 上。
// upstreamID 为空/非法都不会失败，返回的上下文始终带有效标识。
func (p *Pipeline) Begin(ctx context.Context, upstreamID string) (context.Context, correlation.Context, bool) {
	corr, replaced := correlation.Resolve(ctx, p.Correlation, upstreamID)
	return correlation.IntoContext(ctx, corr), corr, replaced
}

// Run 以同步方式执行一次依赖调用，记录指标、健康与告警。
// metricName 为指标名；healthReason 为非成功时的原因标签。
func (p *Pipeline) Run(ctx context.Context, metricName, healthReason string, fn DependencyFunc) RunResult {
	parent, _ := correlation.FromContext(ctx)
	span := correlation.ChildSpan(parent)
	spanCtx := correlation.IntoContext(ctx, span)

	start := p.now()
	outcome, callErr := fn(spanCtx)
	latency := p.now().Sub(start)
	if callErr != nil && outcome == metrics.OutcomeSuccess {
		outcome = metrics.OutcomeFailure
	}

	counter := p.Metrics.Counter(metricName)
	counter.Observe(latency, outcome)

	now := p.now()
	probe := health.ProbeResult{At: now, OK: outcome == metrics.OutcomeSuccess}
	switch outcome {
	case metrics.OutcomeTimeout:
		probe.Timeout = true
		probe.Reason = healthReason
		probe.OK = false
	case metrics.OutcomeFailure:
		probe.Reason = healthReason
	}
	p.Health.Record(probe)
	verdict := p.Health.Evaluate(now)

	value := 0.0
	if outcome != metrics.OutcomeSuccess {
		value = 1
	}
	newAlerts := p.Alerts.Push(alerting.Event{At: now, Name: metricName + ".failure", Value: value})

	_ = callErr
	return RunResult{
		Corr:     span,
		Outcome:  outcome,
		Latency:  latency,
		Verdict:  verdict,
		NewAlert: newAlerts,
	}
}

// RunFanOut 并发执行多个依赖调用。每个调用通过 correlation.Fork 得到独立
// span 副本，共享同一 trace/关联标识且互不串号；全部完成后汇总结果。
func (p *Pipeline) RunFanOut(ctx context.Context, metricName, reason string, fns ...DependencyFunc) []RunResult {
	parent, _ := correlation.FromContext(ctx)
	results := make([]RunResult, len(fns))
	var wg sync.WaitGroup
	for i, fn := range fns {
		wg.Add(1)
		go func(i int, fn DependencyFunc) {
			defer wg.Done()
			forked := correlation.Fork(parent)
			forkedCtx := correlation.IntoContext(ctx, forked)
			results[i] = p.Run(forkedCtx, metricName, reason, fn)
		}(i, fn)
	}
	wg.Wait()
	return results
}

// RunInBackground 在独立 goroutine 中执行，适合 fire-and-forget 后台任务。
// 关联上下文在提交时 Fork 快照，不依赖请求 context 生命周期，避免请求结束
// 导致标识丢失；返回的 channel 在任务完成后关闭并送达结果。
func (p *Pipeline) RunInBackground(ctx context.Context, metricName, reason string, fn DependencyFunc) <-chan RunResult {
	parent, _ := correlation.FromContext(ctx)
	snapshot := correlation.Fork(parent)
	ch := make(chan RunResult, 1)
	go func() {
		bgCtx := correlation.IntoContext(context.Background(), snapshot)
		res := p.Run(bgCtx, metricName, reason, fn)
		ch <- res
		close(ch)
	}()
	return ch
}

func (p *Pipeline) now() time.Time {
	p.nowMu.Lock()
	defer p.nowMu.Unlock()
	return p.nowFn()
}

// SetClock 仅供测试注入固定时钟。
func (p *Pipeline) SetClock(fn func() time.Time) {
	p.nowMu.Lock()
	defer p.nowMu.Unlock()
	p.nowFn = fn
}
