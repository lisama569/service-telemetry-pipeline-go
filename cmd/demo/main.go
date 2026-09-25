// Command demo 在同一进程内演示关联标识传播、指标采集、健康判定与告警评估，
// 覆盖同步调用、并发扇出与后台任务三种模式。
package main

import (
	"context"
	"fmt"
	"time"

	"github.com/lisama569/service-telemetry-pipeline-go/pkg/alerting"
	"github.com/lisama569/service-telemetry-pipeline-go/pkg/metrics"
	"github.com/lisama569/service-telemetry-pipeline-go/pkg/pipeline"
)

func main() {
	p := pipeline.New()
	_ = p.Alerts.AddRule(alerting.Rule{
		ID: "demo-failures", Metric: "demo.dep.failure",
		Window: 10 * time.Second, Threshold: 3,
		Severity: alerting.SeverityCritical, Version: 1,
	})

	// 1) 正常上游标识：同步调用。
	ctx, corr, _ := p.Begin(context.Background(), "upstream-req-001")
	fmt.Printf("[同步] 上游标识继承 corr=%s origin=%s\n", corr.CorrelationID(), corr.Origin())
	r := p.Run(ctx, "demo.dep", "ok", succeed())
	fmt.Printf("[同步] span=%s 结果=%s 健康=%s (%s)\n",
		r.Corr.SpanID(), r.Outcome, r.Verdict.State, r.Verdict.Reason)

	// 2) 畸形上游标识：链路不中断，本地替换。
	ctx2, corr2, replaced2 := p.Begin(context.Background(), "非法 标识")
	r2 := p.Run(ctx2, "demo.dep", "malformed", succeed())
	fmt.Printf("[畸形] replaced=%v corr=%s origin=%s 仍完成调用=%s\n",
		replaced2, corr2.CorrelationID(), corr2.Origin(), r2.Outcome)

	// 3) 并发扇出：每个分支独立 span，共享 corr/trace。
	fns := make([]pipeline.DependencyFunc, 5)
	for i := range fns {
		fns[i] = succeed()
	}
	results := p.RunFanOut(ctx, "demo.dep", "fanout", fns...)
	for i, res := range results {
		fmt.Printf("[并发] 分支%d corr=%s span=%s\n", i, res.Corr.CorrelationID(), res.Corr.SpanID())
	}

	// 4) 后台任务：Fork 快照，请求结束不影响标识传播。
	ch := p.RunInBackground(ctx, "demo.dep", "bg", fail())
	bg := <-ch
	fmt.Printf("[后台] corr=%s bgSpan=%s 结果=%s\n",
		bg.Corr.CorrelationID(), bg.Corr.SpanID(), bg.Outcome)

	// 5) 指标快照守恒校验。
	for _, s := range p.Metrics.SnapshotAll() {
		fmt.Printf("[指标] %s 总数=%d(成功=%d 失败=%d 超时=%d) 守恒=%v 平均=%s 最大=%s\n",
			s.Name, s.Count, s.Success, s.Failure, s.Timeout,
			s.Success+s.Failure+s.Timeout == s.Count, s.AvgLatency(), s.MaxLatency())
	}
}

func succeed() pipeline.DependencyFunc {
	return func(ctx context.Context) (metrics.Outcome, error) {
		return metrics.OutcomeSuccess, nil
	}
}

func fail() pipeline.DependencyFunc {
	return func(ctx context.Context) (metrics.Outcome, error) {
		return metrics.OutcomeFailure, context.DeadlineExceeded
	}
}
