package pipeline

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lisama569/service-telemetry-pipeline-go/pkg/correlation"
	"github.com/lisama569/service-telemetry-pipeline-go/pkg/metrics"
)

// scriptedDep 按脚本返回结果，模拟依赖超时/失败/成功。
type scriptedDep struct {
	mu      sync.Mutex
	outcome []metrics.Outcome
	idx     int
}

func (d *scriptedDep) call() DependencyFunc {
	return func(ctx context.Context) (metrics.Outcome, error) {
		d.mu.Lock()
		defer d.mu.Unlock()
		o := d.outcome[d.idx%len(d.outcome)]
		d.idx++
		if o == metrics.OutcomeTimeout {
			return o, context.DeadlineExceeded
		}
		return o, nil
	}
}

func TestMalformedUpstreamIDDoesNotBreakChain(t *testing.T) {
	env := newTestEnv("dep.pay")
	dep := &scriptedDep{outcome: []metrics.Outcome{metrics.OutcomeSuccess}}

	ctx, corr, replaced := env.p.Begin(context.Background(), "bad id!!")
	if !replaced || corr.Origin() != correlation.OriginMalformed || !correlation.ValidID(corr.CorrelationID()) {
		t.Fatalf("malformed upstream must be replaced, got origin=%s id=%q", corr.Origin(), corr.CorrelationID())
	}
	res := env.p.Run(ctx, "dep.pay", "malformed-chain-test", dep.call())
	if res.Outcome != metrics.OutcomeSuccess {
		t.Fatal("chain must complete successfully despite malformed upstream id")
	}
	snap, _ := env.p.Metrics.Snapshot("dep.pay")
	if snap.Count != 1 || snap.Success != 1 {
		t.Fatalf("metric not recorded: %+v", snap)
	}
	t.Logf("关联标识[畸形不中断链路] 上游=%q -> corr=%s origin=%s 调用结果=%s 健康判定=%s 依据=%s 指标count=%d",
		"bad id!!", res.Corr.CorrelationID(), res.Corr.Origin(), res.Outcome, res.Verdict.State, res.Verdict.Reason, snap.Count)
}

func TestDependencyTimeoutThenRecovery(t *testing.T) {
	env := newTestEnv("dep.db")
	phase := []metrics.Outcome{
		metrics.OutcomeTimeout, metrics.OutcomeTimeout, metrics.OutcomeTimeout,
		metrics.OutcomeSuccess, metrics.OutcomeSuccess, metrics.OutcomeSuccess,
	}
	dep := &scriptedDep{outcome: phase}

	ctx, corr, _ := env.p.Begin(context.Background(), "upstream-tx-1")
	var results []RunResult
	for i := 0; i < len(phase); i++ {
		// 后三次调用前把时钟拨到 11s 之后：旧超时探测滑出健康窗口，
		// 同时告警的第一个窗口到期。
		if i == 3 {
			env.clock.jump(11 * time.Second)
		}
		res := env.p.Run(ctx, "dep.db", "timeout->recovery", dep.call())
		results = append(results, res)
		if i == 2 {
			// 超时时窗口尚未到期，用 Tick 闭窗触发告警（水位推进）。
			env.p.Alerts.Tick(env.clock.current())
		}
		if i == 5 {
			// 再拨过 [1010,1020) 窗口边界并 Tick：零失败窗口关闭 => 告警恢复。
			env.clock.jump(9 * time.Second)
			env.p.Alerts.Tick(env.clock.current())
		}
	}

	if results[2].Verdict.State != healthTimeout() {
		t.Fatalf("during timeout phase verdict = %s (%s)", results[2].Verdict.State, results[2].Verdict.Reason)
	}
	if results[5].Verdict.State != healthHealthy() {
		t.Fatalf("after recovery verdict = %s (%s), must not be cached", results[5].Verdict.State, results[5].Verdict.Reason)
	}
	snap, _ := env.p.Metrics.Snapshot("dep.db")
	if snap.Count != 6 || snap.Timeout != 3 || snap.Success != 3 || snap.Failure != 0 {
		t.Fatalf("metrics not conserved: %+v", snap)
	}
	if snap.Success+snap.Failure+snap.Timeout != snap.Count {
		t.Fatal("success+failure+timeout != count")
	}
	all := env.p.Alerts.Alerts()
	if len(all) == 0 {
		t.Fatal("timeout phase should raise an alert")
	}
	last := all[len(all)-1]
	if last.Active() {
		t.Fatalf("alert should recover after healthy window, firedAt=%s", last.FiredAt)
	}
	t.Logf("场景[超时->恢复] corr=%s 指标守恒 count=%d(成功=%d 超时=%d 失败=%d) 健康: %s(依据:%s) -> %s(依据:%s) 告警=%s v%d 触发窗口[%s,%s) 恢复于%s 失败原因区分=timeout(DeadlineExceeded) vs failure",
		corr.CorrelationID(), snap.Count, snap.Success, snap.Timeout, snap.Failure,
		results[2].Verdict.State, results[2].Verdict.Reason,
		results[5].Verdict.State, results[5].Verdict.Reason,
		last.RuleID, last.Version, last.WindowStart.Format("15:04:05"), last.WindowEnd.Format("15:04:05"),
		last.RecoveredAt.Format("15:04:05"))
}

func TestFanOutConservationAndCorrelation(t *testing.T) {
	env := newTestEnv("dep.fanout")
	dep := &scriptedDep{outcome: []metrics.Outcome{metrics.OutcomeSuccess}}

	ctx, root, _ := env.p.Begin(context.Background(), "fanout-root")
	const n = 100
	fns := make([]DependencyFunc, n)
	for i := range fns {
		fns[i] = dep.call()
	}
	results := env.p.RunFanOut(ctx, "dep.fanout", "fanout", fns...)

	if len(results) != n {
		t.Fatalf("results = %d", len(results))
	}
	spans := make(map[string]int)
	for _, r := range results {
		if r.Corr.CorrelationID() != root.CorrelationID() {
			t.Fatal("fanout lost correlation id")
		}
		spans[r.Corr.SpanID()]++
	}
	for sid, c := range spans {
		if c != 1 {
			t.Fatalf("span %s duplicated %d times (串号)", sid, c)
		}
	}
	snap, _ := env.p.Metrics.Snapshot("dep.fanout")
	if snap.Count != n || snap.Success != int64(n) {
		t.Fatalf("fanout metrics not conserved: %+v", snap)
	}
	t.Logf("场景[并发扇出] rootCorr=%s 分支数=%d span唯一=%d 指标count=%d成功=%d 守恒依据=原子计数相加=总数",
		root.CorrelationID(), n, len(spans), snap.Count, snap.Success)
}

func TestBackgroundTaskKeepsCorrelation(t *testing.T) {
	env := newTestEnv("dep.bg")
	dep := &scriptedDep{outcome: []metrics.Outcome{metrics.OutcomeFailure}}

	ctx, root, _ := env.p.Begin(context.Background(), "bg-root")
	ch := env.p.RunInBackground(ctx, "dep.bg", "background failure", dep.call())
	res := <-ch
	if res.Corr.CorrelationID() != root.CorrelationID() {
		t.Fatal("background task lost correlation id")
	}
	if res.Outcome != metrics.OutcomeFailure {
		t.Fatalf("outcome = %s", res.Outcome)
	}
	snap, _ := env.p.Metrics.Snapshot("dep.bg")
	if snap.Failure != 1 {
		t.Fatalf("background metric missing: %+v", snap)
	}
	t.Logf("场景[后台任务] rootCorr=%s bgCorr=%s bgSpan=%s 结果=%s 指标失败数=%d 判定依据=Fork快照脱离请求context仍可关联",
		root.CorrelationID(), res.Corr.CorrelationID(), res.Corr.SpanID(), res.Outcome, snap.Failure)
}

func TestConcurrentMixedPipelinesRaceFree(t *testing.T) {
	env := newTestEnv("dep.race")
	var totalOK, totalTO atomic.Int64
	var wg sync.WaitGroup
	for g := 0; g < 20; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, _, _ := env.p.Begin(context.Background(), "")
			dep := &scriptedDep{outcome: []metrics.Outcome{metrics.OutcomeSuccess, metrics.OutcomeTimeout}}
			for i := 0; i < 20; i++ {
				r := env.p.Run(ctx, "dep.race", "race", dep.call())
				if r.Outcome == metrics.OutcomeSuccess {
					totalOK.Add(1)
				} else {
					totalTO.Add(1)
				}
			}
		}()
	}
	wg.Wait()
	snap, _ := env.p.Metrics.Snapshot("dep.race")
	if snap.Count != totalOK.Load()+totalTO.Load() || snap.Success != totalOK.Load() || snap.Timeout != totalTO.Load() {
		t.Fatalf("conservation broken: snap=%+v ok=%d to=%d", snap, totalOK.Load(), totalTO.Load())
	}
	t.Logf("场景[全链路并发竞态] goroutine=20 总调用=%d 成功=%d 超时=%d 指标守恒=OK 判定依据=-race 无竞态且分类相加=总数",
		snap.Count, snap.Success, snap.Timeout)
}
