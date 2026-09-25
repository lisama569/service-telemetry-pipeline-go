// Package pipeline 集成测试: 验证关联、指标、健康与告警在同一进程内
// 同步调用、并发处理与后台任务下保持一致且可解释。
package pipeline

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/lisama569/service-telemetry-pipeline-go/alert"
	"github.com/lisama569/service-telemetry-pipeline-go/correlation"
	"github.com/lisama569/service-telemetry-pipeline-go/health"
	"github.com/lisama569/service-telemetry-pipeline-go/metrics"
)

var errDep = errors.New("dependency error")

// handle 模拟一次请求处理: 建立关联上下文, 记录指标, 上报探针结果。
func handle(ctx context.Context, tr *correlation.Tracker, reg *metrics.Registry,
	hc *health.Checker, upstream string, fail bool, now time.Time) (context.Context, correlation.Context) {
	ctx, tc := tr.Ensure(ctx, upstream)
	reg.Counter("requests_total").Inc(1)
	start := time.Now()
	if fail {
		reg.Counter("requests_failed").Inc(1)
		hc.Record(health.ProbeResult{At: now, Err: errDep, Duration: 3 * time.Second})
	} else {
		hc.Record(health.ProbeResult{At: now, Duration: 10 * time.Millisecond})
	}
	reg.Latency("request_latency").Observe(time.Since(start))
	return ctx, tc
}

func TestPipelineConcurrentAndBackground(t *testing.T) {
	tr := correlation.NewTracker(nil)
	reg := metrics.NewRegistry()
	hc := health.NewChecker(health.Config{
		Window:           30 * time.Second,
		FailureThreshold: 3,
		Timeout:          2 * time.Second,
		BlipTolerance:    1,
	})
	ev := alert.NewEvaluator(alert.Rule{
		Name: "failures", Version: 1, Window: time.Minute, Threshold: 1, MinCount: 3,
	})
	base := time.Now()

	const syncCalls = 10
	const goroutines = 8
	const perGoroutine = 20
	const backgroundJobs = 4
	const perJob = 10

	rootCtx := context.Background()
	var wg sync.WaitGroup

	// 同步调用
	for i := 0; i < syncCalls; i++ {
		_, tc := handle(rootCtx, tr, reg, hc, "", false, base)
		t.Logf("sync correlation=%s", tc)
	}

	// 并发处理: 共享一个根上下文, TraceID 不得串号
	sharedCtx, root := tr.Ensure(rootCtx, "")
	var mu sync.Mutex
	traceIDs := map[string]int{}
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < perGoroutine; j++ {
				childCtx, child := tr.Spawn(sharedCtx)
				_ = childCtx
				mu.Lock()
				traceIDs[child.TraceID]++
				mu.Unlock()
				reg.Counter("requests_total").Inc(1)
				reg.Latency("request_latency").Observe(time.Millisecond)
			}
		}()
	}

	// 后台任务: 部分失败, 驱动健康与告警判定
	for i := 0; i < backgroundJobs; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < perJob; j++ {
				fail := j%2 == 0
				_, tc := handle(rootCtx, tr, reg, hc, "", fail, base.Add(time.Duration(j)*time.Second))
				if fail {
					ev.Ingest(alert.Event{At: base.Add(time.Duration(j) * time.Second), Key: "failures", Value: 1})
				}
				_ = tc
			}
		}(i)
	}
	wg.Wait()

	// 指标守恒
	total := reg.Counter("requests_total").Value()
	failed := reg.Counter("requests_failed").Value()
	wantTotal := int64(syncCalls + goroutines*perGoroutine + backgroundJobs*perJob)
	wantFailed := int64(backgroundJobs * perJob / 2)
	t.Logf("指标守恒: total=%d(期望%d) failed=%d(期望%d)", total, wantTotal, failed, wantFailed)
	if total != wantTotal || failed != wantFailed {
		t.Fatalf("指标不守恒: total=%d/%d failed=%d/%d", total, wantTotal, failed, wantFailed)
	}
	lat := reg.Latency("request_latency").Snapshot()
	if lat.Count != wantTotal {
		t.Fatalf("耗时样本丢失: %d/%d", lat.Count, wantTotal)
	}

	// 关联守恒: 并发派生全部落在同一 TraceID
	if len(traceIDs) != 1 {
		t.Fatalf("并发处理串号: %v", traceIDs)
	}
	for id, n := range traceIDs {
		if id != root.TraceID || n != goroutines*perGoroutine {
			t.Fatalf("TraceID 传播错误: %s x%d", id, n)
		}
		t.Logf("关联守恒: trace=%s 覆盖 %d 个并发派生任务", id, n)
	}

	// 健康判定: 失败率 50% 且为超时错误
	v := hc.Evaluate(base.Add(30 * time.Second))
	t.Logf("健康判定: verdict=%s", v)
	if v.State == health.StateHealthy {
		t.Fatalf("存在持续超时故障却被判健康: %s", v)
	}

	// 告警判定: 失败事件达到阈值应触发
	d, _ := ev.Evaluate("failures", 1, base.Add(30*time.Second))
	t.Logf("告警判定: decision=%s", d)
	if !d.Firing {
		t.Fatalf("失败事件达到阈值未触发: %s", d)
	}
}
