package pipeline

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lisama569/service-telemetry-pipeline-go/alert"
	"github.com/lisama569/service-telemetry-pipeline-go/correlation"
	"github.com/lisama569/service-telemetry-pipeline-go/health"
)

var errFeatureDep = errors.New("dependency error")

// logicalClock 测试用逻辑时钟。
type logicalClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *logicalClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *logicalClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

// newTestPipeline 组装一条注入逻辑时钟与确定性 ID 的流程，保证报告可复现。
func newTestPipeline(clock *logicalClock) *Pipeline {
	var seq atomic.Int64
	return New(Config{
		Health: health.Config{
			Window:           30 * time.Second,
			FailureThreshold: 3,
			Timeout:          2 * time.Second,
			BlipTolerance:    1,
		},
		Rules: []alert.Rule{
			{Name: AlertKeyFailures, Version: 1, Window: time.Minute, Threshold: 1, MinCount: 3},
		},
		Now: clock.Now,
		TrackerOpts: []correlation.Option{
			correlation.WithIDFunc(func() string { return fmt.Sprintf("%032x", seq.Add(1)) }),
		},
	})
}

func TestIDReinheritedAfterRequestEnds(t *testing.T) {
	clock := &logicalClock{t: time.Date(2026, 10, 6, 0, 0, 0, 0, time.UTC)}
	p := newTestPipeline(clock)
	upstream := "abcdef0123456789"

	tc1, _ := correlation.FromContext(mustHandle(t, p, upstream, nil))
	tc2, _ := correlation.FromContext(mustHandle(t, p, upstream, nil))
	t.Logf("第一次=%s 第二次=%s 判定依据: 请求结束标识已归还, 可重新继承", tc1, tc2)
	if tc1.Source != correlation.SourceInherited || tc2.Source != correlation.SourceInherited {
		t.Fatalf("两次都应继承同一上游标识: %s / %s", tc1, tc2)
	}
	if p.Tracker.Active() != 0 {
		t.Fatalf("请求全部结束后不应有残留活动标识, Active=%d", p.Tracker.Active())
	}
}

// mustHandle 执行一次请求并返回上下文。
func mustHandle(t *testing.T, p *Pipeline, upstream string, err error) context.Context {
	t.Helper()
	ctx, _ := p.HandleRequest(context.Background(), upstream, func(context.Context) error { return err })
	return ctx
}

func TestInFlightDuplicateReplacedConcurrently(t *testing.T) {
	clock := &logicalClock{t: time.Date(2026, 10, 6, 0, 0, 0, 0, time.UTC)}
	p := newTestPipeline(clock)
	upstream := "abcdef0123456789"

	const n = 8
	var entered atomic.Int64
	release := make(chan struct{})
	results := make(chan correlation.Context, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, _ := p.HandleRequest(context.Background(), upstream, func(ctx context.Context) error {
				if entered.Add(1) == n {
					close(release) // 全部在处理中后才放行, 保证并发在飞
				}
				<-release
				return nil
			})
			tc, _ := correlation.FromContext(ctx)
			results <- tc
		}()
	}
	wg.Wait()
	close(results)

	// 并发在飞: 恰好一个继承, 其余全部判重替换; RequestID 互不串号。
	inherited := 0
	seen := map[string]bool{}
	for tc := range results {
		t.Logf("并发在飞=%s", tc)
		switch tc.Source {
		case correlation.SourceInherited:
			inherited++
			if tc.RequestID != upstream {
				t.Fatalf("继承的 RequestID 应等于上游标识: %s", tc)
			}
		case correlation.SourceReplacedDuplicate:
		default:
			t.Fatalf("并发同标识只应继承或判重替换, 得到 %s", tc)
		}
		if seen[tc.RequestID] {
			t.Fatalf("RequestID 串号: %s", tc.RequestID)
		}
		seen[tc.RequestID] = true
	}
	if inherited != 1 {
		t.Fatalf("恰好一个请求应继承上游标识, 实际 %d", inherited)
	}

	// 全部结束后标识应归还, 判重记录清空, 同一上游标识可重新继承。
	if p.Tracker.Active() != 0 {
		t.Fatalf("全部结束后标识应归还, Active=%d", p.Tracker.Active())
	}
	tc, _ := correlation.FromContext(mustHandle(t, p, upstream, nil))
	t.Logf("结束后=%s 判定依据: 在飞请求已全部归还, 标识可重新继承", tc)
	if tc.Source != correlation.SourceInherited {
		t.Fatalf("并发结束后应能重新继承, 得到 %s", tc)
	}
}

func TestInFlightDuplicateReplacedDeterministic(t *testing.T) {
	clock := &logicalClock{t: time.Date(2026, 10, 6, 0, 0, 0, 0, time.UTC)}
	p := newTestPipeline(clock)
	upstream := "abcdef0123456789"

	// 第一个请求仍在处理中(通过嵌套调用模拟在飞), 第二个同标识请求必须被替换。
	outerCtx, _ := p.HandleRequest(context.Background(), upstream, func(ctx context.Context) error {
		innerCtx, _ := p.HandleRequest(context.Background(), upstream, func(context.Context) error { return nil })
		inner, _ := correlation.FromContext(innerCtx)
		t.Logf("在飞重复=%s 判定依据: 外层请求未结束, 同标识须替换", inner)
		if inner.Source != correlation.SourceReplacedDuplicate {
			t.Fatalf("在飞重复标识应被替换, 得到 %s", inner)
		}
		return nil
	})
	outer, _ := correlation.FromContext(outerCtx)
	t.Logf("外层=%s 判定依据: 合法上游标识, 继承", outer)
	if outer.Source != correlation.SourceInherited {
		t.Fatalf("外层应继承, 得到 %s", outer)
	}
	// 外层归还后, 同一标识可再次继承。
	tc, _ := correlation.FromContext(mustHandle(t, p, upstream, nil))
	if tc.Source != correlation.SourceInherited {
		t.Fatalf("归还后应能重新继承, 得到 %s", tc)
	}
}

func TestBackgroundFollowsParentTrace(t *testing.T) {
	clock := &logicalClock{t: time.Date(2026, 10, 6, 0, 0, 0, 0, time.UTC)}
	p := newTestPipeline(clock)

	const jobs = 4
	children := make(chan correlation.Context, jobs)
	var parentTrace string
	var wg sync.WaitGroup
	p.HandleRequest(context.Background(), "0123456789abcdef", func(ctx context.Context) error {
		parent, _ := correlation.FromContext(ctx)
		parentTrace = parent.TraceID
		for i := 0; i < jobs; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				bgCtx, _ := p.SpawnBackground(ctx, func(context.Context) error { return nil })
				child, _ := correlation.FromContext(bgCtx)
				children <- child
			}()
		}
		wg.Wait()
		return nil
	})
	close(children)
	seen := map[string]bool{}
	for child := range children {
		t.Logf("后台任务=%s 判定依据: 派生自请求 trace=%s, 不另起链路", child, parentTrace)
		if child.TraceID != parentTrace || child.TraceID == "" {
			t.Fatalf("后台任务未挂在父链路上: parent=%s child=%s", parentTrace, child.TraceID)
		}
		if child.RequestID == "" || seen[child.RequestID] {
			t.Fatalf("后台任务 RequestID 为空或串号: %s", child.RequestID)
		}
		seen[child.RequestID] = true
	}
	if len(seen) != jobs {
		t.Fatalf("后台任务丢失: 期望 %d, 得到 %d", jobs, len(seen))
	}
}

func TestReportEmptyAndReproducible(t *testing.T) {
	// 无数据: 不报错、不卡住, 各节有占位说明。
	clock := &logicalClock{t: time.Date(2026, 10, 6, 0, 0, 0, 0, time.UTC)}
	p := newTestPipeline(clock)
	empty := p.Report().String()
	t.Logf("无数据报告:\n%s", empty)
	for _, section := range []string{"关联标识走向", "健康结论变化", "告警触发与恢复", "指标汇总"} {
		if !strings.Contains(empty, section) {
			t.Fatalf("报告缺少小节 %q", section)
		}
	}
	if !strings.Contains(empty, "(无数据)") {
		t.Fatal("无数据时应有占位说明")
	}

	// 同样输入重跑, 报告完全一致, 且与全局指标汇总一致。
	run := func() (string, map[string]int64) {
		clock := &logicalClock{t: time.Date(2026, 10, 6, 0, 0, 0, 0, time.UTC)}
		p := newTestPipeline(clock)
		// 3 次成功 -> 4 次依赖超时(耗时超 Timeout) -> 推进时钟 -> 3 次恢复。
		ok := func(context.Context) error { clock.Advance(10 * time.Millisecond); return nil }
		timeout := func(context.Context) error { clock.Advance(3 * time.Second); return errFeatureDep }
		for i := 0; i < 3; i++ {
			p.HandleRequest(context.Background(), "", ok)
		}
		for i := 0; i < 4; i++ {
			p.HandleRequest(context.Background(), "", timeout)
		}
		clock.Advance(2 * time.Minute)
		for i := 0; i < 3; i++ {
			p.HandleRequest(context.Background(), "", ok)
		}
		rep := p.Report()
		return rep.String(), rep.Counters
	}
	r1, c1 := run()
	r2, c2 := run()
	if r1 != r2 {
		t.Fatalf("同样输入重跑报告不一致:\n--- 第一次 ---\n%s\n--- 第二次 ---\n%s", r1, r2)
	}
	t.Logf("可复现报告:\n%s", r1)
	if fmt.Sprint(c1) != fmt.Sprint(c2) {
		t.Fatalf("两次运行计数不一致: %v / %v", c1, c2)
	}
	if c1[MetricRequestsTotal] != 10 || c1[MetricRequestsFailed] != 4 {
		t.Fatalf("报告计数与场景不符: %v", c1)
	}
	// 报告中的健康与告警变化: 超时阶段判 unhealthy/dependency_timeout, 恢复后翻转。
	for _, want := range []string{"unhealthy", "dependency_timeout", "触发", "恢复"} {
		if !strings.Contains(r1, want) {
			t.Fatalf("报告应包含 %q 的变化记录", want)
		}
	}
}

func TestReportConsistentWithGlobalMetrics(t *testing.T) {
	clock := &logicalClock{t: time.Date(2026, 10, 6, 0, 0, 0, 0, time.UTC)}
	p := newTestPipeline(clock)
	for i := 0; i < 5; i++ {
		mustHandle(t, p, "", nil)
	}
	mustHandle(t, p, "", errFeatureDep)
	rep := p.Report()
	global := p.Metrics.Counters()
	if fmt.Sprint(rep.Counters) != fmt.Sprint(global) {
		t.Fatalf("报告计数与全局汇总不一致: report=%v global=%v", rep.Counters, global)
	}
	t.Logf("报告与全局汇总一致: %v", rep.Counters)
}
