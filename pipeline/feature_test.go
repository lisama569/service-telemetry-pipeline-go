package pipeline

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lisama569/service-telemetry-pipeline-go/alert"
	"github.com/lisama569/service-telemetry-pipeline-go/correlation"
	"github.com/lisama569/service-telemetry-pipeline-go/health"
	"github.com/lisama569/service-telemetry-pipeline-go/metrics"
)

var errBoom = errors.New("boom")

// stepClock 测试用步进时钟，保证报告可复现。
type stepClock struct {
	mu sync.Mutex
	t  time.Time
}

func newStepClock() *stepClock {
	return &stepClock{t: time.Date(2026, 10, 6, 0, 0, 0, 0, time.UTC)}
}

func (c *stepClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *stepClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

func newTestPipeline(clk *stepClock) *Pipeline {
	hc := health.NewChecker(health.Config{
		Window:           30 * time.Second,
		FailureThreshold: 3,
		Timeout:          2 * time.Second,
		BlipTolerance:    1,
	})
	ev := alert.NewEvaluator(alert.Rule{
		Name: "failures", Version: 1, Window: time.Minute, Threshold: 1, MinCount: 3,
	})
	return New(correlation.NewTracker(nil), metrics.NewRegistry(), hc, ev,
		Options{Now: clk.Now, AlertKey: "failures"})
}

// 请求结束（正常与出错）后标识归还，可被后续请求重新继承。
func TestRequestEndReleasesID(t *testing.T) {
	p := newTestPipeline(newStepClock())
	upstream := strings.Repeat("a", 32)

	_, tc1, _ := p.HandleRequest(context.Background(), upstream, func(context.Context) error { return nil })
	_, tc2, err := p.HandleRequest(context.Background(), upstream, func(context.Context) error { return errBoom })
	_, tc3, _ := p.HandleRequest(context.Background(), upstream, func(context.Context) error { return nil })
	t.Logf("tc1=%s tc2=%s(err=%v) tc3=%s 判定依据: 每次请求结束（含出错）都归还标识, 后续可重新继承",
		tc1, tc2, err, tc3)

	for i, tc := range []correlation.Context{tc1, tc2, tc3} {
		if tc.Source != correlation.SourceInherited || tc.RequestID != upstream {
			t.Fatalf("第 %d 次应重新继承 %s, 得到 %s", i+1, upstream, tc)
		}
	}
	if err == nil {
		t.Fatal("第二次请求应返回错误")
	}
	if got := p.Tracker.Active(); got != 0 {
		t.Fatalf("请求全部结束后判重记录应清空, active=%d", got)
	}
}

// 仍在处理中的相同上游标识必须被判重替换；并发下只有一个能继承。
func TestInFlightDuplicateReplaced(t *testing.T) {
	p := newTestPipeline(newStepClock())
	upstream := strings.Repeat("c", 32)

	held := make(chan correlation.Context, 1)
	release := make(chan struct{})
	done := make(chan struct{})
	go func() {
		_, _, _ = p.HandleRequest(context.Background(), upstream, func(ctx context.Context) error {
			tc, _ := correlation.FromContext(ctx)
			held <- tc
			<-release
			return nil
		})
		close(done)
	}()
	tcA := <-held

	_, tcB, _ := p.HandleRequest(context.Background(), upstream, func(context.Context) error { return nil })
	t.Logf("A(在途)=%s B(同标识)=%s 判定依据: 在途重复标识被替换", tcA, tcB)
	if tcB.Source != correlation.SourceReplacedDuplicate {
		t.Fatalf("在途重复应被替换, 得到 %s", tcB.Source)
	}
	if tcB.RequestID == upstream || tcB.TraceID == tcA.TraceID {
		t.Fatal("被替换的标识不得与在途请求串号")
	}

	// 并发同一上游标识: 恰好一个继承, 其余全部替换, 不误判不串号。
	const n = 16
	sources := make([]correlation.IDSource, n)
	ids := make([]string, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, tc, _ := p.HandleRequest(context.Background(), upstream, func(context.Context) error { return nil })
			sources[i] = tc.Source
			ids[i] = tc.RequestID
		}(i)
	}
	wg.Wait()
	inherited := 0
	seen := map[string]bool{}
	for i := range sources {
		if sources[i] == correlation.SourceInherited {
			inherited++
		} else if sources[i] != correlation.SourceReplacedDuplicate {
			t.Fatalf("并发第 %d 个请求来源异常: %s", i, sources[i])
		}
		if seen[ids[i]] {
			t.Fatalf("并发下串号: %s", ids[i])
		}
		seen[ids[i]] = true
	}
	t.Logf("并发 %d 个同标识请求: inherited=%d replaced=%d (A 仍在途)", n, inherited, n-inherited)
	if inherited != 0 { // A 仍持有该标识, 并发的全部应被替换
		t.Fatalf("A 在途期间并发请求不应继承该标识, inherited=%d", inherited)
	}

	close(release)
	<-done
	// A 结束后标识归还, 可再次继承。
	_, tcC, _ := p.HandleRequest(context.Background(), upstream, func(context.Context) error { return nil })
	t.Logf("A 结束后: %s 判定依据: 标识已归还, 重新继承", tcC)
	if tcC.Source != correlation.SourceInherited {
		t.Fatalf("结束后应能继承, 得到 %s", tcC.Source)
	}
}

// 后台任务挂在派生它的请求的同一条 TraceID 上。
func TestBackgroundFollowsParentTrace(t *testing.T) {
	p := newTestPipeline(newStepClock())
	reqCtx, tcReq, _ := p.HandleRequest(context.Background(), "", func(context.Context) error { return nil })

	var wg sync.WaitGroup
	wg.Add(1)
	var child correlation.Context
	p.SpawnBackground(reqCtx, "job-x", func(ctx context.Context) error {
		defer wg.Done()
		child, _ = correlation.FromContext(ctx)
		return nil
	})
	wg.Wait()
	t.Logf("parent=%s child=%s 判定依据: 后台任务经 Spawn 派生, TraceID 不变, RequestID 重新生成", tcReq, child)
	if child.TraceID != tcReq.TraceID {
		t.Fatalf("后台任务另起了链路: parent=%s child=%s", tcReq, child)
	}
	if child.RequestID == tcReq.RequestID {
		t.Fatal("后台任务 RequestID 应与父任务不同")
	}
	if got := p.Tracker.Active(); got != 0 {
		t.Fatalf("后台任务结束后标识应归还, active=%d", got)
	}
	s := p.Summary()
	if !s.Conserved() {
		t.Fatalf("指标不守恒: %+v", s)
	}
}

// 无数据时报告正常输出; 同样输入重跑报告可复现且与全局汇总一致。
func TestReportEmptyAndReproducible(t *testing.T) {
	empty := newTestPipeline(newStepClock()).Report()
	out := empty.String()
	t.Logf("无数据报告:\n%s", out)
	for _, section := range []string{"== 关联标识走向 ==", "== 指标守恒 ==", "== 健康结论变化 ==", "== 告警触发与恢复 =="} {
		if !strings.Contains(out, section) {
			t.Fatalf("报告缺少段落 %q", section)
		}
	}
	if !strings.Contains(out, "无关联记录") || !strings.Contains(out, "无告警判定记录") {
		t.Fatal("无数据时各段落应给出明确说明")
	}

	// 固定输入场景跑两遍, 报告（遮蔽随机标识后）必须一致。
	scenario := func() Report {
		clk := newStepClock()
		p := newTestPipeline(clk)
		up := strings.Repeat("d", 32)
		for i := 0; i < 2; i++ {
			p.HandleRequest(context.Background(), up, func(context.Context) error { return nil })
			clk.Advance(time.Second)
		}
		for i := 0; i < 3; i++ { // 超时故障, 驱动健康与告警
			p.HandleRequest(context.Background(), up, func(context.Context) error {
				clk.Advance(3 * time.Second)
				return errBoom
			})
			p.CheckHealth(clk.Now())
			clk.Advance(time.Second)
		}
		clk.Advance(61 * time.Second) // 故障滑出窗口
		p.HandleRequest(context.Background(), up, func(context.Context) error { return nil })
		p.CheckHealth(clk.Now())
		return p.Report()
	}
	r1, r2 := scenario(), scenario()
	mask := regexp.MustCompile(`[0-9a-f]{32}`).ReplaceAllString
	s1, s2 := mask(r1.String(), "<id>"), mask(r2.String(), "<id>")
	if s1 != s2 {
		t.Fatalf("同样输入重跑报告不一致:\n--- 第一次 ---\n%s\n--- 第二次 ---\n%s", s1, s2)
	}
	t.Logf("可复现报告（遮蔽标识后）:\n%s", s1)

	// 报告数字与全局汇总一致。
	clk := newStepClock()
	p := newTestPipeline(clk)
	p.HandleRequest(context.Background(), "", func(context.Context) error { return errBoom })
	p.HandleRequest(context.Background(), "", func(context.Context) error { return nil })
	rep, sum := p.Report(), p.Summary()
	want := fmt.Sprintf("started=%d finished=%d failed=%d background=%d",
		sum.Started, sum.Finished, sum.Failed, sum.Background)
	t.Logf("报告指标行=%q 汇总=%+v", rep.Metrics[0], sum)
	if rep.Metrics[0] != want {
		t.Fatalf("报告与全局汇总不一致: %q != %q", rep.Metrics[0], want)
	}
	if rep.Conserved != sum.Conserved() || !rep.Conserved {
		t.Fatalf("守恒结论不一致或不守恒: report=%v summary=%v", rep.Conserved, sum.Conserved())
	}
}

// 依赖先超时后恢复: 健康结论与告警各自变化可解释, 告警触发去重且能恢复。
func TestPipelineTimeoutToRecoveryAndAlert(t *testing.T) {
	clk := newStepClock()
	p := newTestPipeline(clk)

	fail := func(context.Context) error { clk.Advance(3 * time.Second); return errBoom }
	ok := func(context.Context) error { return nil }

	var verdicts []health.Verdict
	for i := 0; i < 4; i++ {
		_, tc, _ := p.HandleRequest(context.Background(), "", fail)
		v := p.CheckHealth(clk.Now())
		verdicts = append(verdicts, v)
		t.Logf("第 %d 次超时: correlation=%s verdict=%s", i+1, tc, v)
		clk.Advance(time.Second)
	}
	if verdicts[2].State != health.StateUnhealthy || verdicts[2].Reason != health.ReasonDependencyTimeout {
		t.Fatalf("连续超时应判 unhealthy/dependency_timeout, 得到 %s", verdicts[2])
	}

	for i := 0; i < 3; i++ { // 依赖恢复
		p.HandleRequest(context.Background(), "", ok)
		clk.Advance(time.Second)
	}
	clk.Advance(61 * time.Second) // 故障样本滑出健康与告警窗口
	p.HandleRequest(context.Background(), "", ok)
	v := p.CheckHealth(clk.Now())
	t.Logf("恢复后: verdict=%s", v)
	if v.State != health.StateHealthy {
		t.Fatalf("故障滑出窗口后应恢复健康, 得到 %s", v)
	}

	rep := p.Report()
	t.Logf("报告:\n%s", rep)
	triggers, recovered := 0, false
	for _, line := range rep.Alerts {
		if strings.Contains(line, "triggered=true") {
			triggers++
		}
	}
	if len(rep.Alerts) > 0 && strings.Contains(rep.Alerts[len(rep.Alerts)-1], "firing=false") {
		recovered = true
	}
	t.Logf("告警: 触发次数=%d 恢复=%v 判定依据: firing 状态去重, 窗口滑出后恢复", triggers, recovered)
	if triggers != 1 {
		t.Fatalf("firing 期间重复触发, 期望恰好 1 次, 得到 %d", triggers)
	}
	if !recovered {
		t.Fatal("故障滑出窗口后告警应恢复")
	}
	if !rep.Conserved {
		t.Fatal("指标不守恒")
	}
}
