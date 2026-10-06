// demo 一键跑完同步、并发与后台任务混合场景，展示关联、指标、健康、
// 告警四块如何协同，并在结尾输出人可读的判定报告。
//
// 运行: go run ./cmd/demo
//
// 全程使用固定起点的步进时钟，同样输入重跑输出一致（并发段落的日志
// 行顺序可能随调度略有变化，但所有计数、判定结论与时间戳一致）。
package main

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/lisama569/service-telemetry-pipeline-go/alert"
	"github.com/lisama569/service-telemetry-pipeline-go/correlation"
	"github.com/lisama569/service-telemetry-pipeline-go/health"
	"github.com/lisama569/service-telemetry-pipeline-go/metrics"
	"github.com/lisama569/service-telemetry-pipeline-go/pipeline"
)

var errDep = errors.New("dependency timeout")

// clock 固定起点、手动步进的时钟，保证报告可复现。
type clock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *clock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *clock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

func main() {
	clk := &clock{t: time.Date(2026, 10, 6, 0, 0, 0, 0, time.UTC)}

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
	p := pipeline.New(tr, reg, hc, ev, pipeline.Options{Now: clk.Now, AlertKey: "failures"})

	// run 处理一次同步请求；fail 时模拟依赖超时（耗时 3s 超过 Timeout）。
	run := func(upstream string, fail bool) (context.Context, correlation.Context) {
		ctx, tc, err := p.HandleRequest(context.Background(), upstream, func(context.Context) error {
			if fail {
				clk.Advance(3 * time.Second)
				return errDep
			}
			clk.Advance(10 * time.Millisecond)
			return nil
		})
		fmt.Printf("  %s err=%v\n", tc, err)
		clk.Advance(time.Second)
		return ctx, tc
	}

	fmt.Println("--- 阶段1: 同步请求（生成/继承/畸形替换） ---")
	run("", false)                      // generated
	run(strings.Repeat("b", 32), false) // inherited
	run("not-a-hex-id!", false)         // replaced_invalid
	fmt.Printf("  健康: %s\n", p.CheckHealth(clk.Now()))

	fmt.Println("--- 阶段2: 在途判重与结束后归还复用 ---")
	upstream := strings.Repeat("a", 32)
	held := make(chan correlation.Context, 1)
	release := make(chan struct{})
	done := make(chan struct{})
	go func() { // 请求 A 持有标识不结束
		_, _, _ = p.HandleRequest(context.Background(), upstream, func(ctx context.Context) error {
			tc, _ := correlation.FromContext(ctx)
			held <- tc
			<-release
			return nil
		})
		close(done)
	}()
	tcA := <-held
	fmt.Printf("  A 持有中: %s\n", tcA)
	_, tcB, _ := p.HandleRequest(context.Background(), upstream, func(context.Context) error { return nil })
	fmt.Printf("  B 同标识在途: %s (判重: %s)\n", tcB, tcB.Source)
	close(release)
	<-done // A 结束，标识归还
	_, tcC, _ := p.HandleRequest(context.Background(), upstream, func(context.Context) error { return nil })
	fmt.Printf("  C 结束后复用: %s (判定: %s)\n", tcC, tcC.Source)
	clk.Advance(time.Second)

	fmt.Println("--- 阶段3: 并发处理（同一 TraceID 不串号） ---")
	rootCtx, root := tr.Ensure(context.Background(), "")
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 5; j++ {
				childCtx, _ := tr.Spawn(rootCtx)
				p.HandleRequest(childCtx, "", func(context.Context) error { return nil })
			}
		}()
	}
	wg.Wait()
	fmt.Printf("  20 个并发派生任务全部挂在 trace=%s\n", root.TraceID)
	clk.Advance(time.Second)

	fmt.Println("--- 阶段4: 后台任务挂在派生它的请求链路上 ---")
	reqCtx, tcReq, _ := p.HandleRequest(context.Background(), "", func(context.Context) error { return nil })
	fmt.Printf("  请求: %s\n", tcReq)
	var bgWg sync.WaitGroup
	for i := 0; i < 2; i++ {
		bgWg.Add(1)
		p.SpawnBackground(reqCtx, fmt.Sprintf("job-%d", i), func(ctx context.Context) error {
			defer bgWg.Done()
			tc, _ := correlation.FromContext(ctx)
			fmt.Printf("  后台任务: %s (与请求同 trace)\n", tc)
			return nil
		})
	}
	bgWg.Wait()
	clk.Advance(time.Second)

	fmt.Println("--- 阶段5: 依赖先超时、后恢复 ---")
	for i := 0; i < 4; i++ { // 连续超时，驱动健康与告警越阈
		run("", true)
		fmt.Printf("  健康: %s\n", p.CheckHealth(clk.Now()))
	}
	for i := 0; i < 3; i++ { // 依赖恢复
		run("", false)
	}
	clk.Advance(61 * time.Second) // 故障样本滑出健康窗口与告警窗口
	run("", false)
	run("", false)
	fmt.Printf("  恢复后健康: %s\n", p.CheckHealth(clk.Now()))

	fmt.Println("--- 判定报告 ---")
	fmt.Print(p.Report())
}
