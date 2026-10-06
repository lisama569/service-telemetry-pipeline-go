// telemetry-demo 一键跑通混合场景（同步请求 + 并发后台任务 + 依赖先超时后恢复），
// 过程中打印各环节关联标识，结束后输出完整判定报告。
//
// 为保证报告可复现，演示使用逻辑时钟与确定性 ID 生成器：
// 同样的输入重跑，报告内容完全一致。
package main

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lisama569/service-telemetry-pipeline-go/alert"
	"github.com/lisama569/service-telemetry-pipeline-go/correlation"
	"github.com/lisama569/service-telemetry-pipeline-go/health"
	"github.com/lisama569/service-telemetry-pipeline-go/pipeline"
)

// logicalClock 逻辑时钟，并发安全。
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

// dependency 模拟下游依赖：recoverAt 之前调用超时，之后恢复。
type dependency struct {
	clock     *logicalClock
	recoverAt time.Time
}

var errTimeout = errors.New("dependency timeout")

func (d *dependency) Call() error {
	if d.clock.Now().Before(d.recoverAt) {
		d.clock.Advance(3 * time.Second) // 超时路径：耗时超过健康判定的 Timeout
		return errTimeout
	}
	d.clock.Advance(10 * time.Millisecond) // 恢复后：快速成功
	return nil
}

func main() {
	base := time.Date(2026, 10, 6, 0, 0, 0, 0, time.UTC)
	clock := &logicalClock{t: base}
	var seq atomic.Int64
	idFunc := func() string { return fmt.Sprintf("%032x", seq.Add(1)) }

	p := pipeline.New(pipeline.Config{
		Health: health.Config{
			Window:           30 * time.Second,
			FailureThreshold: 3,
			Timeout:          2 * time.Second,
			BlipTolerance:    1,
		},
		Rules: []alert.Rule{
			{Name: pipeline.AlertKeyFailures, Version: 1, Window: time.Minute, Threshold: 1, MinCount: 3},
		},
		Now:         clock.Now,
		TrackerOpts: []correlation.Option{correlation.WithIDFunc(idFunc)},
	})
	root := context.Background()
	log := func(phase string, ctx context.Context) {
		tc, _ := correlation.FromContext(ctx)
		fmt.Printf("[%-12s] %s\n", phase, tc)
	}

	// 阶段 1: 同步请求 —— 覆盖缺失/合法/非法上游标识, 以及标识归还后的再次继承。
	fmt.Println("== 阶段1: 同步请求 ==")
	upstream := "aaaa0000bbbb0000"
	for _, up := range []string{"", upstream, "!!not-hex!!", upstream} {
		ctx, _ := p.HandleRequest(root, up, func(context.Context) error {
			clock.Advance(10 * time.Millisecond)
			return nil
		})
		log("sync", ctx)
	}

	// 阶段 2: 一个请求派生出并发后台任务 —— 全部挂在该请求的 TraceID 上。
	fmt.Println("== 阶段2: 请求派生并发后台任务 ==")
	ctx, _ := p.HandleRequest(root, "cccc0000dddd0000", func(ctx context.Context) error {
		var wg sync.WaitGroup
		for i := 0; i < 8; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				bgCtx, _ := p.SpawnBackground(ctx, func(context.Context) error { return nil })
				log("background", bgCtx)
			}()
		}
		wg.Wait()
		return nil
	})
	log("fanout-root", ctx)

	// 阶段 3: 依赖超时 —— 连续失败驱动健康与告警判定。
	fmt.Println("== 阶段3: 依赖超时 ==")
	dep := &dependency{clock: clock, recoverAt: base.Add(2 * time.Minute)}
	for i := 0; i < 4; i++ {
		ctx, err := p.HandleRequest(root, "", func(context.Context) error { return dep.Call() })
		log("dep-timeout", ctx)
		if err == nil {
			fmt.Println("预期依赖超时, 实际成功")
		}
	}

	// 阶段 4: 时钟推进越过窗口, 依赖恢复 —— 健康结论翻转, 告警恢复。
	fmt.Println("== 阶段4: 依赖恢复 ==")
	clock.Advance(2 * time.Minute)
	for i := 0; i < 3; i++ {
		ctx, err := p.HandleRequest(root, "", func(context.Context) error { return dep.Call() })
		log("dep-recovered", ctx)
		if err != nil {
			fmt.Printf("预期依赖恢复, 实际失败: %v\n", err)
		}
	}

	// 输出判定报告。
	fmt.Println()
	fmt.Print(p.Report().String())
}
