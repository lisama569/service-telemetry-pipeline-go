package health

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

var errBoom = errors.New("boom")

func cfg() Config {
	return Config{
		Window:           30 * time.Second,
		FailureThreshold: 3,
		Timeout:          2 * time.Second,
		BlipTolerance:    1,
	}
}

func TestTransientBlip(t *testing.T) {
	c := NewChecker(cfg())
	now := time.Now()
	c.Record(ProbeResult{At: now, Err: errBoom, Duration: 10 * time.Millisecond})
	v := c.Evaluate(now)
	t.Logf("verdict=%s 判定依据: 单次失败未超抖动容忍", v)
	if v.State != StateDegraded || v.Reason != ReasonTransientBlip {
		t.Fatalf("期望 degraded/transient_blip, 得到 %s", v)
	}
}

func TestSustainedFailure(t *testing.T) {
	c := NewChecker(cfg())
	now := time.Now()
	for i := 0; i < 3; i++ {
		c.Record(ProbeResult{At: now.Add(time.Duration(i) * time.Second), Err: errBoom, Duration: 10 * time.Millisecond})
	}
	v := c.Evaluate(now.Add(3 * time.Second))
	t.Logf("verdict=%s 判定依据: 窗口内失败数达到阈值且非超时", v)
	if v.State != StateUnhealthy || v.Reason != ReasonSustainedFailure {
		t.Fatalf("期望 unhealthy/sustained_failure, 得到 %s", v)
	}
}

func TestDependencyTimeout(t *testing.T) {
	c := NewChecker(cfg())
	now := time.Now()
	for i := 0; i < 3; i++ {
		c.Record(ProbeResult{At: now.Add(time.Duration(i) * time.Second), Err: errBoom, Duration: 3 * time.Second})
	}
	v := c.Evaluate(now.Add(3 * time.Second))
	t.Logf("verdict=%s 判定依据: 窗口内失败多数为超时", v)
	if v.State != StateUnhealthy || v.Reason != ReasonDependencyTimeout {
		t.Fatalf("期望 unhealthy/dependency_timeout, 得到 %s", v)
	}
}

// TestDependencyTimeoutToRecovery 覆盖依赖从超时到恢复的场景。
func TestDependencyTimeoutToRecovery(t *testing.T) {
	c := NewChecker(cfg())
	base := time.Now()
	// 阶段一: 依赖持续超时
	for i := 0; i < 3; i++ {
		c.Record(ProbeResult{At: base.Add(time.Duration(i) * time.Second), Err: errBoom, Duration: 5 * time.Second})
	}
	v1 := c.Evaluate(base.Add(3 * time.Second))
	t.Logf("阶段一 verdict=%s 判定依据: 依赖连续超时", v1)
	if v1.Reason != ReasonDependencyTimeout {
		t.Fatalf("阶段一期望 dependency_timeout, 得到 %s", v1)
	}
	// 阶段二: 依赖恢复, 旧故障滑出窗口
	recovered := base.Add(40 * time.Second)
	for i := 0; i < 3; i++ {
		c.Record(ProbeResult{At: recovered.Add(time.Duration(i) * time.Second), Duration: 20 * time.Millisecond})
	}
	v2 := c.Evaluate(recovered.Add(3 * time.Second))
	t.Logf("阶段二 verdict=%s 判定依据: 故障样本滑出窗口, 实时重算不缓存", v2)
	if v2.State != StateHealthy || v2.Reason != ReasonOK {
		t.Fatalf("恢复后期望 healthy/ok, 得到 %s", v2)
	}
}

func TestNoCacheMasking(t *testing.T) {
	c := NewChecker(cfg())
	now := time.Now()
	v0 := c.Evaluate(now)
	if v0.State != StateHealthy {
		t.Fatalf("初始应健康, 得到 %s", v0)
	}
	// 判定后立刻注入故障, 再次判定必须立即反映, 证明无缓存掩盖。
	for i := 0; i < 3; i++ {
		c.Record(ProbeResult{At: now, Err: errBoom, Duration: time.Millisecond})
	}
	v1 := c.Evaluate(now)
	t.Logf("verdict=%s 判定依据: 故障注入后立即重判", v1)
	if v1.State != StateUnhealthy {
		t.Fatalf("缓存掩盖了真实故障: %s", v1)
	}
}

func TestConcurrentRecordConservation(t *testing.T) {
	c := NewChecker(Config{Window: time.Minute, FailureThreshold: 100000, BlipTolerance: 100000})
	now := time.Now()
	const workers = 16
	const perWorker = 250
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < perWorker; j++ {
				r := ProbeResult{At: now, Duration: time.Millisecond}
				if j%2 == 0 {
					r.Err = errBoom
				}
				c.Record(r)
			}
		}(i)
	}
	wg.Wait()
	v := c.Evaluate(now)
	wantFailures := workers * perWorker / 2
	t.Logf("verdict=%s 判定依据: 并发写入后样本与失败数守恒", v)
	if v.State != StateDegraded {
		t.Fatalf("高阈值下应 degraded, 得到 %s", v)
	}
	var samples, failures int
	if _, err := fmt.Sscanf(v.Detail, "window=1m0s samples=%d failures=%d", &samples, &failures); err != nil {
		t.Fatalf("解析 detail 失败: %v", err)
	}
	if samples != workers*perWorker || failures != wantFailures {
		t.Fatalf("不守恒: samples=%d(期望%d) failures=%d(期望%d)", samples, workers*perWorker, failures, wantFailures)
	}
}
