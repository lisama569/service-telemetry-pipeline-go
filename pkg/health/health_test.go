package health

import (
	"testing"
	"time"
)

func at(sec int) time.Time { return time.Unix(int64(sec), 0) }

func TestHealthy(t *testing.T) {
	c := NewChecker(10*time.Second, 3)
	for i := 0; i < 5; i++ {
		c.Record(ProbeResult{At: at(i), OK: true})
	}
	v := c.Evaluate(at(5))
	if v.State != StateHealthy {
		t.Fatalf("state = %s (%s)", v.State, v.Reason)
	}
	t.Logf("健康[全部成功] 判定=%s 成功=%d 失败=%d 超时=%d 依据=%s",
		v.State, v.Success, v.Failure, v.Timeout, v.Reason)
}

func TestUnknownUntilEnoughSamples(t *testing.T) {
	c := NewChecker(10*time.Second, 3)
	c.Record(ProbeResult{At: at(0), OK: true})
	c.Record(ProbeResult{At: at(1), OK: false, Reason: "conn refused"})
	v := c.Evaluate(at(2))
	if v.State != StateUnknown {
		t.Fatalf("state = %s, want unknown", v.State)
	}
	t.Logf("健康[样本不足] 判定=%s 依据=%s", v.State, v.Reason)
}

func TestTimeoutDominates(t *testing.T) {
	c := NewChecker(10*time.Second, 3)
	c.Record(ProbeResult{At: at(0), OK: true})
	c.Record(ProbeResult{At: at(1), Timeout: true, Reason: "read timeout 500ms"})
	c.Record(ProbeResult{At: at(2), Timeout: true, Reason: "read timeout 500ms"})
	c.Record(ProbeResult{At: at(3), Timeout: true, Reason: "read timeout 500ms"})
	v := c.Evaluate(at(4))
	if v.State != StateTimeout {
		t.Fatalf("state = %s (%s), want timeout", v.State, v.Reason)
	}
	t.Logf("健康[依赖超时] 判定=%s 成功=%d 失败=%d 超时=%d 依据=%s 原因=%s",
		v.State, v.Success, v.Failure, v.Timeout, v.Reason, c.DescribeReasons(at(4)))
}

func TestSustainedDown(t *testing.T) {
	c := NewChecker(10*time.Second, 3)
	c.Record(ProbeResult{At: at(0), OK: true})
	for i := 1; i <= 3; i++ {
		c.Record(ProbeResult{At: at(i), Reason: "500 internal error"})
	}
	v := c.Evaluate(at(4))
	if v.State != StateDown {
		t.Fatalf("state = %s (%s), want down", v.State, v.Reason)
	}
	t.Logf("健康[持续故障] 判定=%s 成功=%d 失败=%d 超时=%d 依据=%s",
		v.State, v.Success, v.Failure, v.Timeout, v.Reason)
}

func TestFlakyTransient(t *testing.T) {
	c := NewChecker(10*time.Second, 3)
	c.Record(ProbeResult{At: at(0), OK: true})
	c.Record(ProbeResult{At: at(1), Reason: "502 transient"})
	c.Record(ProbeResult{At: at(2), OK: true})
	c.Record(ProbeResult{At: at(3), OK: true})
	v := c.Evaluate(at(4))
	if v.State != StateFlaky {
		t.Fatalf("state = %s (%s), want flaky", v.State, v.Reason)
	}
	t.Logf("健康[短暂抖动] 判定=%s 成功=%d 失败=%d 超时=%d 依据=%s",
		v.State, v.Success, v.Failure, v.Timeout, v.Reason)
}

func TestTimeoutRecoversToHealthy(t *testing.T) {
	c := NewChecker(10*time.Second, 3)
	// 阶段一：连续超时。
	c.Record(ProbeResult{At: at(0), Timeout: true, Reason: "upstream slow"})
	c.Record(ProbeResult{At: at(1), Timeout: true, Reason: "upstream slow"})
	c.Record(ProbeResult{At: at(2), Timeout: true, Reason: "upstream slow"})
	if v := c.Evaluate(at(3)); v.State != StateTimeout {
		t.Fatalf("phase1 state = %s, want timeout", v.State)
	}
	// 阶段二：依赖恢复，旧超时记录滑出 10 秒窗口后全部成功。
	for i := 11; i <= 13; i++ {
		c.Record(ProbeResult{At: at(i), OK: true})
	}
	v := c.Evaluate(at(14))
	if v.State != StateHealthy {
		t.Fatalf("phase2 state = %s (%s), want healthy (不得用缓存掩盖真实恢复)", v.State, v.Reason)
	}
	t.Logf("健康[超时->恢复] 判定=%s 窗口内成功=%d 失败=%d 超时=%d 依据=%s",
		v.State, v.Success, v.Failure, v.Timeout, v.Reason)
}

func TestNoCachingMasksRealFailure(t *testing.T) {
	c := NewChecker(10*time.Second, 3)
	for i := 0; i < 5; i++ {
		c.Record(ProbeResult{At: at(i), OK: true})
	}
	healthy := c.Evaluate(at(5))
	// 旧记录滑出窗口后立刻连续超时，评估必须反映真实故障，而非沿用 healthy。
	c.Record(ProbeResult{At: at(11), Timeout: true, Reason: "sudden stall"})
	c.Record(ProbeResult{At: at(12), Timeout: true, Reason: "sudden stall"})
	c.Record(ProbeResult{At: at(13), Timeout: true, Reason: "sudden stall"})
	v := c.Evaluate(at(14))
	if healthy.State != StateHealthy || v.State != StateTimeout {
		t.Fatalf("states = %s then %s, want healthy then timeout", healthy.State, v.State)
	}
	t.Logf("健康[故障不被缓存掩盖] 判定序列=%s -> %s 依据=%s", healthy.State, v.State, v.Reason)
}
