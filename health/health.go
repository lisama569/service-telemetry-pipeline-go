// Package health 基于探针历史判定健康状态。
//
// 判定语义（均基于滑动窗口内的实时历史，不缓存结论）：
//   - 持续故障：窗口内失败数达到 FailureThreshold 且多数非超时 → Unhealthy/sustained_failure；
//   - 依赖超时：窗口内失败数达到阈值且多数为超时（Duration >= Timeout）→ Unhealthy/dependency_timeout；
//   - 短暂抖动：窗口内失败数不超过 BlipTolerance → Degraded/transient_blip；
//   - 其余：Healthy/ok。
//
// Evaluate 每次调用都重新扫描窗口内历史，因此故障恢复后结论立即翻转，
// 不会被旧缓存掩盖。
package health

import (
	"fmt"
	"sync"
	"time"
)

// State 健康状态。
type State int

const (
	StateHealthy State = iota
	StateDegraded
	StateUnhealthy
)

// String 返回可读状态，便于日志打印。
func (s State) String() string {
	switch s {
	case StateHealthy:
		return "healthy"
	case StateDegraded:
		return "degraded"
	case StateUnhealthy:
		return "unhealthy"
	default:
		return "unknown"
	}
}

// Reason 判定原因，使失败原因可区分。
type Reason string

const (
	ReasonOK                Reason = "ok"
	ReasonSustainedFailure  Reason = "sustained_failure"
	ReasonDependencyTimeout Reason = "dependency_timeout"
	ReasonTransientBlip     Reason = "transient_blip"
)

// ProbeResult 一次探针结果。
type ProbeResult struct {
	At       time.Time
	Err      error
	Duration time.Duration
}

// Verdict 一次健康判定结论。
type Verdict struct {
	State  State
	Reason Reason
	Detail string
}

// String 便于在日志中打印判定依据。
func (v Verdict) String() string {
	return fmt.Sprintf("state=%s reason=%s detail=%s", v.State, v.Reason, v.Detail)
}

// Config Checker 配置。
type Config struct {
	// Window 滑动窗口长度，窗口外的历史不参与判定。
	Window time.Duration
	// FailureThreshold 窗口内失败数达到该值判定为 Unhealthy。
	FailureThreshold int
	// Timeout 单次探针耗时达到该值视为依赖超时。
	Timeout time.Duration
	// BlipTolerance 窗口内失败数不超过该值视为短暂抖动。
	BlipTolerance int
}

func (cfg Config) withDefaults() Config {
	if cfg.Window <= 0 {
		cfg.Window = time.Minute
	}
	if cfg.FailureThreshold <= 0 {
		cfg.FailureThreshold = 3
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = 5 * time.Second
	}
	if cfg.BlipTolerance < 0 {
		cfg.BlipTolerance = 0
	}
	return cfg
}

// Checker 基于滑动窗口内的探针历史做判定，不缓存结论。
type Checker struct {
	mu      sync.Mutex
	cfg     Config
	history []ProbeResult // 按时间升序，窗口外条目在记录/判定时裁剪
}

// NewChecker 创建 Checker。
func NewChecker(cfg Config) *Checker {
	return &Checker{cfg: cfg.withDefaults()}
}

// Record 记录一次探针结果，并发安全。
func (c *Checker) Record(r ProbeResult) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.history = append(c.history, r)
	c.pruneLocked(r.At)
}

// pruneLocked 丢弃相对 at 已滑出窗口的历史。
func (c *Checker) pruneLocked(at time.Time) {
	cutoff := at.Add(-c.cfg.Window)
	keep := 0
	for keep < len(c.history) && c.history[keep].At.Before(cutoff) {
		keep++
	}
	if keep > 0 {
		c.history = append([]ProbeResult(nil), c.history[keep:]...)
	}
}

// Evaluate 基于 now 时刻窗口内历史实时判定；每次调用重新计算，不缓存。
func (c *Checker) Evaluate(now time.Time) Verdict {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.pruneLocked(now)

	failures, timeouts := 0, 0
	for _, r := range c.history {
		if r.Err != nil {
			failures++
			if r.Duration >= c.cfg.Timeout {
				timeouts++
			}
		}
	}
	detail := fmt.Sprintf("window=%s samples=%d failures=%d timeouts=%d threshold=%d blip_tolerance=%d",
		c.cfg.Window, len(c.history), failures, timeouts, c.cfg.FailureThreshold, c.cfg.BlipTolerance)

	switch {
	case failures >= c.cfg.FailureThreshold && timeouts*2 >= failures:
		return Verdict{State: StateUnhealthy, Reason: ReasonDependencyTimeout, Detail: detail}
	case failures >= c.cfg.FailureThreshold:
		return Verdict{State: StateUnhealthy, Reason: ReasonSustainedFailure, Detail: detail}
	case failures > c.cfg.BlipTolerance:
		// 超过抖动容忍但未达故障阈值：降级，按主导失败类型给出原因。
		reason := ReasonSustainedFailure
		if timeouts*2 >= failures {
			reason = ReasonDependencyTimeout
		}
		return Verdict{State: StateDegraded, Reason: reason, Detail: detail}
	case failures > 0:
		return Verdict{State: StateDegraded, Reason: ReasonTransientBlip, Detail: detail}
	default:
		return Verdict{State: StateHealthy, Reason: ReasonOK, Detail: detail}
	}
}
