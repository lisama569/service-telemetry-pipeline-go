// Package health 基于实时探测记录判定依赖健康状态。
//
// 判定语义（只基于最近一个时间窗口内的真实探测记录，不做任何结论缓存，
// 因此不会用缓存掩盖真实故障）：
//
//	StateTimeout —— 窗口内超时样本占主导（>=50%），代表依赖响应迟缓；
//	StateDown    —— 非超时故障连续出现且窗口成功率低于阈值，代表持续故障；
//	StateFlaky   —— 成功与失败混杂，代表短暂抖动；
//	StateHealthy —— 窗口内全部成功（且样本数达标）；
//	StateUnknown —— 窗口内尚无足够样本。
package health

import (
	"fmt"
	"strings"
	"sync"
	"time"
)

// State 健康结论。
type State int

const (
	StateUnknown State = iota
	StateHealthy
	StateFlaky
	StateTimeout
	StateDown
)

func (s State) String() string {
	switch s {
	case StateHealthy:
		return "healthy"
	case StateFlaky:
		return "flaky"
	case StateTimeout:
		return "timeout"
	case StateDown:
		return "down"
	default:
		return "unknown"
	}
}

// ProbeResult 单次探测结果。At 为探测完成时间（事件时间）。
type ProbeResult struct {
	At      time.Time
	Timeout bool
	OK      bool
	Reason  string
}

// Verdict 健康判定结论及其判定依据。
type Verdict struct {
	State       State
	Reason      string
	Window      string
	Success     int
	Failure     int
	Timeout     int
	EvaluatedAt time.Time
}

// Checker 保存近期探测记录并做实时判定，并发安全。
type Checker struct {
	window           time.Duration
	minSamples       int // 得出非 Unknown 结论所需的最少样本数
	failureThreshold int // 判定持续故障所需的连续非超时失败数

	mu      sync.Mutex
	records []ProbeResult
}

// NewChecker 创建健康判定器：window 为观测窗口，failureThreshold 为
// 判定持续故障所需的连续失败数。
func NewChecker(window time.Duration, failureThreshold int) *Checker {
	if failureThreshold < 1 {
		failureThreshold = 1
	}
	return &Checker{
		window:           window,
		minSamples:       3,
		failureThreshold: failureThreshold,
		records:          make([]ProbeResult, 0, 64),
	}
}

// Record 追加一条探测记录（允许乱序到达，按事件时间重放窗口）。
func (c *Checker) Record(r ProbeResult) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.records = append(c.records, r)
}

// prune 丢弃早于 now-window 的记录，并按事件时间排序。调用方持锁。
func (c *Checker) prune(now time.Time) []ProbeResult {
	cutoff := now.Add(-c.window)
	kept := c.records[:0]
	for _, r := range c.records {
		if !r.At.Before(cutoff) {
			kept = append(kept, r)
		}
	}
	c.records = kept
	sortResults(c.records)
	return c.records
}

// Evaluate 以 now 为准实时计算窗口内的健康结论。每次都重新计算，不缓存。
func (c *Checker) Evaluate(now time.Time) Verdict {
	c.mu.Lock()
	records := c.prune(now)
	snapshot := make([]ProbeResult, len(records))
	copy(snapshot, records)
	c.mu.Unlock()

	v := Verdict{Window: c.window.String(), EvaluatedAt: now}
	for _, r := range snapshot {
		switch {
		case r.Timeout:
			v.Timeout++
		case r.OK:
			v.Success++
		default:
			v.Failure++
		}
	}
	total := len(snapshot)

	switch {
	case total < c.minSamples:
		v.State = StateUnknown
		v.Reason = fmt.Sprintf("insufficient samples: %d < %d", total, c.minSamples)
	case v.Timeout*2 >= total:
		v.State = StateTimeout
		v.Reason = fmt.Sprintf("timeouts dominate window: %d/%d >= 50%%", v.Timeout, total)
	default:
		// 统计窗口尾部（按时间最新方向）连续非超时失败数。
		consecutiveFailures := 0
		for i := len(snapshot) - 1; i >= 0; i-- {
			r := snapshot[i]
			if r.Timeout || r.OK {
				break
			}
			consecutiveFailures++
		}
		successRate := float64(v.Success) / float64(total)
		if consecutiveFailures >= c.failureThreshold && successRate < 0.5 {
			v.State = StateDown
			v.Reason = fmt.Sprintf("sustained failures: %d consecutive failures, success rate %.0f%%",
				consecutiveFailures, successRate*100)
		} else if v.Success == total {
			v.State = StateHealthy
			v.Reason = fmt.Sprintf("all %d probes succeeded", total)
		} else {
			v.State = StateFlaky
			v.Reason = fmt.Sprintf("mixed results in window: %d ok, %d fail, %d timeout",
				v.Success, v.Failure, v.Timeout)
		}
	}
	return v
}

// DescribeReasons 返回窗口内非成功探测的原因摘要，便于日志解释。
func (c *Checker) DescribeReasons(now time.Time) string {
	c.mu.Lock()
	records := c.prune(now)
	snapshot := make([]ProbeResult, len(records))
	copy(snapshot, records)
	c.mu.Unlock()
	reasons := make([]string, 0)
	for _, r := range snapshot {
		if r.OK && !r.Timeout {
			continue
		}
		kind := "failure"
		if r.Timeout {
			kind = "timeout"
		}
		reasons = append(reasons, fmt.Sprintf("%s@%s:%s", kind, r.At.Format(time.RFC3339), r.Reason))
	}
	return strings.Join(reasons, "; ")
}
