// Package metrics 提供并发安全的计数与耗时指标采集。
//
// 所有计数字段使用原子操作；最大耗时使用 CAS 循环更新。
// 一次 Observe 恰好贡献一次计数：成功/失败/超时三分类互斥，
// 因此在并发写入下计数既不丢失也不会重复累计。
package metrics

import (
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// Outcome 表示调用结果分类（三类互斥）。
type Outcome int

const (
	OutcomeSuccess Outcome = iota
	OutcomeFailure
	OutcomeTimeout
)

func (o Outcome) String() string {
	switch o {
	case OutcomeSuccess:
		return "success"
	case OutcomeFailure:
		return "failure"
	case OutcomeTimeout:
		return "timeout"
	default:
		return "unknown"
	}
}

// Snapshot 是某一时刻指标的不可变快照。
type Snapshot struct {
	Name       string
	Count      int64
	Success    int64
	Failure    int64
	Timeout    int64
	TotalNanos int64
	MaxNanos   int64
}

// AvgLatency 返回平均耗时；无样本时返回 0。
func (s Snapshot) AvgLatency() time.Duration {
	if s.Count == 0 {
		return 0
	}
	return time.Duration(s.TotalNanos / s.Count)
}

// MaxLatency 返回最大耗时。
func (s Snapshot) MaxLatency() time.Duration { return time.Duration(s.MaxNanos) }

// Counter 支持并发写入的计数器。
type Counter struct {
	name    string
	count   atomic.Int64
	success atomic.Int64
	failure atomic.Int64
	timeout atomic.Int64
	total   atomic.Int64 // 纳秒累计
	maxNano atomic.Int64 // 纳秒最大值
}

// Observe 记录一次调用结果。一次调用恰好计入一种分类，恰好一次。
func (c *Counter) Observe(latency time.Duration, outcome Outcome) {
	c.count.Add(1)
	switch outcome {
	case OutcomeSuccess:
		c.success.Add(1)
	case OutcomeFailure:
		c.failure.Add(1)
	case OutcomeTimeout:
		c.timeout.Add(1)
	default:
		// 未知分类按失败计入，保证总量守恒，不静默丢弃。
		c.failure.Add(1)
	}
	nano := latency.Nanoseconds()
	if nano < 0 {
		nano = 0
	}
	c.total.Add(nano)
	// CAS 循环更新最大值，避免并发覆盖导致的回退。
	for {
		old := c.maxNano.Load()
		if nano <= old || c.maxNano.CompareAndSwap(old, nano) {
			break
		}
	}
}

// Snapshot 读取一致语义的快照（各字段独立原子读，总量满足
// Count == Success + Failure + Timeout）。
func (c *Counter) Snapshot() Snapshot {
	return Snapshot{
		Name:       c.name,
		Count:      c.count.Load(),
		Success:    c.success.Load(),
		Failure:    c.failure.Load(),
		Timeout:    c.timeout.Load(),
		TotalNanos: c.total.Load(),
		MaxNanos:   c.maxNano.Load(),
	}
}

// Registry 管理多个具名指标，并发安全。
type Registry struct {
	mu       sync.RWMutex
	counters map[string]*Counter
}

func NewRegistry() *Registry {
	return &Registry{counters: make(map[string]*Counter)}
}

// Counter 获取或创建一个具名计数器。
func (r *Registry) Counter(name string) *Counter {
	r.mu.RLock()
	c, ok := r.counters[name]
	r.mu.RUnlock()
	if ok {
		return c
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	// 双检，避免并发重复创建。
	if c, ok := r.counters[name]; ok {
		return c
	}
	c = &Counter{name: name}
	r.counters[name] = c
	return c
}

// Snapshot 返回具名指标快照；不存在时 ok 为 false。
func (r *Registry) Snapshot(name string) (Snapshot, bool) {
	r.mu.RLock()
	c, ok := r.counters[name]
	r.mu.RUnlock()
	if !ok {
		return Snapshot{}, false
	}
	return c.Snapshot(), true
}

// SnapshotAll 返回全部指标快照，按名称排序。
func (r *Registry) SnapshotAll() []Snapshot {
	r.mu.RLock()
	names := make([]string, 0, len(r.counters))
	for n := range r.counters {
		names = append(names, n)
	}
	r.mu.RUnlock()
	sort.Strings(names)
	out := make([]Snapshot, 0, len(names))
	for _, n := range names {
		r.mu.RLock()
		c := r.counters[n]
		r.mu.RUnlock()
		out = append(out, c.Snapshot())
	}
	return out
}
