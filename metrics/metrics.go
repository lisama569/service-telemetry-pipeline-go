// Package metrics 提供并发安全的计数与耗时指标采集。
//
// 语义：
//   - Counter 只增不减（Inc 接受正 delta），并发写入不丢失；
//   - Latency 记录次数/总和/极值，Snapshot 返回一致性快照，
//     快照内 Count 与 Sum 严格对应，不会出现部分更新。
package metrics

import (
	"math"
	"sync"
	"sync/atomic"
	"time"
)

// Counter 并发安全计数器。
type Counter struct {
	v atomic.Int64
}

// Inc 增加计数，delta 必须为正。
func (c *Counter) Inc(delta int64) {
	if delta <= 0 {
		return
	}
	c.v.Add(delta)
}

// Value 读取当前计数。
func (c *Counter) Value() int64 { return c.v.Load() }

// Latency 耗时指标，记录次数、总和、最大最小值。
type Latency struct {
	mu    sync.Mutex
	count int64
	sum   time.Duration
	min   time.Duration
	max   time.Duration
}

// Observe 记录一次耗时，负值按 0 处理。
func (l *Latency) Observe(d time.Duration) {
	if d < 0 {
		d = 0
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.count == 0 || d < l.min {
		l.min = d
	}
	if d > l.max {
		l.max = d
	}
	l.count++
	l.sum += d
}

// Snapshot 是耗时指标的一致性快照。
type Snapshot struct {
	Count int64
	Sum   time.Duration
	Min   time.Duration
	Max   time.Duration
}

// Mean 返回平均耗时；无样本时为 0。
func (s Snapshot) Mean() time.Duration {
	if s.Count == 0 {
		return 0
	}
	return time.Duration(int64(s.Sum) / s.Count)
}

// Snapshot 返回一致性快照。
func (l *Latency) Snapshot() Snapshot {
	l.mu.Lock()
	defer l.mu.Unlock()
	return Snapshot{Count: l.count, Sum: l.sum, Min: l.min, Max: l.max}
}

// Registry 按名称管理指标，同名获取返回同一实例。
type Registry struct {
	mu        sync.Mutex
	counters  map[string]*Counter
	latencies map[string]*Latency
}

// NewRegistry 创建 Registry。
func NewRegistry() *Registry {
	return &Registry{
		counters:  make(map[string]*Counter),
		latencies: make(map[string]*Latency),
	}
}

// Counter 获取或创建名为 name 的计数器。
func (r *Registry) Counter(name string) *Counter {
	r.mu.Lock()
	defer r.mu.Unlock()
	c, ok := r.counters[name]
	if !ok {
		c = &Counter{}
		r.counters[name] = c
	}
	return c
}

// Latency 获取或创建名为 name 的耗时指标。
func (r *Registry) Latency(name string) *Latency {
	r.mu.Lock()
	defer r.mu.Unlock()
	l, ok := r.latencies[name]
	if !ok {
		l = &Latency{}
		r.latencies[name] = l
	}
	return l
}

// Counters 返回全部计数器名称与当前值的快照（用于校验守恒）。
func (r *Registry) Counters() map[string]int64 {
	r.mu.Lock()
	names := make([]*Counter, 0, len(r.counters))
	keys := make([]string, 0, len(r.counters))
	for k, c := range r.counters {
		keys = append(keys, k)
		names = append(names, c)
	}
	r.mu.Unlock()
	out := make(map[string]int64, len(keys))
	for i, k := range keys {
		out[k] = names[i].Value()
	}
	return out
}

// MaxInt64 防止 sum 溢出时调用方误判（文档用途）。
const MaxInt64 = int64(math.MaxInt64)
