package alerting

import (
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"
)

// agg 是某规则某窗口内的聚合状态。
type agg struct {
	start time.Time
	sum   float64
	count int
}

// ruleState 是一条已注册规则的运行时状态。
type ruleState struct {
	rule Rule
	// pending 未闭窗聚合，key 为窗口起点 UnixNano（各规则桶独立，互不干扰）。
	pending map[int64]*agg
	// closed 已闭窗窗口起点集合，每个窗口只判定一次。
	closed map[int64]struct{}
	// active 是否存在尚未恢复的告警。
	active bool
}

// Evaluator 按事件时间水位评估窗口，并发安全。
type Evaluator struct {
	mu sync.Mutex

	// rules 按 id 存放规则；同一 id 只保留最新版本（规则变更=换版本）。
	rules map[string]*ruleState
	// order 记录规则 id 的注册顺序，保证输出确定性。
	order []string

	// watermark[metric] 已见最大事件时间。
	watermark map[string]time.Time

	alerts []Alert

	// lateDropped 统计所属窗口已闭后才到、被拒绝的迟到事件数。
	lateDropped int
}

// NewEvaluator 创建空评估器。
func NewEvaluator() *Evaluator {
	return &Evaluator{
		rules:     make(map[string]*ruleState),
		watermark: make(map[string]time.Time),
	}
}

// AddRule 注册规则。同一 ID 再次添加视为规则变更：必须带更大的 Version，
// 旧窗口产生的历史告警保留旧版本号，新窗口按新版本评估。
func (e *Evaluator) AddRule(rule Rule) error {
	if err := rule.validate(); err != nil {
		return err
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if existing, ok := e.rules[rule.ID]; ok {
		if rule.Version <= existing.rule.Version {
			return fmt.Errorf("rule %s version %d must be greater than existing version %d",
				rule.ID, rule.Version, existing.rule.Version)
		}
		existing.rule = rule
		return nil
	}
	e.rules[rule.ID] = &ruleState{
		rule:    rule,
		pending: make(map[int64]*agg),
		closed:  make(map[int64]struct{}),
	}
	e.order = append(e.order, rule.ID)
	return nil
}

// Tick 由后台时钟驱动，把所有 metric 的水位推进到 now，
// 关闭所有已到边界的窗口（即使之后再无该 metric 的事件），返回新触发的告警。
func (e *Evaluator) Tick(now time.Time) []Alert {
	e.mu.Lock()
	defer e.mu.Unlock()
	var fired []Alert
	metrics := make(map[string]struct{})
	for _, id := range e.order {
		metrics[e.rules[id].rule.Metric] = struct{}{}
	}
	for m := range metrics {
		wm := e.watermark[m]
		if now.After(wm) {
			wm = now
			e.watermark[m] = wm
		}
		for _, rs := range e.ruleStatesFor(m) {
			fired = append(fired, e.closeWindows(rs, wm)...)
		}
	}
	return fired
}

// LateDropped 返回因迟到（所属窗口已闭）被拒绝的事件数。
func (e *Evaluator) LateDropped() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.lateDropped
}

// Push 送入一个事件并推进相关规则的水位，返回本次推进新触发的告警。
// 乱序但落入未闭窗口的事件正常计入；已闭窗口的迟到事件被拒绝并计数。
func (e *Evaluator) Push(ev Event) []Alert {
	e.mu.Lock()
	defer e.mu.Unlock()
	if ev.Name == "" {
		return nil
	}

	wm := e.watermark[ev.Name]
	if !ev.At.Before(wm) {
		wm = ev.At
		e.watermark[ev.Name] = wm
	}

	states := e.ruleStatesFor(ev.Name)

	// 1) 先用新水位关闭所有 end <= wm 的窗口并判定（每个窗口恰好一次）。
	var fired []Alert
	for _, rs := range states {
		fired = append(fired, e.closeWindows(rs, wm)...)
	}

	// 2) 再决定事件归属：若其窗口已闭，属于迟到事件，拒绝。
	for _, rs := range states {
		ws := windowStart(ev.At, rs.rule.Window)
		if _, closed := rs.closed[ws.UnixNano()]; closed {
			e.lateDropped++
			return fired
		}
	}

	// 3) 落入各规则仍打开的窗口桶。
	for _, rs := range states {
		ws := windowStart(ev.At, rs.rule.Window)
		key := ws.UnixNano()
		g := rs.pending[key]
		if g == nil {
			g = &agg{start: ws}
			rs.pending[key] = g
		}
		g.sum += ev.Value
		g.count++
	}
	return fired
}

// closeWindows 关闭规则 rs 下所有结束时间不晚于 wm 的窗口并做判定。
// 调用方持锁。
func (e *Evaluator) closeWindows(rs *ruleState, wm time.Time) []Alert {
	var keys []int64
	for key := range rs.pending {
		start := time.Unix(0, key)
		if !start.Add(rs.rule.Window).After(wm) {
			keys = append(keys, key)
		}
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })

	var fired []Alert
	for _, key := range keys {
		g := rs.pending[key]
		delete(rs.pending, key)
		if _, ok := rs.closed[key]; ok {
			continue // 双保险：窗口不重复判定。
		}
		rs.closed[key] = struct{}{}
		if a := e.judge(rs, g, wm); a != nil {
			fired = append(fired, *a)
		}
	}
	return fired
}

// judge 对一个刚关闭的窗口做阈值判定，处理触发去重与恢复。调用方持锁。
// 判定条件：窗口内事件值之和 >= 阈值。
func (e *Evaluator) judge(rs *ruleState, g *agg, closedAt time.Time) *Alert {
	breached := g.sum >= rs.rule.Threshold
	switch {
	case breached && !rs.active:
		// 仅在不存在未恢复告警时触发，杜绝活动期内重复告警。
		rs.active = true
		a := Alert{
			RuleID:      rs.rule.ID,
			Version:     rs.rule.Version,
			Severity:    rs.rule.Severity,
			WindowStart: g.start,
			WindowEnd:   g.start.Add(rs.rule.Window),
			Value:       g.sum,
			Threshold:   rs.rule.Threshold,
			FiredAt:     closedAt,
		}
		e.alerts = append(e.alerts, a)
		return &a
	case breached:
		// 已活动：连续超阈值窗口不重复触发。
		return nil
	case rs.active:
		// 一个低于阈值的窗口关闭 => 活动告警恢复；之后再次超阈值可重新触发。
		if idx := e.findActive(rs.rule.ID); idx >= 0 {
			e.alerts[idx].RecoveredAt = closedAt
		}
		rs.active = false
	}
	return nil
}

func (e *Evaluator) findActive(ruleID string) int {
	for i := len(e.alerts) - 1; i >= 0; i-- {
		if e.alerts[i].RuleID == ruleID && e.alerts[i].Active() {
			return i
		}
	}
	return -1
}

// ActiveAlerts 返回当前仍活动的告警副本，按规则注册顺序排列。
func (e *Evaluator) ActiveAlerts(now time.Time) []Alert {
	e.mu.Lock()
	defer e.mu.Unlock()
	var out []Alert
	for _, id := range e.order {
		if rs, ok := e.rules[id]; ok && rs.active {
			if idx := e.findActive(id); idx >= 0 {
				out = append(out, e.alerts[idx])
			}
		}
	}
	_ = now
	return out
}

// Alerts 返回评估器产生过的全部告警（含已恢复），顺序即触发顺序。
func (e *Evaluator) Alerts() []Alert {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make([]Alert, len(e.alerts))
	copy(out, e.alerts)
	return out
}

func (e *Evaluator) ruleStatesFor(metric string) []*ruleState {
	var out []*ruleState
	for _, id := range e.order {
		if rs := e.rules[id]; rs.rule.Metric == metric {
			out = append(out, rs)
		}
	}
	return out
}

// ErrNoRules 表示重放时没有提供任何规则。
var ErrNoRules = errors.New("replay requires at least one rule")

// Replay 使用给定（历史版本）规则对一批事件做确定性重放。
// 事件先按事件时间稳定排序，乱序输入也得到完全一致的历史判定。
func Replay(rules []Rule, events []Event) (*Evaluator, error) {
	if len(rules) == 0 {
		return nil, ErrNoRules
	}
	ev := NewEvaluator()
	for _, r := range rules {
		if err := ev.AddRule(r); err != nil {
			return nil, err
		}
	}
	sorted := make([]Event, len(events))
	copy(sorted, events)
	sort.SliceStable(sorted, func(i, j int) bool {
		if sorted[i].At.Equal(sorted[j].At) {
			return sorted[i].Name < sorted[j].Name
		}
		return sorted[i].At.Before(sorted[j].At)
	})
	for _, e2 := range sorted {
		ev.Push(e2)
	}
	return ev, nil
}
