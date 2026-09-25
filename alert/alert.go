// Package alert 基于时间窗口的告警规则评估。
//
// 语义：
//   - 窗口为滑动窗口 [t-Window, t]，t 为评估时刻（取事件时间与已见最新时间的较大者）；
//   - 乱序事件按时间排序插入，评估时刻不后退，因此迟到事件仍计入当前窗口，不漏判；
//   - 触发去重：规则处于 firing 状态时不重复触发，恢复（评估转为不命中）后可再次触发；
//   - 规则携带版本，历史版本配置被保留，Evaluate 可按指定版本重放判定，保证可复现。
package alert

import (
	"fmt"
	"sort"
	"sync"
	"time"
)

// Event 一条待评估事件。
type Event struct {
	At    time.Time
	Key   string
	Value float64
}

// Rule 告警规则，携带版本以保证规则变更后历史判定可复现。
type Rule struct {
	Name      string
	Version   int
	Window    time.Duration
	Threshold float64
	// MinCount 窗口内命中事件数达到该值才触发；<=0 时按 1 处理。
	MinCount int
	// Compare 返回 true 表示事件值命中规则；nil 时按 ">=" 处理。
	Compare func(v, threshold float64) bool
}

func (r Rule) compare(v float64) bool {
	if r.Compare != nil {
		return r.Compare(v, r.Threshold)
	}
	return v >= r.Threshold
}

func (r Rule) minCount() int {
	if r.MinCount <= 0 {
		return 1
	}
	return r.MinCount
}

// Decision 一次评估结论。
type Decision struct {
	RuleName    string
	RuleVersion int
	Firing      bool
	Count       int
	WindowStart time.Time
	WindowEnd   time.Time
}

// String 便于在日志中打印判定依据。
func (d Decision) String() string {
	return fmt.Sprintf("rule=%s v%d firing=%v count=%d window=[%s,%s]",
		d.RuleName, d.RuleVersion, d.Firing, d.Count,
		d.WindowStart.Format(time.RFC3339Nano), d.WindowEnd.Format(time.RFC3339Nano))
}

// ruleState 单条规则（当前版本）的运行状态。
type ruleState struct {
	events     []Event // 按 At 升序
	latestSeen time.Time
	firing     bool
}

// Evaluator 按规则评估事件流，去重并在恢复后允许再次触发。
type Evaluator struct {
	mu      sync.Mutex
	rules   map[string]Rule         // 当前版本
	history map[string]map[int]Rule // 全部历史版本，用于复现
	states  map[string]*ruleState   // 按规则名
}

// NewEvaluator 创建 Evaluator；同名规则以最高版本为当前版本，旧版本保留用于复现。
func NewEvaluator(rules ...Rule) *Evaluator {
	e := &Evaluator{
		rules:   make(map[string]Rule),
		history: make(map[string]map[int]Rule),
		states:  make(map[string]*ruleState),
	}
	for _, r := range rules {
		e.addRuleLocked(r)
	}
	return e
}

func (e *Evaluator) addRuleLocked(r Rule) {
	if e.history[r.Name] == nil {
		e.history[r.Name] = make(map[int]Rule)
	}
	e.history[r.Name][r.Version] = r
	cur, ok := e.rules[r.Name]
	if !ok || r.Version > cur.Version {
		e.rules[r.Name] = r
	}
	if e.states[r.Name] == nil {
		e.states[r.Name] = &ruleState{}
	}
}

// UpdateRule 注册规则新版本；旧版本保留，历史判定仍可复现。
func (e *Evaluator) UpdateRule(r Rule) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.addRuleLocked(r)
}

// Ingest 摄入一条事件（允许乱序），返回当前规则版本的评估结论及是否新触发。
func (e *Evaluator) Ingest(ev Event) (Decision, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	rule, ok := e.rules[ev.Key]
	if !ok {
		return Decision{}, false
	}
	st := e.states[ev.Key]

	// 按时间有序插入，容忍乱序事件。
	pos := sort.Search(len(st.events), func(i int) bool { return st.events[i].At.After(ev.At) })
	st.events = append(st.events, Event{})
	copy(st.events[pos+1:], st.events[pos:])
	st.events[pos] = ev

	// 评估时刻不后退：迟到事件仍按最新时刻的窗口评估。
	if ev.At.After(st.latestSeen) {
		st.latestSeen = ev.At
	}
	d := evaluateLocked(rule, st.events, st.latestSeen)

	triggered := false
	if d.Firing && !st.firing {
		triggered = true
	}
	st.firing = d.Firing
	return d, triggered
}

// Evaluate 在指定时间点按指定版本复现某规则的判定。
// 事件流是共享的，版本决定窗口与阈值，因此历史判定可复现。
func (e *Evaluator) Evaluate(ruleName string, version int, now time.Time) (Decision, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	versions, ok := e.history[ruleName]
	if !ok {
		return Decision{}, false
	}
	rule, ok := versions[version]
	if !ok {
		return Decision{}, false
	}
	st := e.states[ruleName]
	return evaluateLocked(rule, st.events, now), true
}

// evaluateLocked 纯函数式评估：在给定事件集与时刻下按规则计算结论。
func evaluateLocked(rule Rule, events []Event, now time.Time) Decision {
	start := now.Add(-rule.Window)
	count := 0
	for _, ev := range events {
		if ev.At.Before(start) || ev.At.After(now) {
			continue
		}
		if rule.compare(ev.Value) {
			count++
		}
	}
	return Decision{
		RuleName:    rule.Name,
		RuleVersion: rule.Version,
		Firing:      count >= rule.minCount(),
		Count:       count,
		WindowStart: start,
		WindowEnd:   now,
	}
}
