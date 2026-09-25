// Package alerting 提供基于时间窗口的告警规则评估、去重与历史复现。
//
// 核心语义：
//   - 窗口为按 epoch 对齐的固定翻转窗口（tumbling window），边界归属明确：
//     事件按事件时间（Event.At）归入 [k*W, (k+1)*W)，边界点属于左闭窗口；
//   - 按事件时间维护水位 watermark：窗口只有在被更晚事件"越过"后才闭窗判定一次，
//     乱序但未迟到（仍落在未闭窗口内）的事件照常计入，不漏判；
//   - 已闭窗口之后迟到的事件被拒绝（计数可查），杜绝重复触发或篡改历史；
//   - 每个 (规则版本, 窗口起点) 最多触发一次告警（天然去重）；
//   - 一个未超阈值的窗口关闭后，活动告警转为已恢复；之后再次超阈值会重新触发；
//   - 规则带不可变版本号，告警记录其评估时的版本；Replay 用给定规则版本对
//     历史事件做确定性重放，历史判定可复现。
package alerting

import (
	"errors"
	"time"
)

// Severity 告警级别。
type Severity int

const (
	SeverityWarning Severity = iota
	SeverityCritical
)

func (s Severity) String() string {
	switch s {
	case SeverityCritical:
		return "critical"
	default:
		return "warning"
	}
}

// Rule 时间窗口告警规则。规则值不可变；变更规则请用新版本替换。
// 判定条件：窗口内事件值之和 >= Threshold。
type Rule struct {
	ID        string
	Metric    string
	Window    time.Duration
	Threshold float64
	Severity  Severity
	Version   int
}

func (r Rule) validate() error {
	switch {
	case r.ID == "":
		return errors.New("rule id is empty")
	case r.Metric == "":
		return errors.New("rule metric is empty")
	case r.Window <= 0:
		return errors.New("rule window must be positive")
	case r.Threshold < 0:
		return errors.New("rule threshold must be non-negative")
	default:
		return nil
	}
}

// Event 送入评估的事件，自带事件时间，允许乱序到达。
type Event struct {
	At    time.Time
	Name  string
	Value float64
}

// Alert 一次触发记录及其生命周期。
type Alert struct {
	RuleID      string
	Version     int
	Severity    Severity
	WindowStart time.Time
	WindowEnd   time.Time
	Value       float64
	Threshold   float64
	FiredAt     time.Time // 判定触发时刻（闭窗时刻，即 watermark）
	RecoveredAt time.Time // 恢复时刻；零值表示仍处于活动状态
}

// Active 报告告警是否仍处于活动状态。
func (a Alert) Active() bool { return a.RecoveredAt.IsZero() }

// windowStart 按 epoch 对齐计算 t 所属窗口起点。
func windowStart(t time.Time, w time.Duration) time.Time {
	u := t.UnixNano()
	step := int64(w)
	return time.Unix(0, u-u%step)
}
