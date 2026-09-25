package alerting

import (
	"testing"
	"time"
)

func at(sec int) time.Time { return time.Unix(int64(sec), 0) }

func newRule() Rule {
	return Rule{ID: "err-rate", Metric: "errors", Window: 10 * time.Second, Threshold: 3, Severity: SeverityCritical, Version: 1}
}

func TestWindowBoundaryFiresOnce(t *testing.T) {
	// 边界语义：[0,10) 一桶，事件恰好落在 9s 计入第一窗；水位越过 10s 才闭窗判定。
	e := NewEvaluator()
	if err := e.AddRule(newRule()); err != nil {
		t.Fatal(err)
	}
	// 窗口 [0,10)：3 个失败事件达到阈值。
	e.Push(Event{At: at(1), Name: "errors", Value: 1})
	e.Push(Event{At: at(5), Name: "errors", Value: 1})
	e.Push(Event{At: at(9), Name: "errors", Value: 1}) // 9s 属于 [0,10)
	if n := len(e.Alerts()); n != 0 {
		t.Fatalf("window must not close before watermark passes 10s, got %d alerts", n)
	}
	// Tick 推进时钟到 10s：[0,10) 到达边界闭窗判定（水位按 metric 维护，
	// 无事件 metric 由 Tick 驱动闭窗）。
	fired := e.Tick(at(10))
	if len(e.Alerts()) != 1 {
		t.Fatalf("want exactly 1 alert, got %d", len(e.Alerts()))
	}
	a := e.Alerts()[0]
	if !a.WindowStart.Equal(at(0)) || !a.WindowEnd.Equal(at(10)) || a.Value != 3 {
		t.Fatalf("alert window/value wrong: %+v", a)
	}
	if len(fired) != 1 {
		t.Fatalf("Push should return the newly fired alert, got %d", len(fired))
	}
	t.Logf("告警[窗口边界] 规则=%s(v%d) 窗口=[%s,%s) 聚合值=%v 阈值=%v 触发时刻=%s 判定依据=值之和>=阈值",
		a.RuleID, a.Version, a.WindowStart.Format("15:04:05"), a.WindowEnd.Format("15:04:05"),
		a.Value, a.Threshold, a.FiredAt.Format("15:04:05"))
}

func TestOutOfOrderNoMiss(t *testing.T) {
	e := NewEvaluator()
	_ = e.AddRule(newRule())
	// 故意乱序送入，只要窗口未闭就必须全部计入。
	e.Push(Event{At: at(8), Name: "errors", Value: 1})
	e.Push(Event{At: at(2), Name: "errors", Value: 1})
	e.Push(Event{At: at(5), Name: "errors", Value: 1})
	e.Push(Event{At: at(11), Name: "errors", Value: 0}) // 闭 [0,10)
	if n := len(e.Alerts()); n != 1 {
		t.Fatalf("out-of-order events must not miss judgement, alerts=%d", n)
	}
	t.Logf("告警[乱序不漏判] 乱序事件 8s,2s,5s 全部计入 [0,10) 触发=%d 判定依据=闭窗前按事件时间归桶",
		len(e.Alerts()))
}

func TestDedupeAndRetriggerAfterRecovery(t *testing.T) {
	e := NewEvaluator()
	_ = e.AddRule(newRule())
	// [0,10) 超阈值 -> 触发 1 次。
	e.Push(Event{At: at(1), Name: "errors", Value: 5})
	e.Push(Event{At: at(11), Name: "errors", Value: 0}) // 闭 [0,10) => fire
	// [10,20) 仍然超阈值，告警活动中 -> 不重复触发。
	e.Push(Event{At: at(12), Name: "errors", Value: 5})
	e.Push(Event{At: at(21), Name: "errors", Value: 0}) // 闭 [10,20) => 不重复
	if n := len(e.Alerts()); n != 1 {
		t.Fatalf("active alert must dedupe, got %d alerts", n)
	}
	// [20,30) 无失败 -> 恢复。
	e.Push(Event{At: at(31), Name: "errors", Value: 0}) // 闭 [20,30) => recover
	a := e.Alerts()[0]
	if a.Active() || !a.RecoveredAt.Equal(at(31)) {
		t.Fatalf("alert should recover at 31s, got recoveredAt=%v active=%v", a.RecoveredAt, a.Active())
	}
	// [30,40) 再次超阈值 -> 允许重新触发。
	e.Push(Event{At: at(32), Name: "errors", Value: 9})
	e.Push(Event{At: at(41), Name: "errors", Value: 0}) // 闭 [30,40) => fire again
	if n := len(e.Alerts()); n != 2 {
		t.Fatalf("should retrigger after recovery, got %d alerts", n)
	}
	second := e.Alerts()[1]
	if second.Active() != true || second.Value != 9 {
		t.Fatalf("second alert wrong: %+v", second)
	}
	t.Logf("告警[去重与恢复后再触发] 触发次数=%d (首次窗口[%s,%s) 恢复于%s, 二次窗口[%s,%s)) 判定依据=活动中抑制/健康窗闭窗恢复",
		len(e.Alerts()),
		a.WindowStart.Format("15:04:05"), a.WindowEnd.Format("15:04:05"), a.RecoveredAt.Format("15:04:05"),
		second.WindowStart.Format("15:04:05"), second.WindowEnd.Format("15:04:05"))
}

func TestLateEventRejected(t *testing.T) {
	e := NewEvaluator()
	_ = e.AddRule(newRule())
	e.Push(Event{At: at(1), Name: "errors", Value: 0})
	e.Push(Event{At: at(20), Name: "errors", Value: 0}) // 水位越过，[0,10) 已闭
	// 迟到到已闭窗口 [0,10) 的事件必须被拒绝，不能改变历史判定。
	got := e.Push(Event{At: at(2), Name: "errors", Value: 100})
	if len(got) != 0 || e.LateDropped() != 1 {
		t.Fatalf("late event must be dropped, fired=%d dropped=%d", len(got), e.LateDropped())
	}
	for _, a := range e.Alerts() {
		if a.WindowStart.Equal(at(0)) {
			t.Fatal("closed window must not be retroactively fired by late event")
		}
	}
	t.Logf("告警[迟到拒绝] 闭窗后事件值=100 被丢弃 lateDropped=%d 判定依据=窗口已判定不可篡改",
		e.LateDropped())
}

func TestRuleChangeVersionAndReplay(t *testing.T) {
	rulesV1 := []Rule{newRule()}
	events := []Event{
		{At: at(1), Name: "errors", Value: 5},
		{At: at(11), Name: "errors", Value: 0},
		{At: at(15), Name: "errors", Value: 5},
		{At: at(21), Name: "errors", Value: 0},
		{At: at(31), Name: "errors", Value: 0},
		{At: at(35), Name: "errors", Value: 9},
		{At: at(41), Name: "errors", Value: 0},
	}
	ev1, err := Replay(rulesV1, events)
	if err != nil {
		t.Fatal(err)
	}
	firstAlerts := ev1.Alerts()
	if len(firstAlerts) != 2 {
		t.Fatalf("v1 replay alerts = %d, want 2", len(firstAlerts))
	}

	// 用同样事件 + v2 规则（阈值放宽到 10，永不触发）重放，历史判定按规则版本变化且可复现。
	rulesV2 := []Rule{{ID: "err-rate", Metric: "errors", Window: 10 * time.Second, Threshold: 10, Severity: SeverityWarning, Version: 2}}
	ev2, err := Replay(rulesV2, events)
	if err != nil {
		t.Fatal(err)
	}
	if n := len(ev2.Alerts()); n != 0 {
		t.Fatalf("v2 replay alerts = %d, want 0", n)
	}

	// 复现性：同样输入再跑一次必须逐字段一致。
	ev1b, _ := Replay(rulesV1, events)
	a := ev1.Alerts()
	b := ev1b.Alerts()
	if len(a) != len(b) {
		t.Fatal("replay not reproducible")
	}
	for i := range a {
		if a[i] != b[i] {
			t.Fatalf("replay mismatch at %d: %+v vs %+v", i, a[i], b[i])
		}
	}
	// 乱序输入重放也必须得到相同结果（重放前按事件时间稳定排序）。
	shuffled := []Event{events[6], events[0], events[5], events[1], events[4], events[2], events[3]}
	ev1c, _ := Replay(rulesV1, shuffled)
	c := ev1c.Alerts()
	for i := range a {
		if a[i] != c[i] {
			t.Fatalf("shuffled replay mismatch at %d", i)
		}
	}
	t.Logf("告警[规则版本与复现] v1(阈值3)触发=%d 次版本号=%d; v2(阈值10)触发=0; 乱序重放结果一致 判定依据=规则不可变版本+事件时间排序重放",
		len(firstAlerts), firstAlerts[0].Version)
}

func TestAddRuleVersionMonotonic(t *testing.T) {
	e := NewEvaluator()
	_ = e.AddRule(newRule())
	if err := e.AddRule(newRule()); err == nil {
		t.Fatal("re-adding same version must fail")
	}
	v2 := newRule()
	v2.Version = 2
	if err := e.AddRule(v2); err != nil {
		t.Fatalf("higher version should be accepted: %v", err)
	}
}
