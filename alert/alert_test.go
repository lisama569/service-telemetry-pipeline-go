package alert

import (
	"testing"
	"time"
)

var base = time.Date(2026, 9, 25, 10, 0, 0, 0, time.UTC)

func latencyRule() Rule {
	return Rule{
		Name:      "high_latency",
		Version:   1,
		Window:    time.Minute,
		Threshold: 500,
		MinCount:  3,
	}
}

func TestTriggerDedupAndRecovery(t *testing.T) {
	ev := NewEvaluator(latencyRule())
	// 连续 3 次命中达到 MinCount, 仅第一次触发。
	triggers := 0
	for i := 0; i < 3; i++ {
		d, fired := ev.Ingest(Event{At: base.Add(time.Duration(i) * time.Second), Key: "high_latency", Value: 900})
		t.Logf("decision=%s 判定依据: 第 %d 次命中", d, i+1)
		if fired {
			triggers++
		}
	}
	if triggers != 1 {
		t.Fatalf("触发未去重: 期望 1 次, 得到 %d 次", triggers)
	}
	// 继续命中仍不重复触发。
	_, fired := ev.Ingest(Event{At: base.Add(3 * time.Second), Key: "high_latency", Value: 900})
	if fired {
		t.Fatal("firing 期间不得重复触发")
	}
	// 恢复: 窗口滑过, 命中数低于阈值。
	d, fired := ev.Ingest(Event{At: base.Add(2 * time.Minute), Key: "high_latency", Value: 100})
	t.Logf("decision=%s 判定依据: 旧事件滑出窗口, 规则恢复", d)
	if d.Firing || fired {
		t.Fatalf("恢复判定错误: %s fired=%v", d, fired)
	}
	// 恢复后可再次触发。
	triggers = 0
	for i := 0; i < 3; i++ {
		_, fired := ev.Ingest(Event{At: base.Add(2*time.Minute + time.Duration(i+1)*time.Second), Key: "high_latency", Value: 900})
		if fired {
			triggers++
		}
	}
	if triggers != 1 {
		t.Fatalf("恢复后应能再次触发且仅一次, 得到 %d 次", triggers)
	}
}

func TestOutOfOrderEvents(t *testing.T) {
	ev := NewEvaluator(latencyRule())
	// 先摄入较新事件, 再乱序补入窗口内的旧事件, 不得漏判。
	ev.Ingest(Event{At: base.Add(10 * time.Second), Key: "high_latency", Value: 900})
	ev.Ingest(Event{At: base.Add(20 * time.Second), Key: "high_latency", Value: 900})
	d, fired := ev.Ingest(Event{At: base.Add(5 * time.Second), Key: "high_latency", Value: 900}) // 乱序迟到
	t.Logf("decision=%s 判定依据: 乱序事件计入当前窗口", d)
	if !fired {
		t.Fatalf("乱序事件漏判: %s", d)
	}
	// 同一乱序事件重复摄入会重复计数 —— 通过评估时刻不后退保证窗口一致,
	// 这里验证窗口外乱序事件不会造成误判。
	d2, _ := ev.Ingest(Event{At: base.Add(-2 * time.Minute), Key: "high_latency", Value: 900})
	if d2.Count != 3 {
		t.Fatalf("窗口外乱序事件不应计入: count=%d", d2.Count)
	}
}

func TestRuleVersionReproducible(t *testing.T) {
	r1 := latencyRule()
	ev := NewEvaluator(r1)
	for i := 0; i < 3; i++ {
		ev.Ingest(Event{At: base.Add(time.Duration(i) * time.Second), Key: "high_latency", Value: 900})
	}
	// 规则变更: 阈值提高, 同样事件不再触发。
	r2 := r1
	r2.Version = 2
	r2.Threshold = 1000
	ev.UpdateRule(r2)
	d2, ok := ev.Evaluate("high_latency", 2, base.Add(3*time.Second))
	t.Logf("decision=%s 判定依据: 按 v2 阈值重放", d2)
	if !ok || d2.Firing {
		t.Fatalf("v2 判定错误: %s", d2)
	}
	// 历史版本判定可复现: v1 仍应触发。
	d1, ok := ev.Evaluate("high_latency", 1, base.Add(3*time.Second))
	t.Logf("decision=%s 判定依据: 按 v1 阈值重放, 结果须可复现", d1)
	if !ok || !d1.Firing || d1.Count != 3 {
		t.Fatalf("v1 复现失败: %s", d1)
	}
	// 未知版本不得判定。
	if _, ok := ev.Evaluate("high_latency", 99, base); ok {
		t.Fatal("未知版本不应可判定")
	}
}

func TestUnknownKeyIgnored(t *testing.T) {
	ev := NewEvaluator(latencyRule())
	if _, fired := ev.Ingest(Event{At: base, Key: "nope", Value: 1}); fired {
		t.Fatal("无规则事件不应触发")
	}
}
