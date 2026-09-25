package metrics

import (
	"sync"
	"testing"
	"time"
)

func TestCounterConcurrentConservation(t *testing.T) {
	const workers = 32
	const perWorker = 1000
	c := &Counter{}
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < perWorker; j++ {
				c.Inc(1)
			}
		}()
	}
	wg.Wait()
	got := c.Value()
	want := int64(workers * perWorker)
	t.Logf("counter 守恒校验: 期望=%d 实际=%d", want, got)
	if got != want {
		t.Fatalf("并发写入丢失: 期望 %d, 得到 %d", want, got)
	}
}

func TestLatencyConcurrentConservation(t *testing.T) {
	const workers = 16
	const perWorker = 500
	l := &Latency{}
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < perWorker; j++ {
				l.Observe(time.Duration(i+j) * time.Microsecond)
			}
		}(i)
	}
	wg.Wait()
	snap := l.Snapshot()
	// 独立重算期望总和
	var wantSum time.Duration
	for i := 0; i < workers; i++ {
		for j := 0; j < perWorker; j++ {
			wantSum += time.Duration(i+j) * time.Microsecond
		}
	}
	t.Logf("latency 守恒校验: count=%d sum=%s min=%s max=%s mean=%s",
		snap.Count, snap.Sum, snap.Min, snap.Max, snap.Mean())
	if snap.Count != workers*perWorker {
		t.Fatalf("计数丢失: 期望 %d, 得到 %d", workers*perWorker, snap.Count)
	}
	if snap.Sum != wantSum {
		t.Fatalf("耗时累计不守恒: 期望 %s, 得到 %s", wantSum, snap.Sum)
	}
	if snap.Min != 0 || snap.Max != time.Duration(workers-1+perWorker-1)*time.Microsecond {
		t.Fatalf("极值错误: min=%s max=%s", snap.Min, snap.Max)
	}
}

func TestRegistrySameInstance(t *testing.T) {
	r := NewRegistry()
	c1 := r.Counter("requests")
	c2 := r.Counter("requests")
	if c1 != c2 {
		t.Fatal("同名计数器必须返回同一实例, 否则重复累计")
	}
	c1.Inc(1)
	if c2.Value() != 1 {
		t.Fatal("同名计数器状态未共享")
	}
	if r.Latency("latency") != r.Latency("latency") {
		t.Fatal("同名耗时指标必须返回同一实例")
	}
}

func TestCounterRejectsNonPositive(t *testing.T) {
	c := &Counter{}
	c.Inc(0)
	c.Inc(-5)
	if c.Value() != 0 {
		t.Fatal("非正 delta 不得改变计数")
	}
}
