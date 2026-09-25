package metrics

import (
	"sync"
	"testing"
	"time"
)

func TestConcurrentObserveConservation(t *testing.T) {
	r := NewRegistry()
	c := r.Counter("dep.api")

	const goroutines = 50
	const perG = 200 // 每 goroutine：100 成功 / 60 失败 / 40 超时
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		go func() {
			defer wg.Done()
			for i := 0; i < perG; i++ {
				switch {
				case i < 100:
					c.Observe(time.Duration(i+1)*time.Millisecond, OutcomeSuccess)
				case i < 160:
					c.Observe(2*time.Millisecond, OutcomeFailure)
				default:
					c.Observe(5*time.Millisecond, OutcomeTimeout)
				}
			}
		}()
	}
	wg.Wait()

	s := c.Snapshot()
	want := int64(goroutines * perG)
	// 守恒律 1：总数精确，不多不少。
	if s.Count != want {
		t.Fatalf("count = %d, want %d (丢失或重复累计)", s.Count, want)
	}
	// 守恒律 2：三类互斥且相加等于总数。
	if s.Success+s.Failure+s.Timeout != s.Count {
		t.Fatalf("分类不守恒: %d+%d+%d != %d", s.Success, s.Failure, s.Timeout, s.Count)
	}
	if s.Success != int64(goroutines*100) || s.Failure != int64(goroutines*60) || s.Timeout != int64(goroutines*40) {
		t.Fatalf("分类计数错误: ok=%d fail=%d timeout=%d", s.Success, s.Failure, s.Timeout)
	}
	if s.MaxLatency() != 100*time.Millisecond {
		t.Fatalf("max = %v, want 100ms", s.MaxLatency())
	}
	t.Logf("指标[并发守恒] counter=%s count=%d(成功=%d 失败=%d 超时=%d) 平均耗时=%s 最大耗时=%s 判定依据=三类原子计数相加=总数",
		s.Name, s.Count, s.Success, s.Failure, s.Timeout, s.AvgLatency(), s.MaxLatency())
}

func TestRegistryDeduplicatesCounter(t *testing.T) {
	r := NewRegistry()
	a := r.Counter("x")
	b := r.Counter("x")
	if a != b {
		t.Fatal("same name must return same counter")
	}
	a.Observe(time.Millisecond, OutcomeSuccess)
	s, ok := r.Snapshot("x")
	if !ok || s.Count != 1 {
		t.Fatal("snapshot mismatch")
	}
	all := r.SnapshotAll()
	if len(all) != 1 {
		t.Fatalf("SnapshotAll len = %d", len(all))
	}
}

func TestOutcomeString(t *testing.T) {
	if OutcomeSuccess.String() != "success" || OutcomeFailure.String() != "failure" || OutcomeTimeout.String() != "timeout" {
		t.Fatal("outcome labels wrong")
	}
}
