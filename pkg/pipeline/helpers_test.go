package pipeline

import (
	"time"

	"github.com/lisama569/service-telemetry-pipeline-go/pkg/alerting"
	"github.com/lisama569/service-telemetry-pipeline-go/pkg/health"
)

func healthTimeout() health.State { return health.StateTimeout }
func healthHealthy() health.State { return health.StateHealthy }

func failureRule(metric string) alerting.Rule {
	return alerting.Rule{
		ID: "rule-" + metric, Metric: metric + ".failure", Window: 10 * time.Second,
		Threshold: 3, Severity: alerting.SeverityCritical, Version: 1,
	}
}

type testEnv struct {
	p     *Pipeline
	clock *fakeClock
}

func newTestEnv(ruleMetric string) *testEnv {
	clock := newFakeClock()
	p := NewWithConfig(Config{HealthWindow: 10 * time.Second, FailureThreshold: 3})
	p.SetClock(clock.now)
	if ruleMetric != "" {
		_ = p.Alerts.AddRule(failureRule(ruleMetric))
	}
	return &testEnv{p: p, clock: clock}
}

var _ = time.Second
