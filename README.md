# service-telemetry-pipeline-go

单进程内的本地可观测性流水线：在**同步调用、并发扇出、后台任务**三种执行模式下，
统一处理请求关联标识传播、指标采集、健康状态判定与基于时间窗口的告警评估。
仅依赖 Go 标准库，无外部后端，全部结论可在进程内复现与解释。

## 包结构

| 包 | 职责 |
| --- | --- |
| `pkg/correlation` | 关联标识与追踪上下文的生成、继承、跨 goroutine 传播 |
| `pkg/metrics` | 并发安全的计数与耗时指标（原子操作） |
| `pkg/health` | 基于滑动窗口实时探测记录的健康状态判定（无结论缓存） |
| `pkg/alerting` | 按事件时间水位的固定窗口告警：去重、恢复、历史重放 |
| `pkg/pipeline` | 把以上组件串成一条流程，提供 `Run` / `RunFanOut` / `RunInBackground` |
| `cmd/demo` | 三种模式的可运行示例 |

## 1. 关联标识传播规则

标识格式：`^[A-Za-z0-9_-]{1,64}$`。进入进程时由 `correlation.Resolve` 归一化，
四种来源可明确区分（`Origin`）：

| 上游情况 | 处理方式 | Origin | 是否中断链路 |
| --- | --- | --- | --- |
| 未提供（空） | 本地生成 32 字符 hex 新标识 | `generated` | 否 |
| 格式非法（含空格/中文/超长/非法字符） | **拒绝透传**，本地生成替换值，仅来源标记 | `malformed` | 否 |
| 合法且首次出现 | 原样继承 | `inherited` | 否 |
| 合法但本进程已见过 | 沿用原值（保证可关联），但显式标记 | `duplicate` | 否 |

- 每个 `correlation.Context` 是**不可变值类型**，含 `CorrelationID / TraceID /
  SpanID / ParentSpanID / Origin`，没有任何共享可变状态。
- `ChildSpan(parent)`：同步嵌套调用，同一 `CorrelationID/TraceID`，
  `ParentSpanID = 父 SpanID`，新随机 `SpanID`。
- `Fork(parent)`：goroutine / 后台任务使用，共享 `CorrelationID/TraceID`
  以保证可关联，但每个 Fork 得到**独立 span**；200 个并发 Fork 无串号
  （见 `TestConcurrentForkNoCrossTalk`）。
- 传播载体是标准 `context.Context`（`IntoContext` / `FromContext`）。
- 后台任务在提交时 Fork 快照并脱离请求 context（用 `context.Background()`
  挂载快照），请求结束/取消不会导致标识丢失。

不变式：任何路径拿到的 `CorrelationID` 都非空且合法；同一次入口请求在所有
同步/并发/后台分支上的 `CorrelationID` 与 `TraceID` 一致，`SpanID` 全树唯一。

## 2. 指标与健康语义

### 指标（`pkg/metrics`）

每个具名 `Counter` 记录：总数、成功、失败、超时、累计耗时、最大耗时。

- 全部计数使用 `atomic.Int64`；最大耗时用 CAS 循环更新，并发覆盖不会回退。
- 结果分三类**互斥**：`success / failure / timeout`，一次 `Observe` 恰好计入
  一类、恰好一次。
- 守恒不变式（测试强制断言）：
  `Count == Success + Failure + Timeout`，并发 50×200=10000 次写入精确相等，
  `-race` 下无竞态。
- 耗时提供平均值与最大值；负耗时归零，不污染统计。

### 健康（`pkg/health`）

`Checker` 保留最近一个时间窗口（默认 30s，测试中 10s）内的真实探测记录，
**每次 `Evaluate` 都实时重算，不缓存结论**，因此不会用缓存掩盖真实故障/恢复：

| 结论 | 判定依据 |
| --- | --- |
| `unknown` | 窗口内样本不足 `minSamples`（默认 3） |
| `timeout` | 窗口内超时样本占比 ≥ 50% —— 依赖响应迟缓 |
| `down` | 窗口尾部连续非超时失败数 ≥ 阈值 **且** 成功率 < 50% —— 持续故障 |
| `flaky` | 成功与失败混杂、未达持续故障标准 —— 短暂抖动 |
| `healthy` | 窗口内样本全部成功 |

`timeout` 与普通 `failure` 分开计数与归因，因此“依赖超时”和“明确报错”是
两种可区分的失败原因；`DescribeReasons` 输出每条非成功探测的时间戳与原因。
探测按事件时间归窗，允许乱序到达；旧记录滑出窗口即不参与判定。

## 3. 告警窗口与去重策略（`pkg/alerting`）

- **固定翻转窗口（tumbling window）**，按 epoch 对齐：事件按其事件时间
  `Event.At` 归入 `[k·W, (k+1)·W)`，边界点属于左闭窗口。
- **水位（watermark）推进**：每个 metric 维护已见最大事件时间；仅当窗口
  结束时间 `<= watermark` 时闭窗，且每个窗口**只判定一次**。乱序但仍落在
  未闭窗口内的事件照常计入（不漏判）。
- 无后续事件的 metric 由 `Evaluator.Tick(now)` 驱动闭窗（后台时钟调用）。
- **迟到拒绝**：所属窗口已闭后才到达的事件被拒绝并计入 `LateDropped()`，
  历史判定不会被迟到事件篡改（不会补触发）。
- **去重**：规则处于活动告警期间，后续连续超阈值窗口不重复触发。
- **恢复与再触发**：一个低于阈值的窗口关闭后，活动告警标记
  `RecoveredAt`（恢复）；之后窗口再次超阈值会**重新触发**为一条新告警。
- 触发条件：窗口内事件值之和 `>= Threshold`（pipeline 中每个失败事件值为 1，
  即“窗口内失败次数”阈值）。
- **规则版本与历史复现**：规则不可变，变更必须带更大的 `Version`
  （`AddRule` 拒绝回退版本）；告警记录评估时的版本号。`Replay(rules, events)`
  先把事件按事件时间稳定排序再重放，乱序输入也得到逐字段一致的结果，
  可用历史版本规则复盘历史判定。

## 4. Pipeline 三种执行模式

```go
p := pipeline.New()
ctx, corr, replaced := p.Begin(r.Context(), r.Header.Get("X-Correlation-ID"))

res  := p.Run(ctx, "dep.db", "reason", fn)     // 同步：ChildSpan
ress := p.RunFanOut(ctx, "dep.db", "r", fns…)  // 并发：每分支 Fork
ch   := p.RunInBackground(ctx, "job", "r", fn) // 后台：Fork 快照 + context.Background()
```

每次执行都会：记录指标 → 追加健康探测并实时判定 → 推送失败事件给告警评估器。
`RunResult` 同时带回关联上下文、结果分类、耗时、健康判定依据与新告警。

## 5. 验证方法

```bash
# 全量单测（含竞态检测）
go test ./... -race -v

# 可运行演示
go run ./cmd/demo
```

测试覆盖与要求对应关系：

| 场景 | 测试 |
| --- | --- |
| 上游标识缺失/畸形/重复/正常 | `pkg/correlation/*_test.go` 四个 Resolve 用例 |
| 跨并发与嵌套传播不串号 | `TestConcurrentForkNoCrossTalk`、`TestFanOutConservationAndCorrelation` |
| 并发指标写入守恒、不重复累计 | `TestConcurrentObserveConservation`（`-race`） |
| 持续故障 / 依赖超时 / 短暂抖动区分 | `TestSustainedDown` / `TestTimeoutDominates` / `TestFlakyTransient` |
| 超时→恢复且不被缓存掩盖 | `TestTimeoutRecoversToHealthy`、`TestNoCachingMasksRealFailure`、`TestDependencyTimeoutThenRecovery` |
| 窗口边界与乱序不漏判 | `TestWindowBoundaryFiresOnce`、`TestOutOfOrderNoMiss` |
| 去重、恢复后再触发、迟到拒绝 | `TestDedupeAndRetriggerAfterRecovery`、`TestLateEventRejected` |
| 规则版本变更与历史复现 | `TestRuleChangeVersionAndReplay`、`TestAddRuleVersionMonotonic` |
| 畸形标识端到端不中断链路 | `TestMalformedUpstreamIDDoesNotBreakChain` |

所有关键用例的 `t.Logf` 均打印关联标识与判定依据（如
`corr=... 健康: timeout(依据:timeouts dominate window: 3/3 >= 50%) -> healthy(依据:all 3 probes succeeded)`），
用 `-v` 可查看。
