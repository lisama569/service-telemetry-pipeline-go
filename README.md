# service-telemetry-pipeline-go

本仓库实现一套本地（单进程）可观测性流程，覆盖四个子系统，保证请求关联、指标与健康状态、告警判定在同步调用、并发处理与后台任务下保持一致且可解释。

| 包 | 职责 |
|---|---|
| `correlation` | 关联标识与追踪上下文的生成、继承与传播 |
| `metrics` | 并发安全的计数与耗时指标采集 |
| `health` | 基于滑动窗口探针历史的健康状态判定 |
| `alert` | 基于时间窗口的告警规则评估 |
| `pipeline` | 跨子系统集成测试 |

## 关联标识传播规则（correlation）

- 每个链路有 `TraceID`（整条链路不变）与 `RequestID`（每个任务唯一）。
- `Tracker.Ensure(ctx, upstreamID)` 按上游标识情况处理，**任何情况都不中断链路**，且处理方式可区分（`Context.Source`）：
  - `generated`：上游标识缺失，本地生成；
  - `inherited`：上游标识合法（默认 8~64 位十六进制），原样继承；
  - `replaced_invalid`：上游标识格式非法，丢弃并生成新标识；
  - `replaced_duplicate`：上游标识与进程内活动标识重复（原子检查+登记），生成新标识。
- `ctx` 中已有上下文时 `Ensure` 直接沿用，绝不覆盖，防止串号。
- `Tracker.Spawn(ctx)` 用于派生并发任务/后台任务：**TraceID 继承，RequestID 重新生成**，跨 goroutine 不串号、不丢失。
- 任务结束后调用 `Tracker.Release(requestID)` 释放活动标识。

## 指标与健康语义（metrics / health）

- `Counter` 只增（拒绝非正 delta），基于 `atomic`，并发写入不丢失；`Latency` 记录次数/总和/极值，`Snapshot` 返回一致性快照（Count 与 Sum 严格对应）。
- `Registry` 同名指标返回同一实例，避免重复累计。
- `health.Checker` 在滑动窗口内实时判定，**不缓存结论**（`Evaluate` 每次重新扫描窗口历史），故障注入立即反映、恢复后立即翻转，不用缓存掩盖真实故障。判定结论可区分：
  - `unhealthy / sustained_failure`：窗口内失败数 ≥ `FailureThreshold` 且多数非超时；
  - `unhealthy / dependency_timeout`：失败数达阈值且多数为超时（`Duration >= Timeout`）；
  - `degraded / transient_blip`：失败数 ≤ `BlipTolerance` 的短暂抖动；
  - `healthy / ok`：窗口内无失败。
- 每次判定附带 `Detail`（窗口、样本数、失败数、超时数、阈值），判定依据可解释。

## 告警窗口与去重策略（alert）

- 窗口为滑动窗口 `[t-Window, t]`，t 为评估时刻，取**事件时间与已见最新时间的较大者**——评估时刻不后退，乱序/迟到事件按时间排序插入后仍计入当前窗口，不漏判；窗口外的乱序事件不计入，不误判。
- 触发条件：窗口内命中（`Compare(value, Threshold)`，默认 `>=`）事件数 ≥ `MinCount`。
- 去重：规则处于 firing 状态时不重复触发；评估转为不命中（恢复）后状态复位，可再次触发。
- 可复现：规则携带 `Version`，`UpdateRule` 保留全部历史版本；`Evaluate(name, version, now)` 用指定版本的窗口与阈值在共享事件流上重放，规则变更后历史判定仍可复现。

## 验证方法

```bash
go build ./...
go vet ./...
go test ./... -race -v
```

测试覆盖：

- **并发写入守恒**（`metrics`、`health`、`pipeline`）：多 goroutine 写入后计数/耗时/样本数与独立重算的期望值严格相等，`-race` 下通过。
- **上游标识畸形**（`correlation`）：缺失/非法/重复/合法四类输入分别得到 `generated`/`replaced_invalid`/`replaced_duplicate`/`inherited`，链路均不中断；并发派生 TraceID 不串号。
- **依赖从超时到恢复**（`health`）：连续超时判 `dependency_timeout`，故障样本滑出窗口后实时重判为 `healthy`，验证无缓存掩盖。
- **告警**（`alert`）：触发去重、恢复后再次触发、乱序事件不漏判不误判、按历史版本复现判定。
- 单测日志均打印关联标识（`trace=... request=... source=...`）与判定依据（`verdict=...` / `decision=...`）。
