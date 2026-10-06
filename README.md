# service-telemetry-pipeline-go

本仓库实现一套本地（单进程）可观测性流程，覆盖四个子系统，保证请求关联、指标与健康状态、告警判定在同步调用、并发处理与后台任务下保持一致且可解释。

| 包 | 职责 |
|---|---|
| `correlation` | 关联标识与追踪上下文的生成、继承与传播 |
| `metrics` | 并发安全的计数与耗时指标采集 |
| `health` | 基于滑动窗口探针历史的健康状态判定 |
| `alert` | 基于时间窗口的告警规则评估 |
| `pipeline` | 可复用的处理流程（拼装四个子系统）与判定报告 |
| `cmd/demo` | 一键运行入口：同步+并发+后台任务混合场景 |

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

### 标识复用与判重边界

- **什么时候可以复用**：请求结束（正常返回或中途出错）调用 `Release` 后，该上游标识立即归还，后续请求携带同一标识可重新 `inherited`。`pipeline.HandleRequest` / `SpawnBackground` 已在结束时自动 `Release`，无需手工调用。
- **判重边界**：判重只针对"当前仍在处理中"的请求——同一上游标识在第一个请求未结束时再次到达，才会判 `replaced_duplicate`；请求之间串行复用同一标识不算重复。登记与检查在同一把锁内原子完成，并发下既不误判也不串号。
- **容量上限**：判重记录只含在途标识，正常情况随并发数增长、不随历史请求增长；另有兜底上限 `DefaultMaxActive=4096`（可用 `NewTrackerWithCapacity` 调整），在途数异常超过上限时按登记先后（FIFO）淘汰最旧条目，内存不随历史请求无界增长。被淘汰的在途标识可能失去判重保护，这是有界内存的明确取舍。

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

## 可复用流程与判定报告（pipeline / cmd/demo）

- `pipeline.New(tracker, registry, checker, evaluator, Options)` 把四个子系统拼成一个流程，传 `nil` 即用默认实例：
  - `HandleRequest(ctx, upstreamID, fn)`：建立/继承关联上下文 → 执行 fn → 记录指标与探针 → 失败时摄入告警事件 → 结束时（无论成败）归还标识；
  - `SpawnBackground(reqCtx, name, fn)`：从某个请求的上下文派生后台任务，**挂在该请求的 TraceID 上，不另起链路**，结束同样归还标识；
  - `CheckHealth(now)`：做一次健康判定并记录结论变化；
  - `Options.Now` 可注入时钟，同样输入重跑得到可复现报告。
- `Report()` 输出人可读判定报告，包含四段：
  1. **关联标识走向**：每次请求的生成/继承/替换/派生/归还（含上游标识与判定来源）；
  2. **指标守恒**：内部账本（started/finished/failed/background）与 Registry 计数器、耗时样本数的一致性校验；
  3. **健康结论变化**：按 state/reason 归纳的判定翻转序列（含窗口内样本/失败/超时数）；
  4. **告警触发与恢复**：按 firing 状态归纳的触发与恢复时刻。
  没有任何数据时各段落输出"无记录"说明，不报错不卡住；报告数字与 `Summary()` 全局汇总严格一致。
- 一键运行：`go run ./cmd/demo`，依次演示同步请求（生成/继承/畸形替换）、在途判重与结束后复用、并发不串号、后台任务挂链、依赖先超时后恢复，结尾打印完整判定报告。

## 验证方法

```bash
go build ./...
go vet ./...
go test ./... -race -v
go run ./cmd/demo   # 一键跑完混合场景并输出判定报告
```

测试覆盖：

- **并发写入守恒**（`metrics`、`health`、`pipeline`）：多 goroutine 写入后计数/耗时/样本数与独立重算的期望值严格相等，`-race` 下通过。
- **上游标识畸形**（`correlation`）：缺失/非法/重复/合法四类输入分别得到 `generated`/`replaced_invalid`/`replaced_duplicate`/`inherited`，链路均不中断；并发派生 TraceID 不串号。
- **依赖从超时到恢复**（`health`）：连续超时判 `dependency_timeout`，故障样本滑出窗口后实时重判为 `healthy`，验证无缓存掩盖。
- **告警**（`alert`）：触发去重、恢复后再次触发、乱序事件不漏判不误判、按历史版本复现判定。
- **标识生命周期**（`correlation`、`pipeline`）：请求结束（含出错）后标识归还、可重新继承；在途重复标识仍被替换；并发同标识恰好零误判零串号；判重记录不超过容量上限。
- **后台任务挂链**（`pipeline`）：`SpawnBackground` 派生的任务与父请求同 TraceID、不同 RequestID。
- **判定报告**（`pipeline`）：无数据正常输出；同样输入重跑报告一致（遮蔽随机标识后逐字节相同）；报告数字与全局汇总一致；依赖超时→恢复场景下健康结论与告警触发/恢复序列可解释。
- 单测日志均打印关联标识（`trace=... request=... source=...`）与判定依据（`verdict=...` / `decision=...`）。

## 本轮改动对原有行为的影响

- `correlation`：`NewTracker` 行为不变（默认容量 4096 的判重记录，正常服务远在上限之下）；新增 `NewTrackerWithCapacity`、`Active()`；`Release`/`Ensure`/`Spawn` 语义不变。
- `metrics` / `health` / `alert`：未改动，原有用例全部回归通过。
- `pipeline`：原集成测试保留；新增可复用流程与报告为纯增量。
