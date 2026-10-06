# service-telemetry-pipeline-go

本仓库实现一套本地（单进程）可观测性流程，覆盖四个子系统，保证请求关联、指标与健康状态、告警判定在同步调用、并发处理与后台任务下保持一致且可解释。

| 包 | 职责 |
|---|---|
| `correlation` | 关联标识与追踪上下文的生成、继承与传播 |
| `metrics` | 并发安全的计数与耗时指标采集 |
| `health` | 基于滑动窗口探针历史的健康状态判定 |
| `alert` | 基于时间窗口的告警规则评估 |
| `pipeline` | 可直接复用的处理流程（组装四个子系统）与运行报告 |
| `cmd/telemetry-demo` | 一键跑通混合场景的演示入口 |

## 关联标识传播规则（correlation）

- 每个链路有 `TraceID`（整条链路不变）与 `RequestID`（每个任务唯一）。
- `Tracker.Ensure(ctx, upstreamID)` 按上游标识情况处理，**任何情况都不中断链路**，且处理方式可区分（`Context.Source`）：
  - `generated`：上游标识缺失，本地生成；
  - `inherited`：上游标识合法（默认 8~64 位十六进制），原样继承；
  - `replaced_invalid`：上游标识格式非法，丢弃并生成新标识；
  - `replaced_duplicate`：上游标识与进程内活动标识重复（原子检查+登记），生成新标识。
- `ctx` 中已有上下文时 `Ensure` 直接沿用，绝不覆盖，防止串号。
- `Tracker.Spawn(ctx)` 用于派生并发任务/后台任务：**TraceID 继承，RequestID 重新生成**，跨 goroutine 不串号、不丢失。
- **标识复用与判重边界**：判重只针对"当前仍在处理中"的标识。任务结束（无论成功或出错）后调用 `Tracker.Release(requestID)` 归还，该标识即可被后续请求重新继承（`inherited`）；只有在飞请求之间才会判 `replaced_duplicate`。`pipeline.HandleRequest`/`SpawnBackground` 已用 `defer` 自动归还，嵌套调用（ctx 已有上下文）不会误归还父任务的标识。
- **判重记录容量上限**：活动标识集合默认上限 `DefaultMaxActive=4096`（可用 `correlation.WithMaxActive(n)` 调整）。正常归还时集合规模只取决于在飞请求数，不随历史请求增长；上限是漏调 `Release` 的兜底——超限按登记先后（FIFO）淘汰最旧记录，被淘汰的标识可重新继承。`Tracker.Active()` 可观察当前记录规模。
- `correlation.WithIDFunc(f)` 可注入确定性 ID 生成器（TraceID 与 RequestID 都由它生成），配合逻辑时钟可让运行报告完全可复现；默认仍为加密随机，行为不变。

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

## 可复用流程与运行报告（pipeline）

`pipeline.New(Config)` 一次组装四个子系统，调用方无需自行拼接：

- `HandleRequest(ctx, upstreamID, fn)`：建立关联上下文 → 记录指标（`requests_total`/`requests_failed`/`request_latency`）→ 执行 `fn` → 按结果上报探针与告警事件（失败按 `failures` 键）→ 每次请求结束实时重估健康与告警 → `defer` 归还标识。返回的 `error` 即 `fn` 的 error，中途出错同样归还标识。
- `SpawnBackground(ctx, fn)`：从请求上下文派生后台任务，**TraceID 挂在派生它的请求链路上，不另起链路**，RequestID 重新生成避免串号。
- `Report()`：输出人可读的判定报告，包含四节：
  - **关联标识走向**：每次请求/后台任务的时间、类型、上游标识、`trace/request/source`、结果（ok/error）；
  - **健康结论变化**：仅在状态或原因翻转时记录一条（含 `Detail` 判定依据）；
  - **告警触发与恢复**：仅在 firing 翻转时记录一条（含窗口与命中数）；
  - **指标汇总**：计数器与耗时快照，直接取自全局 `Metrics` 注册表，与全局汇总严格一致。
- 无数据时各节输出 `(无数据)` 占位，不报错、不卡住。
- **可复现**：报告内容全部由运行期记录与注册表快照生成；关联记录按 (时间, 类型, 上游标识, RequestID) 排序，并发下到达顺序不定也不影响输出。注入逻辑时钟（`Config.Now`）与确定性 ID 生成器（`correlation.WithIDFunc`）后，同样输入重跑得到完全一致的报告。

## 演示入口（cmd/telemetry-demo）

```bash
go run ./cmd/telemetry-demo
```

一键跑完混合场景：同步请求（覆盖缺失/合法/非法上游标识与归还后重新继承）→ 一个请求派生 8 个并发后台任务（全部挂在该请求的 TraceID 上）→ 依赖先超时（连续失败，健康判 `unhealthy/dependency_timeout`、告警触发）→ 时钟推进越过窗口后依赖恢复（健康翻转回 `healthy`、告警恢复）。过程中打印各环节关联标识，结束后输出完整判定报告；演示使用逻辑时钟与确定性 ID，重跑报告逐字节一致。

## 验证方法

```bash
go build ./...
go vet ./...
go test ./... -race -v
go run ./cmd/telemetry-demo   # 混合场景 + 判定报告, 重跑输出一致
```

测试覆盖：

- **并发写入守恒**（`metrics`、`health`、`pipeline`）：多 goroutine 写入后计数/耗时/样本数与独立重算的期望值严格相等，`-race` 下通过。
- **上游标识畸形**（`correlation`）：缺失/非法/重复/合法四类输入分别得到 `generated`/`replaced_invalid`/`replaced_duplicate`/`inherited`，链路均不中断；并发派生 TraceID 不串号。
- **依赖从超时到恢复**（`health`）：连续超时判 `dependency_timeout`，故障样本滑出窗口后实时重判为 `healthy`，验证无缓存掩盖。
- **告警**（`alert`）：触发去重、恢复后再次触发、乱序事件不漏判不误判、按历史版本复现判定。
- **标识生命周期**（`correlation`、`pipeline`）：请求结束（含出错）后标识归还可重新继承；在飞重复标识（含并发在飞）仍被替换且互不串号；判重记录受容量上限约束、超限 FIFO 淘汰。
- **后台任务链路**（`pipeline`）：`SpawnBackground` 派生的任务与父请求同 TraceID、RequestID 各异。
- **运行报告**（`pipeline`）：无数据正常输出；同样输入重跑报告逐字节一致；报告计数与全局指标汇总一致；依赖先超时后恢复的场景中健康与告警的变化均入报告。
- 单测日志均打印关联标识（`trace=... request=... source=...`）与判定依据（`verdict=...` / `decision=...` / 来源判定说明）。

## 本轮改动对原有行为的影响

- `correlation.NewTracker` 签名由 `(Validator)` 变为 `(Validator, ...Option)`，原有单参调用方式完全兼容。
- TraceID 改由可注入的 ID 生成器产生，默认仍是加密随机，默认行为不变；`Release` 语义不变（现在只删除存在的记录并维护 FIFO 顺序）。
- `metrics`/`health`/`alert` 三个包零改动，原有用例全部保持通过；`pipeline` 包原有集成测试不受影响。
