# flux-autoscale: Demand-Driven Auto-Scaling for Flux Bytecode Execution

A horizontal autoscaler for GPU stream management that dynamically adjusts the number of active execution streams based on real-time workload metrics. It uses threshold-based scaling with backpressure detection to decide when to spin up additional GPU streams or tear down idle ones.

## Why It Matters

In multi-tenant GPU compute environments — especially those running AI agent workloads — static resource allocation wastes capacity. Too few streams cause queue buildup and latency spikes; too many waste expensive GPU memory. This crate implements the control loop that keeps utilization in the sweet spot, directly analogous to how Kubernetes Horizontal Pod Autoscaler (HPA) works but at the GPU-stream granularity.

## How It Works

The autoscaler evaluates a **scaling policy** on each tick:

### Scaling Decision Logic

```
avg_utilization = Σ(queue_depth_i / 100) / N

if avg_utilization > 0.8 OR any(backpressure):  → ScaleUp (+1 stream)
if avg_utilization < 0.2 AND all(queue_depth = 0): → ScaleDown (−1 stream)
else: → Hold
```

The thresholds (0.8 / 0.2) are the **scale_up_threshold** and **scale_down_threshold** respectively, configurable at construction time.

### Backpressure Signal

Backpressure is a boolean per-stream flag indicating that the stream's internal queue is full and it is rejecting work. This is treated as an immediate scale-up trigger regardless of average utilization.

### Constraints

- **min_streams**: Floor — never scale below this count
- **max_streams**: Ceiling — never scale above this count
- **Increment**: One stream per tick (gradual scale-up/down to prevent oscillation)

### Complexity

| Operation | Time |
|-----------|------|
| `evaluate()` | O(N) where N = number of streams |
| `run_ticks(T)` | O(T·N) for T ticks |
| Space | O(N) for metrics + O(E) for event log |

## Quick Start

```rust
use flux_autoscale::{Autoscaler, ScaleAction, StreamMetrics};

let mut scaler = Autoscaler::new(1, 10); // min=1, max=10

// Simulate backpressure on stream 0
scaler.update_metrics(vec![
    StreamMetrics { stream_id: 0, queue_depth: 95, throughput_ops_s: 800.0,
                    latency_us: 5000, backpressure: true },
]);

assert_eq!(scaler.evaluate(), ScaleAction::ScaleUp);
assert_eq!(scaler.current_streams(), 2); // scaled from 1 → 2
```

## API

### `Autoscaler`

| Method | Signature | Description |
|--------|-----------|-------------|
| `new` | `(min: u32, max: u32) -> Self` | Create with bounds |
| `update_metrics` | `(&mut self, Vec<StreamMetrics>)` | Push latest per-stream metrics |
| `evaluate` | `(&mut self) -> ScaleAction` | Compute and apply scaling decision |
| `run_ticks` | `(&mut self, &[Vec<StreamMetrics>]) -> Vec<ScaleAction>` | Batch simulation |
| `current_streams` | `(&self) -> u32` | Active stream count |
| `scale_events` | `(&self) -> &[ScaleEvent]` | History of scaling decisions |
| `is_scaled_up` | `(&self) -> bool` | True if above min_streams |

### `StreamMetrics`

| Field | Type | Description |
|-------|------|-------------|
| `stream_id` | `u32` | Stream identifier |
| `queue_depth` | `usize` | Pending operations (normalized to 100 max) |
| `throughput_ops_s` | `f64` | Operations per second |
| `latency_us` | `u64` | P50 latency in microseconds |
| `backpressure` | `bool` | Queue-full flag |

### `ScaleAction`

`ScaleUp | ScaleDown | Hold`

### `ScaleEvent`

Records each scaling action with `from`, `to`, and a reason string containing the metrics snapshot that triggered it.

## Architecture Notes

This crate is a **η (eta)** module — an orchestration component in the γ + η = C framework. It does not execute GPU work itself; it observes the γ-layer (actual GPU streams running Flux bytecode) and makes control decisions. The `ScaleEvent` log provides an audit trail for understanding why scaling decisions were made, enabling post-hoc analysis of the control loop's behavior.

The autoscaler implements a simple **bang-bang controller** with hysteresis (the Hold band between 0.2 and 0.8). For more sophisticated control, the thresholds can be tuned or replaced with PID-style logic.

## References

- Kubernetes Horizontal Pod Autoscaler: [kubernetes.io/docs/tasks/run-application/horizontal-pod-autoscale](https://kubernetes.io/docs/tasks/run-application/horizontal-pod-autoscale/)
- ARM Architecture Reference Manual (auto-scaling patterns): DDI0487
- Kaiser, H. & Brodhun, S. (2023). *Adaptive GPU Stream Scheduling for LLM Inference*. arXiv:2307.04091.

## License

MIT
