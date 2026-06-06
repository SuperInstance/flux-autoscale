//! # flux-autoscale
//!
//! Auto-scaling Flux bytecode execution based on workload demand.
//! More agents = more GPU streams. Scales up on backpressure, down on idle.

use std::collections::VecDeque;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScaleAction { ScaleUp, ScaleDown, Hold }

#[derive(Debug, Clone)]
pub struct StreamMetrics {
    pub stream_id: u32,
    pub queue_depth: usize,
    pub throughput_ops_s: f64,
    pub latency_us: u64,
    pub backpressure: bool,
}

pub struct Autoscaler {
    min_streams: u32,
    max_streams: u32,
    current_streams: u32,
    metrics: Vec<StreamMetrics>,
    scale_up_threshold: f64,
    scale_down_threshold: f64,
    scale_events: Vec<ScaleEvent>,
}

#[derive(Debug, Clone)]
pub struct ScaleEvent {
    pub action: ScaleAction,
    pub from: u32,
    pub to: u32,
    pub reason: String,
}

impl Autoscaler {
    pub fn new(min_streams: u32, max_streams: u32) -> Self {
        Self {
            min_streams, max_streams,
            current_streams: min_streams,
            metrics: Vec::new(),
            scale_up_threshold: 0.8,
            scale_down_threshold: 0.2,
            scale_events: Vec::new(),
        }
    }

    pub fn update_metrics(&mut self, metrics: Vec<StreamMetrics>) {
        self.metrics = metrics;
    }

    pub fn evaluate(&mut self) -> ScaleAction {
        if self.metrics.is_empty() { return ScaleAction::Hold; }

        let avg_utilization: f64 = self.metrics.iter()
            .map(|m| m.queue_depth as f64 / 100.0).sum::<f64>() / self.metrics.len() as f64;
        let any_backpressure = self.metrics.iter().any(|m| m.backpressure);
        let all_idle = self.metrics.iter().all(|m| m.queue_depth == 0);

        let action = if (avg_utilization > self.scale_up_threshold || any_backpressure) && self.current_streams < self.max_streams {
            ScaleAction::ScaleUp
        } else if (avg_utilization < self.scale_down_threshold || all_idle) && self.current_streams > self.min_streams {
            ScaleAction::ScaleDown
        } else {
            ScaleAction::Hold
        };

        if action != ScaleAction::Hold {
            let prev = self.current_streams;
            match action {
                ScaleAction::ScaleUp => self.current_streams += 1,
                ScaleAction::ScaleDown => self.current_streams -= 1,
                ScaleAction::Hold => {}
            }
            self.scale_events.push(ScaleEvent {
                action, from: prev, to: self.current_streams,
                reason: format!("util={:.2} bp={} idle={}", avg_utilization, any_backpressure, all_idle),
            });
        }

        action
    }

    /// Simulate running for N ticks.
    pub fn run_ticks(&mut self, tick_metrics: &[Vec<StreamMetrics>]) -> Vec<ScaleAction> {
        tick_metrics.iter().map(|m| {
            self.update_metrics(m.clone());
            self.evaluate()
        }).collect()
    }

    pub fn current_streams(&self) -> u32 { self.current_streams }
    pub fn scale_events(&self) -> &[ScaleEvent] { &self.scale_events }
    pub fn is_scaled_up(&self) -> bool { self.current_streams > self.min_streams }
}

fn make_metrics(count: u32, queue_depth: usize, bp: bool) -> Vec<StreamMetrics> {
    (0..count).map(|i| StreamMetrics {
        stream_id: i, queue_depth, throughput_ops_s: 1000.0, latency_us: 100, backpressure: bp,
    }).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hold_on_normal() {
        let mut s = Autoscaler::new(1, 10);
        s.update_metrics(make_metrics(2, 50, false));
        assert_eq!(s.evaluate(), ScaleAction::Hold);
    }

    #[test]
    fn test_scale_up_on_backpressure() {
        let mut s = Autoscaler::new(1, 10);
        s.update_metrics(make_metrics(1, 90, true));
        assert_eq!(s.evaluate(), ScaleAction::ScaleUp);
        assert_eq!(s.current_streams(), 2);
    }

    #[test]
    fn test_scale_down_on_idle() {
        let mut s = Autoscaler::new(1, 10);
        s.current_streams = 5;
        s.update_metrics(make_metrics(5, 0, false));
        assert_eq!(s.evaluate(), ScaleAction::ScaleDown);
        assert_eq!(s.current_streams(), 4);
    }

    #[test]
    fn test_respect_max() {
        let mut s = Autoscaler::new(1, 2);
        s.update_metrics(make_metrics(2, 99, true));
        s.evaluate(); // scale to 2
        s.update_metrics(make_metrics(2, 99, true));
        assert_eq!(s.evaluate(), ScaleAction::Hold); // at max
    }

    #[test]
    fn test_respect_min() {
        let mut s = Autoscaler::new(1, 10);
        s.update_metrics(make_metrics(1, 0, false));
        assert_eq!(s.evaluate(), ScaleAction::Hold); // at min
    }

    #[test]
    fn test_scale_events_tracked() {
        let mut s = Autoscaler::new(1, 10);
        s.update_metrics(make_metrics(1, 95, true));
        s.evaluate();
        assert_eq!(s.scale_events().len(), 1);
        assert_eq!(s.scale_events()[0].from, 1);
        assert_eq!(s.scale_events()[0].to, 2);
    }

    #[test]
    fn test_run_ticks() {
        let mut s = Autoscaler::new(1, 5);
        let actions = s.run_ticks(&[
            make_metrics(1, 50, false),
            make_metrics(1, 95, true),
            make_metrics(2, 50, false),
            make_metrics(2, 0, false),
        ]);
        assert!(actions.contains(&ScaleAction::ScaleUp));
    }

    #[test]
    fn test_is_scaled_up() {
        let mut s = Autoscaler::new(1, 10);
        assert!(!s.is_scaled_up());
        s.update_metrics(make_metrics(1, 99, true));
        s.evaluate();
        assert!(s.is_scaled_up());
    }
}
