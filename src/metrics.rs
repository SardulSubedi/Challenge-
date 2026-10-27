use serde::Serialize;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};

/// Fixed-memory, allocation-free log2 histogram. Percentiles are bucket upper
/// bounds; max is exact. Concurrent window boundaries are approximate.
pub struct Histogram {
    buckets: [AtomicU64; 65],
    max: AtomicU64,
}
impl Default for Histogram {
    fn default() -> Self {
        Self {
            buckets: std::array::from_fn(|_| AtomicU64::new(0)),
            max: AtomicU64::new(0),
        }
    }
}
#[derive(Default, Debug, Serialize)]
pub struct Distribution {
    pub samples: u64,
    pub p50_upper_us: u64,
    pub p99_upper_us: u64,
    pub max_us: u64,
}
impl Histogram {
    pub fn record(&self, micros: u64) {
        let bucket = (64 - micros.leading_zeros()) as usize;
        self.buckets[bucket].fetch_add(1, Relaxed);
        self.max.fetch_max(micros, Relaxed);
    }
    pub fn take(&self) -> Distribution {
        let buckets = self.buckets.each_ref().map(|b| b.swap(0, Relaxed));
        let samples: u64 = buckets.iter().sum();
        let percentile = |percent: u64| {
            let target = samples.saturating_mul(percent).div_ceil(100);
            if target == 0 {
                return 0;
            }
            let mut count = 0;
            for (i, n) in buckets.iter().enumerate() {
                count += n;
                if count >= target {
                    return if i == 64 { u64::MAX } else { (1u64 << i) - 1 };
                }
            }
            0
        };
        Distribution {
            samples,
            p50_upper_us: percentile(50),
            p99_upper_us: percentile(99),
            max_us: self.max.swap(0, Relaxed),
        }
    }
}
#[derive(Default)]
pub struct Pipeline {
    pub engine_queue: Histogram,
    pub receive_to_sink: Histogram,
    pub output_queue: Histogram,
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn percentiles_bound_samples_and_reset_between_windows() {
        let h = Histogram::default();
        for _ in 0..99 {
            h.record(12);
        }
        h.record(1000);
        let result = h.take();
        assert_eq!(result.samples, 100);
        assert_eq!(result.p99_upper_us, 15);
        assert_eq!(result.max_us, 1000);
        assert_eq!(h.take().samples, 0);
    }
}
