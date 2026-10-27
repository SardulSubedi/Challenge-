use std::time::{Duration, Instant};

/// Rate monitoring is advisory: an inactive market is not evidence of a disconnect.
/// Resubscribe conservatively, with a cooldown, rather than emitting invented updates.
pub struct Activity {
    created: Instant,
    last: Instant,
    window: Instant,
    count: u64,
    baseline: f64,
    last_repair: Instant,
    initialized: bool,
}

impl Activity {
    pub fn new(now: Instant) -> Self {
        Self {
            created: now,
            last: now,
            window: now,
            count: 0,
            baseline: 0.0,
            last_repair: now,
            initialized: false,
        }
    }
    pub fn snapshot(&mut self, now: Instant) {
        self.last = now;
        self.initialized = true;
    }
    pub fn delta(&mut self, now: Instant) {
        self.last = now;
        self.count += 1;
    }
    pub fn repair_reason(&mut self, now: Instant, quiet: Duration) -> Option<&'static str> {
        if !self.initialized
            && now.duration_since(self.created) >= Duration::from_secs(10)
            && now.duration_since(self.last_repair) >= Duration::from_secs(10)
        {
            self.last_repair = now;
            return Some("initial snapshot missing");
        }
        if now.duration_since(self.last_repair) < Duration::from_secs(60) {
            return None;
        }
        let mut reason = None;
        if now.duration_since(self.last) >= quiet {
            reason = Some("symbol quiet timeout");
        }
        if now.duration_since(self.window) >= Duration::from_secs(60) {
            let rate = self.count as f64 * 60.0 / now.duration_since(self.window).as_secs_f64();
            if self.baseline >= 10.0 && rate < self.baseline * 0.1 {
                reason = Some("symbol update rate below 10% of historical baseline");
            } else {
                self.baseline = if self.baseline == 0.0 {
                    rate
                } else {
                    self.baseline * 0.8 + rate * 0.2
                };
            }
            self.count = 0;
            self.window = now;
        }
        if reason.is_some() {
            self.last_repair = now;
        }
        reason
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn missing_initial_snapshot_is_repaired_without_waiting_for_quiet_cooldown() {
        let now = Instant::now();
        let mut activity = Activity::new(now);
        assert_eq!(
            activity.repair_reason(now + Duration::from_secs(9), Duration::from_secs(300)),
            None
        );
        assert_eq!(
            activity.repair_reason(now + Duration::from_secs(10), Duration::from_secs(300)),
            Some("initial snapshot missing")
        );
        assert_eq!(
            activity.repair_reason(now + Duration::from_secs(11), Duration::from_secs(300)),
            None
        );
        activity.snapshot(now + Duration::from_secs(12));
        assert_eq!(
            activity.repair_reason(now + Duration::from_secs(20), Duration::from_secs(300)),
            None
        );
    }
    #[test]
    fn quiet_and_rate_drop_are_cooled_down() {
        let n = Instant::now();
        let mut a = Activity::new(n);
        a.snapshot(n);
        for _ in 0..100 {
            a.delta(n);
        }
        assert!(
            a.repair_reason(n + Duration::from_secs(60), Duration::from_secs(300))
                .is_none()
        );
        assert_eq!(
            a.repair_reason(n + Duration::from_secs(120), Duration::from_secs(300)),
            Some("symbol update rate below 10% of historical baseline")
        );
        assert!(
            a.repair_reason(n + Duration::from_secs(121), Duration::from_secs(300))
                .is_none()
        );
        assert!(
            a.repair_reason(n + Duration::from_secs(301), Duration::from_secs(300))
                .is_some()
        );
    }
}
