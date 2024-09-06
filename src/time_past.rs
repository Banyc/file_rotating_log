use std::sync::Arc;

pub trait TimeContains: core::fmt::Debug + Sync + Send {
    fn matches(&self, interval: Interval) -> bool;
}

#[derive(Debug, Clone)]
pub struct TimePast {
    prev: Option<jiff::Zoned>,
    time_contains: Arc<dyn TimeContains>,
}
impl TimePast {
    pub fn new(time_contains: Arc<dyn TimeContains>) -> Self {
        Self {
            prev: None,
            time_contains,
        }
    }

    pub fn poll(&mut self, now: jiff::Zoned) -> bool {
        let interval = Interval {
            exclusive_start: self.prev.clone(),
            inclusive_end: now.clone(),
        };
        self.prev = Some(now);
        self.time_contains.matches(interval)
    }
}

#[derive(Debug, Clone)]
pub struct Interval {
    pub exclusive_start: Option<jiff::Zoned>,
    pub inclusive_end: jiff::Zoned,
}
