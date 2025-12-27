use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};

pub struct ProgressCounter {
    label: &'static str,
    interval: u64,
    count: AtomicU64,
}

impl ProgressCounter {
    pub fn new(label: &'static str, interval: u64) -> Self {
        let counter = Self {
            label,
            interval: interval.max(1),
            count: AtomicU64::new(0),
        };
        counter.print(0);
        counter
    }

    pub fn inc(&self, delta: u64) {
        let prev = self.count.fetch_add(delta, Ordering::SeqCst);
        let current = prev + delta;
        // Print if we crossed an interval boundary
        if prev / self.interval < current / self.interval {
            self.print(current);
        }
    }

    pub fn finish(&self) {
        self.print(self.count.load(Ordering::SeqCst));
        eprintln!();
    }

    fn print(&self, current: u64) {
        use std::io::Write;
        eprint!("\r{}: {}", self.label, current);
        let _ = std::io::stderr().flush();
    }
}

pub fn build_tag_map<'a, I>(tags: I) -> HashMap<String, String>
where
    I: Iterator<Item = (&'a str, &'a str)>,
{
    tags.map(|(k, v)| (k.to_string(), v.to_string())).collect()
}
