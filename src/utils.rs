use crate::config::TagMatch;
use std::collections::HashMap;
use std::io::Write;

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
        eprint!("\r{}: {}", self.label, current);
        let _ = std::io::stderr().flush();
    }
}

pub fn glob_match(pattern: &str, value: &str) -> bool {
    if pattern == "*" {
        return true;
    }

    let parts: Vec<&str> = pattern.split('*').collect();
    if parts.len() == 1 {
        return pattern == value;
    }

    let mut remaining = value;
    if !pattern.starts_with('*') {
        let prefix = parts.first().unwrap();
        if !remaining.starts_with(prefix) {
            return false;
        }
        remaining = &remaining[prefix.len()..];
    }

    if !pattern.ends_with('*') {
        let suffix = parts.last().unwrap();
        if !remaining.ends_with(suffix) {
            return false;
        }
    }

    for part in parts.iter().filter(|p| !p.is_empty()) {
        match remaining.find(part) {
            Some(idx) => {
                remaining = &remaining[idx + part.len()..];
            }
            None => return false,
        }
    }

    true
}

pub fn build_tag_map<'a, I>(tags: I) -> HashMap<String, String>
where
    I: Iterator<Item = (&'a str, &'a str)>,
{
    tags.map(|(k, v)| (k.to_string(), v.to_string())).collect()
}

pub fn matches_tag(tag_match: &TagMatch, tags: &HashMap<String, String>) -> bool {
    let Some(tag_val) = tags.get(&tag_match.tag) else {
        return false;
    };

    if let Some(value) = &tag_match.value {
        return tag_val == value;
    }

    if !tag_match.values.is_empty() {
        return tag_match
            .values
            .iter()
            .any(|pattern| glob_match(pattern, tag_val));
    }

    true
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn glob_match_supports_star_suffix() {
        assert!(glob_match("*_link", "motorway_link"));
        assert!(!glob_match("*_link", "motorway"));
    }

    #[test]
    fn matches_tag_values_with_glob() {
        let mut tags = HashMap::new();
        tags.insert("highway".to_string(), "trunk_link".to_string());
        let tag_match = TagMatch {
            tag: "highway".to_string(),
            value: None,
            values: vec!["primary".to_string(), "*_link".to_string()],
        };
        assert!(matches_tag(&tag_match, &tags));
    }
}
