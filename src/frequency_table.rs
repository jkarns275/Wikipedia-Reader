use lazy_static;
use std::collections::HashMap;
use page::Page;

pub struct FrequencyTable(pub HashMap<String, usize>);

impl FrequencyTable {
    pub fn new() -> Self {
        FrequencyTable(HashMap::new())
    }

    pub fn new_from(s: HashMap<String, usize>) -> Self {
        FrequencyTable(s)
    }

    pub fn new_from_str(s: &str) -> Self {
        let mut this = FrequencyTable::new();
        for word_str in s.split_whitespace() {
            let word = word_str.to_string();
            this.inc(word);
        }
        this
    }

    pub fn inc(&mut self, key: String) {
        let x = self.0.entry(key).or_insert(0);
        *x += 1;
    }

    /// Compares two FrequencyTables and returns a f64 which represents their
    /// difference. The closer to 1, the more similar.
    /// The algorithm used is a spin off of cosine similarity.
    pub fn compare(&self, other: &Self) -> f64 {
        let num = self.mul(other);
        let denom = self.abs() * other.abs();
        if denom == 0.0 {
            0.0
        } else {
            num / denom
        }
    }

    pub fn sum(&self) -> f64 {
        let mut acc = 0.0;
        for key in self.0.keys() {
            acc += *self.0.get(key).unwrap() as f64;
        }
        acc
    }

    pub fn abs(&self) -> f64 {
        let mut acc = 0.0;
        for key in self.0.keys() {
            let freq = *self.0.get(key).unwrap();
            acc += (freq * freq) as f64;
        }
        acc.sqrt()
    }

    pub fn mul(&self, rhs: &Self) -> f64 {
        let mut acc = 0.0;
        for key in self.0.keys() {
            if rhs.0.contains_key(key) {
                acc += (self.0.get(key).unwrap() * rhs.0.get(key).unwrap()) as f64;
            }
        }
        acc
    }
}
