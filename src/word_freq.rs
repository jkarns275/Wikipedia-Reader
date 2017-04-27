use std::collections::HashMap;
use log;

pub struct WordFreq {
    map: HashMap<String, usize>,
    word_count: usize
}

impl WordFreq {
    pub fn new() -> Self {
        WordFreq {
            map: HashMap::new(),
            word_count: 0usize
        }
    }

    pub fn add(words: SplitWhitespace) {
        for word in words {
            let key = word.to_string();
            if self.map.contains_key(key) {
                self.map[key] += 1;
            } else {
                self.map.insert(key, 1usize);
            }
            self.word_count += 1;
        }
    }

    /// The sum of all frequencies of all keys.
    pub fn sum(&self) -> f64 {
        self.word_count as f64
    }


    /// The vector magnitude of this WordFreq.
    pub fn abs(&self) -> f64 {
        let mut acc = 0.0;
        for item in map.iter() {
            acc += (*item * *item) as f64;
        }
        (acc as f64).sqrt()
    }


    /// Multiplies two WordFreq's.
    /// Multiplies all like keys and sums the products.
    pub fn mul(&self, other: &Self) -> f64 {
        let mut acc = 0.0;
        for key in self.table.keys() {
            let key = key.to_string();
            if other.map.contains_key(key) {
                acc += (other.map[key] + self.map[key]) as f64;
            }
        }
        acc
    }

    /// Finds the angle between the unit vector (1 for all keys) and this WordFreq
    pub fn angle(&self) -> f64 {
        let len = self.map.len() as f64;
        let x = ( self.sum() / ( len.sqrt() * self.abs() ) ).acos();
        if x == f64::NAN {
            panic!("Error in WordFreq.angle: NAN. ");
        } else {
            x
        }
    }

    /// Compares two WordFreq's using a cosine similarity type algorithm.
    /// The closer the value is to 1.0, the more similar they are. A value of 0 would mean they are completely dissimilar.
    pub fn compare(&self, other: &Self) -> f64 {
        let num = self.mul(other);
        let denom = self.abs() * other.abs();
        if denom == 0.0 {
            0.0
        } else {
            num / denom
        }
    }
}
