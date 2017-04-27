use std::io::{ Seek, SeekFrom, Read, Write };
use std::io;
use random_access_file::Serialize;
use cfile_rs::CFile;
use std::f64;
use random_access_file;
use check;

/// Simple edge struct
pub struct Edge {
    /// The node this edge leads to.
    pub to: usize,

    /// The weight of this node
    pub weight: f64
}

impl Edge {
    pub fn new(to: usize, weight: f64) -> Edge {
        Edge {
            to: to,
            weight: weight
        }
    }
}

impl Serialize for Edge {
    type DeserializeOutput = Edge;
    fn deserialize(from: &mut Read) -> Result<Self, io::Error> {
        let to;
        check!(u64::deserialize(from), to);
        let weight;
        check!(f64::deserialize(from), weight);
        Ok(Edge::new(to as usize, weight))
    }
    fn serialize(&self, to: &mut Write) -> Result<(), io::Error> {
        check!((self.to as u64).serialize(to));
        check!(self.weight.serialize(to));
        Ok(())
    }
    fn serialized_len(&self) -> u64 {
        16
    }
}
