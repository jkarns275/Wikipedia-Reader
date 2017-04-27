use std::usize;
use random_access_file;
use random_access_file::Serialize;
use std::io;
use std::io::{ Read, Write };
use edge::Edge;
use std::u64;
use check;

pub struct Node {
    /// The id for the Node. This can be used simply to distinguish nodes, or as an index to some data cache, or anything else.
    pub id: usize,

    /// A list of all the connections this node makes.
    pub edges: Vec<Edge>,

    /// Used to mark if this node has been visited in some of the algorithms.
    pub marker: usize,

    /// Used to mark the next node in a path from Node to Node. If the next node in the path doesn't exist it will be None.
    pub next: usize
}

/// Public methods for Node
impl Node {
    pub fn new(id: usize) -> Self {
        Node {
            edges: vec![],
            marker: 0,
            next: usize::MAX,
            id: id
        }
    }

    /// Connects this node with node 'to', and weight 'weight'
    pub fn connect_with(&mut self, to: usize, weight: f64) {
        let edge = Edge::new(to, weight);
        self.edges.push(edge)
    }
}

/// Serialization stuff for Node
struct EdgeList<'a>(pub &'a Vec<Edge>);

impl<'a> Serialize for EdgeList<'a> {
    type DeserializeOutput = Vec<Edge>;

    fn deserialize(read: &mut Read) -> Result<Vec<Edge>, io::Error> {
        let len;
        check!(u64::deserialize(read), len);
        let mut r = vec![];
        for i in 0..len {
            let edge;
            check!(Edge::deserialize(read), edge);
            r.push(edge);
        }
        Ok(r)
    }
    fn serialize(&self, to: &mut Write) -> Result<(), io::Error> {
        check!((self.0.len() as u64).serialize(to));
        for i in 0..self.0.len() {
            check!(self.0[i].serialize(to));
        }
        Ok(())
    }

    fn serialized_len(&self) -> u64 {
        8 + (16 * self.0.len()) as u64
    }
}

impl Serialize for Node {
    type DeserializeOutput = Node;

    fn deserialize(from: &mut Read) -> Result<Self, io::Error> {
        let id;
        check!(u64::deserialize(from), id);
        let edges;
        check!(EdgeList::deserialize(from), edges);

        Ok(Node {
            edges: edges,
            marker: 0,
            next: usize::MAX,
            id: id as usize
        })
    }

    fn serialize(&self, to: &mut Write) -> Result<(), io::Error> {
        check!(self.id.serialize(to));
        check!(EdgeList(&self.edges).serialize(to));
        Ok(())
    }

    fn serialized_len(&self) -> u64 {
        8 + (8 + (16 * self.edges.len())) as u64
    }
}
