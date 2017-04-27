use random_access_file::Serialize;
use cfile_rs::CFile;
use std::hash::Hash;
use std::collections::HashMap;
use std::sync::Arc;
use std::ops::Deref;
use std::{ u64, f64 };
use priority_queue::{ PriorityQueue, GraphNode };
use std::io;
use std::io::{ Write, Read };
use node::Node;
use persistable_hash::PersistableHashMap;
#[macro_use]
use check;
/// A graph represented by a hashmap of Nodes. The nodes reference each other.
pub struct Graph {
    pub nodes: Vec<Node>,
    marker: usize,
}

/// Public methods for Graph
impl<'a> Graph {
    pub fn new() -> Self {
        Graph { nodes: vec![], marker: 0 }
    }

    pub fn count(&self) -> usize { self.nodes.len() }

    /// Returns the weight to get from one edge to another, returns None if there is no connection
    pub fn weight(&self, from: usize, to: usize) -> Option<f64> {
        for edge in self.nodes[from].edges.iter() {
            if edge.to == to {
                return Some(edge.weight);
            }
        }
        None
    }

    /// Adds a node to the tree, returns its value (i.e. its index)
    pub fn add(&mut self) -> usize {
        let n = self.nodes.len();
        let node = Node::new(n);
        self.nodes.push(node);
        n
    }

    /// Connects node from with node to with the supplied weight. Returns None if either of the nodes
    /// are not in the graph.
    pub fn connect(&mut self, from: usize, to: usize, weight: f64) -> Option<()> {
        if self.nodes.len() > from && self.nodes.len() > to {
            self.nodes[from].connect_with(to, weight);
            Some(())
        } else {
            None
        }
    }

    /// Creates a min spanning tree
    pub fn min_spanning_tree(&mut self, from: usize) -> Option<ResultTree> {
        if from >= self.nodes.len() { return None }

        self.marker += 1;
        self.nodes[from].marker = self.marker;

        let mut pq = PriorityQueue::new();
        let mut dist = vec![f64::INFINITY; self.nodes.len()];
        pq.push(GraphNode::new(from, 0.0));
        dist[from] = 0.0;

        while !pq.empty() {
            let top = pq.poll().unwrap();
            let current_min = top.0;
            self.nodes[current_min].marker = self.marker;
            for i in 0..self.nodes[current_min].edges.len() {
                let dest = self.nodes[current_min].edges[i].to;
                let weight = self.nodes[current_min].edges[i].weight;
                if dist[dest] > weight {
                    self.nodes[dest].next = current_min;
                    dist[dest] = weight;
                    pq.push(GraphNode(dest, dist[dest]));
                }
            }
        }
        Some(ResultTree::new(&*self, from))
    }

    /// Creates a shortest path tree using Dijkstra's algorithm
    pub fn shortest_path_tree(&mut self, from: usize) -> Option<ResultTree> {
        if from >= self.nodes.len() { return None }

        self.marker += 1;
        self.nodes[from].marker = self.marker;

        let mut pq = PriorityQueue::new();
        let mut dist = vec![f64::INFINITY; self.nodes.len()];
        pq.push(GraphNode::new(from, 0.0));
        dist[from] = 0.0;

        while !pq.empty() {
            let top = pq.poll().unwrap();
            let current_min = top.0;
            for i in 0..self.nodes[current_min].edges.len() {
                let dest = self.nodes[current_min].edges[i].to;
                let weight = self.nodes[current_min].edges[i].weight;
                if dist[dest] > dist[current_min] + weight {
                    self.nodes[dest].marker = self.marker;
                    self.nodes[dest].next = current_min;
                    dist[dest] = dist[current_min] + weight;
                    pq.push(GraphNode(dest, dist[dest]));
                }
            }
        }
        Some(ResultTree::new(&*self, from))
    }

    /// Finds the shortest path between two nodes using Prim's algorithm. Returns None if no such path exists.
    pub fn shortest_path(&mut self, from: usize, to: usize) -> Option<(Vec<usize>, f64)> {
        if from >= self.nodes.len() { return None }

        self.marker += 1;
        self.nodes[from].marker = self.marker;

        let mut pq = PriorityQueue::new();
        let mut dist = vec![f64::INFINITY; self.nodes.len()];
        pq.push(GraphNode::new(from, 0.0));
        dist[from] = 0.0;

        while !pq.empty() {
            let top = pq.poll().unwrap();
            let current_min = top.0;
            for i in 0..self.nodes[current_min].edges.len() {
                let dest = self.nodes[current_min].edges[i].to;
                let weight = self.nodes[current_min].edges[i].weight;
                if dist[dest] > dist[current_min] + weight {
                    self.nodes[dest].marker = self.marker;
                    self.nodes[dest].next = current_min;
                    dist[dest] = dist[current_min] + weight;
                    if dest == to { break; }
                    pq.push(GraphNode(dest, dist[dest]));
                }
            }
        }

        let mst = ResultTree::new(&*self, from);
        mst.path_to(to)
    }
}

/// A structure that contains a min spainning tree, or a shortest path tree.
/// Allows retreival of paths from the given root to another node.
pub struct ResultTree<'a> {
    graph: &'a Graph,
    root: usize
}

impl<'a> ResultTree<'a> {
    pub fn new(graph: &'a Graph, root: usize) -> ResultTree<'a> {
        ResultTree { graph: graph, root: root }
    }

    pub fn path_to(&self, to: usize) -> Option<(Vec<usize>, f64)> {
        if self.graph.nodes[to].marker != self.graph.marker { return None; }

        let mut ret = vec![to];
        let mut weight = 0.0;
        let mut ind = 0;
        let mut current = &self.graph.nodes[to];

        while current.id != self.root {
            ret.push(current.next);
            let t = self.graph.weight(ret[ind + 1], ret[ind]);
            log!("Debug", "w = {:?}", t);
            if t.is_some() { weight += t.unwrap() }
            current = &self.graph.nodes[current.next as usize];
            ind += 1;
        }
        ret.reverse();
        Some((ret, weight))
    }

    /// Checks if the tree spans :-)
    pub fn spans(&self) -> bool {
        for i in 0..self.graph.nodes.len() {
            match self.path_to(i) {
                Some(_) => continue,
                None => return false
            }
        }
        return true;
    }
}

/// Serialization stuff for Graph
struct NodeList<'a>(pub &'a Vec<Node>);

impl<'a> Serialize for NodeList<'a> {
    type DeserializeOutput = Vec<Node>;

    fn deserialize(read: &mut Read) -> Result<Vec<Node>, io::Error> {
        let len;
        check!(u64::deserialize(read), len);
        let mut r = vec![];
        for i in 0..len {
            let edge;
            check!(Node::deserialize(read), edge);
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
        let mut sum = 8;
        for i in self.0.iter(){
            sum += i.serialized_len();
        }
        sum
    }
}

impl Serialize for Graph {
    type DeserializeOutput = Graph;

    fn deserialize(read: &mut Read) -> Result<Self, io::Error> {
        let nodes;
        check!(NodeList::deserialize(read), nodes);
        Ok(Graph {
            nodes: nodes,
            marker: 0
        })
    }

    fn serialize(&self, to: &mut Write) -> Result<(), io::Error> {
        check!(NodeList(&self.nodes).serialize(to));
        Ok(())
    }

    fn serialized_len(&self) -> u64 {
        let mut sum = 0;
        for i in self.nodes.iter() {
            sum += i.serialized_len();
        }
        sum
    }
}

pub struct AssociatedResultTree<'a, T: 'a + Eq + Hash> {
    graph: &'a AssociatedGraph<T>,
    root: usize
}

impl<'a, T> AssociatedResultTree<'a, T> where T: Eq + Hash {
    pub fn new(graph: &'a AssociatedGraph<T>, root: usize) -> AssociatedResultTree<'a, T> {
        AssociatedResultTree { graph: graph, root: root }
    }

    pub fn path_to(&self, to: &T) -> Option<Path<T>> {
        if self.graph.graph.nodes[self.graph.items[to]].marker != self.graph.graph.marker { return None }

        let mut ret = vec![self.graph.items[to]];
        let mut weight = 0.0;
        let mut current = &self.graph.graph.nodes[ret[0]];
        let mut ind = 0;
        while current.id != self.root {
            ret.push(current.next);
            if let Some(x) = self.graph.graph.weight(ret[ind], ret[ind + 1]) {
                weight += x;
            }
            current = &self.graph.graph.nodes[current.next];
            ind += 1;
        }

        ret.reverse();
        Some(Path {
            path: ret.into_iter().map(|x| self.graph.lookup[&x].clone()).collect::<Vec<Arc<T>>>(),
            distance: weight,
        })
    }

    pub fn path_to_with_weight(&self, to: &T) -> Option<WeightedPath<T>> {
        if self.graph.graph.nodes[self.graph.items[to]].marker != self.graph.graph.marker { return None }

        let mut ret = vec![(self.graph.items[to], 0.0)];
        let mut weight = 0.0;
        let mut current = &self.graph.graph.nodes[self.graph.items[to]];
        let mut ind = 0;
        while current.id != self.root {
            let next = current.next;
            if let Some(x) = self.graph.graph.weight(ret[ind].0, next) {
                weight += x;
                ret.push((current.next, x));
            }
            current = &self.graph.graph.nodes[current.next as usize];
            ind += 1;
        }

        ret.reverse();
        Some(WeightedPath {
            path: ret.into_iter().map(|x| (self.graph.lookup[&x.0].clone(), x.1)).collect::<Vec<(Arc<T>, f64)>>(),
            distance: weight,
        })
    }

    pub fn spans(&self) -> bool {
        for i in 0..self.graph.graph.nodes.len() {
            match self.path_to(&*self.graph.lookup[&i]) {
                Some(_) => continue,
                None => return false
            }
        }
        return true;
    }
}



pub struct AssociatedGraph<T> where T: Hash + Eq {
    graph: Graph,
    items: HashMap<Arc<T>, usize>,
    lookup: HashMap<usize, Arc<T>>
}

impl<T> AssociatedGraph<T> where T: Hash + Eq {

    /// Creates a new AssociatedGraph
    pub fn new() -> Self {
        AssociatedGraph { graph: Graph::new(), items: HashMap::new(), lookup: HashMap::new() }
    }

    /// Returns a vec containing all of the keys found in the graph.
    pub fn keys(&self) -> Vec<Arc<T>> {
        self.items.keys().into_iter().map(|x| x.clone()).collect()
    }

    pub fn len(&self) -> usize { self.items.len() }

    /// Adds an element to the graph, creates a key / value pair consisting of they key and the corresponding index in the Graph.
    /// If the key is already in the AssociatedGraph, this will return Err(())
    pub fn add(&mut self, key: T) -> Result<(), ()> {
        if self.items.contains_key(&key) {
            Err(())
        } else {
            let value = self.graph.add();
            let item = Arc::new(key);
            self.items.insert(item.clone(), value);
            self.lookup.insert(value, item.clone());
            Ok(())
        }
    }

    /// Connects two elements in the graph.
    /// If either of the keys arent in the Hash it will return Err(())
    pub fn connect(&mut self, from: &T, to: &T, weight: f64) -> Option<()> {
        if self.items.contains_key(from) && self.items.contains_key(to) {
            let from_ind = self.items[from];
            let to_ind = self.items[to];
            self.graph.connect(from_ind, to_ind, weight).unwrap();
            Some(())
        } else {
            None
        }
    }

    pub fn get_id(&self, key: &T) -> Option<usize> {
        if self.contains_key(key) {
            Some(self.items[key])
        } else {
            None
        }
    }

    pub fn contains_key(&self, has: &T) -> bool {
        self.items.contains_key(has)
    }

    /// Creates a shortest path tree
    pub fn shortest_path_tree(&mut self, from: &T) -> Option<AssociatedResultTree<T>> {
        if !self.items.contains_key(from) { return None }
        let _ = self.graph.shortest_path_tree(self.items[from]);
        Some(AssociatedResultTree {
            graph: self,
            root: self.items[from]
        })
    }

    /// Creates a min spanning tree
    pub fn min_spanning_tree(&mut self, from: &T) -> Option<AssociatedResultTree<T>> {
        if !self.items.contains_key(from) { return None }
        let _ = self.graph.min_spanning_tree(self.items[from]);
        Some(AssociatedResultTree {
            graph: self,
            root: self.items[from]
        })
    }

    /// Returns an iterator of all of the connections a given node has.
    pub fn connections(&self, k: &T) -> Option<Path<T>> {
        if self.items.contains_key(k) {
            let index = self.items[k];
            let mut v = Vec::with_capacity(self.graph.nodes[index].edges.len());
            for edge in self.graph.nodes[index].edges.iter() {
                v.push(self.lookup[&edge.to].clone());
            }
            Some(Path {
                distance: f64::NAN,
                path: v,
            })
        } else {
            None
        }
    }

    pub fn shortest_path(&mut self, from: &T, to: &T) -> Option<Path<T>> {
        if !(self.items.contains_key(from) && self.items.contains_key(to)) { return None }
        let path = self.graph.shortest_path(self.items[from], self.items[to]);
        if path.is_none() { return None }
        let (p, distance) = path.unwrap();
        let v = p.into_iter().map(|x| self.lookup[&x].clone()).collect::<Vec<Arc<T>>>();
        let path = Path {
            path: v,
            distance: distance,
        };
        Some(path)
    }
}

impl<T> Serialize for AssociatedGraph<T> where T: Serialize + Eq + Hash, T::DeserializeOutput: Eq + Hash + Serialize {
    type DeserializeOutput = AssociatedGraph<T::DeserializeOutput>;

    fn deserialize(read: &mut Read) -> Result<Self::DeserializeOutput, io::Error> {
        let graph;
        check!(Graph::deserialize(read), graph);
        let mut temp_map: HashMap<T::DeserializeOutput, usize>;
        check!(PersistableHashMap::<T, usize>::deserialize(read), temp_map);
        let mut items = HashMap::new();
        let mut lookup = HashMap::new();
        for (k, v) in temp_map.into_iter() {
            let rc = Arc::new(k);
            items.insert(rc.clone(), v);
            lookup.insert(v, rc);
        }
        Ok(AssociatedGraph {
            graph: graph,
            items: items,
            lookup: lookup
        })
    }

    fn serialize(&self, to: &mut Write) -> Result<(), io::Error> {
        check!(self.graph.serialize(to));
        check!(self.items.len().serialize(to));
        for (k, v) in self.items.iter() {
            check!((**k).serialize(to));
            check!(v.serialize(to));
        }
        Ok(())
    }

    fn serialized_len(&self) -> u64 {
        let mut sum = 8 + self.graph.serialized_len();
        for (k, v) in self.items.iter() {
            sum += k.serialized_len() + v.serialized_len();
        }
        sum
    }
}

impl<T> AssociatedGraph<T> where T: Hash + Eq + Serialize, T::DeserializeOutput: Eq + Hash + Serialize {
    pub fn persist(&self, to: &str) -> Result<(), io::Error> {
        let mut file;
        check!(CFile::open(to, "w+"), file);
        check!(self.serialize(&mut file));
        Ok(())
    }

    pub fn from_disk(from: &str) -> Result<AssociatedGraph<T::DeserializeOutput>, io::Error> {
        let mut file;

        check!(CFile::open(from, "r+"), file);
        let graph;
        check!(AssociatedGraph::<T>::deserialize(&mut file), graph);
        Ok(graph)
    }
}

pub struct Path<T> where T: Hash + Eq {
    path: Vec<Arc<T>>,
    distance: f64,
}

impl<T> Path<T> where T: Hash + Eq {
    pub fn weight(&self) -> f64 {
        self.distance
    }
}

impl<T> IntoIterator for Path<T> where T: Hash + Eq {
    type Item = Arc<T>;
    type IntoIter = IntoIter<T>;

    fn into_iter(self) -> Self::IntoIter {
        IntoIter { pq: self, index: 0 }
    }
}

/// IntoIter helper struct.
pub struct IntoIter<T> where T: Hash + Eq {
    /// Path to be iterated over
    pq: Path<T>,

    /// The current index (so after the first call it will be 1, before that it will be zero)
    index: usize
}

/// The actual way the Path iterates
impl<T> Iterator for IntoIter<T> where T: Hash + Eq {
    type Item = Arc<T>;

    fn next(&mut self) -> Option<Arc<T>> {
        let x = self.pq.path.pop();
        if x.is_some() {
            self.index += 1;
            Some(x.unwrap())
        } else {
            None
        }
    }
}

pub struct WeightedPath<T> where T: Hash + Eq {
    path: Vec<(Arc<T>, f64)>,
    distance: f64,
}

impl<T> WeightedPath<T> where T: Hash + Eq {
    pub fn weight(&self) -> f64 {
        self.distance
    }
}

impl<T> IntoIterator for WeightedPath<T> where T: Hash + Eq {
    type Item = (Arc<T>, f64);
    type IntoIter = WeightedIntoIter<T>;

    fn into_iter(self) -> Self::IntoIter {
        WeightedIntoIter { pq: self, index: 0 }
    }
}

/// IntoIter helper struct.
pub struct WeightedIntoIter<T> where T: Hash + Eq {
    /// Path to be iterated over
    pq: WeightedPath<T>,

    /// The current index (so after the first call it will be 1, before that it will be zero)
    index: usize
}

/// The actual way the Path iterates
impl<T> Iterator for WeightedIntoIter<T> where T: Hash + Eq {
    type Item = (Arc<T>, f64);

    fn next(&mut self) -> Option<(Arc<T>, f64)> {
        let x = self.pq.path.pop();
        if x.is_some() {
            self.index += 1;
            Some(x.unwrap())
        } else {
            None
        }
    }
}
