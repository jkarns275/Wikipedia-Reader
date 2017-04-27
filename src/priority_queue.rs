use std::cmp::Ordering;

/// A generic PriorityQueue
pub struct PriorityQueue<T> where T: Ord {
    /// A heap represented by a Vec. 
    arr: Vec<T>
}

/// Private methods
impl<T> PriorityQueue<T> where T: Ord {
    /// Calculates the index of the right child of ind. There is no gaurentee this is a valid index.
    #[inline(always)]
    fn right_child(ind: usize) -> usize { Self::left_child(ind) + 1 }

    /// Calculates the index of the left child of ind. There is no gaurentee this is a valid index.
    #[inline(always)]
    fn left_child(ind: usize)  -> usize { (ind << 1) + 1}

    /// Calculates the index of the parent of ind.
    #[inline(always)]
    fn parent(ind: usize)      -> usize { if ind == 0 { 0 } else { (ind - 1) >> 1 } }

    /// Fixes the heap after adding an element.
    /// Starts from the last index and swaps its way up until it is in a valid position.
    fn adjust_after_push(&mut self) {
        let mut index = self.arr.len() - 1;
        while index != 0 {
            let parent = Self::parent(index);
            if self.arr[index] < self.arr[parent] {
                self.arr.swap(index, parent);
                index = parent;
            } else {
                break;
            }
        }
    }

    /// Fixes a node that is potentially in the wrong spot.
    fn adjust_after_decrease(&mut self, mut index: usize) {
        loop {
            let left = Self::left_child(index);
            let right = Self::right_child(index);
            if left >= self.arr.len() { break; }
            let least = {
                if right >= self.arr.len() || self.arr[left] < self.arr[right] { left }
                else { right }
            };
            if self.arr[index] > self.arr[least] {
                self.arr.swap(index, least);
                index = least;
                continue;
            }
            break;
        }
    }

    /// Fixes the node in position 0 after removing the first element and replacing it with the last (in the poll method)
    fn adjust_after_poll(&mut self) {
        self.adjust_after_decrease(0);
    }

    fn search(&self, item: &T) -> Option<usize> {
        //self.in_search(item, 0)
        let mut ind = 0;
        loop {
            if self.arr[ind] == *item {
                return Some(ind)
            } else if self.arr[ind] > *item {
                return None
            } else {
                ind += 1;
            }
        }
        None
    }
}

/// Public methods for PriorityQueue.
impl<T> PriorityQueue<T> where T: Ord {
    /// Creates a new empty priority queue
    pub fn new() -> Self {
        PriorityQueue {
            arr: vec![]
        }
    }

    /// Adds an element to the heap and ensures it is still a heap; if not it makes it so.
    pub fn push(&mut self, item: T) {
        self.arr.push(item);
        self.adjust_after_push();
    }

    /// Pushes every element in items onto the queue, ordering them accordingly.
    pub fn append<I>(&mut self, items: I) where I: Iterator<Item=T> {
        for item in items {
            self.push(item);
        }
    }

    /// Checks if the queue contains the element
    pub fn contains(&self, item: &T) -> bool {
        match self.search(item) {
            Some(_) => true,
            None => false
        }
    }

    /// Removes an element to the heap and ensures it is still a heap; if not it makes it so.
    pub fn poll(&mut self) -> Option<T> {
        if self.arr.len() == 0 {
            None
        } else if self.arr.len() == 1 {
            self.arr.pop()
        } else {
            let ind = self.arr.len() - 1;
            self.arr.swap(0, ind);
            let result = self.arr.pop();
            self.adjust_after_poll();
            result
        }
    }

    /// Returns true of the queue is empty, otherwise false.
    pub fn empty(&self) -> bool {
        self.arr.len() == 0
    }
}




pub struct GraphNode(pub usize, pub f64);

impl GraphNode {
    pub fn new(u: usize, f: f64) -> GraphNode { GraphNode(u, f) }
}

impl Eq for GraphNode {}

impl Ord for GraphNode {
    fn cmp(&self, other: &Self) -> Ordering {
        self.1.partial_cmp(&other.1).unwrap()
    }
}

impl PartialOrd for GraphNode {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.1.partial_cmp(&other.1)
    }
}

impl PartialEq for GraphNode {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}


impl PriorityQueue<GraphNode> {

    /// A really awful search function :)
    fn node_search(&self, node: &GraphNode) -> Option<usize> {
        for i in 0..self.arr.len() {
            if self.arr[i] == *node {
                return Some(i);
            }
        }
        return None;
    }

    pub fn decrease_priority(&mut self, node: usize, new_value: f64) -> Option<()> {
        let index = self.node_search(&GraphNode(node, 0.0));
        if index.is_some() {
            let index = index.unwrap();
            if self.arr[index].1 > new_value {
                self.arr[index].1 = new_value;
                self.adjust_after_decrease(index);
            }
            Some(())
        } else {
            None
        }
    }
}

/// An implementation for IntoIterator for PriorityQueue<T>.
/// This will allow priority queues to be used in for loops.
/// The for loops will consume the priority queue.
impl<T> IntoIterator for PriorityQueue<T> where T: Ord {
    type Item = T;
    type IntoIter = IntoIter<T>;

    fn into_iter(self) -> IntoIter<T> {
        IntoIter { pq: self, index: 0 }
    }
}

/// IntoIter helper struct.
pub struct IntoIter<T> where T: Ord {
    /// PriorityQueue to be iterated over
    pq: PriorityQueue<T>,

    /// The current index (so after the first call it will be 1, before that it will be zero)
    index: usize
}

/// The actual way the priority queue gets iterated over..
/// Calls poll until it returns None
impl<T> Iterator for IntoIter<T> where T: Ord {
    type Item = T;

    fn next(&mut self) -> Option<T> {
        let x = self.pq.poll();
        if x.is_some() {
            self.index += 1;
        }
        x
    }
}
