use std::io::{ Seek, SeekFrom, Read, Write };
use std::io;
use random_access_file::Serialize;
use cfile_rs::CFile;
use std::marker::PhantomData;
use std::fmt::Debug;

/// An incredibly useful macro. It will check an expression of type Error<T, Z>,
/// if it is Err(err) it will RETURN in whatever function it is placed in with
/// error, otherwise it will continue in the function. This cuts down on the amount
/// of error checking code that will clog things up.
/// Optionally, it will also store the value x in $v (e.g. if it is Ok(x), $v = x).
macro_rules! check {
    ( $e:expr ) => (
    match $e {
        Ok(_) => {},
    Err(e) => return Err(e)
        }
    );
    ( $e:expr, $v:ident) => (
        match $e {
            Ok(r) => $v = r,
            Err(e) => return Err(e)
        }
    )
}

#[derive(PartialEq, Eq)]
struct Entry {
    pub key: u64,
    pub value: u64,
}

impl Entry {
    fn new(key: u64, value: u64) -> Entry {
        Entry { key: key, value: value }
    }

    fn len(from: &mut CFile) -> Result<u64, io::Error> {
        let len;
        check!(u64::deserialize(from), len);
        Ok(len)
    }

    fn pair(from: &mut CFile) -> Result<(u64, u64), io::Error> {
        let key;
        let value;
        check!(u64::deserialize(from), key);
        check!(u64::deserialize(from), value);
        Ok((key, value))
    }

    fn read_entry(pos: u64, from: &mut CFile) -> Result<Entry, io::Error> {
        check!(from.seek(SeekFrom::Start(pos)));
        Self::deserialize(from)
    }
}

impl Serialize for Entry
    where {
    type DeserializeOutput = Entry;

    fn serialize(&self, to: &mut Write) -> Result<(), io::Error> {
        check!(self.key.serialize(to));
        check!(self.value.serialize(to));
        Ok(())
    }
    fn deserialize(from: &mut Read) -> Result<Entry, io::Error> {
        let key;
        let val;
        check!(u64::deserialize(from), key);
        check!(u64::deserialize(from), val);
        Ok(
            Entry {
                key: key,
                value: val
            }
        )
    }
    fn serialized_len(&self) -> u64 {
        8 + 8 + self.key.serialized_len()
    }
}
const T: usize = 24;
const NUM_CHILDREN: usize = 2 * T;
const NUM_ENTRIES: usize = NUM_CHILDREN - 1;
const IS_NONE: u64 = 0xFFFFFFFFFFFFFFFFu64;
struct Node {
    /// Disk positions of Entries. If the value is IS_NONE, there is no key.
    pub entries: [u64; NUM_ENTRIES],

    /// Disk positions of Entries (child entries). If the value is IS_NONE, there is no child.
    pub children: [u64; NUM_CHILDREN],

    pub len: u64,
    pub leaf: bool
}

impl Node {
    pub fn new() -> Node {
        Node {
            entries: [0u64; NUM_ENTRIES],
            children: [0u64; NUM_CHILDREN],
            len: 0,
            leaf: true,
        }
    }
}

impl Serialize for Node {
    type DeserializeOutput = Self;
    fn serialize(&self, to: &mut Write) -> Result<(), io::Error> {
         for i in 0..NUM_ENTRIES {
             check!(self.entries[i].serialize(to));
         }
         for i in 0..NUM_CHILDREN {
             check!(self.children[i].serialize(to));
         }
         check!(self.len.serialize(to));
         if self.leaf {
             check!(1u8.serialize(to));
         } else {
             check!(0u8.serialize(to));
         }
         Ok(())
    }
    fn deserialize(from: &mut Read) -> Result<Node, io::Error> {
        let mut keys = [0u64; NUM_ENTRIES];
        let mut children = [0u64; NUM_CHILDREN];

        for i in 0..NUM_ENTRIES {
            let res;
            check!(u64::deserialize(from), res);
            keys[i] = res;
        }
        for i in 0..NUM_CHILDREN {
            let res;
            check!(u64::deserialize(from), res);
            children[i] = res;
        }
        let len;
        check!(u64::deserialize(from), len);

        let is_leaf;
        check!(u8::deserialize(from), is_leaf);
        Ok (
            Node {
                entries: keys,
                children: children,
                len: len,
                leaf: is_leaf != 0u8
            }
        )
    }
    fn serialized_len(&self) -> u64 {
        (NUM_ENTRIES * 8 + NUM_CHILDREN * 8 + 8) as u64
    }
}



pub struct PTree<K, V> where K: Serialize + Eq + PartialOrd + PartialEq,
                             V: Serialize,
                             K::DeserializeOutput: Serialize + Eq + PartialOrd + PartialEq,
                             V::DeserializeOutput: Serialize {
    treefile: CFile,
    keyfile: CFile,
    valfile: CFile,
    root_location: u64,
    phantom_k: PhantomData<K>,
    phantom_v: PhantomData<V>
}


impl<K, V> PTree<K, V> where K: Serialize + Eq + PartialOrd + PartialEq + Debug,
                       V: Serialize,
                       V::DeserializeOutput: Serialize,
                       K::DeserializeOutput: Serialize + Eq + PartialOrd + PartialEq + Debug {

    pub fn new(path: &str) -> Result<PTree<K, V>, io::Error> {
        let mut treefile;
        check!(CFile::open(&(path.to_string() + ".tree"), "w+"), treefile);
        let keyfile;
        check!(CFile::open(&(path.to_string() + ".key"), "w+"), keyfile);
        let valfile;
        check!(CFile::open(&(path.to_string() + ".val"), "w+"), valfile);

        let node = Node::new();
        check!(treefile.seek(SeekFrom::Start(0)));
        check!(8u64.serialize(&mut treefile));
        let _ = node.serialize(&mut treefile);
        Ok(
            PTree {
                keyfile: keyfile,
                valfile: valfile,
                treefile: treefile,
                root_location: 8,
                phantom_k: PhantomData {},
                phantom_v: PhantomData {}
            }
        )
    }

    pub fn open(path: &str) -> Result<PTree<K, V>, io::Error> {
        let mut treefile;
        check!(CFile::open(&(path.to_string() + ".tree"), "r+"), treefile);
        let keyfile;
        check!(CFile::open(&(path.to_string() + ".key"), "r+"), keyfile);
        let valfile;
        check!(CFile::open(&(path.to_string() + ".val"), "r+"), valfile);
        check!(treefile.seek(SeekFrom::Start(0)));
        let root;
        check!(u64::deserialize(&mut treefile), root);

        Ok(
            PTree {
                keyfile: keyfile,
                treefile: treefile,
                valfile: valfile,
                root_location: root,
                phantom_k: PhantomData {},
                phantom_v: PhantomData {}
            }
        )
    }

    // Since this tree doesn't support deleting of objects, reading keys from the keyfile until there is an error is actually a viable
    // way to get all of the keys. Probably a very frowned upon way though.
    pub fn keys(&mut self) -> Result<Vec<K::DeserializeOutput>, io::Error> {
        let mut keys = vec![];
        check!(self.keyfile.seek(SeekFrom::Start(0)));
        loop {
            match K::deserialize(&mut self.keyfile) {
                Ok(r) => keys.push(r),
                Err(_) => break
            }
        }
        Ok(keys)
    }

    fn split_child(&mut self, x: &mut Node, x_loc: u64, child: usize) -> Result<(), io::Error> {
        let mut y;
        check!(self.read_node(x.children[child]), y);
        let y_loc = x.children[child];
        let mut z = Node::new();
        z.leaf = y.leaf;
        z.len = T as u64 - 1;
        for j in 0..T - 1 {
            z.entries[j] = y.entries[j + T];
        }
        if !y.leaf {
            for j in 0..T {
                z.children[j] = y.children[j + T]
            }
        }
        y.len = T as u64 - 1;
        for j in (child + 1 .. (x.len + 1) as usize).rev() {
            x.children[j + 1] = x.children[j];
        }
        let z_loc;
        check!(self.write_node(&z), z_loc);
        x.children[child as usize + 1] = z_loc;
        for j in (child as i64 .. x.len as i64).rev() {
            x.entries[j as usize + 1] = x.entries[j as usize];
        }
        x.len += 1;
        x.entries[child] = y.entries[T - 1]; // Minus one because arrays are base zero etc.
        check!(self.update_node(&x, x_loc));
        check!(self.update_node(&y, y_loc));
        Ok(())
    }

    pub fn insert(&mut self, k: &K::DeserializeOutput, v: &V::DeserializeOutput) -> Result<(), io::Error> {
        let mut r;
        let r_loc = self.root_location;
        check!(self.root(), r);
        if r.len == NUM_ENTRIES as u64 {
            let mut s = Node::new();
            let s_loc;
            s.leaf = false;
            s.len = 0;
            s.children[0] = r_loc;
            check!(self.write_node(&s), s_loc);
            self.root_location = s_loc;
            check!(self.treefile.seek(SeekFrom::Start(0)));
            check!(self.root_location.serialize(&mut self.treefile));
            check!(self.split_child(&mut s, s_loc, 0));
            check!(self.insert_nonfull(&mut s, s_loc, k, v));
        } else {
            check!(self.insert_nonfull(&mut r, r_loc, k, v));
        }
        Ok(())
    }

    fn insert_nonfull(&mut self, x: &mut Node, x_loc: u64, k: &K::DeserializeOutput, v: &V::DeserializeOutput) -> Result<(), io::Error> {
        let mut i = x.len as i64;
        if x.leaf {
            if i > 0 {
                i -= 1;
                let mut k_i;
                check!(self.read_key(x.entries[i as usize]), k_i);
                while i >= 0 && k < &k_i {
                    x.entries[i as usize + 1] = x.entries[i as usize];
                    i -= 1;
                    if i >= 0 {
                        check!(self.read_key(x.entries[i as usize]), k_i);
                    }
                }
                let entry_loc;
                check!(self.write_entry(k, v), entry_loc);
                x.len += 1;
                x.entries[(i + 1) as usize] = entry_loc;
                check!(self.update_node(x, x_loc));
                Ok(())
            } else {
                let entry_loc;
                check!(self.write_entry(k, v), entry_loc);
                x.entries[0] = entry_loc;
                x.len += 1;
                check!(self.update_node(x, x_loc));
                Ok(())
            }
        } else {
            let mut k_i;
            i -= 1;
            check!(self.read_key(x.entries[i as usize]), k_i);
            while i >= 0 && k < &k_i {
                i -= 1;
                if i >= 0 { check!(self.read_key(x.entries[i as usize]), k_i); }
            }
            i = i + 1;
            let x_child_i;
            check!(self.read_node(x.children[i as usize]), x_child_i);
            if x_child_i.len == NUM_ENTRIES as u64 {
                check!(self.split_child(x, x_loc, i as usize));
                let k_i;
                check!(self.read_key(x.entries[i as usize]), k_i);
                if k > &k_i {
                    i += 1
                }
            }
            let mut c_i;
            check!(self.read_node(x.children[i as usize]), c_i);
            self.insert_nonfull(&mut c_i, x.children[i as usize], k, v)
        }
    }

    pub fn contains_key(&mut self, k: &K::DeserializeOutput) -> Result<bool, io::Error> {
        let root = self.root_location;
        self.contains_key_rec(k, root)
    }

    fn contains_key_rec(&mut self, k: &K::DeserializeOutput, pos: u64) -> Result<bool, io::Error> {
        let x;
        check!(self.read_node(pos), x);
        if x.len == 0 { return Ok(false) }
        let mut k_i: K::DeserializeOutput;
        check!(self.read_key(x.entries[0]), k_i);
        let mut i = 0;
        while i < x.len && k > &k_i {
            i += 1;
            if i < x.len {
                check!(self.read_key(x.entries[i as usize]), k_i);
            }
        }
        if i < x.len && k == &k_i {
            Ok(true)
        } else if x.leaf {
            Ok(false)
        } else {
            self.contains_key_rec(k, x.children[i as usize])
        }
    }

    pub fn search(&mut self, k: &K::DeserializeOutput) -> Result<Option<V::DeserializeOutput>, io::Error> {
        let root = self.root_location;
        self.search_rec(k, root)
    }

    fn search_rec(&mut self, k: &K::DeserializeOutput, pos: u64) -> Result<Option<V::DeserializeOutput>, io::Error> {
        let x;
        check!(self.read_node(pos), x);
        if x.len == 0 { return Ok(None); }
        let mut k_i: K::DeserializeOutput;
        check!(self.read_key(x.entries[0]), k_i);
        let mut i = 0;

        while i < x.len && k > &k_i {
            i += 1;
            if i < x.len {
                check!(self.read_key(x.entries[i as usize]), k_i);
            }
        }
        if i < x.len && k == &k_i {
            let ret;
            check!(self.read_value(x.entries[i as usize]), ret);
            Ok(Some(ret))
        } else if x.leaf {
            Ok(None)
        } else {
            self.search_rec(k, x.children[i as usize])
        }
    }

    fn write_entry(&mut self, k: &K::DeserializeOutput, v: &V::DeserializeOutput) -> Result<u64, io::Error> {
        let key_pos;
        let val_pos;
        check!(self.write_key(k), key_pos);
        check!(self.write_val(v), val_pos);

        let entry = Entry { key: key_pos, value: val_pos };

        check!(self.treefile.seek(SeekFrom::End(0)));
        let pos;
        check!(self.treefile.current_pos(), pos);
        check!(entry.serialize(&mut self.treefile));
        Ok(pos)
    }

    fn write_key(&mut self, k: &K::DeserializeOutput) -> Result<u64, io::Error> {
        check!(self.keyfile.seek(SeekFrom::End(0)));
        let pos;
        check!(self.keyfile.current_pos(), pos);
        check!(k.serialize(&mut self.keyfile));
        Ok(pos)
    }

    fn write_val(&mut self, v: &V::DeserializeOutput) -> Result<u64, io::Error> {
        check!(self.valfile.seek(SeekFrom::End(0)));
        let pos;
        check!(self.valfile.current_pos(), pos);
        check!(v.serialize(&mut self.valfile));
        Ok(pos)
    }


    fn write_node(&mut self, node: &Node) -> Result<u64, io::Error> {
        check!(self.treefile.seek(SeekFrom::End(0)));
        let pos;
        check!(self.treefile.current_pos(), pos);
        check!(node.serialize(&mut self.treefile));
        Ok(pos)
    }

    fn update_node(&mut self, node: &Node, pos: u64) -> Result<(), io::Error> {
        check!(self.treefile.seek(SeekFrom::Start(pos)));
        check!(node.serialize(&mut self.treefile));
        Ok(())
    }

    fn read_entry(&mut self, pos: u64) -> Result<Entry, io::Error> {
        check!(self.treefile.seek(SeekFrom::Start(pos)));
        Entry::deserialize(&mut self.treefile)
    }

    fn root(&mut self) -> Result<Node, io::Error> {
        let root = self.root_location;
        self.read_node(root)
    }

    fn read_node(&mut self, pos: u64) -> Result<Node, io::Error> {
        check!(self.treefile.seek(SeekFrom::Start(pos)));
        Node::deserialize(&mut self.treefile)
    }
    fn read_value(&mut self, pos: u64) -> Result<V::DeserializeOutput, io::Error> {
        let entry;
        check!(self.read_entry(pos), entry);
        check!(self.valfile.seek(SeekFrom::Start(entry.value)));
        V::deserialize(&mut self.valfile)
    }
    fn read_key(&mut self, pos: u64) -> Result<K::DeserializeOutput, io::Error> {
        let entry;
        check!(self.read_entry(pos), entry);
        check!(self.keyfile.seek(SeekFrom::Start(entry.key)));
        K::deserialize(&mut self.keyfile)
    }
}
