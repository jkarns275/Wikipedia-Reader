use std::hash::{ Hash, Hasher };
use std::collections::hash_map::DefaultHasher;
use std::io::{ Seek, SeekFrom, Read, Write };
use std::io;
use random_access_file::Serialize;
use cfile_rs::CFile;
use std::marker::PhantomData;

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

struct Entry<K, V>
    where   K: Hash + Eq {
    pub key: K,
    pub value: V,
    pub hash: u64,
    pub next: u64
}

impl<K, V> Entry<K, V>
    where   K: Hash + Eq + Serialize,
            V: Serialize {
    fn new(key: K, value: V, hash: u64, next: u64) -> Entry<K, V> {
        Entry {
            key: key,
            value: value,
            hash: hash,
            next: next
        }
    }

    /// Returns meta data about an entry. To be specific, the pointer to the next entry, it's hash,
    /// and the key. They key isn't exactly meta data but you're going to have to deal with it.
    fn meta_data(from: &mut CFile) -> Result<(u64, u64, K::DeserializeOutput), io::Error> {
        let next;
        check!(u64::deserialize(from), next);
        check!(from.seek(SeekFrom::Current(8 * 1)));
        let hash;
        check!(u64::deserialize(from), hash);
        let key;
        check!(K::deserialize(from), key);
        Ok((next, hash, key))
    }

    fn next_and_hash(from: &mut CFile) -> Result<(u64, u64), io::Error> {
        let next;
        check!(u64::deserialize(from), next);
        check!(from.seek(SeekFrom::Current(8 * 1)));
        let hash;
        check!(u64::deserialize(from), hash);
        Ok((next, hash))
    }

    fn next(from: &mut CFile) -> Result<u64, io::Error> {
        let next;
        check!(u64::deserialize(from), next);
        Ok(next)
    }

    fn size(from: &mut CFile) -> Result<u64, io::Error> {
        check!(u64::deserialize(from));
        let size;
        check!(u64::deserialize(from), size);
        Ok(size)
    }
}

impl<K, V> Serialize for Entry<K, V>
    where   K: Hash + Eq + Serialize,
            K::DeserializeOutput: Hash + Eq,
            V: Serialize {
    type DeserializeOutput = Entry<K::DeserializeOutput, V::DeserializeOutput>;

    fn serialize(&self, to: &mut Write) -> Result<(), io::Error> {
        check!(self.next.serialize(to));

        check!((self.value.serialized_len() + self.key.serialized_len()).serialize(to));
        check!(self.hash.serialize(to));
        check!(self.key.serialize(to));
        check!(self.value.serialize(to));
        Ok(())
    }
    fn deserialize(from: &mut Read) -> Result<Entry<K::DeserializeOutput, V::DeserializeOutput>, io::Error> {
        let next_res = u64::deserialize(from);

        // size_of res isnt in entry
        let _ = u64::deserialize(from);

        let hash_res = u64::deserialize(from);
        let key_res = K::deserialize(from);
        let val_res = V::deserialize(from);
        if let Err(e)       = next_res  { Err(e) }
        else if let Err(e)  = hash_res  { Err(e) }
        else if let Err(e)  = key_res   { Err(e) }
        else if let Err(e)  = val_res   { Err(e) }
        else                            { Ok(Entry {
            next: next_res.unwrap(),
            key: key_res.unwrap(),
            value: val_res.unwrap(),
            hash: hash_res.unwrap()
        }) }
    }
    fn serialized_len(&self) -> u64 {
        8 + 8 + 8 + self.key.serialized_len() + self.value.serialized_len()
    }
}

/*
                            ********************************
                            * PERSISTANT HASH TABLE LAYOUT *
                            ********************************

manifest file:

    Contains indices to the dat file for each hash. This points to a linked list of sorts,
    where each element is laid out as such:

        64 bits -> An unsigned integer that points to the next element. If it is equal to u64::MAX,
                   there is no next element.
        64 bits -> An unsigned integer that represents (n + m) / 8, or in other words, the number
                   of bytes taken up by the key and value
        64 bits -> The hash of the key.
        n bits  -> The key.
        m bits  -> The value.

    The layout of the manifest file itself:

        64 bits -> An unsigned integer that represents the length of the hashtable.
        64 bits -> An unsigned integer that represents the number of elements in the hashtable.
        length * 64 bits -> An array of indices to the dat file.

dat file:

    Contains all of the binary data contained in the hash table. The first 64 bits is an unsigned
    integer containing a index for this file to the first FREE BLOCK of memory. That block of
    memory has the same layout as a linked list element, except the n + m bits can be written over,
    and the net element is the next free block. So:

        64 bits -> Pointer to the linked list of free memory. This value will be equal to NO_ELEMENT
                    if there is no free memory, meaning you have to write to the end of the file.
        n bits  -> All the binary data.
*/

pub struct PHash<K, V> where
    K: Hash + Serialize + Eq, V: Serialize,
    K::DeserializeOutput: Hash + Eq {
    manifest: CFile,
    dat: CFile,
    len: usize,
    pub count: usize,
    phantom_k: PhantomData<K>,
    phantom_v: PhantomData<V>
}

use std::u64;

static NO_ELEMENT: u64 = u64::MAX;
static DEFAULT_TABLE_SIZE: u64 = 16u64;
static MANIFEST_OFFSET: u64 = 16u64;

impl<K, V> PHash<K, V> where
    K: Hash + Serialize + Eq, V: Serialize,
    K::DeserializeOutput: Hash + Eq {

    /// Creates a new PHash.
    /// If a PHash with the same name has already been created, it will be overwritten, or this
    /// function will return Err
    pub fn new(path: &str) -> Result<PHash<K, V>, io::Error> {
        let mut manifest;
        check!(CFile::open(&(path.to_string() + ".manifest"), "w+"), manifest);
        let mut dat;
        check!(CFile::open(&(path.to_string() + ".dat"), "w+"), dat);

        check!(NO_ELEMENT.serialize(&mut dat));

        check!(DEFAULT_TABLE_SIZE.serialize(&mut manifest));

        check!(0u64.serialize(&mut manifest));

        for _ in 0..DEFAULT_TABLE_SIZE {
            check!(NO_ELEMENT.serialize(&mut manifest))
        }

        Ok(PHash {
            manifest: manifest,
            dat: dat,
            count: 0,
            len: 16,
            phantom_k: PhantomData {},
            phantom_v: PhantomData {}
        })
    }

    pub fn open(path: &str) -> Result<PHash<K, V>, io::Error> {
        let mut manifest;
        check!(CFile::open(&(path.to_string() + ".manifest"), "r+"), manifest);
        let dat;
        check!(CFile::open(&(path.to_string() + ".dat"), "r+"), dat);

        check!(manifest.seek(SeekFrom::Start(0)));
        let len;
        check!(u64::deserialize(&mut manifest), len);
        let count;
        check!(u64::deserialize(&mut manifest), count);

        Ok(PHash {
            manifest: manifest,
            dat: dat,
            count: count as usize,
            len: len as usize,
            phantom_k: PhantomData {},
            phantom_v: PhantomData {}
        })

    }

    fn set_next(&mut self, set_next: u64, to_this: u64) -> Result<(), io::Error> {
        let _ = self.dat.seek(SeekFrom::Start(set_next));
        match to_this.serialize(&mut self.dat) {
            Ok(()) => Ok(()),
            Err(e) => Err(e)
        }
    }

    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    fn increment_count(&mut self) -> Result<(), io::Error> {
        let _ = self.manifest.seek(SeekFrom::Start(8));
        self.count += 1;
        match (self.count as u64).serialize(&mut self.manifest) {
            Ok(_) => Ok(()),
            Err(e) => Err(e)
        }
    }

    fn double_len(&mut self) -> Result<(), io::Error> {
        let _ = self.manifest.seek(SeekFrom::Start(0));
        self.len *= 2;
        check!(self.len.serialize(&mut self.manifest));
        Ok(())
    }

    fn clear_buckets(&mut self) -> Result<(), io::Error> {
        check!(self.manifest.seek(SeekFrom::Start(MANIFEST_OFFSET)));
        for _ in 0..self.len {
            check!(NO_ELEMENT.serialize(&mut self.manifest));
        }
        Ok(())
    }

    fn free_space(&mut self) -> Result<u64, io::Error> {
        check!(self.dat.seek(SeekFrom::Start(0)));
        let x;
        check!(u64::deserialize(&mut self.dat), x);
        Ok(x)
    }

    fn add_free_space(&mut self, ptr: u64) -> Result<(), io::Error> {
        match self.free_space() {
            Ok(head_ptr) => {
                if head_ptr == NO_ELEMENT {
                    let _ = self.dat.seek(SeekFrom::Start(0));
                    let _ = ptr.serialize(&mut self.dat);
                    if let Err(e) = self.set_next(ptr, NO_ELEMENT) {
                        return Err(e)
                    }
                    Ok(())
                } else {
                    if let Err(e) = self.set_next(ptr, head_ptr) {
                        return Err(e);
                    }
                    let _ = self.dat.seek(SeekFrom::Start(0));
                    match ptr.serialize(&mut self.dat) {
                        Ok(()) => Ok(()),
                        Err(e) => Err(e)
                    }
                }
            },
            Err(e) => Err(e)
        }
    }

    fn bucket_location(&mut self, hash: u64) -> Result<u64, io::Error> {
        let _ = self.manifest.seek(SeekFrom::Start(MANIFEST_OFFSET + 8*(hash & (self.len as u64 - 1))));
        match u64::deserialize(&mut self.manifest) {
            Ok(e) => Ok(e),
            Err(e) => {
                Err(e)
            }
        }
    }

    fn set_bucket_location(&mut self, hash: u64, location: u64) -> Result<(), io::Error> {
        let _ = self.manifest.seek(SeekFrom::Start(MANIFEST_OFFSET + 8*(hash & (self.len as u64 - 1))));
        match location.serialize(&mut self.manifest) {
            Ok(_) => Ok(()),
            Err(e) => Err(e)
        }
    }

    fn last_element(&mut self, mut list: u64) -> Result<u64, io::Error> {
        let mut last = list;
        while list != NO_ELEMENT {
            last = list;
            check!(self.dat.seek(SeekFrom::Start(last)));
            check!(Entry::<i8, i8>::next(&mut self.dat), list);
        }
        Ok(last)
    }

    fn write_entry(&mut self, k: &K, v: &V, hash: u64) -> Result<u64, io::Error> {
        let mut size = v.serialized_len() + k.serialized_len();
        let mut freespace;
        check!(self.free_space(), freespace);
        if freespace == NO_ELEMENT {
            check!(self.dat.seek(SeekFrom::End(0)));
        } else {
            let mut last = NO_ELEMENT;
            while freespace != NO_ELEMENT {
                check!(self.dat.seek(SeekFrom::Start(freespace)));
                let next;
                check!(u64::deserialize(&mut self.dat), next);
                let entry_size;
                check!(u64::deserialize(&mut self.dat), entry_size);
                if entry_size >= size {
                    size = entry_size;
                    if last == NO_ELEMENT {
                        check!(self.dat.seek(SeekFrom::Start(0)));
                        check!(next.serialize(&mut self.dat));
                    } else {
                        check!(self.dat.seek(SeekFrom::Start(last)));
                        check!(next.serialize(&mut self.dat));
                    }
                    check!(self.dat.seek(SeekFrom::Start(freespace)));
                    break;
                } else {
                    freespace = next;
                }
                last = next;
            }
            if freespace == NO_ELEMENT {
                check!(self.dat.seek(SeekFrom::End(0)));
            }
        }
        let loc;
        check!(self.dat.current_pos(), loc);
        check!(NO_ELEMENT.serialize(&mut self.dat));
        check!(size.serialize(&mut self.dat));
        check!(hash.serialize(&mut self.dat));
        check!(k.serialize(&mut self.dat));
        check!(v.serialize(&mut self.dat));
        Ok(loc)
    }

    fn link_all(&mut self) -> Result<u64, io::Error> {
        let mut start = NO_ELEMENT;
        let mut last = NO_ELEMENT;
        for i in 0..self.len as u64 {
            if last == NO_ELEMENT {
                check!(self.bucket_location(i), start);
                if start == NO_ELEMENT { continue }
                check!(self.last_element(start), last);
            } else {
                let cur_bucket;
                check!(self.bucket_location(i), cur_bucket);
                if cur_bucket == NO_ELEMENT { continue }
                check!(self.set_next(last, cur_bucket));
                check!(self.last_element(cur_bucket), last);
            }
        }
        Ok(start)
    }

    /// Will dynamically resize the size of the hash once alpha is > .75 (alpha being
    /// the total number of elements divided by the number of slots).
    pub fn insert(&mut self, key: &K, value: &V) -> Result<(), io::Error> {
        let hash = self.hash(&key);
        match self.bucket_location(hash) {
            Ok(x) => {
                if x == NO_ELEMENT {
                    let result = self.write_entry(key, value, hash);
                    match result {
                        Ok(loc) => {
                            let _ = self.set_bucket_location(hash, loc);
                        },
                        Err(e) => return Err(e)
                    }
                } else {
                    let result = self.write_entry(key, value, hash);
                    let current_head_loc = self.bucket_location(hash);
                    if result.is_err() {
                        return Err(result.err().unwrap())
                    } else if current_head_loc.is_err() {
                        return Err(current_head_loc.err().unwrap())
                    } else {
                        let location = result.unwrap();
                        let r1 = self.set_next(location, current_head_loc.unwrap());
                        let r2 = self.set_bucket_location(hash, location);
                        if r1.is_err() {
                            return Err(r1.err().unwrap())
                        } else if r2.is_err() {
                            return Err(r2.err().unwrap())
                        } else {
                            let _ = self.increment_count();
                        }
                    }
                }
            },
            Err(e) => {
                return Err(e)
            }
        }
        // TODO: Fix this
        if self.count as f32 / self.len as f32 >= 0.75 {
            match self.link_all() {
                Ok(head) => {
                    check!(self.double_len());
                    check!(self.clear_buckets());
                    let mut cur = head;
                    while cur != NO_ELEMENT {
                        let _ = self.dat.seek(SeekFrom::Start(cur));
                        match Entry::<K, V>::next_and_hash(&mut self.dat) {
                            Ok((next, hash)) => {
                                match self.bucket_location(hash) {
                                    Ok(location) => {
                                        let _ = self.dat.seek(SeekFrom::Start(cur));
                                        let _ = location.serialize(&mut self.dat);
                                        let _ = self.set_bucket_location(hash, cur);
                                        cur = next;
                                    },
                                    Err(e) => return Err(e)
                                }
                            },
                            Err(e) => return Err(e)
                        }

                    }
                },
                Err(e) => return Err(e)
            }
        }
        Ok(())
    }

    pub fn remove(&mut self, key: &K::DeserializeOutput) -> Result<bool, io::Error> {
        let hash = self.hash(&key);
        let loc_res = self.bucket_location(hash);
        if let Ok(loc) = loc_res {
            if loc == NO_ELEMENT {
                Ok(false)
            } else {
                // Save the pointer to the current entrie
                let mut cur_pos = loc;
                let mut prev = loc;

                // Move to the current entry
                check!(self.dat.seek(SeekFrom::Start(loc)));

                // loop until we:
                //  1 - Reach the end with no match, return None
                //  2 - Find a match, return Some
                //
                // if neither of those things happen we seek to the next entry and repeat
                loop {
                    let meta = Entry::<K, V>::meta_data(&mut self.dat);
                    match meta {
                        Err(e) => return Err(e),
                        Ok((next, entry_hash, entry_key)) => {
                            if hash == entry_hash && *key == entry_key {
                                if loc == cur_pos {
                                    check!(self.set_bucket_location(hash, NO_ELEMENT));
                                } else {
                                    check!(self.dat.seek(SeekFrom::Start(cur_pos)));
                                    check!(self.set_next(prev, next));
                                }
                                check!(self.add_free_space(cur_pos));
                                return Ok(true)
                            } else if next == NO_ELEMENT {
                                return Ok(false)
                            } else {
                                check!(self.dat.seek(SeekFrom::Start(next)));
                                prev = cur_pos;
                                cur_pos = next;
                            }
                        }
                    };
                }
            }
        } else {
            Ok(false)
        }
    }

    pub fn get(&mut self, key: &K::DeserializeOutput) -> Option<V::DeserializeOutput> {
        let hash = self.hash(&key);
        let loc_res = self.bucket_location(hash);
        if let Ok(loc) = loc_res {
            if loc == NO_ELEMENT {
                None
            } else {
                // Save the pointer to the current entry
                let mut cur_pos = loc;

                // Move to the current entry
                let _ = self.dat.seek(SeekFrom::Start(loc));

                // loop until we:
                //  1 - Reach the end with no match, return None
                //  2 - Find a match, return Some
                //
                // if neither of those things happen we seek to the next entry and repeat
                loop {
                    let meta = Entry::<K, V>::meta_data(&mut self.dat);
                    match meta {
                        Err(_) => return None,
                        Ok((next, entry_hash, entry_key)) => {

                            if hash == entry_hash && *key == entry_key {
                                let _ = self.dat.seek(SeekFrom::Start(cur_pos));
                                match Entry::<K, V>::deserialize(&mut self.dat) {
                                    Ok(entry) => return Some(entry.value),
                                    Err(_)    => return None
                                };
                            } else if next == NO_ELEMENT {
                                return None
                            } else {
                                let _ = self.dat.seek(SeekFrom::Start(next));
                                cur_pos = next;
                            }
                        }
                    };
                }

            }
        } else {
            None
        }
    }

    fn hash<T: Hash>(&self, k: &T) -> u64 {
        let mut hasher = DefaultHasher::new();
        k.hash(&mut hasher);
        hasher.finish()
    }

    pub fn contains_key(&mut self, key: &K::DeserializeOutput) -> Result<bool, Option<io::Error>> {
        let hash = self.hash(&key);
        let loc_res = self.bucket_location(hash);
        if let Ok(loc) = loc_res {
            if loc == NO_ELEMENT {
                Err(None)
            } else {
                // Move to the current entry
                let _ = self.dat.seek(SeekFrom::Start(loc));

                // loop until we:
                //  1 - Reach the end with no match, return false
                //  2 - Find a match, return true
                //
                // if neither of those things happen we seek to the next entry and repeat
                loop {
                    let meta = Entry::<K, V>::meta_data(&mut self.dat);
                    match meta {
                        Err(_) => return Err(None),
                        Ok((next, entry_hash, entry_key)) => {
                            if hash == entry_hash && *key == entry_key {
                                return Ok(true)
                            } else if next == NO_ELEMENT {
                                return Err(None)
                            } else {
                                let _ = self.dat.seek(SeekFrom::Start(next));
                            }
                        }
                    };
                }

            }
        } else {
            Err(None)
        }
    }
}
