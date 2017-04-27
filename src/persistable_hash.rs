use std::collections::HashMap;
use random_access_file::Serialize;
use std::hash::Hash;
use std::io;
use std::io::{ Read, Write };

pub struct PersistableHashMap<'a, K: 'a + Eq + Hash + Serialize, V: 'a + Serialize>(&'a HashMap<K, V>);

impl<'a, K, V> PersistableHashMap<'a, K, V> where
    K: 'a + Eq + Hash + Serialize,
    V: 'a + Serialize {
    pub fn new(s: &'a HashMap<K, V>) -> Self {
        PersistableHashMap(s)
    }
}

impl<'a, K, V> Serialize for PersistableHashMap<'a, K, V>
    where
    K: Eq + Hash + Serialize,
    V: Serialize,
    K::DeserializeOutput: Eq + Hash + Serialize,
    V::DeserializeOutput: Serialize {
    type DeserializeOutput = HashMap<K::DeserializeOutput, V::DeserializeOutput>;

    fn deserialize(read: &mut Read) -> Result<Self::DeserializeOutput, io::Error> {
        let mut hashmap = HashMap::new();
        let len;
        check!(u64::deserialize(read), len);
        for i in 0..len {
            let k;
            check!(K::deserialize(read), k);
            let v;
            check!(V::deserialize(read), v);
            hashmap.insert(k, v);
        }
        Ok(hashmap)
    }

    fn serialize(&self, to: &mut Write) -> Result<(), io::Error> {
        check!((self.0.len() as u64).serialize(to));
        for (k, v) in self.0.iter() {
            check!(k.serialize(to));
            check!(v.serialize(to));
        }
        Ok(())
    }

    fn serialized_len(&self) -> u64 {
        let mut sum = 8;
        for (k, v) in self.0.iter() {
            sum += k.serialized_len() + v.serialized_len();
        }
        sum
    }
}
