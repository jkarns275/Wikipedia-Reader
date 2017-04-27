use hyper::client;
use hyper::client::*;
use hyper::net::HttpsConnector;
use hyper_native_tls::NativeTlsClient;
use select::document::Document;
use select::predicate::{Predicate, Attr, Class, Name};
use regex::Regex;
use lazy_static;
use frequency_table::FrequencyTable;
use persistable_hash::PersistableHashMap;
use std::io;
use random_access_file::Serialize;
use std::io::{ Write, Read };

pub struct Page {
    pub url: String,
    pub word_freq: FrequencyTable,
    pub links: Vec<String>
}

lazy_static! {
    static ref LINK_REGEX: Regex = Regex::new(r#"^/wiki/[a-zA-Z0-9_\-+\(\)]+$"#).unwrap();
}

impl Page {
    pub fn new(url: &str, html: &str) -> Page {
        let re = Regex::new(r#"^/wiki/[a-zA-Z0-9_\-+\(\)]+$"#).unwrap();
        let document = Document::from(html);
        let mut corpus = String::new();
        let mut links = vec![];
        let space = " ".to_string();
        for node in document.find(Name("p")) {
            corpus += node.text().as_str();
            corpus.push(' ');
        }
        for node in document.find(Name("a")) {
            if let Some(link) = node.attr("href") {
                for cap in re.captures_iter(link) {
                    links.push(cap[0].to_string());
                }
            }
        }
        let word_freq = FrequencyTable::new_from_str(corpus.as_ref());
        Page {
            url: url.to_string(),
            links: links,
            word_freq: word_freq
        }
    }
}

impl Serialize for Page {
    type DeserializeOutput = Page;
    fn deserialize(from: &mut Read) -> Result<Self, io::Error> {
        let url;
        check!(String::deserialize(from), url);
        let word_freq;
        check!(PersistableHashMap::<String, usize>::deserialize(from), word_freq);
        let len;
        check!(u64::deserialize(from), len);
        let mut v = Vec::with_capacity(len as usize);
        for i in 0..len {
            let s;
            check!(String::deserialize(from), s);
            v.push(s);
        }
        Ok(Page {
            url: url,
            word_freq: FrequencyTable(word_freq),
            links: v
        })
    }
    fn serialize(&self, to: &mut Write) -> Result<(), io::Error> {
        check!(self.url.serialize(to));
        check!(PersistableHashMap::new(&self.word_freq.0).serialize(to));
        check!(self.links.len().serialize(to));
        for i in self.links.iter() {
            check!(i.serialize(to));
        }
        Ok(())
    }
    fn serialized_len(&self) -> u64 {
        let mut len = 8 + 8 + PersistableHashMap::new(&self.word_freq.0).serialized_len() + 8;
        for x in self.links.iter() {
            len += x.serialized_len();
        }
        len
    }
}
