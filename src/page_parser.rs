use hyper::client;
use hyper::client::*;
use hyper::net::HttpsConnector;
use hyper_native_tls::NativeTlsClient;
use select::document::Document;
use select::predicate::{Predicate, Attr, Class, Name};
use regex::Regex;
use lazy_static;

pub struct Page {
    pub corpus: String,
    pub links: Vec<String>
}

lazy_static! {
    static ref LINK_REGEX: Regex = Regex::new(r#"^/wiki/[a-zA-Z0-9_\-+\(\)]+$"#).unwrap();
}

impl Page {
    pub fn new(html: &str, client: Client) -> Page {
        let re = Regex::new(r#"^/wiki/[a-zA-Z0-9_\-+\(\)]+$"#).unwrap();
        let document = Document::from(html);
        let mut corpus = String::new();
        let mut links = vec![];
        for node in document.find(Name("p")) {
            corpus += node.text().as_str();
        }
        for node in document.find(Name("a")) {
            if let Some(link) = node.attr("href") {
                for cap in re.captures_iter(link) {
                    links.push(cap[0].to_string());
                    println!("Link: {}", &cap[0]);
                }
            }
        }
        Page {
            corpus: corpus,
            links: links
        }
    }
}
