/// External dependencies
#[macro_use]
extern crate lazy_static;
extern crate cfile_rs;
extern crate random_access_file;

extern crate select;
use select::document::Document;
use select::predicate::{Predicate, Attr, Class, Name};

extern crate hyper;
extern crate hyper_native_tls;
use hyper::client;
use hyper::client::*;
use hyper::net::HttpsConnector;
use hyper_native_tls::NativeTlsClient;

extern crate regex;
use regex::Regex;

extern crate iron;
use iron::prelude::*;
use iron::headers::ContentType;
use iron::mime::{ Mime, SubLevel, TopLevel };
use iron::status::Status;

extern crate params;
use params::{ Params, Value };

extern crate router;
use router::Router;


/// Local imports
#[macro_use]
mod check;

#[macro_use]
mod log;

mod persistable_hash;

mod priority_queue;
use priority_queue::PriorityQueue;

mod frequency_table;

mod page;
use page::Page;

mod ptree;
use ptree::PTree;

mod edge;

mod node;

mod graph;
use graph::AssociatedGraph;

use std::io;
use std::io::{ Read, Write, SeekFrom };
use std::collections::{ HashSet, BTreeSet, HashMap };
use std::collections::vec_deque::VecDeque;
use std::fs;
use std::rc::Rc;
use std::cell::RefCell;
use std::env;
use std::sync::{ Arc, Mutex, RwLock };
use std::thread;
use std::time::{ SystemTime, Duration };

/// The page to begin from.
const SEED_PAGE: &'static str = "/wiki/Astronomy";

/// The number of pages to be loaded and stuck into the graph.
const NUM_PAGES: i32 = 50000;

const NUM_THREADS: i32 = 32;

fn load_pages(n: i32) {
    log!("Info", "Creating cache...");
    let mut cache;
    match PTree::<String, Page>::open("data/cache") {
        Ok(x) => cache = Arc::new(RwLock::new(x)),
        Err(e) => {
            error!("Failed to open cache, {}", e);
            return;
        }
    };
    log!("Info", "Creating cache-set...");
    let mut visited;
    match PTree::<String, u8>::new("data/temp_visited") {
        Ok(x) => visited = Arc::new(RwLock::new(x)),
        Err(e) => {
            error!("Failed to create visited PTree, {}", e);
            return;
        }
    }
    let mut links;
    match PTree::<u64, String>::new("data/temp_link") {
        Ok(x) => links = Arc::new(RwLock::new(x)),
        Err(e) => {
            error!("Failed to create temp_link PTree, {}", e);
            return;
        }
    }

    let WIKIPEDIA_DOMAIN: String = "https://en.wikipedia.org".to_string();
    // Number of pages downloaded
    let mut dld_pages = Arc::new(RwLock::new(0));
    // index in links to find the next url
    let mut set_ind = Arc::new(RwLock::new(0));
    // the number of urls total, if set_ind >= set_max then there are no more urls
    let mut set_max = Arc::new(RwLock::new(0));
    // a vector containing all of the handles to all of the threads
    let mut handles = vec![];

    let ssl = NativeTlsClient::new().unwrap();
    let connector = HttpsConnector::new(ssl);
    let client = Arc::new(Client::with_connector(connector));
    let done = Arc::new(Mutex::new(false));
    let count = Arc::new(RwLock::new(0));

    for i in 0..NUM_THREADS {
        let client = client.clone();
        let visited = visited.clone();
        let links = links.clone();
        let cache = cache.clone();
        let set_ind = set_ind.clone();
        let set_max = set_max.clone();
        let dld_pages = dld_pages.clone();
        let count = count.clone();
        let mut page_url = SEED_PAGE.to_string();
        let WIKIPEDIA_DOMAIN = WIKIPEDIA_DOMAIN.to_string();
        handles.push(thread::spawn(move || {
            thread::sleep_ms(100);
            let new_page = || -> Option<String> {
                thread::sleep_ms(5);
                let mut ret = String::new();
                loop {
                    match links.write().unwrap().search(&*set_ind.read().unwrap()) {
                        Ok(Some(e)) => ret = e,
                        Ok(None) => {
                            if *dld_pages.read().unwrap() >= n { return None }
                            continue
                        }
                        Err(e) => {
                            error!("Failed to read from links tree, encountered error \"{}\"", e);
                            return None
                        }
                    }
                    let result = {
                        visited.write().unwrap().contains_key(&ret)
                    };
                    if let Ok(x) = result {
                        *set_ind.write().unwrap() += 1;
                        if x { continue }
                        else { return Some(ret) }
                    } else if let Err(e) = result {
                        error!("Failed to read from visited tree, error \"{}\"", e);
                        return None
                    }
                }
                return Some(ret)
            };

            if i != 0 {  *set_ind.write().unwrap() += 1; page_url = new_page().unwrap(); }

            while *dld_pages.read().unwrap() < n + 1 {
                let result = {
                    let mut cache = cache.write().unwrap();
                    cache.search(&page_url)
                };
                match result {
                    Ok(Some(x)) => {
                        for link in x.links.into_iter() {
                            let result = {
                                visited.write().unwrap().contains_key(&link)
                            };
                            match result {
                                Ok(x) => if x { continue },
                                Err(e) => {
                                    error!("Error reading from visited tree, \"{}\"", e);
                                    continue;
                                }
                            }
                            let mut x = set_max.write().unwrap();
                            *x += 1;
                            links.write().unwrap().insert(&*x, &link);
                        }
                        visited.write().unwrap().insert(&page_url, &0);
                        //print!("@");
                        *count.write().unwrap() += 1;
                        let result = new_page();
                        if let Some(x) = new_page() {
                            page_url = x;
                        } else {
                            break;
                        }
                    },
                    result @ _ => {
                        if let Err(e) = result {
                            error!("Encountered error \"{}\" while reading from cache.", e);
                        }
                        let mut resp = client.get(&(WIKIPEDIA_DOMAIN.clone() + page_url.as_ref())).send().unwrap();
                        let mut html = String::new();
                        resp.read_to_string(&mut html);
                        *dld_pages.write().unwrap() += 1;
                        let mut page = Page::new(page_url.as_ref(), html.as_ref());
                        cache.write().unwrap().insert(&page_url, &page);
                        for link in page.links.into_iter() {
                            match visited.write().unwrap().contains_key(&link) {
                                Ok(x) => if x { continue },
                                Err(e) => {
                                    error!("Error reading from visited tree, \"{}\"", e);
                                    continue;
                                }
                            }
                            let mut x = set_max.write().unwrap();
                            *x += 1;
                            links.write().unwrap().insert(&*x, &link);
                        }
                        //print!("#");
                        visited.write().unwrap().insert(&page_url, &0);
                        *count.write().unwrap() += 1;
                    },
                }
            }
        }));
    }

    let handle = {
        let done = done.clone();
        let count = count.clone();
        thread::spawn(move || {
            let mut time = SystemTime::now();
            let mut i = 0;
            let mut last_count = 0;
            loop {
                if *done.lock().unwrap() { break }
                thread::sleep_ms(500);
                i += 1;
                match time.elapsed() {
                    Ok(elapsed) => {
                        let t = (elapsed.subsec_nanos() / 1000000) as u64;
                        let read = *count.read().unwrap();
                        print!("{}[2K\r\t<{} Pages / sec> <Page {}>", std::char::from_u32(27).unwrap(), (read - last_count) as f64 / (t as f64 / 1000.0), read);
                        io::stdout().flush().unwrap();
                        for i in 0..(i % 3) { print!(".") }
                        last_count = read;
                        //*count.write().unwrap() = 0;
                        time = SystemTime::now();
                    },
                    Err(e) => {}
                }
            }
        })

    };
    for x in handles.into_iter() {
        x.join();
    }
    *done.lock().unwrap() = true;
    //handle.join();
}

fn create_graph(n: i32) {
    let ssl = NativeTlsClient::new().unwrap();
    let connector = HttpsConnector::new(ssl);
    let client = Client::with_connector(connector);
    let mut cache: PTree<String, Page> = match PTree::<String, Page>::open("data/cache") {
        Ok(x) => x,
        Err(e) => {
            error!("Failed to open cache, attempting to create a new one. {}", e);
            match PTree::<String, Page>::new("data/cache") {
                Ok(x) => x,
                Err(e) => {
                    error!("FATAL: Failed to create new cache! Encountered error \"{}\"", e);
                    panic!("");
                }
            }
        }
    };

    let WIKIPEDIA_DOMAIN: String = "https://en.wikipedia.org".to_string();
    let mut graph = AssociatedGraph::new();
    let mut page_count = 1;
    let mut page_url = SEED_PAGE.to_string();
    let mut links = VecDeque::new();
    let mut pages = HashMap::new();

    while page_count < n + 1 {
        page_count += 1;
        match cache.search(&page_url) {
            Ok(Some(x)) => {
                let page = x;
                log!("Info", "Found page \"{}\" in cache", page_url);
                links.append(&mut page.links.iter().map(|x| x.clone()).collect());
                pages.insert(page_url.clone(), page);
                loop {
                    page_url = links.pop_front().unwrap();
                    if pages.contains_key(&page_url) {
                        continue
                    } else {
                        break
                    }
                }
            },
            result @ _ => {
                if let Err(e) = result {
                    error!("Encountered error \"{}\" while reading from cache.", e);
                }
                log!("Info", "Requesting page '{}{}'", WIKIPEDIA_DOMAIN, page_url);
                let mut resp = client.get(&(WIKIPEDIA_DOMAIN.clone() + page_url.as_ref())).send().unwrap();
                let mut html = String::new();
                resp.read_to_string(&mut html);

                let mut page = Page::new(page_url.as_ref(), html.as_ref());

                links.append(&mut page.links.iter().map(|x| x.clone()).collect());
                match cache.insert(&page_url, &page) {
                    Ok(()) => {},
                    Err(e) => error!("Failed to cache page, encountered error \"{}\"", e)
                }
                pages.insert(page_url.clone(), page);
                loop {
                    page_url = links.pop_front().unwrap();
                    if pages.contains_key(&page_url) {
                        continue;
                    } else {
                        break;
                    }
                }
            },
        }
    }

    for page in pages.values() {
        graph.add(page.url.clone());
        for link in page.links.iter() {
            if pages.contains_key(link) {
                graph.add(link.to_string());
                graph.connect(&page.url, link, 1.1 - page.word_freq.compare(&pages[link].word_freq));
            }
        }
    }
    match graph.persist("data/pers") {
        Ok(()) => log!("Log", "Created persistant graph."),
        Err(e) => {
            error!("Failed to persist graph, encountered error \"{}\"", e);
            panic!("");
        }
    }
}

fn load_graph() -> Result<AssociatedGraph<String>, io::Error> {
    match AssociatedGraph::<String>::from_disk("data/pers") {
        Ok(graph) => return Ok(graph),
        Err(e)    => error!("Failed to load persisted graph, attempting to create new graph.")
    }
    create_graph(NUM_PAGES);
    match AssociatedGraph::<String>::from_disk("data/pers") {
        Ok(graph) => return Ok(graph),
        Err(e)    => error!("FATAL: Failed to load persisted graph even after creation. Encountered error \"{}\"", e)
    }
    panic!("");
}

fn server() {
    log!("Log", "Attempting to load graph");
    let graph = match load_graph() {
        Ok(graph) => graph,
        Err(e) => {
            error!("FATAL: Failed to load graph, encountered error \"{}\"", e);
            panic!("");
        }
    };
    log!("Log", "Successfully loaded graph with {} nodes", graph.len());

    let mut list1 = "        <select id=\"list1\">\n".to_string();
    let mut list2 = "        <select id=\"list2\">\n".to_string();

    let option = "            <option value=\"REPLACE_ME\">REPLACE_ME</option>\n";
    for key in graph.keys() {
        let option = option.replace("REPLACE_ME", key.as_ref());
        list1 += option.as_ref();
        list2 += option.as_ref();
    }

    list1 += "        </select>";
    list2 += "        </select>";

    let page_string = include_str!("../html/index.html")
        .to_string()
        .replace("/* % List1 % */", list1.as_str())
        .replace("/* % List2 % */", list2.as_str());
    let page = Arc::new(RwLock::new(page_string));
    let whole_page = Arc::new(RwLock::new(include_str!("../html/graph.html").to_string()));
    let script = Arc::new(RwLock::new(include_str!("../js/script.js").to_string()));
    let whole_script = Arc::new(RwLock::new(include_str!("../js/whole_graph_script.js").to_string()));
    let graph = Arc::new(RwLock::new(graph));
    let graph_clone = graph.clone();

    let mut router = Router::new();
    router.get("/", move |r: &mut iron::Request| {
        log!("Server", "serving / ...");
        let mut resp = iron::Response::with((Status::Ok, page.read().unwrap().clone()));
        resp.headers.set(ContentType(Mime(TopLevel::Text, SubLevel::Html, vec![])));
        Ok(resp)
    }, "index");
    router.get("/graph", move |r: &mut iron::Request| {
        log!("Server", "serving /graph ...");
        let mut resp = iron::Response::with((Status::Ok, whole_page.read().unwrap().clone()));
        resp.headers.set(ContentType(Mime(TopLevel::Text, SubLevel::Html, vec![])));
        Ok(resp)
    }, "graph");
    router.get("/script.js", move |r: &mut iron::Request| {
        log!("Server", "serving /script.js ...");
        let mut resp = iron::Response::with((Status::Ok, script.read().unwrap().clone()));
        resp.headers.set(ContentType(Mime(TopLevel::Text, SubLevel::Javascript, vec![])));
        Ok(resp)
    }, "script.js");
    router.get("/whole_graph_script.js", move |r: &mut iron::Request| {
        log!("Server", "serving /script.js ...");
        let mut resp = iron::Response::with((Status::Ok, whole_script.read().unwrap().clone()));
        resp.headers.set(ContentType(Mime(TopLevel::Text, SubLevel::Javascript, vec![])));
        Ok(resp)
    }, "whole_script.js");
    router.post("/network", move |r: &mut iron::Request| {
        log!("Server", "serving /network ...");
        let map = r.get_ref::<Params>().unwrap();

        let path = graph_clone.read().unwrap().keys();
        let mut json_nodes = "[\n".to_string();
        json_nodes.reserve(path.len() * 256);
        let mut json_edges = "[\n".to_string();
        json_edges.reserve(path.len() * 256);

        let num_nodes = path.len();
        let mut cid = 0;
        for node in path {
            let id = graph_clone.read().unwrap().get_id(&node).unwrap();
            json_nodes += format!("    {{ \"id\": {}, \"label\": \"{}\", \"cid\": {}, \"group\": {}}}\n,",
                               id,
                               node.as_ref(),
                               cid,
                               10 * cid / num_nodes).as_ref();
            for connection in graph_clone.read().unwrap().connections(&node).unwrap() {
                let to = graph_clone.read().unwrap().get_id(&connection).unwrap();
                json_edges += format!("    {{ \"from\": {}, \"to\": {}, \"arrows\": \"to\" }}\n,", id, to).as_ref();
            }
            cid += 1;
        }
        let _ = json_nodes.pop();
        let _ = json_edges.pop();
        json_nodes += "]";
        json_edges += "]";

        let final_json = format!("{{  \"nodes\": {},\n  \"edges\": {} \n}}", json_nodes, json_edges);
        log!("Debug", "FINAL JSON LEN {}", final_json.len());
        let mut resp = iron::Response::with((Status::Ok, final_json));
        resp.headers.set(ContentType(Mime(TopLevel::Text, SubLevel::Json, vec![])));
        Ok(resp)
    }, "network");
    router.post("/path", move |r: &mut iron::Request| {
        log!("Server", "serving /path ...");
        let map = r.get_ref::<Params>().unwrap();

        let to;
        let from;

        match map.find(&["to"]) {
            Some(&Value::String(ref t)) => to = t.clone(),
            _ => return Ok(iron::Response::with(iron::status::NotFound))
        }
        match map.find(&["from"]) {
            Some(&Value::String(ref f)) => from = f.clone(),
            _ => return Ok(iron::Response::with(iron::status::NotFound))
        }

        let path;
        {
            let mut graph = graph.write().unwrap();
            let p = graph.shortest_path(&to, &from);
            if p.is_none() { return Ok(iron::Response::with(iron::status::NotFound)) }
            path = p.unwrap();
        }
        let mut json_nodes = "[\n".to_string();
        json_nodes.reserve(2048);
        let mut json_edges = "[\n".to_string();
        json_edges.reserve(2048);

        let mut y = 0;
        let mut x = 0;
        let mut last_id = 0x12345678;
        for node in path {
            let id = graph.read().unwrap().get_id(&node).unwrap();
            json_nodes += format!("    {{ \"id\": {}, \"label\": \"{}\", \"x\": {}, \"y\": {} }}\n,",
                               graph.read().unwrap().get_id(&node).unwrap(),
                               node.as_ref(),
                               x,
                               y).as_ref();
            if last_id != 0x12345678 {
                json_edges += format!("    {{ \"from\": {}, \"to\": {}, \"arrows\": \"to\" }}\n,", last_id, id).as_ref();
            }
            last_id = id;
            x += 250;
            y -= 100;
        }
        let _ = json_nodes.pop();
        let _ = json_edges.pop();
        json_nodes += "]";
        json_edges += "]";

        let final_json = format!("{{  \"nodes\": {},\n  \"edges\": {} \n}}", json_nodes, json_edges);
        log!("Debug", "{}", final_json);
        let mut resp = iron::Response::with((Status::Ok, final_json));
        resp.headers.set(ContentType(Mime(TopLevel::Text, SubLevel::Json, vec![])));
        Ok(resp)
    }, "path");

    Iron::new(router).http("localhost:1243").unwrap();
}

fn clean() {
    if let Err(e) = fs::remove_dir_all("data") {
        error!("Failed to remove data directory, encountered error \"{}\"", e);
    }
    if let Err(e) = fs::create_dir("data") {
        error!("Failed to create data directory, encountered error \"{}\"", e);
    }
}

fn test() {
    let mut graph = load_graph().unwrap();
    {
        let st = graph.shortest_path_tree(&"/wiki/Objective-C".to_string()).unwrap();
        if !st.spans() {
            error!("Does not scpan :(");
            return;
        }
    }
}

fn main() {
    //test();
    let mut args = env::args();
    let mut should_clean: bool = false;
    let mut should_serve: bool = false;
    let mut should_create: bool = false;
    let mut should_load: bool = false;
    let mut n = 10i32;
    for arg in args {
        if arg.as_str() == "clean" {
            should_clean = true;
        } else if arg.as_str() == "serve" || arg.as_str() == "server" {
            should_serve = true;
        } else if arg.as_str() == "create" {
            should_create = true;
        } else if arg.as_str() == "load" {
            should_load = true;
        } else if let Ok(x) = arg.parse::<i32>() {
            n = x;
        }
    }
    if should_clean {
        clean();
    }
    if should_create {
        create_graph(n);
    }
    if should_load {
        load_pages(n);
    }
    if should_serve {
        server();
    }
}
