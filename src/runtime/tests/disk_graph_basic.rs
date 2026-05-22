#![cfg(feature = "disk_graph")]

use linkml_runtime::rdf_import_store_disk::DiskRdfImportStore;
use tempfile::tempdir;

#[test]
fn opens_empty_store() {
    let dir = tempdir().unwrap();
    let _store = DiskRdfImportStore::open_empty(dir.path()).expect("open");
}

#[test]
fn reopens_existing_path_clean() {
    let dir = tempdir().unwrap();
    {
        let _store = DiskRdfImportStore::open_empty(dir.path()).expect("first open");
    }
    let _store = DiskRdfImportStore::open_empty(dir.path()).expect("second open (clean)");
}

const TINY_NT: &str = "\
<http://example.org/a> <http://example.org/p> <http://example.org/b> .
<http://example.org/a> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://example.org/T> .
<http://example.org/b> <http://example.org/q> \"hello\" .
";

#[test]
fn loads_tiny_ntriples() {
    let dir = tempdir().unwrap();
    let store =
        DiskRdfImportStore::from_ntriples(std::io::Cursor::new(TINY_NT), dir.path()).unwrap();
    assert_eq!(store.triple_count(), 3);
}

use linkml_runtime::triple_source::TripleSource;
use oxrdf::{NamedNode, NamedOrBlankNode};

#[test]
fn triples_for_subject_finds_all() {
    let dir = tempdir().unwrap();
    let store =
        DiskRdfImportStore::from_ntriples(std::io::Cursor::new(TINY_NT), dir.path()).unwrap();
    let s = NamedOrBlankNode::NamedNode(NamedNode::new_unchecked("http://example.org/a"));
    let triples: Vec<_> = store.triples_for_subject(&s).collect();
    assert_eq!(triples.len(), 2);
}

#[test]
fn subjects_for_predicate_object_finds_typed() {
    let dir = tempdir().unwrap();
    let store =
        DiskRdfImportStore::from_ntriples(std::io::Cursor::new(TINY_NT), dir.path()).unwrap();
    let p = NamedNode::new_unchecked("http://www.w3.org/1999/02/22-rdf-syntax-ns#type");
    let o = NamedNode::new_unchecked("http://example.org/T");
    let subs: Vec<_> = store.subjects_for_predicate_object(&p, &o).collect();
    assert_eq!(subs.len(), 1);
    assert_eq!(subs[0].to_string(), "<http://example.org/a>");
}

#[test]
fn objects_for_subject_predicate_finds_object() {
    let dir = tempdir().unwrap();
    let store =
        DiskRdfImportStore::from_ntriples(std::io::Cursor::new(TINY_NT), dir.path()).unwrap();
    let s = NamedOrBlankNode::NamedNode(NamedNode::new_unchecked("http://example.org/a"));
    let p = NamedNode::new_unchecked("http://example.org/p");
    let objs: Vec<_> = store.objects_for_subject_predicate(&s, &p).collect();
    assert_eq!(objs.len(), 1);
    assert_eq!(objs[0].to_string(), "<http://example.org/b>");
}

#[test]
fn len_matches_triple_count() {
    let dir = tempdir().unwrap();
    let store =
        DiskRdfImportStore::from_ntriples(std::io::Cursor::new(TINY_NT), dir.path()).unwrap();
    assert_eq!(store.len(), Some(3));
    assert!(!store.is_empty());
}

#[test]
fn lookup_of_unknown_term_returns_empty() {
    let dir = tempdir().unwrap();
    let store =
        DiskRdfImportStore::from_ntriples(std::io::Cursor::new(TINY_NT), dir.path()).unwrap();
    let s = NamedOrBlankNode::NamedNode(NamedNode::new_unchecked(
        "http://example.org/does-not-exist",
    ));
    let n: usize = store.triples_for_subject(&s).count();
    assert_eq!(n, 0);
}

const TINY_TTL: &str = "\
@prefix : <http://example.org/> .
:a :p :b ;
   a :T .
:b :q \"hello\" .
";

#[test]
fn loads_tiny_turtle() {
    let dir = tempdir().unwrap();
    let store =
        DiskRdfImportStore::from_turtle(std::io::Cursor::new(TINY_TTL), dir.path()).unwrap();
    assert_eq!(store.triple_count(), 3);
}
