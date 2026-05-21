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
