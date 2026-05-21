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
