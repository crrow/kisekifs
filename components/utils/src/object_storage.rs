use std::path::Path;

use opendal::Operator;

pub type ObjectStorage = Operator;
pub type LocalStorage = Operator;

pub fn new_mem_object_storage(root: &str) -> ObjectStorage {
    let mut builder = opendal::services::Memory::default();
    builder.root(root);
    Operator::new(builder).unwrap().finish()
}

#[cfg(test)]
pub fn new_fs_sto() -> Operator {
    let mut builder = opendal::services::Fs::default();
    let temp = tempfile::Builder::new().prefix("").tempdir().unwrap();
    builder.root(temp.into_path().to_str().unwrap());
    Operator::new(builder).unwrap().finish()
}

pub fn new_fs_store<P: AsRef<Path>>(path: P) -> Result<ObjectStorage, opendal::Error> {
    let path = path.as_ref();
    // let temp_dir = path.to_path_buf().join("temp");
    let mut builder = opendal::services::Fs::default();
    builder.root(path.to_string_lossy().as_ref());
    // builder.atomic_write_dir(&temp_dir.to_str().unwrap()); // TODO: review me
    let obj = Operator::new(builder)?.finish();
    Ok(obj)
}

pub fn new_sled_store<P: AsRef<Path>>(path: P) -> Result<ObjectStorage, opendal::Error> {
    let mut builder = opendal::services::Sled::default();
    let path = path.as_ref();
    builder.datadir(path.to_string_lossy().as_ref());
    let obj = Operator::new(builder)?.finish();
    Ok(obj)
}
