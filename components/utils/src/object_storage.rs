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

pub fn new_fs_store(path: &str) -> Result<ObjectStorage, opendal::Error> {
    let temp_dir = format!("{}-temp", path);
    let mut builder = opendal::services::Fs::default();
    builder.root(path);
    builder.atomic_write_dir(&temp_dir); // TODO: review me
    let obj = Operator::new(builder)?.finish();
    Ok(obj)
}
