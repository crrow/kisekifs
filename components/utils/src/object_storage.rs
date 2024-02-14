pub type ObjectStorage = opendal::Operator;
pub type LocalStorage = opendal::Operator;

pub fn new_mem_object_storage(root: &str) -> ObjectStorage {
    let mut builder = opendal::services::Memory::default();
    builder.root(root);
    opendal::Operator::new(builder).unwrap().finish()
}
