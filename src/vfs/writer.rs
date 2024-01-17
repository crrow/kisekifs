use crate::meta::Ino;

#[derive(Debug, Default)]
pub(crate) struct DataWriter {}

impl DataWriter {
    pub fn get_length(&self, ino: Ino) -> u64 {
        todo!()
    }
}
