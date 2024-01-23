use crate::chunk::config::ChunkConfig;
use crate::chunk::err::Result;
use crate::chunk::slice::{RSlice, SliceID};
use crate::chunk::WSlice;
use std::sync::Arc;

#[derive(Debug, Default)]
pub struct ChunkEngineBuilder {
    config: ChunkConfig,
}

impl ChunkEngineBuilder {
    pub fn new() -> Self {
        Self {
            config: ChunkConfig::default(),
        }
    }

    pub fn build(self) -> Result<ChunkEngine> {
        todo!()
    }
}

#[derive(Debug)]
pub struct ChunkEngine(Arc<ChunkEngineInner>);

impl ChunkEngine {
    pub fn reader(&self, id: SliceID, length: usize) -> RSlice {
        todo!()
    }

    pub fn new_writer(&self, id: SliceID) -> WSlice {
        todo!()
    }

    pub fn remove(&self, id: SliceID) -> Result<()> {
        todo!()
    }
}

#[derive(Debug)]
struct ChunkEngineInner {
    storage: opendal::Operator,
    config: ChunkConfig,
}
