use std::time::Instant;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetaContext {
    pub gid: u32,
    pub gid_list: Vec<u32>,
    pub uid: u32,
    pub pid: u32,
    pub check_permission: bool,
    pub start_at: Instant,
}

impl Default for MetaContext {
    fn default() -> Self {
        Self {
            gid: 0,
            gid_list: vec![],
            uid: 0,
            pid: 0,
            check_permission: false,
            start_at: Instant::now(),
        }
    }
}
