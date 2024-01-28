use crate::vfs::storage::sto::ObjectSto;

pub(crate) struct MixedCacheSto {
    mem_cache_sto: MemCacheSto,
    disk_cache_sto: DiskCacheSto,
    object_sto: ObjectSto,
}

pub(crate) struct MemCacheSto {
    object_sto: ObjectSto,
}

pub(crate) struct DiskCacheSto {
    object_sto: ObjectSto,
}
