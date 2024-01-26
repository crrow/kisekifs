pub(crate) trait Cache {
    fn get(&self, key: &str) -> Option<&str>;
    fn set(&self, key: &str, value: &str);
    fn remove(&self, key: &str);
}

struct CacheManager {}

struct MemoryCache {}

struct DiskCache {}
