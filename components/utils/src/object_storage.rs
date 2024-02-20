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

pub fn new_minio_store<P: AsRef<Path>>(path: P) -> Result<ObjectStorage, opendal::Error> {
    // Create s3 backend builder.
    let mut builder = opendal::services::S3::default();
    // Set the root for s3, all operations will happen under this root.
    //
    // NOTE: the root must be absolute path.
    builder.root(path.as_ref().to_string_lossy().as_ref());
    // Set the bucket name. This is required.
    builder.bucket("test");
    // Set the region. This is required for some services, if you don't care about it, for example Minio service, just set it to "auto", it will be ignored.
    builder.region("auto");
    // Set the endpoint.
    //
    // For examples:
    // - "https://s3.amazonaws.com"
    // - "http://127.0.0.1:9000"
    // - "https://oss-ap-northeast-1.aliyuncs.com"
    // - "https://cos.ap-seoul.myqcloud.com"
    //
    // Default to "https://s3.amazonaws.com"
    builder.endpoint("http://127.0.0.1:9000");
    // Set the access_key_id and secret_access_key.
    //
    // OpenDAL will try load credential from the env.
    // If credential not set and no valid credential in env, OpenDAL will
    // send request without signing like anonymous user.
    builder.access_key_id("minioadmin");
    builder.secret_access_key("minioadmin");

    let op: Operator = Operator::new(builder)?.finish();
    Ok(op)
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::{Buf, Bytes};
    use kiseki_common::BLOCK_SIZE;

    #[tokio::test]
    async fn s3() {
        let op = new_minio_store("/tmp/kiseki.data").unwrap();
        let key = "test";
        let data = Bytes::from(vec![1u8; BLOCK_SIZE]);
        op.write(key, data).await.unwrap();
        let read = op.read(key).await.unwrap();
        assert_eq!(read.len(), BLOCK_SIZE);

        let buf = bytes::BytesMut::with_capacity(BLOCK_SIZE);
    }
}
