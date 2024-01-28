use snafu::Snafu;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub(crate) enum StorageError {
    #[snafu(display("unexpected end of file"))]
    EOF,
    #[snafu(display("out of memory: {source}"))]
    OOM {
        source: datafusion_common::DataFusionError,
    },
    #[snafu(display("object storage error: {source}"))]
    ObjectStorageError { source: opendal::Error },
}

pub(crate) type Result<T> = std::result::Result<T, StorageError>;
