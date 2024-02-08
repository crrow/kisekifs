use snafu::Snafu;

#[derive(Snafu, Debug)]
#[snafu(visibility(pub))]
pub enum Error {}

pub type Result<T> = std::result::Result<T, Error>;
