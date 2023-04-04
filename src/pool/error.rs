/// Pool error.
#[derive(thiserror::Error, Debug)]
#[error(transparent)]
pub struct Error(#[from] anyhow::Error);

impl From<lapin::Error> for Error {
    fn from(err: lapin::Error) -> Self {
        Self(err.into())
    }
}
impl From<deadpool::managed::PoolError<Error>> for Error {
    fn from(err: deadpool::managed::PoolError<Error>) -> Self {
        match err {
            deadpool::managed::PoolError::Backend(e) => e,
            err => Self(err.into()),
        }
    }
}
