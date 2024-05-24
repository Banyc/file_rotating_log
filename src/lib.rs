use std::path::Path;

pub mod distributor;
pub mod rotator;
mod table;

pub trait LogWriter: Sized {
    fn flush(&mut self);
    fn open(path: impl AsRef<Path>) -> Self;
    fn file_extension() -> &'static str;
}
