use std::path::Path;

mod cron;
pub mod distributor;
pub mod rotator;
mod table;
pub mod time_past;

pub trait LogWriter: Sized {
    fn flush(&mut self);
    fn open(path: impl AsRef<Path>) -> Self;
    fn file_extension() -> &'static str;
}
