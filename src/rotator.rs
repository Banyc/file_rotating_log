use std::{
    io::{Read, Write},
    num::NonZeroUsize,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::Duration,
};

use crate::{table::Table, LogWriter};

pub fn spawn_flusher<W>(rotator: Arc<Mutex<LogRotator<W>>>, flush_interval: Duration)
where
    W: LogWriter + Sync + Send + 'static,
{
    std::thread::Builder::new()
        .name("LogRotator::flush()".to_string())
        .spawn({
            let rotator = Arc::downgrade(&rotator);
            move || loop {
                std::thread::sleep(flush_interval);
                let Some(rotator) = rotator.upgrade() else {
                    return;
                };
                rotator.lock().unwrap().flush();
            }
        })
        .expect("Failed to spawn the flushing worker thread");
}

#[derive(Debug)]
pub struct LogRotator<W> {
    output_dir: PathBuf,
    table: Table<W>,
    rotation: RotationPolicy,
}
impl<W> LogRotator<W>
where
    W: LogWriter,
{
    pub fn new(output_dir: PathBuf, rotation: RotationPolicy) -> Self {
        let epoch = cur_epoch(&output_dir).map(|e| e + 1).unwrap_or_default();
        let path = log_file_path(&output_dir, epoch, W::file_extension());
        let writer = create_clean_log_writer(path);
        let table = Table::new(writer, epoch);

        let mut this = Self {
            output_dir,
            table,
            rotation,
        };

        this.rotate_file();

        this
    }

    pub fn flush(&mut self) {
        self.table.flush();
    }

    pub fn writer(&mut self) -> &mut W {
        self.table.writer()
    }

    pub fn incr_record_count(&mut self) {
        self.table.incr_record_count();

        // Rotate log file
        if self.rotation.max_records.get() <= self.table.records_written() {
            let new_path = log_file_path(
                &self.output_dir,
                self.table.epoch() + 1,
                W::file_extension(),
            );
            let new_writer = create_clean_log_writer(new_path);
            self.table.replace(new_writer);

            self.rotate_file();
        }
    }

    fn rotate_file(&mut self) {
        let epoch = self.table.epoch();
        write_epoch(&self.output_dir, epoch);
        delete_old_log_file(
            epoch,
            self.rotation.max_epochs,
            &self.output_dir,
            W::file_extension(),
        );
    }
}

#[derive(Debug, Clone)]
pub struct RotationPolicy {
    pub max_records: NonZeroUsize,
    pub max_epochs: usize,
}

fn delete_old_log_file(
    epoch: usize,
    max_epochs: usize,
    output_dir: impl AsRef<Path>,
    extension: &str,
) {
    let del_epoch = epoch.checked_sub(max_epochs);
    if let Some(del_epoch) = del_epoch {
        let del_path = log_file_path(output_dir, del_epoch, extension);
        if del_path.exists() {
            std::fs::remove_file(del_path).expect("Failed to remove outdated log file");
        }
    }
}

fn create_clean_log_writer<W>(path: impl AsRef<Path>) -> W
where
    W: LogWriter,
{
    std::fs::create_dir_all(path.as_ref().parent().unwrap()).expect("Failed to create directories");
    W::open(path)
}

fn write_epoch(output_dir: impl AsRef<Path>, epoch: usize) {
    let path = epoch_file_path(output_dir);
    std::fs::create_dir_all(path.parent().unwrap()).expect("Failed to create directories");
    let mut file = std::fs::File::options()
        .create(true)
        .truncate(true)
        .write(true)
        .open(path)
        .expect("Failed to create an epoch file");
    file.write_all(epoch.to_string().as_bytes())
        .expect("Failed to write epoch to the file");
}

fn cur_epoch(output_dir: impl AsRef<Path>) -> Option<usize> {
    let path = epoch_file_path(output_dir);
    if !path.exists() {
        return None;
    }
    let mut file = std::fs::File::options()
        .read(true)
        .open(&path)
        .expect("Failed to open the epoch file");
    let mut epoch = String::new();
    file.read_to_string(&mut epoch)
        .expect("Failed to read the epoch file");
    let epoch: usize = match epoch.parse() {
        Ok(epoch) => epoch,
        Err(_) => {
            std::fs::remove_file(&path).expect("Failed to delete old epoch file");
            return None;
        }
    };
    Some(epoch)
}

fn epoch_file_path(output_dir: impl AsRef<Path>) -> PathBuf {
    output_dir.as_ref().join("epoch")
}

fn log_file_path(output_dir: impl AsRef<Path>, epoch: usize, extension: &str) -> PathBuf {
    let mut path = output_dir.as_ref().join(epoch.to_string());
    path.set_extension(extension);
    path
}

#[cfg(test)]
mod tests {
    use std::{
        io::Read,
        sync::{Arc, Mutex},
    };

    use serde::Serialize;

    use super::*;

    struct CsvLogWriter {
        writer: csv::Writer<std::fs::File>,
    }
    impl CsvLogWriter {
        pub fn writer(&mut self) -> &mut csv::Writer<std::fs::File> {
            &mut self.writer
        }
    }
    impl LogWriter for CsvLogWriter {
        fn flush(&mut self) {
            self.writer.flush().unwrap();
        }

        fn open(path: impl AsRef<Path>) -> Self {
            let file = std::fs::File::options()
                .write(true)
                .create(true)
                .truncate(true)
                .open(path)
                .expect("Cannot create a log file");
            let writer = csv::Writer::from_writer(file);
            Self { writer }
        }

        fn file_extension() -> &'static str {
            "csv"
        }
    }

    struct Logger {
        rotator: Arc<Mutex<LogRotator<CsvLogWriter>>>,
    }
    impl Logger {
        pub fn new(rotator: Arc<Mutex<LogRotator<CsvLogWriter>>>) -> Self {
            Self { rotator }
        }

        pub fn write<R>(&self, record: &R)
        where
            R: Serialize,
        {
            let mut rotator = self.rotator.lock().unwrap();
            rotator.writer().writer().serialize(record).unwrap();
            rotator.incr_record_count();
        }

        pub fn flush(&self) {
            self.rotator.lock().unwrap().flush();
        }
    }

    #[derive(Serialize)]
    struct TestRecord {
        pub s: &'static str,
        pub n: usize,
    }

    #[test]
    fn test_logger() {
        let dir = tempfile::tempdir().unwrap();
        let log_rotator = LogRotator::new(
            dir.path().to_owned(),
            RotationPolicy {
                max_records: NonZeroUsize::new(2).unwrap(),
                max_epochs: 2,
            },
        );
        let logger = Logger::new(Arc::new(Mutex::new(log_rotator)));
        logger.write(&TestRecord { s: "a", n: 0 });
        logger.write(&TestRecord { s: "b", n: 1 });
        logger.flush();
        let path = log_file_path(dir.path(), 0, "csv");
        assert!(path.exists());
        let mut file = std::fs::File::options().read(true).open(path).unwrap();
        let mut csv = String::new();
        file.read_to_string(&mut csv).unwrap();
        assert_eq!(
            csv,
            r#"s,n
a,0
b,1
"#
        );
    }

    #[test]
    fn test_rotation() {
        let dir = tempfile::tempdir().unwrap();
        let rotator = LogRotator::new(
            dir.path().to_owned(),
            RotationPolicy {
                max_records: NonZeroUsize::new(2).unwrap(),
                max_epochs: 2,
            },
        );
        let logger = Logger::new(Arc::new(Mutex::new(rotator)));

        logger.write(&TestRecord { s: "a", n: 0 });
        logger.flush();
        let path = log_file_path(dir.path(), 0, "csv");
        assert!(path.exists());
        let path = log_file_path(dir.path(), 1, "csv");
        assert!(!path.exists());

        logger.write(&TestRecord { s: "b", n: 1 });
        let path = log_file_path(dir.path(), 0, "csv");
        assert!(path.exists());
        let path = log_file_path(dir.path(), 1, "csv");
        assert!(path.exists());
        let path = log_file_path(dir.path(), 2, "csv");
        assert!(!path.exists());

        logger.write(&TestRecord { s: "c", n: 2 });
        logger.flush();
        let path = log_file_path(dir.path(), 0, "csv");
        assert!(path.exists());
        let path = log_file_path(dir.path(), 1, "csv");
        assert!(path.exists());
        let path = log_file_path(dir.path(), 2, "csv");
        assert!(!path.exists());

        logger.write(&TestRecord { s: "d", n: 3 });
        let path = log_file_path(dir.path(), 0, "csv");
        assert!(!path.exists());
        let path = log_file_path(dir.path(), 1, "csv");
        assert!(path.exists());
        let path = log_file_path(dir.path(), 2, "csv");
        assert!(path.exists());
        let path = log_file_path(dir.path(), 3, "csv");
        assert!(!path.exists());
    }
}
