use std::{
    collections::HashMap,
    path::PathBuf,
    sync::{Arc, Mutex},
    time::Duration,
};

use crate::{
    rotator::{LogRotator, RotationPolicy},
    LogWriter,
};

pub fn spawn_flusher<W>(distributor: Arc<Mutex<LogDistributor<W>>>, flush_interval: Duration)
where
    W: LogWriter + Sync + Send + 'static,
{
    std::thread::Builder::new()
        .name("LogDistributor::flush()".to_string())
        .spawn({
            let distributor = Arc::downgrade(&distributor);
            move || loop {
                std::thread::sleep(flush_interval);
                let Some(distributor) = distributor.upgrade() else {
                    return;
                };
                distributor.lock().unwrap().flush();
            }
        })
        .expect("Failed to spawn the flushing worker thread");
}

#[derive(Debug)]
pub struct LogDistributor<W> {
    output_dir: PathBuf,
    rotators: HashMap<&'static str, LogRotator<W>>,
    rotation: RotationPolicy,
}
impl<W> LogDistributor<W> {
    pub fn new(output_dir: PathBuf, rotation: RotationPolicy) -> Self {
        Self {
            output_dir,
            rotators: HashMap::new(),
            rotation,
        }
    }
}
impl<W> LogDistributor<W>
where
    W: LogWriter,
{
    pub fn flush(&mut self) {
        self.rotators.iter_mut().for_each(|(_, t)| {
            t.flush();
        });
    }
}
impl<W> LogDistributor<W>
where
    W: LogWriter,
{
    pub fn writer(&mut self, table_name: &'static str) -> &mut W {
        let entry = self.rotators.entry(table_name);
        let table = match entry {
            std::collections::hash_map::Entry::Occupied(entry) => entry.into_mut(),
            std::collections::hash_map::Entry::Vacant(entry) => {
                let output_dir = self.output_dir.join(table_name);
                let table = entry.insert(LogRotator::new(output_dir, self.rotation.clone()));
                table
            }
        };
        table.writer()
    }

    pub fn incr_record_count(&mut self, table_name: &str) {
        let Some(table) = self.rotators.get_mut(table_name) else {
            return;
        };
        table.incr_record_count();
    }
}

#[cfg(test)]
mod tests {
    use std::{any::type_name, io::Read, num::NonZeroUsize, path::Path};

    use serde::Serialize;

    use super::*;

    fn log_file_path(
        output_dir: impl AsRef<Path>,
        table_name: &str,
        epoch: usize,
        extension: &str,
    ) -> PathBuf {
        let mut path = output_dir.as_ref().join(table_name).join(epoch.to_string());
        path.set_extension(extension);
        path
    }

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
        distributor: Arc<Mutex<LogDistributor<CsvLogWriter>>>,
    }
    impl Logger {
        pub fn new(distributor: Arc<Mutex<LogDistributor<CsvLogWriter>>>) -> Self {
            Self { distributor }
        }

        pub fn write<R>(&self, record: &R)
        where
            R: Serialize,
        {
            let table_name = type_name::<R>();
            let mut distributor = self.distributor.lock().unwrap();
            distributor
                .writer(table_name)
                .writer()
                .serialize(record)
                .unwrap();
            distributor.incr_record_count(table_name);
        }

        pub fn flush(&self) {
            self.distributor.lock().unwrap().flush();
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
        let distributor = LogDistributor::new(
            dir.path().to_owned(),
            RotationPolicy {
                max_records: NonZeroUsize::new(2).unwrap(),
                max_epochs: 2,
            },
        );
        let logger = Logger::new(Arc::new(Mutex::new(distributor)));
        logger.write(&TestRecord { s: "a", n: 0 });
        logger.write(&TestRecord { s: "b", n: 1 });
        logger.flush();
        let path = log_file_path(dir.path(), type_name::<TestRecord>(), 0, "csv");
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
}
