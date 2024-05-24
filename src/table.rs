use crate::LogWriter;

#[derive(Debug)]
pub struct Table<W> {
    records_written: usize,
    epoch: usize,
    writer: W,
}
impl<W> Table<W>
where
    W: LogWriter,
{
    pub fn new(writer: W, epoch: usize) -> Self {
        Self {
            records_written: 0,
            epoch,
            writer,
        }
    }

    pub fn replace(&mut self, writer: W) {
        self.writer = writer;
        self.epoch += 1;
        self.records_written = 0;
    }

    pub fn writer(&mut self) -> &mut W {
        &mut self.writer
    }

    pub fn incr_record_count(&mut self) {
        self.records_written += 1;
    }

    pub fn flush(&mut self) {
        self.writer.flush();
    }

    pub fn epoch(&self) -> usize {
        self.epoch
    }

    pub fn records_written(&self) -> usize {
        self.records_written
    }
}
