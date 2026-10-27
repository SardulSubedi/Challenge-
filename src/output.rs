use std::{
    fs::OpenOptions,
    io::{BufWriter, Write},
    path::PathBuf,
};

use anyhow::{Context, Result};
use tokio::sync::mpsc;

use crate::{
    latency::Observation,
    util::{
        AlignedSbeWriter,
        dtos::{
            SbeEncode,
            challenge_orderbook::{PriceLevelUpdateDomain, SnapshotDomain},
        },
    },
};

pub const CHUNK_SIZE: usize = 64 << 20;

#[derive(Debug)]
pub enum Record {
    Snapshot(SnapshotDomain),
    Delta(PriceLevelUpdateDomain),
}

impl Record {
    pub fn encode(&self) -> Result<Vec<u8>> {
        match self {
            Self::Snapshot(m) => m.to_bytes(),
            Self::Delta(m) => m.to_bytes(),
        }
    }
}

pub enum Output {
    Record(Record),
    Observation(Observation),
    Flush,
    Checkpoint,
}

pub struct Sink {
    archive: Option<AlignedSbeWriter>,
    stdout: bool,
    latency: Option<BufWriter<std::fs::File>>,
}

impl Sink {
    pub fn open(output: Option<PathBuf>, stdout: bool, latency: Option<PathBuf>) -> Result<Self> {
        // Reserve names with create_new before using the required writer (which otherwise truncates).
        let archive = if let Some(path) = output {
            if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
                std::fs::create_dir_all(parent)?;
            }
            OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&path)
                .with_context(|| {
                    format!(
                        "cannot create archive {}; choose a new file name",
                        path.display()
                    )
                })?;
            Some(AlignedSbeWriter::create(path, CHUNK_SIZE)?)
        } else {
            None
        };
        let latency = if let Some(path) = latency {
            if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
                std::fs::create_dir_all(parent)?;
            }
            Some(BufWriter::new(
                OpenOptions::new().write(true).create_new(true).open(path)?,
            ))
        } else {
            None
        };
        Ok(Self {
            archive,
            stdout,
            latency,
        })
    }

    /// A dedicated blocking thread owns output. Bounded queues propagate pressure; never silently drop data.
    pub fn run(mut self, mut rx: mpsc::Receiver<Output>) -> Result<()> {
        let stdout = std::io::stdout();
        let mut stdout = BufWriter::with_capacity(64 * 1024, stdout.lock());
        let result: Result<()> = (|| {
            while let Some(item) = rx.blocking_recv() {
                match item {
                    Output::Record(record) => {
                        // Write archive first so a broken stdout does not discard the current archived record.
                        if let Some(archive) = &mut self.archive {
                            match &record {
                                Record::Snapshot(m) => archive.write_msg(m)?,
                                Record::Delta(m) => archive.write_msg(m)?,
                            }
                        }
                        if self.stdout {
                            stdout.write_all(&record.encode()?)?;
                            stdout.flush()?;
                        }
                    }
                    Output::Observation(observation) => {
                        if let Some(log) = &mut self.latency {
                            serde_json::to_writer(&mut *log, &observation)?;
                            log.write_all(b"\n")?;
                        }
                    }
                    Output::Flush => {
                        // stdout is unpadded SBE; archive is chunk-aligned. Avoid padding a 64 MiB archive every second.
                        stdout.flush()?;
                        if let Some(log) = &mut self.latency {
                            log.flush()?;
                        }
                    }
                    Output::Checkpoint => {
                        if let Some(archive) = &mut self.archive {
                            archive.flush()?;
                        }
                    }
                }
            }
            Ok(())
        })();
        // Flush the archive even if stdout failed. Returning an error is preferable to silent loss.
        let archive_result = self
            .archive
            .as_mut()
            .map(AlignedSbeWriter::flush)
            .transpose();
        let stdout_result = stdout.flush();
        let latency_result = self.latency.as_mut().map(Write::flush).transpose();
        result?;
        archive_result?;
        stdout_result?;
        latency_result?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        book::Book,
        util::{AlignedSbeReader, OrderbookSchema, dtos::challenge_orderbook::AnyMessage},
    };
    #[test]
    fn shutdown_flushes_archive_using_template_helpers() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.sbe");
        let sink = Sink::open(Some(path.clone()), false, None).unwrap();
        let (tx, rx) = mpsc::channel(8);
        let timestamp =
            chrono::DateTime::from_timestamp_micros(chrono::Utc::now().timestamp_micros()).unwrap();
        let expected = Book::default().snapshot("A", timestamp, timestamp);
        tx.blocking_send(Output::Record(Record::Snapshot(expected.clone())))
            .unwrap();
        drop(tx);
        sink.run(rx).unwrap();
        assert_eq!(std::fs::metadata(&path).unwrap().len(), CHUNK_SIZE as u64);
        let mut reader = AlignedSbeReader::<OrderbookSchema>::open(path).unwrap();
        let Some(AnyMessage::Snapshot(decoded)) = reader.read_msg().unwrap() else {
            panic!()
        };
        assert_eq!(SnapshotDomain::try_from_decoder(decoded).unwrap(), expected);
        assert!(reader.read_msg().unwrap().is_none());
    }
    #[test]
    fn refuses_to_overwrite_archive() {
        let file = tempfile::NamedTempFile::new().unwrap();
        assert!(Sink::open(Some(file.path().to_owned()), false, None).is_err());
    }
}
