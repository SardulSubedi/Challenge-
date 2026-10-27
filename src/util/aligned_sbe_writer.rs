//! [`AlignedSbeWriter`] writes SBE messages in fixed-size, zero-padded file chunks.

use std::{fs::File, io::Write, path::Path};

use super::dtos::SbeEncode;

// Every schema in `dtos` uses the standard eight-byte SBE message header.
const HEADER_LENGTH: usize = 8;
type PendingWrite = std::thread::JoinHandle<std::io::Result<(File, Vec<u8>)>>;

/// Writes SBE messages to a flat file in fixed size, zero padded chunks.
///
/// See [`AlignedSbeReader`](crate::util::AlignedSbeReader) for more details on why
/// this struct exists.
///
/// Every chunk reserves enough trailing zeroes for an empty SBE header, so a
/// reader can stop decoding at the sentinel without carrying a frame across a
/// chunk boundary.
///
/// Call [`Self::flush`] to write the final buffered chunk before dropping the
/// writer.
pub struct AlignedSbeWriter {
    file: Option<File>,
    buf: Vec<u8>,
    chunk: usize,
    spare: Option<Vec<u8>>,
    pending: Option<PendingWrite>,
}

impl AlignedSbeWriter {
    /// Create an [`AlignedSbeWriter`], writing to `path` with `chunk` chunk
    /// size. Clears the file at `path`, if it exists.
    pub fn create(path: impl AsRef<Path>, chunk: usize) -> anyhow::Result<Self> {
        anyhow::ensure!(
            chunk > HEADER_LENGTH,
            "chunk must leave room for SBE data and an SBE header"
        );
        Ok(Self {
            file: Some(File::create(path)?),
            buf: Vec::with_capacity(chunk),
            chunk,
            spare: None,
            pending: None,
        })
    }

    /// Encodes a complete message directly into the chunk buffer. Rotates to a
    /// new chunk if it does not fit in the existing chunk.
    pub fn write_msg<T: SbeEncode + ?Sized>(&mut self, message: &T) -> anyhow::Result<()> {
        self.check_write()?;
        let len = message.encoded_length_with_header()?;
        let payload_limit = self.chunk - HEADER_LENGTH;
        anyhow::ensure!(len <= payload_limit, "SBE message does not fit in a chunk");
        if len > payload_limit - self.buf.len() {
            self.checkpoint()?;
        }

        let start = self.buf.len();
        self.buf.resize(start + len, 0);
        let result = message.encode(&mut self.buf[start..]).and_then(|written| {
            anyhow::ensure!(
                written == len,
                "SBE encoded length mismatch: expected {len} bytes, wrote {written}"
            );
            Ok(())
        });
        if result.is_err() {
            self.buf.truncate(start);
        }
        result
    }

    /// Flush the current buffer to disk, and clear it.
    pub fn flush(&mut self) -> anyhow::Result<()> {
        self.checkpoint()?;
        self.finish_write()
    }

    /// Queue at most one completed chunk while encoding into a second buffer.
    /// A slow previous write provides bounded backpressure; flush waits for all
    /// submitted writes and remains the required shutdown barrier.
    pub fn checkpoint(&mut self) -> anyhow::Result<()> {
        if !self.buf.is_empty() {
            self.finish_write()?;
            let chunk_size = self.chunk;
            let replacement = self
                .spare
                .take()
                .unwrap_or_else(|| Vec::with_capacity(self.chunk));
            let mut chunk = std::mem::replace(&mut self.buf, replacement);
            let mut file = self
                .file
                .take()
                .ok_or_else(|| anyhow::anyhow!("archive writer unavailable after failure"))?;
            self.pending = Some(
                std::thread::Builder::new()
                    .name("sbe-chunk-write".into())
                    .spawn(move || {
                        chunk.resize(chunk_size, 0);
                        file.write_all(&chunk)?;
                        chunk.clear();
                        Ok((file, chunk))
                    })?,
            );
        }
        Ok(())
    }

    pub fn check_write(&mut self) -> anyhow::Result<()> {
        if self
            .pending
            .as_ref()
            .is_some_and(|pending| pending.is_finished())
        {
            self.finish_write()?;
        }
        Ok(())
    }

    fn finish_write(&mut self) -> anyhow::Result<()> {
        if let Some(pending) = self.pending.take() {
            let (file, buffer) = pending
                .join()
                .map_err(|_| anyhow::anyhow!("archive writer thread panicked"))??;
            self.file = Some(file);
            self.spare = Some(buffer);
        }
        Ok(())
    }
}

impl Drop for AlignedSbeWriter {
    fn drop(&mut self) {
        // Never detach an in-flight write. Active buffered data still requires
        // an explicit flush, matching the original helper contract.
        let _ = self.finish_write();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        book::Book,
        util::{
            AlignedSbeSchema, OrderbookSchema,
            dtos::challenge_orderbook::{AnyMessage, SnapshotDomain},
        },
    };

    #[test]
    fn background_chunks_flush_in_order_without_loss_or_torn_messages() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("chunks.sbe");
        let mut writer = AlignedSbeWriter::create(&path, 128).unwrap();
        for i in 0..10 {
            let record = Book::from_levels(vec![(400000, i + 1)], vec![])
                .unwrap()
                .snapshot(&format!("S{i}"), chrono::Utc::now(), chrono::Utc::now());
            writer.write_msg(&record).unwrap();
            if i % 3 == 2 {
                writer.checkpoint().unwrap();
            }
        }
        writer.flush().unwrap();
        let bytes = std::fs::read(path).unwrap();
        assert!(bytes.len() > 128 && bytes.len().is_multiple_of(128));
        let mut symbols = Vec::new();
        for chunk in bytes.as_chunks::<128>().0 {
            let mut offset = 0;
            while offset + HEADER_LENGTH <= chunk.len()
                && chunk[offset..offset + HEADER_LENGTH] != [0; HEADER_LENGTH]
            {
                let decoded = OrderbookSchema::try_decode(chunk, offset).unwrap();
                let length = OrderbookSchema::encoded_length(&decoded).unwrap();
                let AnyMessage::Snapshot(snapshot) = decoded else {
                    panic!("unexpected message");
                };
                symbols.push(SnapshotDomain::try_from_decoder(snapshot).unwrap().symbol);
                offset += length;
            }
        }
        assert_eq!(
            symbols,
            (0..10).map(|i| format!("S{i}")).collect::<Vec<_>>()
        );
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn background_disk_failure_is_returned_by_flush() {
        let mut writer = AlignedSbeWriter::create("/dev/full", 128).unwrap();
        let record = Book::default().snapshot("A", chrono::Utc::now(), chrono::Utc::now());
        writer.write_msg(&record).unwrap();
        writer.checkpoint().unwrap();
        assert!(writer.flush().is_err());
    }
}
