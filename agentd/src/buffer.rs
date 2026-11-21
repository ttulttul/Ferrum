//! A stream's unacknowledged frames, kept in a file so a long disconnect
//! does not hold them in memory.
use std::{
    collections::VecDeque,
    fs::{File, OpenOptions},
    io::{self, Read, Seek, SeekFrom, Write},
    path::PathBuf,
};

pub struct DiskBuffer {
    path: PathBuf,
    file: File,
    /// `(seq, offset, length)` of each frame still held, oldest first.
    entries: VecDeque<(u64, u64, u64)>,
    /// Bytes held by `entries`.
    live: u64,
    /// Where the next frame is written.
    end: u64,
    limit: u64,
}

impl DiskBuffer {
    pub fn create(path: PathBuf, limit: u64) -> io::Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)?;
        Ok(Self {
            path,
            file,
            entries: VecDeque::new(),
            live: 0,
            end: 0,
            limit,
        })
    }

    /// Hold a frame. Unless `force`, refuse it (`Ok(false)`) when it would
    /// take the buffer past its limit.
    pub fn push(&mut self, seq: u64, bytes: &[u8], force: bool) -> io::Result<bool> {
        let len = bytes.len() as u64;
        if !force && self.live + len > self.limit {
            return Ok(false);
        }
        self.file.seek(SeekFrom::Start(self.end))?;
        self.file.write_all(bytes)?;
        self.entries.push_back((seq, self.end, len));
        self.end += len;
        self.live += len;
        Ok(true)
    }

    /// Drop the frames up to and including `seq`.
    pub fn ack(&mut self, seq: u64) -> io::Result<()> {
        while let Some(&(s, _, len)) = self.entries.front() {
            if s > seq {
                break;
            }
            self.entries.pop_front();
            self.live -= len;
        }
        match self.entries.front() {
            None => {
                self.file.set_len(0)?;
                self.end = 0;
            }
            // Reclaim the acknowledged prefix once it outgrows the live part.
            Some(&(_, start, _)) if start > self.limit && start > self.live => {
                self.compact(start)?
            }
            Some(_) => {}
        }
        Ok(())
    }

    fn compact(&mut self, start: u64) -> io::Result<()> {
        let mut live = vec![0; (self.end - start) as usize];
        self.file.seek(SeekFrom::Start(start))?;
        self.file.read_exact(&mut live)?;
        self.file.seek(SeekFrom::Start(0))?;
        self.file.write_all(&live)?;
        self.file.set_len(live.len() as u64)?;
        for entry in &mut self.entries {
            entry.1 -= start;
        }
        self.end -= start;
        Ok(())
    }

    /// The frames after `seq`, oldest first.
    pub fn replay(&mut self, seq: u64) -> io::Result<Vec<Vec<u8>>> {
        let mut frames = Vec::new();
        for &(s, offset, len) in &self.entries {
            if s <= seq {
                continue;
            }
            let mut bytes = vec![0; len as usize];
            self.file.seek(SeekFrom::Start(offset))?;
            self.file.read_exact(&mut bytes)?;
            frames.push(bytes);
        }
        Ok(frames)
    }

    pub fn held(&self) -> u64 {
        self.entries.len() as u64
    }
}

impl Drop for DiskBuffer {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn holds_replays_and_reclaims() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("1.frames");
        let mut buffer = DiskBuffer::create(path.clone(), 10).unwrap();
        assert!(buffer.push(1, b"aaaa", false).unwrap());
        assert!(buffer.push(2, b"bbbb", false).unwrap());
        assert!(!buffer.push(3, b"cccc", false).unwrap());
        assert_eq!(
            buffer.replay(0).unwrap(),
            vec![b"aaaa".to_vec(), b"bbbb".to_vec()]
        );
        buffer.ack(1).unwrap();
        assert!(buffer.push(3, b"cccc", false).unwrap());
        assert_eq!(buffer.replay(2).unwrap(), vec![b"cccc".to_vec()]);
        for seq in 4..20 {
            buffer.ack(seq - 2).unwrap();
            assert!(buffer.push(seq, b"dddd", false).unwrap());
        }
        assert!(std::fs::metadata(&path).unwrap().len() <= 24);
        assert_eq!(buffer.replay(17).unwrap(), vec![b"dddd".to_vec(); 2]);
        assert!(buffer.push(20, b"end", true).unwrap());
        buffer.ack(20).unwrap();
        assert_eq!(buffer.held(), 0);
        assert_eq!(std::fs::metadata(&path).unwrap().len(), 0);
        drop(buffer);
        assert!(!path.exists());
    }
}
