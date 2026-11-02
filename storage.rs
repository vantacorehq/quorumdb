//! Crash-safe write-ahead log + the key-value state machine applied on top of it.
//! Append-only, fsync'd before acknowledging a write — durability over throughput.

use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{self, BufWriter, Write};
use std::path::Path;

use crate::raft::LogEntry;

pub struct Wal {
    file: BufWriter<File>,
}

impl Wal {
    pub fn open<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        let file = OpenOptions::new().create(true).append(true).open(path)?;
        Ok(Self { file: BufWriter::new(file) })
    }

    /// Append an entry and fsync before returning — a write is only durable
    /// once this call completes.
    pub fn append(&mut self, entry: &LogEntry) -> io::Result<()> {
        let encoded = encode_entry(entry);
        self.file.write_all(&encoded)?;
        self.file.flush()?;
        self.file.get_ref().sync_all()?;
        Ok(())
    }

    // TODO: read_all() for replay on startup, truncate_from(index) for
    // conflict resolution during AppendEntries.
}

fn encode_entry(entry: &LogEntry) -> Vec<u8> {
    // length-prefixed binary record: [term:8][index:8][len:4][command bytes]
    let mut buf = Vec::with_capacity(20 + entry.command.len());
    buf.extend_from_slice(&entry.term.to_be_bytes());
    buf.extend_from_slice(&entry.index.to_be_bytes());
    buf.extend_from_slice(&(entry.command.len() as u32).to_be_bytes());
    buf.extend_from_slice(&entry.command);
    buf
}

#[derive(Debug, Clone)]
pub enum Command {
    Put { key: String, value: Vec<u8> },
    Delete { key: String },
}

/// The in-memory key-value store. Rebuilt by replaying the WAL on startup,
/// then kept up to date by applying committed Raft log entries in order.
#[derive(Default)]
pub struct StateMachine {
    data: HashMap<String, Vec<u8>>,
}

impl StateMachine {
    pub fn apply(&mut self, command: Command) {
        match command {
            Command::Put { key, value } => {
                self.data.insert(key, value);
            }
            Command::Delete { key } => {
                self.data.remove(&key);
            }
        }
    }

    pub fn get(&self, key: &str) -> Option<&Vec<u8>> {
        self.data.get(key)
    }

    // TODO: snapshot() to compact the WAL once it grows past a threshold.
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn put_then_get() {
        let mut sm = StateMachine::default();
        sm.apply(Command::Put { key: "a".into(), value: b"1".to_vec() });
        assert_eq!(sm.get("a"), Some(&b"1".to_vec()));
    }

    #[test]
    fn delete_removes_key() {
        let mut sm = StateMachine::default();
        sm.apply(Command::Put { key: "a".into(), value: b"1".to_vec() });
        sm.apply(Command::Delete { key: "a".into() });
        assert_eq!(sm.get("a"), None);
    }
}
