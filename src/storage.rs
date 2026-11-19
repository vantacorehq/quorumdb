//! Optional on-disk record helper plus the in-memory key-value map.
//!
//! `Wal::append` writes one length-prefixed entry and calls `sync_all`.
//! Replay (`read_all`) is not implemented. The in-process cluster does not
//! open this log; it applies committed commands to [`StateMachine`] in memory.

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
        Ok(Self {
            file: BufWriter::new(file),
        })
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

impl Command {
    pub fn encode(&self) -> Vec<u8> {
        match self {
            Command::Put { key, value } => {
                let mut buf = Vec::with_capacity(1 + 4 + key.len() + 4 + value.len());
                buf.push(b'P');
                buf.extend_from_slice(&(key.len() as u32).to_le_bytes());
                buf.extend_from_slice(key.as_bytes());
                buf.extend_from_slice(&(value.len() as u32).to_le_bytes());
                buf.extend_from_slice(value);
                buf
            }
            Command::Delete { key } => {
                let mut buf = Vec::with_capacity(1 + 4 + key.len());
                buf.push(b'D');
                buf.extend_from_slice(&(key.len() as u32).to_le_bytes());
                buf.extend_from_slice(key.as_bytes());
                buf
            }
        }
    }

    pub fn decode(bytes: &[u8]) -> Option<Self> {
        let (tag, rest) = bytes.split_first()?;
        match tag {
            b'P' => {
                if rest.len() < 4 {
                    return None;
                }
                let klen = u32::from_le_bytes(rest[0..4].try_into().ok()?) as usize;
                let key_end = 4usize.checked_add(klen)?;
                if rest.len() < key_end + 4 {
                    return None;
                }
                let key = std::str::from_utf8(rest.get(4..key_end)?).ok()?.to_string();
                let vlen = u32::from_le_bytes(rest[key_end..key_end + 4].try_into().ok()?) as usize;
                let val_end = key_end.checked_add(4)?.checked_add(vlen)?;
                if rest.len() < val_end {
                    return None;
                }
                let value = rest[key_end + 4..val_end].to_vec();
                Some(Command::Put { key, value })
            }
            b'D' => {
                if rest.len() < 4 {
                    return None;
                }
                let klen = u32::from_le_bytes(rest[0..4].try_into().ok()?) as usize;
                let key_end = 4usize.checked_add(klen)?;
                if rest.len() < key_end {
                    return None;
                }
                let key = std::str::from_utf8(rest.get(4..key_end)?).ok()?.to_string();
                Some(Command::Delete { key })
            }
            _ => None,
        }
    }
}

/// In-memory key-value map. Callers apply committed commands themselves.
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
        sm.apply(Command::Put {
            key: "a".into(),
            value: b"1".to_vec(),
        });
        assert_eq!(sm.get("a"), Some(&b"1".to_vec()));
    }

    #[test]
    fn delete_removes_key() {
        let mut sm = StateMachine::default();
        sm.apply(Command::Put {
            key: "a".into(),
            value: b"1".to_vec(),
        });
        sm.apply(Command::Delete { key: "a".into() });
        assert_eq!(sm.get("a"), None);
    }

    #[test]
    fn command_roundtrip() {
        let cmd = Command::Put {
            key: "k".into(),
            value: b"v".to_vec(),
        };
        assert!(
            matches!(Command::decode(&cmd.encode()), Some(Command::Put { key, value }) if key == "k" && value == b"v")
        );
        let del = Command::Delete { key: "k".into() };
        assert!(
            matches!(Command::decode(&del.encode()), Some(Command::Delete { key }) if key == "k")
        );
    }
}
