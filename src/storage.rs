//! Optional on-disk record helper plus the in-memory key-value map.
//!
//! `Wal::append` writes one length-prefixed entry and calls `sync_all`.
//! `read_all` replays every complete entry. `truncate_from` keeps only
//! entries with index <= up_to.

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

    /// Replay every complete entry from the beginning of the file.
    /// Stops at a truncated tail (does not return a partial entry).
    pub fn read_all(path: impl AsRef<Path>) -> io::Result<Vec<LogEntry>> {
        use std::io::Read;
        let mut file = OpenOptions::new().read(true).open(path)?;
        let mut buf = Vec::new();
        file.read_to_end(&mut buf)?;
        Ok(decode_entries(&buf))
    }

    /// Truncate the file so only entries with `index <= up_to` remain.
    pub fn truncate_from(path: impl AsRef<Path>, up_to: u64) -> io::Result<()> {
        let entries = Self::read_all(&path)?;
        let kept: Vec<_> = entries.into_iter().filter(|e| e.index <= up_to).collect();
        let mut file = OpenOptions::new()
            .write(true)
            .truncate(true)
            .create(true)
            .open(path)?;
        for e in &kept {
            file.write_all(&encode_entry(e))?;
        }
        file.sync_all()?;
        Ok(())
    }
}

fn encode_entry(entry: &LogEntry) -> Vec<u8> {
    let mut buf = Vec::with_capacity(20 + entry.command.len());
    buf.extend_from_slice(&entry.term.to_be_bytes());
    buf.extend_from_slice(&entry.index.to_be_bytes());
    buf.extend_from_slice(&(entry.command.len() as u32).to_be_bytes());
    buf.extend_from_slice(&entry.command);
    buf
}

fn decode_entries(buf: &[u8]) -> Vec<LogEntry> {
    let mut out = Vec::new();
    let mut i = 0usize;
    while i + 20 <= buf.len() {
        let term = u64::from_be_bytes(buf[i..i + 8].try_into().unwrap());
        let index = u64::from_be_bytes(buf[i + 8..i + 16].try_into().unwrap());
        let len = u32::from_be_bytes(buf[i + 16..i + 20].try_into().unwrap()) as usize;
        i += 20;
        if i + len > buf.len() {
            break;
        }
        let command = buf[i..i + len].to_vec();
        i += len;
        out.push(LogEntry { term, index, command });
    }
    out
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

    #[test]
    fn wal_append_read_all_truncate() {
        let dir = std::env::temp_dir();
        let path = dir.join(format!("quorumdb-wal-{}.log", std::process::id()));
        let _ = std::fs::remove_file(&path);

        let mut wal = Wal::open(&path).unwrap();
        wal.append(&LogEntry { term: 1, index: 1, command: b"one".to_vec() }).unwrap();
        wal.append(&LogEntry { term: 1, index: 2, command: b"two".to_vec() }).unwrap();
        wal.append(&LogEntry { term: 2, index: 3, command: b"three".to_vec() }).unwrap();
        drop(wal);

        let all = Wal::read_all(&path).unwrap();
        assert_eq!(all.len(), 3);
        assert_eq!(all[0].command, b"one");
        assert_eq!(all[2].index, 3);

        Wal::truncate_from(&path, 2).unwrap();
        let kept = Wal::read_all(&path).unwrap();
        assert_eq!(kept.len(), 2);
        assert_eq!(kept[1].command, b"two");

        let _ = std::fs::remove_file(&path);
    }
}
