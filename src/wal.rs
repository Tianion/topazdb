mod iterator;
use anyhow::Result;

use bytes::{BufMut, Bytes, BytesMut};
use parking_lot::Mutex;
use std::{
    fs::{remove_file, File},
    io::{BufReader, BufWriter, Read, Seek, Write},
    path::{Path, PathBuf},
    sync::atomic::{AtomicBool, Ordering},
};

use crate::block::Entry;

use self::iterator::WalIterator;

enum WalInner {
    WalWriter((BufWriter<File>, u64)),
    WalReader(BufReader<File>),
}

impl WalInner {
    fn append(&mut self, buf: &[u8]) -> Result<u64> {
        if let WalInner::WalWriter((writer, id)) = self {
            writer.write_all(buf)?;
            writer.flush()?;
            *id += 1;
            Ok(*id)
        } else {
            Err(anyhow::anyhow!("only write"))
        }
    }

    fn read_all(&mut self) -> Result<Vec<u8>> {
        if let WalInner::WalReader(reader) = self {
            let mut buf = vec![];
            reader.rewind()?;
            reader.read_to_end(&mut buf)?;
            Ok(buf)
        } else {
            Err(anyhow::anyhow!("only read"))
        }
    }
}

pub struct Wal {
    inner: Mutex<WalInner>,
    path: PathBuf,
    remove_file: AtomicBool,
}

impl Wal {
    /// open a file(only-read)
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let file = File::options().read(true).open(&path)?;
        Ok(Wal {
            inner: Mutex::new(WalInner::WalReader(BufReader::new(file))),
            path: path.as_ref().to_path_buf(),
            remove_file: AtomicBool::new(true),
        })
    }

    pub fn save_file(&self) {
        self.remove_file.store(false, Ordering::Relaxed)
    }

    /// create a file(only-write)
    pub fn create(path: impl AsRef<Path>) -> Result<Self> {
        if path.as_ref().exists() {
            remove_file(&path)?;
        }
        let file = match File::options().create_new(true).append(true).open(&path) {
            Ok(file) => file,
            Err(e) => panic!("{e}: {:?}", path.as_ref()),
        };
        Ok(Wal {
            inner: Mutex::new(WalInner::WalWriter((BufWriter::new(file), 0))),
            path: path.as_ref().to_path_buf(),
            remove_file: AtomicBool::new(true),
        })
    }

    pub fn add(&self, key: &[u8], value: &[u8]) -> Result<u64> {
        let entry = Entry::new(key, value);
        let buf = entry.encode();
        self.inner.lock().append(&buf)
    }

    pub fn add_entries(&self, entries: &[(Bytes, Bytes)]) -> Result<u64> {
        let mut buf = BytesMut::new();
        for (key, value) in entries {
            let entry = Entry::new(key, value);
            buf.put(entry.encode());
        }
        self.inner.lock().append(&buf.freeze())
    }

    pub fn iter(&self) -> Result<WalIterator> {
        let buf = self.inner.lock().read_all()?;

        Ok(WalIterator::create(&buf))
    }
}

impl Drop for Wal {
    fn drop(&mut self) {
        if self.remove_file.load(Ordering::Relaxed) {
            if let Err(e) = remove_file(&self.path) {
                panic!("{e}: {:?}", self.path);
            }
        }
    }
}

#[cfg(test)]
mod test;
