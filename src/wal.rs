mod iterator;
use anyhow::Result;

use log::debug;
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
    WalWriter(BufWriter<File>),
    WalReader(BufReader<File>),
}

impl WalInner {
    fn append(&mut self, buf: &[u8]) -> Result<()> {
        if let WalInner::WalWriter(writer) = self {
            writer.write_all(buf)?;
            writer.flush()?;
            Ok(())
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
        let file = File::options().create_new(true).append(true).open(&path)?;
        Ok(Wal {
            inner: Mutex::new(WalInner::WalWriter(BufWriter::new(file))),
            path: path.as_ref().to_path_buf(),
            remove_file: AtomicBool::new(true),
        })
    }

    pub fn add(&self, key: &[u8], value: &[u8]) -> Result<()> {
        let entry = Entry::new(key, value);
        let buf = entry.encode();
        self.inner.lock().append(&buf)
    }

    pub fn iter(&self) -> Result<WalIterator> {
        let buf = self.inner.lock().read_all()?;

        Ok(WalIterator::create(&buf))
    }
}

impl Drop for Wal {
    fn drop(&mut self) {
        debug!("{:?} begin drop", self.path);
        if self.remove_file.load(Ordering::Relaxed) {
            remove_file(&self.path).unwrap();
        }
    }
}

#[cfg(test)]
mod test;
