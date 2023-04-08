use anyhow::Result;
use bytes::Buf;
use std::{
    fs::{remove_file, File},
    io::{Read, Write},
    os::unix::prelude::{FileExt, OpenOptionsExt},
    path::{Path, PathBuf},
    sync::atomic::{AtomicBool, Ordering},
};

use crate::checksum::{self, CHECKSUM_SIZE};

/// A file object.
#[derive(Debug)]
pub struct FileObject {
    fs: File,
    size: usize,
    file_name: PathBuf,
    remove_file: AtomicBool,
}

impl FileObject {
    pub fn read(&self, offset: usize, len: usize) -> Result<Vec<u8>> {
        let mut buf = vec![0; len];
        self.fs.read_exact_at(&mut buf, offset as u64)?;
        Ok(buf)
    }

    pub fn size(&self) -> usize {
        self.size
    }

    fn create_new(path: impl AsRef<Path>, data: &[u8], o_direct: bool) -> Result<()> {
        let mut op = File::options();
        op.create_new(true).write(true);

        if o_direct {
            op.custom_flags(libc::O_DIRECT | libc::O_SYNC);
        }

        let mut fs = op.open(path)?;
        // fs::write(): data may not actually be written to disk
        fs.write_all(data)?;
        let checksum = checksum::calculate_checksum(data).to_be_bytes();
        fs.write_all(&checksum)?;
        fs.flush()?;
        Ok(())
    }

    /// Create a new file object and write the file to the disk .
    pub fn create(path: impl AsRef<Path>, data: &[u8], o_direct: bool) -> Result<Self> {
        Self::create_new(&path, data, o_direct)?;
        Self::open(path, o_direct)
    }

    /// open file
    pub fn open(path: impl AsRef<Path>, o_direct: bool) -> Result<Self> {
        let mut op = File::options();
        op.read(true);

        if o_direct {
            op.custom_flags(libc::O_DIRECT);
        }

        let mut fs = op.open(&path)?;
        let size = fs.metadata()?.len() as usize;
        let mut buf = Vec::with_capacity(size);
        fs.read_to_end(&mut buf)?;
        let expected = (&buf[size - CHECKSUM_SIZE..]).get_u32();
        checksum::verify_checksum(&buf[..size - CHECKSUM_SIZE], expected)?;

        Ok(Self {
            fs,
            size: size - CHECKSUM_SIZE,
            file_name: path.as_ref().to_path_buf(),
            remove_file: AtomicBool::new(true),
        })
    }

    pub fn save(&self) {
        self.remove_file.store(false, Ordering::Relaxed);
    }
}

impl Drop for FileObject {
    fn drop(&mut self) {
        if self.remove_file.load(Ordering::Relaxed) {
            remove_file(&self.file_name).unwrap();
        }
    }
}

#[cfg(test)]
mod test {
    use tempfile::tempdir;

    use super::FileObject;

    #[test]
    fn create_test() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("1.sst");
        let data = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
        let obj = FileObject::create(path, &data, false).unwrap();
    }

    #[test]
    fn read_test() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("1.sst");
        let data = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
        let obj = FileObject::create(&path, &data, false).unwrap();
        obj.save();
        drop(obj);
        let obj = FileObject::open(path, false).unwrap();
        let data_read = obj.read(0, data.len()).unwrap();
        assert_eq!(data, data_read);
    }
}
