use std::path::{Path, PathBuf};

pub fn sstable_file_path(dir: &Path, id: u64) -> PathBuf {
    dir.join(format!("{id}.sst"))
}

pub fn path_mem(dir: &Path, id: u64) -> PathBuf {
    dir.join(format!("{:05}.mem", id))
}

pub const MEMTABLE_FILE_EXT: &str = ".mem";

pub fn memtable_file_path(dir: impl AsRef<Path>, id: usize) -> PathBuf {
    dir.as_ref().join(format!("{:05}{}", id, MEMTABLE_FILE_EXT))
}

#[cfg(test)]
mod test {
    use std::path::Path;

    use super::{sstable_file_path, path_mem};
    #[test]
    fn test_path_sst() {
        let path = sstable_file_path(Path::new("./"), 1);
        let buf = Path::new("./1.sst").to_path_buf();
        assert_eq!(path, buf)
    }

    #[test]
    fn test_path_mem() {
        let path = path_mem(Path::new("./"), 1);
        let buf = Path::new("./00001.mem").to_path_buf();
        assert_eq!(path, buf)
    }
}
