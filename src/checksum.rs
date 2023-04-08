use anyhow::{Ok, Result};
use crc32fast::Hasher;

pub const CHECKSUM_SIZE: usize = 4;

pub fn calculate_checksum(data: &[u8]) -> u32 {
    let mut hasher = Hasher::new();
    hasher.update(data);
    hasher.finalize()
}

pub fn verify_checksum(data: &[u8], expected: u32) -> Result<()> {
    let actual = calculate_checksum(data);
    if expected == actual {
        return Ok(());
    }
    Err(anyhow::anyhow!(format!(
        "checksum: expected {}, actual {}",
        expected, actual
    )))
}

#[cfg(test)]
mod test {
    use super::{calculate_checksum, verify_checksum};

    #[test]
    fn simple_test() {
        let data = &b"12312nskjdhsdi9823r1y3r9"[..];
        let checksum = calculate_checksum(data);
        verify_checksum(data, checksum).unwrap();
        assert!(verify_checksum(data, 123).is_err())
    }
}
