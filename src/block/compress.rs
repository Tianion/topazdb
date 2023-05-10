use std::fmt;

use anyhow::Result;
use bytes::{BufMut, Bytes, BytesMut};
use lz4;

/*
TODO: should we compress block?
Compression is too slow in bench_iter_create_and_read(benches/sstable_read.rs)
This maybe a performance trap
``` 1000 pairs key-value
bench_iter_read         time:   [1.2000 ns 1.2072 ns 1.2162 ns]
                        change: [-9.9628% -8.3934% -6.8089%] (p = 0.00 < 0.05)
                        Performance has improved.
Found 16 outliers among 100 measurements (16.00%)
  2 (2.00%) high mild
  14 (14.00%) high severe

bench_iter_create_and_read
                        time:   [40.275 µs 40.360 µs 40.451 µs]
                        change: [+26.885% +27.407% +27.999%] (p = 0.00 < 0.05)
                        Performance has regressed.
Found 2 outliers among 100 measurements (2.00%)
  2 (2.00%) high severe
```
 */

// may support more compression methods?
#[derive(PartialEq, Debug, Clone, Copy)]
pub enum CompressOptions {
    Unkown = 0,
    Uncompress = 1,
    Snappy = 2,
    Lz4 = 3,
}

#[cfg(not(tarpaulin_include))]
impl fmt::Display for CompressOptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl From<u8> for CompressOptions {
    fn from(value: u8) -> Self {
        match value {
            3 => CompressOptions::Lz4,
            2 => CompressOptions::Snappy,
            1 => CompressOptions::Uncompress,
            _ => CompressOptions::Unkown,
        }
    }
}

impl From<CompressOptions> for u8 {
    fn from(value: CompressOptions) -> Self {
        match value {
            CompressOptions::Unkown => 0,
            CompressOptions::Uncompress => 1,
            CompressOptions::Snappy => 2,
            CompressOptions::Lz4 => 3,
        }
    }
}

fn snappy_encode(data: &[u8]) -> Result<Bytes> {
    let mut data = snap::raw::Encoder::new().compress_vec(data)?;

    data.push(CompressOptions::Snappy.into());
    Ok(data.into())
}

fn lz4_encode(data: &[u8]) -> Result<Bytes> {
    let mut data = lz4::block::compress(data, None, true)?;
    data.push(CompressOptions::Lz4.into());
    Ok(data.into())
}

/// return compressed data
///
/// Error: buf is too big or too small or Unkown compress option
pub fn encode(data: &[u8], opt: CompressOptions) -> Result<Bytes> {
    match opt {
        CompressOptions::Unkown => Err(anyhow::anyhow!("unkown compress option")),
        CompressOptions::Uncompress => {
            let mut buf = BytesMut::from(data);
            buf.put_u8(CompressOptions::Uncompress.into());
            Ok(buf.freeze())
        }
        CompressOptions::Snappy => snappy_encode(data),
        CompressOptions::Lz4 => lz4_encode(data),
    }
}

pub fn decode(data: &[u8]) -> Result<BytesMut> {
    if data.is_empty() {
        return Err(anyhow::anyhow!("data is empty"));
    }
    let option = *data.last().unwrap();
    let data = &data[..data.len() - 1];
    match CompressOptions::from(option) {
        CompressOptions::Unkown => Err(anyhow::anyhow!("invaild data")),
        CompressOptions::Uncompress => Ok(BytesMut::from(data)),
        CompressOptions::Snappy => {
            let uncompressed = snap::raw::Decoder::new().decompress_vec(data)?;
            Ok(BytesMut::from(uncompressed.as_slice()))
        }
        CompressOptions::Lz4 => {
            let uncompressed = lz4::block::decompress(data, None)?;
            Ok(BytesMut::from(uncompressed.as_slice()))
        }
    }
}

#[cfg(test)]
mod test {
    use crate::block::{compress::CompressOptions, BlockBuilder};

    use super::{decode, encode};

    #[test]
    fn test_option() {
        for i in 0..10 {
            assert_eq!(Into::<CompressOptions>::into(i), CompressOptions::from(i));
        }
    }

    #[test]
    fn test_empty_data() {
        let str = b"";
        assert!(encode(str, CompressOptions::Unkown).is_err());
        assert!(decode(str).is_err());
    }

    #[test]
    fn test_snappy() {
        let mut builder = BlockBuilder::new(2048);
        for i in 0..100 {
            if !builder.add(
                format!("key_{}", i).as_bytes(),
                format!("value_{}", i).as_bytes(),
            ) {
                break;
            }
        }
        let block = builder.build();
        let uncompress_size = block.uncompress_size();
        let compressed = block.encode(CompressOptions::Snappy).unwrap();
        println!(
            "uncompress_size: {uncompress_size}, snappy: {}",
            compressed.len()
        );
        assert!(uncompress_size - compressed.len() > uncompress_size / 10)
    }

    #[test]
    fn test_lz4() {
        let mut builder = BlockBuilder::new(2048);
        for i in 0..100 {
            if !builder.add(
                format!("key_{}", i).as_bytes(),
                format!("value_{}", i).as_bytes(),
            ) {
                break;
            }
        }
        let block = builder.build();
        let uncompress_size = block.uncompress_size();
        let compressed = block.encode(CompressOptions::Lz4).unwrap();
        println!(
            "uncompress_size: {uncompress_size}, lz4: {}",
            compressed.len()
        );
        assert!(uncompress_size - compressed.len() > uncompress_size / 10)
    }

    #[test]
    fn test_compress_and_uncompress_snap() {
        let str = b"a simple string";
        let compressed = encode(str, CompressOptions::Snappy).unwrap();
        let uncompressed = decode(&compressed).unwrap();
        assert_eq!(str[..], uncompressed);
    }

    #[test]
    fn test_compress_and_uncompress_lz4() {
        let str = b"a simple string";
        let compressed = encode(str, CompressOptions::Lz4).unwrap();
        let uncompressed = decode(&compressed).unwrap();
        assert_eq!(str[..], uncompressed);
    }
}
