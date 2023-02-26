use bytes::{Bytes, BytesMut, BufMut};
use snap::raw::{Encoder, Decoder};
use anyhow::Result;

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
#[derive(PartialEq, Debug)]
pub enum CompressOptions {
    Unkown = 0,
    Uncompress = 1,
    Snappy = 2,
}

impl From<u8> for CompressOptions {
    fn from(value: u8) -> Self {
        match value {
            2 => CompressOptions::Snappy,
            1 => CompressOptions::Uncompress,
            _ => CompressOptions::Unkown,
        }
    }
}

impl From<CompressOptions> for u8 {
    fn from(value: CompressOptions) -> Self {
        match value {
            CompressOptions::Uncompress => 1,
            CompressOptions::Snappy => 2,
            CompressOptions::Unkown => 0,
        }
    }
}

/// if compression fail, return uncompressed bytes
/// Now the opt is invaild. The default is Snappy
pub fn encode(data: &[u8], opt: CompressOptions) -> Bytes {
    assert_eq!(opt, CompressOptions::Snappy);
    // compress return Err when buf is too big or too small
    // this err could be ignore
    if let Ok(mut compressed) 
        = Encoder::new().compress_vec(data) 
    {
        compressed.push(CompressOptions::Snappy.into());
        return Bytes::from(compressed);
    }
    let mut buf = BytesMut::from(data);
    buf.put_u8(CompressOptions::Uncompress.into());
    buf.freeze()
}

pub fn decode(data: &[u8]) -> Result<Bytes> {
    if data.is_empty() {
        return Err(anyhow::anyhow!("data is empty"));
    }
    let option = *data.last().unwrap();
    let data = &data[..data.len() - 1];
    match CompressOptions::from(option) {
        CompressOptions::Uncompress => Ok(Bytes::copy_from_slice(data)),
        CompressOptions::Snappy => {
            let uncompressed = Decoder::new().decompress_vec(data)?;
            Ok(Bytes::from(uncompressed))
        }
        CompressOptions::Unkown => Err(anyhow::anyhow!("invaild data")),
    }
}

#[cfg(test)]
mod test {
    use crate::block::{BlockBuilder, compress::CompressOptions};

    use super::{encode, decode};

    #[test]
    fn test_compress() {
        let mut builder = BlockBuilder::new(2048);
        for i in 0..100 {
            if !builder.add(format!("key_{}", i).as_bytes(), format!("value_{}", i).as_bytes()) {
                break;
            }
        }
        let block = builder.build();
        let uncompress_size = block.uncompress_size();
        let compressed = block.encode();
        assert!(uncompress_size - compressed.len() > uncompress_size / 10)
    }

    #[test]
    fn test_compress_and_uncompress() {
        let str = b"a simple string";
        let compressed = encode(str, CompressOptions::Snappy);
        let uncompressed = decode(&compressed).unwrap();
        assert_eq!(str[..], uncompressed);
    }
}
