use bytes::Bytes;

// why not to use bitvec: I need flush data to disk, so using &[u8]/Bytes/Vec<u8> is better.
pub trait BitSliceMut {
    fn bit_set(&mut self, idx: usize, val: bool);
}
pub trait BitSlice {
    fn bit_get(&self, idx: usize) -> bool;
    fn bit_len(&self) -> usize;
}

impl<T: AsMut<[u8]> + AsRef<[u8]>> BitSliceMut for T {
    fn bit_set(&mut self, idx: usize, val: bool) {
        assert!(idx < self.bit_len());
        let pos = idx / 8;
        let offset = idx % 8;
        if val {
            self.as_mut()[pos] |= 1 << offset;
        } else {
            self.as_mut()[pos] &= !(1 << offset);
        }
    }
}

impl<T: AsRef<[u8]>> BitSlice for T {
    fn bit_get(&self, idx: usize) -> bool {
        assert!(idx < self.bit_len());
        let pos = idx / 8;
        let offset = idx % 8;
        (self.as_ref()[pos] & (1 << offset)) > 0
    }
    fn bit_len(&self) -> usize {
        self.as_ref().len() * 8
    }
}

pub struct Bloom {
    /// data of filter in bits. The last element is k
    filter: Vec<u8>,
}

impl Bloom {
    fn delta(h: u64) -> u64 {
        (h >> 34) | (h << 30)
    }

    pub fn from_keys(keys: &[u64], fpp: f64) -> Self {
        assert!(fpp < 1.0);
        let n = keys.len() as f64;
        let m = -(n * fpp.ln()) / std::f64::consts::LN_2.powi(2);

        let k = m / n * std::f64::consts::LN_2.powi(2);
        let k = (k.ceil() as u8).min(15).max(1);
        let mut filter = vec![0; (m.ceil() as usize + 7) / 8 + 1];
        *filter.last_mut().unwrap() = k;

        let limit = (filter.len() - 1) * 8;

        for h in keys {
            let mut h = *h;
            let delta = Self::delta(h);
            for _ in 0..k {
                let bit_pos = (h as usize) % limit;
                filter.bit_set(bit_pos, true);
                h = h.wrapping_add(delta);
            }
        }
        Self { filter }
    }

    pub fn may_contain(&self, mut h: u64) -> bool {
        let delta = Self::delta(h);
        let k = *self.filter.last().unwrap();
        let limit = (self.filter.len() - 1) * 8;
        for _ in 0..k {
            let bit_pos = (h as usize) % limit;
            if !self.filter.bit_get(bit_pos) {
                return false;
            }
            h = h.wrapping_add(delta);
        }
        true
    }

    pub fn encode(self) -> Bytes {
        Bytes::from(self.filter)
    }

    pub fn decode(buf: &[u8]) -> Self {
        Self {
            filter: Vec::from(buf),
        }
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use xxhash_rust::xxh3;
    #[test]
    fn test_small_bloom_filter() {
        let hash: Vec<_> = vec![b"key1".to_vec(), b"key2".to_vec()]
            .into_iter()
            .map(|x| xxh3::xxh3_64(&x))
            .collect();

        let bloom = Bloom::from_keys(&hash, 0.1);

        let check_hash: Vec<_> = vec![
            b"key1".to_vec(),
            b"key2".to_vec(),
            b"key3".to_vec(),
            b"key4".to_vec(),
        ]
        .into_iter()
        .map(|x| xxh3::xxh3_64(&x))
        .collect();

        assert!(bloom.may_contain(check_hash[0]));
        assert!(bloom.may_contain(check_hash[1]));
        assert!(!bloom.may_contain(check_hash[2]));
        assert!(!bloom.may_contain(check_hash[3]));
    }

    #[test]
    fn test_fpp_bloom_filter() {
        let hash: Vec<_> = (0..1000)
            .into_iter()
            .map(|x| format!("key_{x}"))
            .map(|x| xxh3::xxh3_64(x.as_bytes()))
            .collect();
        let fpp = 0.01;
        let bloom = Bloom::from_keys(&hash, fpp);
        for u in hash {
            assert!(bloom.may_contain(u));
        }

        let cnt = (20000..30000)
            .into_iter()
            .map(|x| format!("key_{x}"))
            .map(|x| xxh3::xxh3_64(x.as_bytes()))
            .filter(|x| bloom.may_contain(*x))
            .count();
        assert!(cnt <= (10000.0 * (fpp + 0.005)).ceil() as usize)
    }
}
