use std::fmt::{self, Debug};
use std::ops::BitXor;
use std::str::FromStr;
use std::string::ToString;

use hex::FromHex;
use rand::Rng;
use sha1::{Digest, Sha1};

use crate::error::Error;

#[derive(Clone, Copy, PartialEq, PartialOrd, Eq, Ord, Hash)]
pub struct Key([u8; 20]);

impl Key {
    pub fn rand() -> Self {
        Key(rand::thread_rng().gen::<[u8; 20]>())
    }

    pub fn builder() -> KeyBuilder {
        KeyBuilder {
            hasher: Sha1::new(),
        }
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    /// The log2 of the distance with `other`, between 0 and 160.
    pub fn dist_log2(&self, other: Key) -> usize {
        160 - self.bitxor(other).leading_zeros()
    }

    pub(crate) fn leading_zeros(&self) -> usize {
        let mut ret = 0;
        for i in 0..self.0.len() {
            if self.0[i] == 0 {
                ret += 8
            } else {
                return ret + self.0[i].leading_zeros() as usize;
            }
        }
        ret
    }

    /// Constructs a new random `Key` where the first `index` bits are 0.
    ///
    /// Equivalently, we can say that the key belongs to the range
    /// `[2^(160 - index - 1), 2^(160 - index)]`.
    /// If `index` >= 160, then all the bits are zeros.
    pub(crate) fn rand_in_range(index: usize) -> Self {
        let mut ret = Key::rand();
        let bytes = index / 8;
        let bit = index % 8;
        for i in 0..bytes {
            ret.0[i] = 0;
        }
        if bytes < ret.0.len() {
            ret.0[bytes] &= 0xFF >> (bit);
            ret.0[bytes] |= 1 << (8 - bit - 1);
        }
        ret
    }
}

impl BitXor for Key {
    type Output = Self;

    fn bitxor(mut self, rhs: Self) -> Self::Output {
        self.0
            .iter_mut()
            .zip(rhs.0.iter())
            .for_each(|(x, y)| *x = *x ^ *y);
        self
    }
}

impl Debug for Key {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Key({})", self.to_string())
    }
}

impl FromStr for Key {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let decoded = <[u8; 20]>::from_hex(s).map_err(|_| Error::ParseKey(s.into()))?;
        Ok(Key(decoded))
    }
}

impl ToString for Key {
    fn to_string(&self) -> String {
        hex::encode(self.0)
    }
}

pub struct KeyBuilder {
    hasher: Sha1,
}

impl KeyBuilder {
    pub fn write_bytes(&mut self, bytes: &[u8]) {
        self.hasher.update(bytes);
    }

    pub fn finish(self) -> Key {
        Key(self.hasher.finalize().into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn key_basics() {
        let a = Key::rand_in_range(10);
        let b = Key::rand_in_range(80);
        let c = Key::rand();
        assert!(a > b);
        assert!(a.dist_log2(b) == 150);
        assert!(b.dist_log2(a) == 150);
        assert!(a.dist_log2(a) == 0);
        assert!(b.to_string().as_str()[..20] == "0".repeat(20));
        assert!(a ^ a == Key::rand_in_range(160));
        assert!(a ^ b == b ^ a);
        assert!((a ^ b) ^ (b ^ c) == a ^ c);

        let mut kb = Key::builder();
        kb.write_bytes(b"hello world");
        let k = kb.finish();
        assert_eq!("2aae6c35c94fcfb415dbe95f408b9ce91ee846ed", k.to_string());
    }
}
