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

    /// Constructs a new random `Key` from `[2^(20 - index - 1), 2^(20 - index))`.
    pub(crate) fn rand_in_range(index: usize) -> Self {
        let mut ret = Key::rand();
        let bytes = index / 8;
        let bit = index % 8;
        for i in 0..bytes {
            ret.0[i] = 0;
        }
        ret.0[bytes] &= 0xFF >> (bit);
        ret.0[bytes] |= 1 << (8 - bit - 1);
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
        let mut b = Key::builder();
        b.write_bytes(b"hello world");
        let k = b.finish();
        assert_eq!("2aae6c35c94fcfb415dbe95f408b9ce91ee846ed", k.to_string());

        let a = Key::rand_in_range(80);
        assert_eq!("0".repeat(20), &a.to_string()[..20]);

        let b = Key::rand();
        assert_eq!(a ^ b, a.bitxor(b));
    }
}
