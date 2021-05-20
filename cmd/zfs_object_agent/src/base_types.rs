use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::fmt::*;

/*
 * Things that are stored on disk.
 */
pub trait OnDisk: Serialize + DeserializeOwned {}

#[derive(Serialize, Deserialize, Debug, Copy, Clone, PartialEq, Eq, Ord, PartialOrd)]
pub struct TXG(pub u64);
impl OnDisk for TXG {}
impl Display for TXG {
    fn fmt(&self, f: &mut Formatter) -> Result {
        write!(f, "{:020}", self.0)
    }
}

#[derive(Serialize, Deserialize, Debug, Copy, Clone, PartialEq, Eq, Ord, PartialOrd, Hash)]
pub struct PoolGUID(pub u64);
impl OnDisk for PoolGUID {}
impl Display for PoolGUID {
    fn fmt(&self, f: &mut Formatter) -> Result {
        write!(f, "{:020}", self.0)
    }
}

#[derive(Serialize, Deserialize, Debug, Copy, Clone, PartialEq, Eq, Ord, PartialOrd, Hash)]
pub struct ObjectID(pub u64);
impl OnDisk for ObjectID {}
impl Display for ObjectID {
    fn fmt(&self, f: &mut Formatter) -> Result {
        write!(f, "{:020}", self.0)
    }
}
impl ObjectID {
    pub fn next(&self) -> ObjectID {
        ObjectID(self.0 + 1)
    }
}

#[derive(Serialize, Deserialize, Debug, Copy, Clone, PartialEq, Eq, Ord, PartialOrd, Hash)]
pub struct BlockID(pub u64);
impl OnDisk for BlockID {}
impl Display for BlockID {
    fn fmt(&self, f: &mut Formatter) -> Result {
        write!(f, "{}", self.0)
    }
}
impl BlockID {
    pub fn next(&self) -> BlockID {
        BlockID(self.0 + 1)
    }
}
