use more_asserts::*;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::fmt::*;
use std::ops::Add;

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

// ZettaCache types

#[derive(Serialize, Deserialize, Debug, Copy, Clone, PartialEq, Eq, Ord, PartialOrd)]
pub struct DiskLocation {
    // note: will need to add disk ID to support multiple disks
    pub offset: u64,
}
impl Add<u64> for DiskLocation {
    type Output = DiskLocation;
    fn add(self, rhs: u64) -> Self::Output {
        DiskLocation {
            offset: self.offset + rhs,
        }
    }
}
impl Add<usize> for DiskLocation {
    type Output = DiskLocation;
    fn add(self, rhs: usize) -> Self::Output {
        self + rhs as u64
    }
}

#[derive(Serialize, Deserialize, Debug, Copy, Clone, PartialEq, Eq, Ord, PartialOrd)]
pub struct Extent {
    pub location: DiskLocation,
    // XXX for space efficiency and clarity, make this u32? since it's stored on disk?
    pub size: usize, // note: since we read it into contiguous memory, it can't be more than usize
}

impl Extent {
    pub fn range(&self, relative_offset: usize, size: usize) -> Extent {
        assert_ge!(self.size, relative_offset + size);
        Extent {
            location: self.location + relative_offset,
            size,
        }
    }
}

#[derive(Serialize, Deserialize, Default, Debug, Copy, Clone, PartialEq, Eq, Ord, PartialOrd)]
pub struct CheckpointID(pub u64);
impl CheckpointID {
    pub fn next(&self) -> CheckpointID {
        CheckpointID(self.0 + 1)
    }
}

#[derive(Serialize, Deserialize, Default, Debug, Copy, Clone, PartialEq, Eq, Ord, PartialOrd)]
pub struct Atime(pub u64);
impl Atime {
    pub fn next(&self) -> Atime {
        Atime(self.0 + 1)
    }
}
