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
pub struct Txg(pub u64);
impl OnDisk for Txg {}
impl Display for Txg {
    fn fmt(&self, f: &mut Formatter) -> Result {
        write!(f, "{:020}", self.0)
    }
}

#[derive(Serialize, Deserialize, Debug, Copy, Clone, PartialEq, Eq, Ord, PartialOrd, Hash)]
pub struct PoolGuid(pub u64);
impl OnDisk for PoolGuid {}
impl Display for PoolGuid {
    fn fmt(&self, f: &mut Formatter) -> Result {
        write!(f, "{:020}", self.0)
    }
}

#[derive(Serialize, Deserialize, Debug, Copy, Clone, PartialEq, Eq, Ord, PartialOrd, Hash)]
pub struct ObjectId(pub u64);
impl OnDisk for ObjectId {}
impl Display for ObjectId {
    fn fmt(&self, f: &mut Formatter) -> Result {
        write!(f, "{:020}", self.0)
    }
}
impl ObjectId {
    pub fn next(&self) -> ObjectId {
        ObjectId(self.0 + 1)
    }
}

#[derive(Serialize, Deserialize, Debug, Copy, Clone, PartialEq, Eq, Ord, PartialOrd, Hash)]
pub struct BlockId(pub u64);
impl OnDisk for BlockId {}
impl Display for BlockId {
    fn fmt(&self, f: &mut Formatter) -> Result {
        write!(f, "{}", self.0)
    }
}
impl BlockId {
    pub fn next(&self) -> BlockId {
        BlockId(self.0 + 1)
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
pub struct CheckpointId(pub u64);
impl CheckpointId {
    pub fn next(&self) -> CheckpointId {
        CheckpointId(self.0 + 1)
    }
}

#[derive(Serialize, Deserialize, Default, Debug, Copy, Clone, PartialEq, Eq, Ord, PartialOrd)]
pub struct Atime(pub u64);
impl Atime {
    pub fn next(&self) -> Atime {
        Atime(self.0 + 1)
    }
}
