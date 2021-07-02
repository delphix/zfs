use crate::base_types::*;
use crate::block_access::*;
use crate::block_based_log::*;
use crate::extent_allocator::ExtentAllocator;
use crate::zettacache::AtimeHistogramPhys;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Serialize, Deserialize, Debug, Copy, Clone, PartialEq, Eq, Ord, PartialOrd)]
pub struct IndexKey {
    pub guid: PoolGUID,
    pub block: BlockID,
}

#[derive(Serialize, Deserialize, Debug, Copy, Clone, PartialEq, Eq, Ord, PartialOrd)]
pub struct IndexValue {
    pub location: DiskLocation,
    // XXX remove this and figure out based on which slab it's in?  However,
    // currently we need to return the right buffer size to the kernel, and it
    // isn't passing us the expected read size.  So we need to change some
    // interfaces to make that work right.
    pub size: usize,
    pub atime: Atime,
}

#[derive(Debug, Serialize, Deserialize, Copy, Clone)]
pub struct IndexEntry {
    pub key: IndexKey,
    pub value: IndexValue,
}
impl OnDisk for IndexEntry {}
impl BlockBasedLogEntry for IndexEntry {}

pub struct ZettaCacheIndex {
    pub atime_histogram: AtimeHistogramPhys,
    pub log: BlockBasedLogWithSummary<IndexEntry>,
}

#[derive(Serialize, Deserialize, Debug, Default)]
pub struct ZettaCacheIndexPhys {
    atime_histogram: AtimeHistogramPhys,
    log: BlockBasedLogWithSummaryPhys,
}

impl ZettaCacheIndex {
    pub async fn open(
        block_access: Arc<BlockAccess>,
        extent_allocator: Arc<ExtentAllocator>,
        phys: ZettaCacheIndexPhys,
    ) -> Self {
        Self {
            atime_histogram: phys.atime_histogram,
            log: BlockBasedLogWithSummary::open(block_access, extent_allocator, phys.log).await,
        }
    }

    pub async fn flush(&mut self) -> ZettaCacheIndexPhys {
        ZettaCacheIndexPhys {
            atime_histogram: self.atime_histogram.clone(),
            log: self.log.flush().await,
        }
    }

    pub fn append(&mut self, entry: IndexEntry) {
        self.atime_histogram.insert(entry.value.atime);
        self.log.append(entry);
    }

    pub fn clear(&mut self) {
        self.atime_histogram.clear();
        self.log.clear();
    }
}
