use crate::block_access::*;
use crate::block_based_log::*;
use crate::extent_allocator::ExtentAllocator;
use crate::range_tree::RangeTree;
use crate::{
    base_types::OnDisk,
    block_based_log::{BlockBasedLog, BlockBasedLogEntry},
};
use futures::future;
use futures::stream::*;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Debug, Serialize, Deserialize, Copy, Clone)]
struct SpaceMapExtent {
    offset: u64,
    size: u64,
}

#[derive(Debug, Serialize, Deserialize, Copy, Clone)]
enum SpaceMapEntry {
    Alloc(SpaceMapExtent),
    Free(SpaceMapExtent),
}
impl OnDisk for SpaceMapEntry {}
impl BlockBasedLogEntry for SpaceMapEntry {}

pub struct SpaceMap {
    log: BlockBasedLog<SpaceMapEntry>,
    coverage: SpaceMapExtent,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct SpaceMapPhys {
    log: BlockBasedLogPhys,
    coverage: SpaceMapExtent,
}
impl OnDisk for SpaceMapPhys {}

impl SpaceMapPhys {
    pub fn new(offset: u64, size: u64) -> SpaceMapPhys {
        SpaceMapPhys {
            log: Default::default(),
            coverage: SpaceMapExtent { offset, size },
        }
    }
}

impl SpaceMap {
    pub fn open(
        block_access: Arc<BlockAccess>,
        extent_allocator: Arc<ExtentAllocator>,
        phys: SpaceMapPhys,
    ) -> SpaceMap {
        SpaceMap {
            log: BlockBasedLog::open(block_access, extent_allocator, phys.log),
            coverage: phys.coverage,
        }
    }

    /// Returns rangetree of allocatable segments
    pub async fn load(&self) -> RangeTree {
        let mut rt = RangeTree::new();
        rt.add(self.coverage.offset, self.coverage.size);

        self.log
            .iter()
            .for_each(|entry| {
                match entry {
                    SpaceMapEntry::Alloc(extent) => rt.remove(extent.offset, extent.size),
                    SpaceMapEntry::Free(extent) => rt.add(extent.offset, extent.size),
                }
                future::ready(())
            })
            .await;
        rt
    }

    pub fn alloc(&mut self, offset: u64, size: u64) {
        self.log
            .append(SpaceMapEntry::Alloc(SpaceMapExtent { offset, size }));
    }

    pub fn free(&mut self, offset: u64, size: u64) {
        self.log
            .append(SpaceMapEntry::Free(SpaceMapExtent { offset, size }));
    }

    pub async fn flush(&mut self) -> SpaceMapPhys {
        SpaceMapPhys {
            log: self.log.flush().await,
            coverage: self.coverage,
        }
    }
}
