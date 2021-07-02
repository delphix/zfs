use crate::base_types::*;
use crate::base_types::{Extent, OnDisk};
use crate::block_access::*;
use crate::extent_allocator::ExtentAllocator;
use crate::range_tree::RangeTree;
use crate::space_map::SpaceMap;
use crate::space_map::SpaceMapPhys;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

pub struct BlockAllocator {
    space_map: SpaceMap,
    allocatable: RangeTree,
    allocating: RangeTree,
    freeing: RangeTree,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct BlockAllocatorPhys {
    allocatable: SpaceMapPhys,
}
impl OnDisk for BlockAllocatorPhys {}

impl BlockAllocatorPhys {
    // XXX eventually change this to indicate the size of each of the disks that we're managing
    pub fn new(offset: u64, size: u64) -> BlockAllocatorPhys {
        BlockAllocatorPhys {
            allocatable: SpaceMapPhys::new(offset, size),
        }
    }
}

impl BlockAllocator {
    pub async fn open(
        block_access: Arc<BlockAccess>,
        extent_allocator: Arc<ExtentAllocator>,
        phys: BlockAllocatorPhys,
    ) -> BlockAllocator {
        let space_map = SpaceMap::open(
            block_access.clone(),
            extent_allocator.clone(),
            phys.allocatable,
        );
        BlockAllocator {
            allocatable: space_map.load().await,
            space_map,
            allocating: Default::default(),
            freeing: Default::default(),
        }
    }

    pub async fn flush(&mut self) -> BlockAllocatorPhys {
        // Space freed during this checkpoint is now available for reallocation.
        for (start, size) in self.freeing.iter() {
            self.space_map.free(*start, *size);
            self.allocatable.add(*start, *size);
        }
        self.freeing.clear();
        for (start, size) in self.allocating.iter() {
            self.space_map.alloc(*start, *size);
        }
        self.allocating.clear();

        BlockAllocatorPhys {
            allocatable: self.space_map.flush().await,
        }
    }

    pub fn allocate(&mut self, size: u64) -> Option<Extent> {
        // find first segment where this fits, or largest free segment.
        // XXX keep size-sorted tree as well?
        for (allocatable_offset_ref, allocatable_size_ref) in self.allocatable.iter() {
            let allocatable_offset = *allocatable_offset_ref;
            let allocatable_size = *allocatable_size_ref;

            if allocatable_size >= size {
                self.allocatable.remove(allocatable_offset, size);
                self.allocating.add(allocatable_offset, size);
                return Some(Extent {
                    location: DiskLocation {
                        offset: allocatable_offset,
                    },
                    size: size as usize,
                });
            }
        }
        None
    }

    pub fn free(&mut self, extent: &Extent) {
        // XXX assert that it's not in allocating or allocatable
        self.freeing.add(extent.location.offset, extent.size as u64);
    }
}
