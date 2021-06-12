use crate::range_tree::*;
use crate::zettacache::DiskLocation;
use crate::zettacache::Extent;
use log::*;
use more_asserts::*;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Copy, Clone)]
pub struct ExtentAllocatorPhys {
    pub first_valid_offset: u64,
    pub last_valid_offset: u64,
}

pub struct ExtentAllocator {
    state: std::sync::Mutex<ExtentAllocatorState>,
}

struct ExtentAllocatorState {
    phys: ExtentAllocatorPhys,
    metadata_allocatable: RangeTree,
}

/// Note: no on-disk representation.  Allocated extents must be .claim()ed
/// before .allocate()ing additional extents.
impl ExtentAllocator {
    pub fn open(phys: &ExtentAllocatorPhys) -> ExtentAllocator {
        let mut metadata_allocatable = RangeTree::new();
        metadata_allocatable.add(
            phys.first_valid_offset,
            phys.last_valid_offset - phys.first_valid_offset,
        );
        ExtentAllocator {
            state: std::sync::Mutex::new(ExtentAllocatorState {
                phys: *phys,
                metadata_allocatable,
            }),
        }
    }

    pub fn get_phys(&self) -> ExtentAllocatorPhys {
        self.state.lock().unwrap().phys.clone()
    }

    pub fn claim(&self, extent: &Extent) {
        self.state
            .lock()
            .unwrap()
            .metadata_allocatable
            .remove(extent.location.offset, extent.size as u64);
    }

    pub fn allocate(&self, min_size: usize, max_size: usize) -> Extent {
        let mut state = self.state.lock().unwrap();
        let max_size64 = max_size as u64;

        // find first segment where this fits, or largest free segment.
        // XXX keep size-sorted tree as well?
        let mut best_size = 0;
        let mut best_offset = 0;
        for (offset, size) in state.metadata_allocatable.iter() {
            if *size > best_size {
                best_size = *size;
                best_offset = *offset;
            }
            if *size >= max_size64 {
                best_size = max_size64;
                break;
            }
        }
        assert_le!(best_size, max_size64);

        if best_size < min_size as u64 {
            debug!(
                "no extents of at least {} bytes available; overwriting {} bytes of data blocks at offset {}",
                min_size, max_size, state.phys.last_valid_offset
            );
            best_offset = state.phys.last_valid_offset;
            best_size = max_size64;
            state.phys.last_valid_offset += max_size64;
        } else {
            // remove segment from allocatable
            state.metadata_allocatable.remove(best_offset, best_size);
        }

        let this = Extent {
            location: DiskLocation {
                offset: best_offset,
            },
            size: best_size as usize,
        };
        debug!("allocated {:?} for min={} max={}", this, min_size, max_size);
        this
    }

    /// extent can be a subset of what was previously allocated
    pub fn free(&self, extent: Extent) {
        let mut state = self.state.lock().unwrap();
        state
            .metadata_allocatable
            .add(extent.location.offset, extent.size as u64);
    }
}
