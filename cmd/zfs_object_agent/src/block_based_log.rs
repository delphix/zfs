use crate::base_types::*;
use crate::block_access::BlockAccess;
use crate::extent_allocator::ExtentAllocator;
use crate::zettacache::DiskLocation;
use crate::zettacache::Extent;
use anyhow::Context;
use async_stream::stream;
use futures_core::Stream;
use log::*;
use more_asserts::*;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::cmp::max;
use std::collections::BTreeMap;
use std::ops::Sub;
use std::sync::Arc;

// XXX maybe this is wasteful for the smaller logs?
const DEFAULT_EXTENT_SIZE: usize = 128 * 1024 * 1024;
//const READ_IO_SIZE: usize = 1 * 1024 * 1024;

#[derive(Serialize, Deserialize, Debug, Default, Clone)]
pub struct BlockBasedLogPhys {
    // XXX on-disk format could just be array of extents; offset can be derived
    // from size of previous extents We do need the btree in RAM though so that
    // we can do random reads on the Index (unless the ChunkSummary points
    // directly to the on-disk location)
    extents: BTreeMap<LogOffset, Extent>, // offset -> disk_location, size
    next_chunk: ChunkID,
    next_chunk_offset: LogOffset, // logical byte offset of next chunk to write
}

pub trait BlockBasedLogEntry: 'static + OnDisk + Copy + Clone + Unpin + Send + Sync {}

pub struct BlockBasedLog<T: BlockBasedLogEntry> {
    block_access: Arc<BlockAccess>,
    extent_allocator: Arc<ExtentAllocator>,
    phys: BlockBasedLogPhys,
    pending_entries: Vec<T>,
}

#[derive(Serialize, Deserialize, Debug)]
struct BlockBasedLogChunk<T: BlockBasedLogEntry> {
    chunk: ChunkID,
    #[serde(bound(deserialize = "Vec<T>: DeserializeOwned"))]
    entries: Vec<T>,
}

impl<T: BlockBasedLogEntry> BlockBasedLog<T> {
    pub fn open(
        block_access: Arc<BlockAccess>,
        extent_allocator: Arc<ExtentAllocator>,
        phys: &BlockBasedLogPhys,
    ) -> BlockBasedLog<T> {
        for (_offset, extent) in phys.extents.iter() {
            extent_allocator.claim(extent);
        }
        BlockBasedLog {
            block_access,
            extent_allocator,
            phys: phys.clone(),
            pending_entries: Vec::new(),
        }
    }

    pub fn get_phys(&self) -> BlockBasedLogPhys {
        self.phys.clone()
    }

    pub fn append(&mut self, entry: T) {
        self.pending_entries.push(entry);
        // XXX if too many pending, initiate flush?
    }

    pub async fn flush(&mut self) {
        if self.pending_entries.is_empty() {
            return;
        }

        // XXX add name of this log for debug purposes?
        debug!(
            "flushing {} entries to BlockBasedLog",
            self.pending_entries.len()
        );

        let chunk = BlockBasedLogChunk {
            chunk: self.phys.next_chunk,
            entries: self.pending_entries.split_off(0),
        };

        let mut extent = self.next_write_location();
        let raw_chunk = self.block_access.json_chunk_to_raw(&chunk);
        let raw_size = raw_chunk.len();
        if raw_size > extent.size {
            // free the unused tail of this extent
            self.extent_allocator.free(extent);
            let capacity = match self.phys.extents.iter_mut().next_back() {
                Some((last_offset, last_extent)) => {
                    last_extent.size -= extent.size;
                    LogOffset(last_offset.0 + last_extent.size as u64)
                }
                None => LogOffset(0),
            };

            extent = self
                .extent_allocator
                .allocate(raw_size, max(raw_size, DEFAULT_EXTENT_SIZE));
            self.phys.extents.insert(capacity, extent);
            assert_ge!(extent.size, raw_size);
        }
        trace!("writing {:?} to {:?}", chunk.chunk, extent);
        self.block_access
            .write_raw(extent.location, raw_chunk)
            .await;
        self.phys.next_chunk = self.phys.next_chunk.next();
        self.phys.next_chunk_offset.0 += raw_size as u64;
    }

    // XXX not sure how I feel about using Extent to represent something that
    // isn't something we got from allocate_extent() and stored in
    // BlockBasedLogPhys::extents
    fn next_write_location(&self) -> Extent {
        match self.phys.extents.iter().next_back() {
            Some((offset, extent)) => {
                // There shouln't be any extents after the last (partially-full) one.
                assert_ge!(self.phys.next_chunk_offset, offset);
                let offset_within_extent = self.phys.next_chunk_offset.0 - offset.0;
                // The last extent should go at least to the end of the chunks.
                assert_le!(offset_within_extent as usize, extent.size);
                Extent {
                    location: DiskLocation {
                        offset: extent.location.offset + offset_within_extent,
                    },
                    size: extent.size - offset_within_extent as usize,
                }
            }
            None => Extent {
                location: DiskLocation { offset: 0 },
                size: 0,
            },
        }
    }

    /*
    fn capacity(&self) -> LogOffset {
        match self.phys.extents.iter().next_back() {
            Some((offset, extent)) => offset + extent.size,
            None => 0,
        }
    }
    */

    /// Iterates the on-disk state; panics if there are pending changes.
    pub fn iter(&self) -> impl Stream<Item = T> {
        assert!(self.pending_entries.is_empty());
        // XXX possible to do this without copying `extents`?  Not a huge deal I guess since it should be small.
        let extents = self.phys.extents.clone();
        let block_access = self.block_access.clone();
        let num_chunks = self.phys.next_chunk;
        stream! {
            let mut chunk_id = ChunkID(0);
            for (_offset, extent) in extents.iter() {
                // XXX probably want to do smaller i/os than the entire extent (up to 128MB)
                // XXX also want to issue a few in parallel?
                // XXX if this extent is at the end, we don't need to read the
                // unused part of it (based on next_chunk_offset)
                let extent_bytes = block_access.read_raw(extent.location, extent.size).await;
                // XXX handle checksum error here
                let mut total_consumed = 0;
                while total_consumed < extent_bytes.len() {
                    let chunk_location = DiskLocation {
                        offset: extent.location.offset + total_consumed as u64,
                    };
                    trace!("decoding {:?} from {:?}", chunk_id, chunk_location);
                    let (chunk, consumed): (BlockBasedLogChunk<T>, usize) = block_access
                        .json_chunk_from_raw(&extent_bytes[total_consumed..])
                        .context(format!("{:?} at {:?}", chunk_id, chunk_location,))
                        .unwrap();
                    assert_eq!(chunk.chunk, chunk_id);
                    for entry in chunk.entries {
                        yield entry;
                    }
                    chunk_id = chunk_id.next();
                    total_consumed += consumed;
                    if chunk_id == num_chunks {
                        break;
                    }
                }
            }
        }
    }
}

#[derive(Serialize, Deserialize, Default, Debug, Copy, Clone, PartialEq, Eq, Ord, PartialOrd)]
pub struct LogOffset(u64);

impl Sub<LogOffset> for LogOffset {
    type Output = u64;

    fn sub(self, rhs: LogOffset) -> Self::Output {
        self.0 - rhs.0
    }
}

#[derive(Serialize, Deserialize, Default, Debug, Copy, Clone, PartialEq, Eq, Ord, PartialOrd)]
pub struct ChunkID(u64);
impl ChunkID {
    pub fn next(&self) -> ChunkID {
        ChunkID(self.0 + 1)
    }
}
