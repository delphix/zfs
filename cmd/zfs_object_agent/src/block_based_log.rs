use crate::base_types::*;
use crate::block_access::BlockAccess;
use crate::extent_allocator::ExtentAllocator;
use anyhow::Context;
use async_stream::stream;
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use futures_core::Stream;
use log::*;
use more_asserts::*;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::cmp::max;
use std::cmp::min;
use std::collections::BTreeMap;
use std::fmt::Debug;
use std::ops::Add;
use std::ops::Bound::*;
use std::ops::Sub;
use std::sync::Arc;
use std::time::Instant;

// XXX maybe this is wasteful for the smaller logs?
const DEFAULT_EXTENT_SIZE: usize = 128 * 1024 * 1024;
//const READ_IO_SIZE: usize = 1 * 1024 * 1024;
const ENTRIES_PER_CHUNK: usize = 100;

#[derive(Serialize, Deserialize, Debug, Default, Clone)]
pub struct BlockBasedLogPhys {
    // XXX on-disk format could just be array of extents; offset can be derived
    // from size of previous extents. We do need the btree in RAM though so that
    // we can do random reads on the Index (unless the ChunkSummary points
    // directly to the on-disk location)
    extents: BTreeMap<LogOffset, Extent>, // offset -> disk_location, size
    next_chunk: ChunkID,
    next_chunk_offset: LogOffset, // logical byte offset of next chunk to write
    num_entries: u64,
}

#[derive(Serialize, Deserialize, Debug, Default, Clone)]
pub struct BlockBasedLogWithSummaryPhys {
    this: BlockBasedLogPhys,
    chunk_summary: BlockBasedLogPhys,
}

pub trait BlockBasedLogEntry: 'static + OnDisk + Copy + Clone + Unpin + Send + Sync {}

#[derive(Debug, Serialize, Deserialize, Copy, Clone)]
pub struct BlockBasedLogChunkSummaryEntry<T: BlockBasedLogEntry> {
    offset: LogOffset,
    #[serde(bound(deserialize = "T: DeserializeOwned"))]
    first_entry: T,
}
impl<T: BlockBasedLogEntry> OnDisk for BlockBasedLogChunkSummaryEntry<T> {}
impl<T: BlockBasedLogEntry> BlockBasedLogEntry for BlockBasedLogChunkSummaryEntry<T> {}

pub struct BlockBasedLog<T: BlockBasedLogEntry> {
    block_access: Arc<BlockAccess>,
    extent_allocator: Arc<ExtentAllocator>,
    phys: BlockBasedLogPhys,
    pending_entries: Vec<T>,
}

pub struct BlockBasedLogWithSummary<T: BlockBasedLogEntry> {
    this: BlockBasedLog<T>,
    chunk_summary: BlockBasedLog<BlockBasedLogChunkSummaryEntry<T>>,
    chunks: Vec<BlockBasedLogChunkSummaryEntry<T>>,
}

#[derive(Serialize, Deserialize, Debug)]
struct BlockBasedLogChunk<T: BlockBasedLogEntry> {
    id: ChunkID,
    offset: LogOffset,
    #[serde(bound(deserialize = "Vec<T>: DeserializeOwned"))]
    entries: Vec<T>,
}

impl<T: BlockBasedLogEntry> BlockBasedLog<T> {
    pub fn open(
        block_access: Arc<BlockAccess>,
        extent_allocator: Arc<ExtentAllocator>,
        phys: BlockBasedLogPhys,
    ) -> BlockBasedLog<T> {
        for (_offset, extent) in phys.extents.iter() {
            extent_allocator.claim(extent);
        }
        BlockBasedLog {
            block_access,
            extent_allocator,
            phys,
            pending_entries: Vec::new(),
        }
    }

    pub fn get_phys(&self) -> BlockBasedLogPhys {
        self.phys.clone()
    }

    pub fn len(&self) -> u64 {
        self.phys.num_entries + self.pending_entries.len() as u64
    }

    /// Size of the on-disk representation
    pub fn num_bytes(&self) -> u64 {
        self.phys.next_chunk_offset.0
    }

    pub fn append(&mut self, entry: T) {
        self.pending_entries.push(entry);
        // XXX if too many pending, initiate flush?
    }

    pub async fn flush(&mut self) {
        self.flush_impl(|_, _, _| {}).await
    }

    async fn flush_impl<F>(&mut self, mut new_chunk_fn: F)
    where
        F: FnMut(ChunkID, LogOffset, T),
    {
        if self.pending_entries.is_empty() {
            return;
        }

        let writes_stream = FuturesUnordered::new();
        for pending_entries_chunk in self.pending_entries.chunks(ENTRIES_PER_CHUNK) {
            let chunk = BlockBasedLogChunk {
                id: self.phys.next_chunk,
                offset: self.phys.next_chunk_offset,
                entries: pending_entries_chunk.to_owned(),
            };

            let first_entry = *chunk.entries.first().unwrap();

            let mut extent = self.next_write_location();
            let raw_chunk = self.block_access.json_chunk_to_raw(&chunk);
            let raw_size = raw_chunk.len();
            if raw_size > extent.size {
                // free the unused tail of this extent
                self.extent_allocator.free(&extent);
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
            // XXX add name of this log for debug purposes?
            debug!(
                "flushing BlockBasedLog: writing {:?} ({:?}) with {} entries ({} bytes) to {:?}",
                chunk.id,
                chunk.offset,
                chunk.entries.len(),
                raw_chunk.len(),
                extent.location,
            );
            // XXX would be better to aggregate lots of buffers into one write
            writes_stream.push(self.block_access.write_raw(extent.location, raw_chunk));

            new_chunk_fn(chunk.id, chunk.offset, first_entry);

            self.phys.num_entries += chunk.entries.len() as u64;
            self.phys.next_chunk = self.phys.next_chunk.next();
            self.phys.next_chunk_offset.0 += raw_size as u64;
        }
        writes_stream.for_each(|_| async move {}).await;
        self.pending_entries.truncate(0);
    }

    pub fn clear(&mut self) {
        self.pending_entries.clear();
        for extent in self.phys.extents.values() {
            self.extent_allocator.free(extent);
        }
        self.phys = BlockBasedLogPhys::default();
    }

    fn next_write_location(&self) -> Extent {
        match self.phys.extents.iter().next_back() {
            Some((offset, extent)) => {
                // There shouldn't be any extents after the last (partially-full) one.
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

    /// Iterates the on-disk state; panics if there are pending changes.
    pub fn iter(&self) -> impl Stream<Item = T> {
        assert!(self.pending_entries.is_empty());
        // XXX is it possible to do this without copying self.phys.extents?  Not
        // a huge deal I guess since it should be small.
        let phys = self.phys.clone();
        let block_access = self.block_access.clone();
        let next_chunk_offset = self.phys.next_chunk_offset;
        stream! {
            let mut num_entries = 0;
            let mut chunk_id = ChunkID(0);
            for (offset, extent) in phys.extents.iter() {
                // XXX Probably want to do smaller i/os than the entire extent
                // (which is up to 128MB).  Also want to issue a few in
                // parallel?

                let truncated_extent =
                    extent.range(0, min(extent.size, (next_chunk_offset - *offset) as usize));
                let extent_bytes = block_access.read_raw(truncated_extent).await;
                let mut total_consumed = 0;
                while total_consumed < extent_bytes.len() {
                    let chunk_location = DiskLocation {
                        offset: extent.location.offset + total_consumed as u64,
                    };
                    trace!("decoding {:?} from {:?}", chunk_id, chunk_location);
                    // XXX handle checksum error here
                    let (chunk, consumed): (BlockBasedLogChunk<T>, usize) = block_access
                        .json_chunk_from_raw(&extent_bytes[total_consumed..])
                        .context(format!("{:?} at {:?}", chunk_id, chunk_location,))
                        .unwrap();
                    assert_eq!(chunk.id, chunk_id);
                    for entry in chunk.entries {
                        yield entry;
                        num_entries += 1;
                    }
                    chunk_id = chunk_id.next();
                    total_consumed += consumed;
                    if chunk_id == phys.next_chunk {
                        break;
                    }
                }
            }
            assert_eq!(phys.num_entries, num_entries);
        }
    }
}

impl<T: BlockBasedLogEntry> BlockBasedLogWithSummary<T> {
    pub async fn open(
        block_access: Arc<BlockAccess>,
        extent_allocator: Arc<ExtentAllocator>,
        phys: BlockBasedLogWithSummaryPhys,
    ) -> BlockBasedLogWithSummary<T> {
        let chunk_summary = BlockBasedLog::open(
            block_access.clone(),
            extent_allocator.clone(),
            phys.chunk_summary,
        );

        // load in summary from disk
        let begin = Instant::now();
        let chunks = chunk_summary.iter().collect::<Vec<_>>().await;
        info!(
            "loaded summary of {} chunks ({}KB) in {}ms",
            chunks.len(),
            chunk_summary.num_bytes() / 1024,
            begin.elapsed().as_millis()
        );

        BlockBasedLogWithSummary {
            this: BlockBasedLog::open(block_access.clone(), extent_allocator.clone(), phys.this),
            chunk_summary,
            chunks,
        }
    }

    pub fn get_phys(&self) -> BlockBasedLogWithSummaryPhys {
        BlockBasedLogWithSummaryPhys {
            this: self.this.get_phys(),
            chunk_summary: self.chunk_summary.get_phys(),
        }
    }

    pub fn len(&self) -> u64 {
        self.this.len()
    }

    /// Size of the on-disk representation
    pub fn num_bytes(&self) -> u64 {
        self.this.num_bytes() + self.chunk_summary.num_bytes()
    }

    pub fn append(&mut self, entry: T) {
        self.this.append(entry)
    }

    pub async fn flush(&mut self) {
        let chunks = &mut self.chunks;
        let chunk_summary = &mut self.chunk_summary;
        self.this
            .flush_impl(|chunk_id, offset, first_entry| {
                assert_eq!(ChunkID(chunks.len() as u64), chunk_id);
                let entry = BlockBasedLogChunkSummaryEntry {
                    offset,
                    first_entry,
                };
                chunks.push(entry);
                chunk_summary.append(entry);
            })
            .await;
        // Note: it would be possible to redesign flush_impl() such that it did
        // all the "real" work and then returned a future that would just wait
        // for the i/o to complete.  Then we could be writing to disk both
        // "this" and the summary at the same time.

        self.chunk_summary.flush().await;
    }

    pub fn clear(&mut self) {
        self.this.clear();
        self.chunk_summary.clear();
    }

    /// Iterates the on-disk state; panics if there are pending changes.
    pub fn iter(&self) -> impl Stream<Item = T> {
        self.this.iter()
    }

    /// Returns the exact location/size of this chunk (not the whole contiguous extent)
    fn chunk_extent(&self, chunk_id: usize) -> Extent {
        let chunk_summary = self.chunks[chunk_id];
        let chunk_size = if chunk_id == self.chunks.len() - 1 {
            self.this.phys.next_chunk_offset - chunk_summary.offset
        } else {
            self.chunks[chunk_id + 1].offset - chunk_summary.offset
        } as usize;

        let (extent_offset, extent) = self
            .this
            .phys
            .extents
            .range((Unbounded, Included(chunk_summary.offset)))
            .next_back()
            .unwrap();
        extent.range((chunk_summary.offset - *extent_offset) as usize, chunk_size)
    }

    /// Entries must have been added in sorted order, according to the provided
    /// key-extraction function.  Similar to Vec::binary_search_by_key().
    pub async fn lookup_by_key<B, F>(&self, key: &B, mut f: F) -> Option<T>
    where
        B: Ord + Debug,
        F: FnMut(&T) -> B,
    {
        assert_eq!(ChunkID(self.chunks.len() as u64), self.this.phys.next_chunk);
        // XXX would be nice to also store last entry in the log, so that if we
        // look for something after it, we can return None without reading the
        // last chunk.
        let chunk_id = match self
            .chunks
            .binary_search_by_key(key, |chunk_summary| f(&chunk_summary.first_entry))
        {
            Ok(index) => index,
            Err(index) if index == 0 => return None, // key is before the first chunk, therefore not present
            Err(index) => index - 1,
        };

        let chunk_extent = self.chunk_extent(chunk_id);
        trace!(
            "reading log chunk {} at {:?} to lookup {:?}",
            chunk_id,
            chunk_extent,
            key
        );
        let chunk_bytes = self.this.block_access.read_raw(chunk_extent).await;
        let (chunk, _consumed): (BlockBasedLogChunk<T>, usize) = self
            .this
            .block_access
            .json_chunk_from_raw(&chunk_bytes)
            .unwrap();
        assert_eq!(chunk.id, ChunkID(chunk_id as u64));
        match chunk.entries.binary_search_by_key(key, f) {
            Ok(index) => Some(chunk.entries[index]),
            Err(_) => None,
        }
    }
}

#[derive(Serialize, Deserialize, Default, Debug, Copy, Clone, PartialEq, Eq, Ord, PartialOrd)]
pub struct LogOffset(u64);

impl Add<usize> for LogOffset {
    type Output = LogOffset;

    fn add(self, rhs: usize) -> Self::Output {
        LogOffset(self.0 + rhs as u64)
    }
}
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
