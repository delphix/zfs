use crate::base_types::*;
use crate::block_access::*;
use crate::block_based_log::*;
use crate::extent_allocator::ExtentAllocator;
use crate::extent_allocator::ExtentAllocatorPhys;
use anyhow::Result;
use futures::future;
use futures::stream::*;
use log::*;
use metered::common::*;
use metered::hdr_histogram::AtomicHdrHistogram;
use metered::metered;
use metered::time_source::StdInstantMicros;
use more_asserts::*;
use serde::{Deserialize, Serialize};
use std::collections::btree_map;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;
use std::time::SystemTime;
use tokio::sync::Semaphore;
use tokio::task::JoinHandle;

const SUPERBLOCK_SIZE: usize = 4 * 1024;
//const SUPERBLOCK_MAGIC: u64 = 0x2e11acac4e;
const DEFAULT_CHECKPOINT_RING_BUFFER_SIZE: usize = 1 * 1024 * 1024;
const DEFAULT_SLAB_SIZE: usize = 16 * 1024 * 1024;
const DEFAULT_METADATA_SIZE_PCT: f64 = 10.0; // XXX metadata takes a ton of space due to JSON + no compression.  Can lower this to test forced eviction.
const MAX_PENDING_CHANGES: usize = 100_000; // XXX should be based on RAM usage, ~tens of millions at least

#[derive(Serialize, Deserialize, Debug)]
struct ZettaSuperBlockPhys {
    checkpoint_ring_buffer_size: u32,
    slab_size: u32,
    last_checkpoint_id: CheckpointID,
    last_checkpoint_extent: Extent,
    // XXX put sector size in here too and verify it matches what the disk says now?
    // XXX put disk size in here so we can detect expansion?
}

impl ZettaSuperBlockPhys {
    // XXX when we have multiple disks, will this be stored on a specific one?  Or copied on all of them?
    async fn read(block_access: &BlockAccess) -> Result<ZettaSuperBlockPhys> {
        let raw = block_access
            .read_raw(Extent {
                location: DiskLocation { offset: 0 },
                size: SUPERBLOCK_SIZE,
            })
            .await;
        let (this, _): (Self, usize) = block_access.json_chunk_from_raw(&raw)?;
        debug!("got {:#?}", this);
        Ok(this)
    }

    async fn write(&self, block_access: &BlockAccess) {
        debug!("writing {:#?}", self);
        let raw = block_access.json_chunk_to_raw(self);
        block_access
            .write_raw(DiskLocation { offset: 0 }, raw)
            .await;
    }
}

#[derive(Serialize, Deserialize, Debug)]
struct ZettaCheckpointPhys {
    generation: CheckpointID,
    extent_allocator: ExtentAllocatorPhys,
    last_valid_data_offset: u64, // XXX move to BlockAllocatorPhys
    index: BlockBasedLogWithSummaryPhys,
    chunk_summary: BlockBasedLogPhys,
    operation_log: BlockBasedLogPhys,
}

impl ZettaCheckpointPhys {
    async fn read(block_access: &BlockAccess, extent: Extent) -> ZettaCheckpointPhys {
        let raw = block_access.read_raw(extent).await;
        let (this, _): (Self, usize) = block_access.json_chunk_from_raw(&raw).unwrap();
        debug!("got {:#?}", this);
        this
    }

    /*
    fn all_logs(&self) -> Vec<&BlockBasedLogPhys> {
        vec![&self.index, &self.chunk_summary, &self.operation_log]
    }
    */
}

#[derive(Serialize, Deserialize, Debug, Copy, Clone, PartialEq, Eq, Ord, PartialOrd)]

struct IndexKey {
    guid: PoolGUID,
    block: BlockID,
}

#[derive(Serialize, Deserialize, Debug, Copy, Clone, PartialEq, Eq, Ord, PartialOrd)]
struct IndexValue {
    location: DiskLocation,
    // XXX remove this and figure out based on which slab it's in?  However,
    // currently we need to return the right buffer size to the kernel, and it
    // isn't passing us the expected read size.  So we need to change some
    // interfaces to make that work right.
    size: usize,
    atime: u64,
}

#[derive(Debug)]
enum PendingChange {
    Insert(IndexValue),
    UpdateAtime(IndexValue),
    Remove(),
    RemoveThenInsert(IndexValue),
}

#[derive(Clone)]
pub struct ZettaCache {
    // XXX may need to break up this big lock.  At least we aren't holding it while doing i/o
    state: Arc<tokio::sync::Mutex<ZettaCacheState>>,
    metrics: Arc<ZettaCacheMetrics>,
}

#[derive(Debug, Serialize, Deserialize, Copy, Clone)]
struct IndexEntry {
    key: IndexKey,
    value: IndexValue,
}
impl OnDisk for IndexEntry {}
impl BlockBasedLogEntry for IndexEntry {}

#[derive(Debug, Serialize, Deserialize, Copy, Clone)]
struct ChunkSummaryEntry {
    offset: LogOffset,
    first_key: IndexKey,
}
impl OnDisk for ChunkSummaryEntry {}
impl BlockBasedLogEntry for ChunkSummaryEntry {}

#[derive(Debug, Serialize, Deserialize, Copy, Clone)]
enum OperationLogEntry {
    Insert((IndexKey, IndexValue)),
    Remove(IndexKey),
}
impl OnDisk for OperationLogEntry {}
impl BlockBasedLogEntry for OperationLogEntry {}

struct ZettaCacheState {
    block_access: Arc<BlockAccess>,
    size: u64,
    super_phys: ZettaSuperBlockPhys,
    last_valid_data_offset: u64, // XXX move to a BlockAllocator struct
    pending_changes: BTreeMap<IndexKey, PendingChange>,
    // XXX Given that we have to lock the entire State to do anything, we might
    // get away with this being a Rc?  And the ExtentAllocator doesn't really
    // need the lock inside it.  But hopefully we split up the big State lock
    // and then this is useful.  Same goes for block_access.
    extent_allocator: Arc<ExtentAllocator>,
    index: Arc<tokio::sync::RwLock<BlockBasedLogWithSummary<IndexEntry>>>,
    chunk_summary: BlockBasedLog<ChunkSummaryEntry>,
    // XXX move this to its own file/struct with methods to load, etc?
    operation_log: BlockBasedLog<OperationLogEntry>,
    // When i/o completes, the value will be sent, and the entry can be removed
    // from the tree.  These are needed to prevent the ExtentAllocator from
    // overwriting them while i/o is in flight, and to ensure that writes
    // complete before we complete the next checkpoint.
    outstanding_reads: BTreeMap<IndexValue, Arc<Semaphore>>,
    outstanding_writes: BTreeMap<IndexValue, Arc<Semaphore>>,
}

#[metered(registry=ZettaCacheMetrics)]
impl ZettaCache {
    pub async fn create(path: &str) {
        let block_access = BlockAccess::new(path).await;
        let metadata_start = SUPERBLOCK_SIZE + DEFAULT_CHECKPOINT_RING_BUFFER_SIZE;
        let data_start = block_access.round_up_to_sector(
            metadata_start as u64
                + (DEFAULT_METADATA_SIZE_PCT / 100.0 * block_access.size() as f64) as u64,
        );
        let checkpoint = ZettaCheckpointPhys {
            generation: CheckpointID(0),
            extent_allocator: ExtentAllocatorPhys {
                first_valid_offset: metadata_start as u64,
                last_valid_offset: data_start as u64,
            },
            last_valid_data_offset: data_start as u64,
            index: Default::default(),
            chunk_summary: Default::default(),
            operation_log: Default::default(),
        };
        let raw = block_access.json_chunk_to_raw(&checkpoint);
        assert_le!(raw.len(), DEFAULT_CHECKPOINT_RING_BUFFER_SIZE);
        let checkpoint_size = raw.len();
        block_access
            .write_raw(
                DiskLocation {
                    offset: SUPERBLOCK_SIZE as u64,
                },
                raw,
            )
            .await;
        let phys = ZettaSuperBlockPhys {
            checkpoint_ring_buffer_size: DEFAULT_CHECKPOINT_RING_BUFFER_SIZE as u32,
            slab_size: DEFAULT_SLAB_SIZE as u32,
            last_checkpoint_extent: Extent {
                location: DiskLocation {
                    offset: SUPERBLOCK_SIZE as u64,
                },
                size: checkpoint_size,
            },
            last_checkpoint_id: CheckpointID(0),
        };
        phys.write(&block_access).await;
    }

    pub async fn open(path: &str) -> ZettaCache {
        let block_access = Arc::new(BlockAccess::new(path).await);
        let size = block_access.size();

        // if superblock not present, create new cache
        // XXX need a real mechanism for creating/managing the cache devices
        let phys = match ZettaSuperBlockPhys::read(&block_access).await {
            Ok(phys) => phys,
            Err(_) => {
                Self::create(path).await;
                ZettaSuperBlockPhys::read(&block_access).await.unwrap()
            }
        };

        let checkpoint =
            ZettaCheckpointPhys::read(&block_access, phys.last_checkpoint_extent).await;

        assert_eq!(checkpoint.generation, phys.last_checkpoint_id);

        let metadata_start = SUPERBLOCK_SIZE + phys.checkpoint_ring_buffer_size as usize;
        // XXX pass in the metadata_start to ExtentAllocator::open, rather than
        // having this represented twice in the on-disk format?
        assert_eq!(
            metadata_start as u64,
            checkpoint.extent_allocator.first_valid_offset
        );
        let extent_allocator = Arc::new(ExtentAllocator::open(&checkpoint.extent_allocator));

        let operation_log = BlockBasedLog::open(
            block_access.clone(),
            extent_allocator.clone(),
            checkpoint.operation_log,
        );

        let pending_changes = Self::load_operation_log(&operation_log).await;

        let state = ZettaCacheState {
            block_access: block_access.clone(),
            size,
            pending_changes,
            index: Arc::new(tokio::sync::RwLock::new(
                BlockBasedLogWithSummary::open(
                    block_access.clone(),
                    extent_allocator.clone(),
                    checkpoint.index,
                )
                .await,
            )),
            chunk_summary: BlockBasedLog::open(
                block_access.clone(),
                extent_allocator.clone(),
                checkpoint.chunk_summary,
            ),
            operation_log,
            extent_allocator,
            last_valid_data_offset: checkpoint.last_valid_data_offset,
            super_phys: phys,
            outstanding_reads: BTreeMap::new(),
            outstanding_writes: BTreeMap::new(),
        };

        let this = ZettaCache {
            state: Arc::new(tokio::sync::Mutex::new(state)),
            metrics: Default::default(),
        };

        let my_cache = this.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(60));
            loop {
                interval.tick().await;
                my_cache.state.lock().await.flush_checkpoint().await;
            }
        });

        let my_cache = this.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(10));
            loop {
                interval.tick().await;
                debug!("metrics: {:#?}", my_cache.metrics);
                my_cache.state.lock().await.block_access.dump_metrics();
                //let x = &my_cache.metrics.lookup.response_time.histogram();
            }
        });

        this
    }

    async fn load_operation_log(
        operation_log: &BlockBasedLog<OperationLogEntry>,
    ) -> BTreeMap<IndexKey, PendingChange> {
        let begin = Instant::now();
        let mut num_insert_entries: u64 = 0;
        let mut num_remove_entries: u64 = 0;
        let mut pending_changes = BTreeMap::new();
        operation_log
            .iter()
            .for_each(|entry| {
                match entry {
                    OperationLogEntry::Insert((key, value)) => {
                        match pending_changes.entry(key) {
                            btree_map::Entry::Occupied(mut oe) => match oe.get() {
                                PendingChange::Remove() => {
                                    trace!("insert with existing removal; changing to RemoveThenInsert: {:?} {:?}", key, value);
                                    oe.insert(PendingChange::RemoveThenInsert(value));
                                }
                                pc @ _ => {
                                    panic!(
                                        "Inserting {:?} {:?} into already existing entry {:?}",
                                        key,
                                        value,
                                        pc,
                                    );
                                }
                            },
                            btree_map::Entry::Vacant(ve) => {
                                trace!("insert {:?} {:?}", key, value);
                                ve.insert(PendingChange::Insert(value));
                            }
                        }
                        num_insert_entries += 1;
                    }
                    OperationLogEntry::Remove(key) => {
                        match pending_changes.entry(key) {
                            btree_map::Entry::Occupied(mut oe) => match oe.get() {
                                PendingChange::Insert(value) => {
                                    trace!("remove with existing insert; clearing {:?} {:?}", key, value);
                                    oe.remove();
                                }
                                PendingChange::RemoveThenInsert(value) => {
                                    trace!("remove with existing removetheninsert; changing to remove: {:?} {:?}", key, value);
                                    oe.insert(PendingChange::Remove());
                                }
                                pc @ _ => {
                                    panic!(
                                        "Removing {:?} from already existing entry {:?}",
                                        key,
                                        pc,
                                    );
                                }
                            },
                            btree_map::Entry::Vacant(ve) => {
                                trace!("remove {:?}", key);
                                ve.insert(PendingChange::Remove());
                            }
                        }
                        num_remove_entries += 1;
                    }
                };
                future::ready(())
            })
            .await;
        info!(
            "loaded operation_log from {} inserts and {} removes into {} pending_changes in {}ms",
            num_insert_entries,
            num_remove_entries,
            pending_changes.len(),
            begin.elapsed().as_millis()
        );
        pending_changes
    }

    #[measure(ResponseTime)]
    pub async fn lookup_old(&self, guid: PoolGUID, block: BlockID) -> Option<Vec<u8>> {
        let opt_jh = {
            let mut state = self.state.lock().await;
            // XXX state.lookup() should not be async, so we can drop the state lock before doing any i/o
            state.lookup(guid, block).await
        };
        match opt_jh {
            Some(jh) => Some(jh.await.unwrap()),
            None => None,
        }
    }

    #[measure(HitCount)]
    fn cache_miss_without_index_read(&self, key: &IndexKey) {
        trace!("cache miss without reading index for {:?}", key);
    }

    #[measure(HitCount)]
    fn cache_miss_after_index_read(&self, key: &IndexKey) {
        trace!("cache miss after reading index for {:?}", key);
    }

    #[measure(HitCount)]
    fn cache_hit_without_index_read(&self, key: &IndexKey) {
        trace!("cache hit without reading index for {:?}", key);
    }

    #[measure(HitCount)]
    fn cache_hit_after_index_read(&self, key: &IndexKey) {
        trace!("cache hit after reading index for {:?}", key);
    }

    #[measure(type = ResponseTime<AtomicHdrHistogram, StdInstantMicros>)]
    #[measure(InFlight)]
    #[measure(Throughput)]
    #[measure(HitCount)]
    pub async fn lookup(&self, guid: PoolGUID, block: BlockID) -> Option<Vec<u8>> {
        let key = IndexKey { guid, block };
        let opt_jh = {
            let mut state = self.state.lock().await;
            state.lookup2(key, None)
        };
        match opt_jh {
            // XXX have lookup2() return a future that returns a future; if we
            // already found the location (or None), we can arrange for the last
            // Future to return it
            LookupContinuation::Some(jh) => {
                self.cache_hit_without_index_read(&key);
                Some(jh.await.unwrap())
            }
            LookupContinuation::None => {
                self.cache_miss_without_index_read(&key);
                None
            }
            LookupContinuation::CheckIndex(jh) => match jh.await.unwrap() {
                None => {
                    self.cache_miss_after_index_read(&key);
                    None
                }
                Some(ie) => {
                    let mut state = self.state.lock().await;
                    // XXX would be nice for the type system to enforce that if
                    // we pass Some(value), it can't return CheckIndex
                    match state.lookup2(ie.key, Some(ie.value)) {
                        LookupContinuation::Some(jh) => {
                            self.cache_hit_after_index_read(&key);
                            Some(jh.await.unwrap())
                        }
                        LookupContinuation::None => {
                            self.cache_miss_after_index_read(&key);
                            None
                        }
                        LookupContinuation::CheckIndex(_) => panic!("invalid state"),
                    }
                }
            },
        }
    }

    #[measure(type = ResponseTime<AtomicHdrHistogram, StdInstantMicros>)]
    #[measure(InFlight)]
    #[measure(Throughput)]
    #[measure(HitCount)]
    pub async fn insert(&self, guid: PoolGUID, block: BlockID, buf: Vec<u8>) {
        let mut state = self.state.lock().await;
        state.insert(guid, block, buf);
    }
}

//type LookupContinuation =
//    Box<dyn FnOnce(&mut ZettaCacheState) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> + Send>;
enum LookupContinuation {
    //Some(Box<dyn Future<Output = Vec<u8>>>),
    Some(JoinHandle<Vec<u8>>),
    None,
    CheckIndex(JoinHandle<Option<IndexEntry>>),
}

impl ZettaCacheState {
    /// Retrieve from cache, if available.  We assume that we won't often have
    /// concurrent requests for the same block, so in that case we may read it
    /// multiple times.
    // XXX should not be async, so that caller can drop the ZettaCacheState lock before we wait for io.
    async fn lookup(&mut self, guid: PoolGUID, block: BlockID) -> Option<JoinHandle<Vec<u8>>> {
        let key = IndexKey { guid, block };
        let atime = self.current_atime();
        let pc = self.pending_changes.get_mut(&key);
        let mut value = match pc {
            Some(PendingChange::Insert(value_ref)) => *value_ref,
            Some(PendingChange::RemoveThenInsert(value_ref)) => *value_ref,
            Some(PendingChange::UpdateAtime(value_ref)) => *value_ref,
            Some(PendingChange::Remove()) => return None,
            None => {
                // Check on-disk index.
                // XXX Should not be await-ing, so that caller can drop the
                // ZettaCacheState lock before we wait for io.  Will require
                // that we make sure the index chunk can't go away while we are
                // reading it.
                match self
                    .index
                    .read()
                    .await
                    .lookup_by_key(&key, |entry| entry.key)
                    .await
                {
                    Some(entry) => entry.value,
                    None => return None,
                }
            }
        };

        if value.location.offset < self.extent_allocator.get_phys().last_valid_offset {
            // The metadata overwrote this data, so it's no longer in the cache.
            // Remove from index and return None.
            trace!(
                "{:?} at {:?} was overwritten by metadata allocator; removing from cache",
                key,
                value
            );
            // Note: we could pass in the (mutable) pending_change reference,
            // which would let evict_block() avoid looking it up again.  But
            // this is not a common code path, and this interface seems cleaner.
            self.evict_block(key);
            return None;
        }

        trace!("cache hit: reading {:?} from {:?}", key, value);
        value.atime = atime;

        // Note: we're just updating the atime, which is not logged to the
        // operation_log.  If we crash, recent atime updates will be lost.
        // XXX on clean shutdown, log the atimes?
        match pc {
            Some(PendingChange::Insert(value_ref)) => {
                *value_ref = value;
            }
            Some(PendingChange::RemoveThenInsert(value_ref)) => {
                *value_ref = value;
            }
            Some(PendingChange::UpdateAtime(value_ref)) => {
                *value_ref = value;
            }
            Some(PendingChange::Remove()) => panic!("invalid state"),
            None => {
                // only in Index, not pending_changes
                trace!(
                    "adding UpdateAtime to pending_changes {:?} {:?}",
                    key,
                    value
                );
                self.pending_changes
                    .insert(key, PendingChange::UpdateAtime(value));
            }
        }

        // If there's a write to this location in progress, we will need to wait for it to complete before reading.
        // Since we won't be able to remove the entry from outstanding_writes after we wait, we just get the semaphore.
        let write_sem_opt = self
            .outstanding_writes
            .get_mut(&value)
            .map(|arc| arc.clone());

        let sem = Arc::new(Semaphore::new(0));
        let sem2 = sem.clone();
        self.outstanding_reads.insert(value, sem);
        let block_access = self.block_access.clone();
        // XXX the caller just waits on this, so it would be cleaner to just return the closure.
        Some(tokio::spawn(async move {
            if let Some(write_sem) = write_sem_opt {
                trace!("{:?} at {:?}: waiting for outstanding write", key, value);
                let _permit = write_sem.acquire().await.unwrap();
            }

            let vec = block_access
                .read_raw(Extent {
                    location: value.location,
                    size: value.size,
                })
                .await;
            sem2.add_permits(1);
            vec
        }))
    }

    // If called with value_opt=Some(), this won't return CheckIndex.
    // XXX would be nice to enforce this with the type system, i.e. have
    // different functions.  But these would share a lot of code, so we'd have
    // to figure out how to factor it out into yet another function
    fn lookup2(&mut self, key: IndexKey, value_opt: Option<IndexValue>) -> LookupContinuation {
        let atime = self.current_atime();

        // Note: we're here because there was no PendingChange for this key, but
        // since we dropped the lock, a PendingChange may have been inserted
        // since then.  So we need to check for a PendingChange before using the
        // value from the index.
        let pc = self.pending_changes.get_mut(&key);
        let mut value = match pc {
            Some(PendingChange::Insert(value_ref)) => *value_ref,
            Some(PendingChange::RemoveThenInsert(value_ref)) => *value_ref,
            Some(PendingChange::UpdateAtime(value_ref)) => *value_ref,
            Some(PendingChange::Remove()) => return LookupContinuation::None,
            None => {
                // use value from on-disk index
                match value_opt {
                    Some(value) => value,
                    None => {
                        let my_index = self.index.clone();
                        // XXX the caller just waits on this, so it would be cleaner to just return the closure.
                        trace!("lookup has no pending_change; checking index for {:?}", key);
                        return LookupContinuation::CheckIndex(tokio::spawn(async move {
                            my_index
                                .read()
                                .await
                                .lookup_by_key(&key, |entry| entry.key)
                                .await
                            // XXX When the await completes, the
                            // RwLockReadGuard will be dropped, so we aren't
                            // holding the index's lock when we pass the
                            // value back to the 2nd execution of this
                            // function.  That means that the index could
                            // change after we do the lookup, and we could
                            // be using a stale value.  This should usually
                            // be OK since changes to the index go through
                            // the pending_changes first, and we are
                            // checking the pending_changes again.  But it
                            // could be that the pending change is made and
                            // then the index rewritten to reflect that,
                            // before we get the 2nd call.
                        }));
                    }
                }
            }
        };

        if value.location.offset < self.extent_allocator.get_phys().last_valid_offset {
            // The metadata overwrote this data, so it's no longer in the cache.
            // Remove from index and return None.
            trace!(
                "{:?} at {:?} was overwritten by metadata allocator; removing from cache",
                key,
                value
            );
            // Note: we could pass in the (mutable) pending_change reference,
            // which would let evict_block() avoid looking it up again.  But
            // this is not a common code path, and this interface seems cleaner.
            self.evict_block(key);
            return LookupContinuation::None;
        }

        trace!("cache hit: reading {:?} from {:?}", key, value);
        value.atime = atime;

        // Note: we're just updating the atime, which is not logged to the
        // operation_log.  If we crash, recent atime updates will be lost.
        // XXX on clean shutdown, log the atimes?
        match pc {
            Some(PendingChange::Insert(value_ref)) => {
                *value_ref = value;
            }
            Some(PendingChange::RemoveThenInsert(value_ref)) => {
                *value_ref = value;
            }
            Some(PendingChange::UpdateAtime(value_ref)) => {
                *value_ref = value;
            }
            Some(PendingChange::Remove()) => panic!("invalid state"),
            None => {
                // only in Index, not pending_changes
                trace!(
                    "adding UpdateAtime to pending_changes {:?} {:?}",
                    key,
                    value
                );
                // XXX would be nice to have saved the btreemap::Entry so we
                // don't have to traverse the tree again.
                self.pending_changes
                    .insert(key, PendingChange::UpdateAtime(value));
            }
        }

        // If there's a write to this location in progress, we will need to wait for it to complete before reading.
        // Since we won't be able to remove the entry from outstanding_writes after we wait, we just get the semaphore.
        let write_sem_opt = self
            .outstanding_writes
            .get_mut(&value)
            .map(|arc| arc.clone());

        let sem = Arc::new(Semaphore::new(0));
        let sem2 = sem.clone();
        self.outstanding_reads.insert(value, sem);
        let block_access = self.block_access.clone();

        // XXX the caller just waits on this, so it would be cleaner to just return the closure.
        LookupContinuation::Some(tokio::spawn(async move {
            if let Some(write_sem) = write_sem_opt {
                trace!("{:?} at {:?}: waiting for outstanding write", key, value);
                let _permit = write_sem.acquire().await.unwrap();
            }

            let vec = block_access
                .read_raw(Extent {
                    location: value.location,
                    size: value.size,
                })
                .await;
            sem2.add_permits(1);
            vec
        }))
    }

    fn evict_block(&mut self, key: IndexKey) {
        match self.pending_changes.get_mut(&key) {
            Some(PendingChange::Insert(value)) => {
                // The operation_log has an Insert for this key, and the key
                // is not in the Index.  We don't need a
                // PendingChange::Removal since there's nothing to remove
                // from the index.
                //assert_eq!(*value_ref, value);
                trace!("removing Insert from pending_changes {:?} {:?}", key, value);
                self.pending_changes.remove(&key);
            }
            Some(PendingChange::RemoveThenInsert(value)) => {
                // The operation_log has a Remove, and then an Insert for
                // this key, so the key is in the Index.  We need a
                // PendingChange::Remove so that the Index entry won't be
                // found.
                //assert_eq!(*value_ref, value);
                trace!(
                    "changing RemoveThenInsert to Remove in pending_changes {:?} {:?}",
                    key,
                    value,
                );
                self.pending_changes.insert(key, PendingChange::Remove());
            }
            Some(PendingChange::UpdateAtime(value)) => {
                // It's just an atime update, so the operation_log doesn't
                // have an Insert for this key, but the key is in the
                // Index.
                //assert_eq!(*value_ref, value);
                trace!(
                    "changing UpdateAtime to Remove in pending_changes {:?} {:?}",
                    key,
                    value,
                );
                self.pending_changes.insert(key, PendingChange::Remove());
            }
            Some(PendingChange::Remove()) => {
                panic!("invalid state");
            }
            None => {
                // only in Index, not pending_changes
                trace!("adding Remove to pending_changes {:?}", key);
                self.pending_changes.insert(key, PendingChange::Remove());
            }
        }
        trace!("adding Remove to operation_log {:?}", key);
        self.operation_log.append(OperationLogEntry::Remove(key));
    }

    /// Insert this block to the cache, if space and performance parameters
    /// allow.  It may be a recent cache miss, or a recently-written block.
    fn insert(&mut self, guid: PoolGUID, block: BlockID, buf: Vec<u8>) {
        let buf_size = buf.len();
        let aligned_size = self.block_access.round_up_to_sector(buf.len());

        let aligned_buf = if buf_size == aligned_size {
            buf
        } else {
            // pad to sector size
            let mut tail: Vec<u8> = Vec::new();
            tail.resize(aligned_size - buf_size, 0);
            // XXX copying data around; have caller pass in larger buffer?  or
            // at least let us pass the unaligned buf to write_raw() which
            // already has to copy it around to get the pointer aligned.
            [buf, tail].concat()
        };
        let location_opt = self.allocate_block(aligned_buf.len());
        if location_opt.is_none() {
            return;
        }
        let location = location_opt.unwrap();

        // XXX if this is past the last block of the main index, we can write it
        // there (and location_dirty:false) instead of logging it

        let key = IndexKey { guid, block };
        let value = IndexValue {
            atime: self.current_atime(),
            location,
            size: buf_size,
        };

        // XXX we'd like to assert that this is not already in the index
        // (otherwise we would need to use a PendingChange::RemoveThenInsert).
        // However, this is not an async fn so we can't do the read here.  We
        // could spawn a new task, but currently reading the index requires the
        // big lock.

        match self.pending_changes.entry(key) {
            btree_map::Entry::Occupied(mut oe) => match oe.get() {
                PendingChange::Remove() => {
                    trace!(
                        "adding RemoveThenInsert to pending_changes {:?} {:?}",
                        key,
                        value
                    );
                    oe.insert(PendingChange::RemoveThenInsert(value));
                }
                _ => {
                    // Already in cache; ignore this insertion request?  Or panic?
                    todo!();
                }
            },
            btree_map::Entry::Vacant(ve) => {
                trace!("adding Insert to pending_changes {:?} {:?}", key, value);
                ve.insert(PendingChange::Insert(value));
            }
        }

        /* XXX this seems more clear than the above, but it will typically access the btree twice
        // XXX maybe make pending_changes its own struct and we can have a method to "upgrade to insert"
        match self.pending_changes.get_mut(&key) {
            Some(pc @ PendingChange::Remove()) => {
                *pc = PendingChange::RemoveThenInsert(value);
            }
            Some(_) => {
                // Already in cache; ignore this insertion request?  Or panic?
                todo!();
                }
            }
            None => {
                self.pending_changes
                    .insert(key, PendingChange::Insert(value));
            }
        }
        */

        trace!("adding Insert to operation_log {:?} {:?}", key, value);
        self.operation_log
            .append(OperationLogEntry::Insert((key, value)));

        let sem = Arc::new(Semaphore::new(0));
        let sem2 = sem.clone();
        let block_access = self.block_access.clone();
        tokio::spawn(async move {
            block_access.write_raw(location, aligned_buf).await;
            sem2.add_permits(1);
        });
        // note: we don't need to insert before initiating the write, because we
        // have exclusive access to the State, so nobody can see the
        // outstanding_writes until we are done
        self.outstanding_writes.insert(value, sem);
    }

    // XXX change to return a newtype?
    pub fn current_atime(&self) -> u64 {
        // XXX change to minutes of running pool?
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs()
    }

    /// returns offset, or None if there's no space
    fn allocate_block(&mut self, size: usize) -> Option<DiskLocation> {
        let end = self.last_valid_data_offset + size as u64;
        if end < self.size {
            let location_opt = Some(DiskLocation {
                offset: self.last_valid_data_offset,
            });
            self.last_valid_data_offset = end;
            location_opt
        } else {
            debug!("block allocation of {} bytes failed", size);
            None
        }
    }

    async fn flush_checkpoint(&mut self) {
        debug!(
            "flushing checkpoint {:?}",
            self.super_phys.last_checkpoint_id.next()
        );

        let begin = Instant::now();

        // Wait for all outstanding reads, so that if the ExtentAllocator needs
        // to overwrite some blocks, there aren't any outstanding i/os to that
        // region.
        // XXX It would be better to only do wait for the reads that are in the
        // region that we're overwriting.  But it will be tricky to do the
        // waiting down in the ExtentAllocator.  If we get that working, we'll
        // still need to clean up the outstanding_reads entries that have
        // completed, at some point.  Even as-is, letting them accumulate for a
        // whole checkpoint might not be great.  We might want a "cleaner" to
        // run every second and remove completed entries.  Or have the read task
        // lock the outstanding_reads and remove itself (which might perform
        // worse due to contention on the global lock).
        for (_value, sem) in &mut self.outstanding_reads {
            let _permit = sem.acquire().await.unwrap();
        }
        self.outstanding_reads.clear();

        // Wait for all outstanding writes, for the same reason as reads, and
        // also so that if we crash, the blocks referenced by the
        // index/operation_log will actually have the correct contents.
        for (_value, sem) in &mut self.outstanding_writes {
            let _permit = sem.acquire().await.unwrap();
        }
        self.outstanding_writes.clear();

        if self.pending_changes.len() > MAX_PENDING_CHANGES {
            self.merge_pending_changes().await;
        }

        future::join3(
            self.index.write().await.flush(),
            self.chunk_summary.flush(),
            self.operation_log.flush(),
        )
        .await;

        let checkpoint = ZettaCheckpointPhys {
            generation: self.super_phys.last_checkpoint_id.next(),
            extent_allocator: self.extent_allocator.get_phys(),
            index: self.index.read().await.get_phys(),
            chunk_summary: self.chunk_summary.get_phys(),
            operation_log: self.operation_log.get_phys(),
            last_valid_data_offset: self.last_valid_data_offset,
        };

        let mut checkpoint_location = self.super_phys.last_checkpoint_extent.location
            + self.super_phys.last_checkpoint_extent.size;

        let raw = self.block_access.json_chunk_to_raw(&checkpoint);
        if raw.len()
            > (checkpoint.extent_allocator.first_valid_offset - checkpoint_location.offset) as usize
        {
            // Out of space; go back to the beginning of the checkpoint space.
            checkpoint_location.offset = SUPERBLOCK_SIZE as u64;
            assert_le!(
                raw.len(),
                (self.super_phys.last_checkpoint_extent.location.offset
                    - checkpoint_location.offset) as usize,
            );
            // XXX The above assertion could fail if there isn't enough
            // checkpoint space for 3 checkpoints (the existing one that
            // we're writing before, the one we're writing, and the space
            // after the existing one that we aren't using).  Note that we
            // could in theory reduce this to 2 checkpoints if we allowed a
            // single checkpoint to wrap around (part of it at the end and
            // then part at the beginning of the space).
        }
        debug!("writing to {:?}: {:#?}", checkpoint_location, checkpoint);

        self.super_phys.last_checkpoint_extent = Extent {
            location: checkpoint_location,
            size: raw.len(),
        };
        self.block_access.write_raw(checkpoint_location, raw).await;

        self.super_phys.last_checkpoint_id = self.super_phys.last_checkpoint_id.next();
        self.super_phys.write(&self.block_access).await;

        self.extent_allocator.checkpoint_done();

        debug!(
            "completed checkpoint {:?} in {}ms",
            self.super_phys.last_checkpoint_id,
            begin.elapsed().as_millis()
        );
    }

    async fn merge_pending_changes(&mut self) {
        let begin = Instant::now();
        // XXX when we are continually merging, over multiple checkpoints, we
        // will probably want the BlockBasedLog to know about multiple
        // generations, and therefore we'd keep the one BlockBasedLog but create
        // a new generation (as we do with ObjectBasedLog).
        let mut new_index = BlockBasedLogWithSummary::open(
            self.block_access.clone(),
            self.extent_allocator.clone(),
            Default::default(),
        )
        .await;
        let mut old_index = self.index.write().await;
        old_index.flush().await;
        info!(
            "writing new index to merge {} pending changes into index of {} entries ({} MB)",
            self.pending_changes.len(),
            old_index.len(),
            old_index.num_bytes() / 1024 / 1024,
        );
        // XXX load operation_log and verify that the pending_changes match it?
        let mut pending_changes_iter = self.pending_changes.iter().peekable();
        old_index
            .iter()
            .for_each(|entry| {
                // First, process any pending changes which are before this
                // index entry, which must be all Inserts (Removes,
                // RemoveThenInserts, and AtimeUpdates refer to existing Index
                // entries).
                trace!("next index entry: {:?}", entry);
                while let Some((pc_key, PendingChange::Insert(pc_value))) =
                    pending_changes_iter.peek()
                {
                    if **pc_key >= entry.key {
                        break;
                    }
                    // Add this new entry to the index
                    new_index.append(IndexEntry {
                        key: **pc_key,
                        value: *pc_value,
                    });
                    pending_changes_iter.next();
                }

                let next_pc_opt = pending_changes_iter.peek();
                match next_pc_opt {
                    Some((pc_key, PendingChange::Remove())) => {
                        if **pc_key == entry.key {
                            // Don't write this entry to the new generation.
                            // this pending change is consumed
                            pending_changes_iter.next();
                        } else {
                            // There shouldn't be a pending removal of an entry that doesn't exist in the index.
                            assert_gt!(**pc_key, entry.key);
                            new_index.append(entry);
                        }
                    }
                    Some((pc_key, PendingChange::Insert(_pc_value))) => {
                        // Insertions are processed above.  There can't be an
                        // index entry with the same key.  If there were, it has
                        // to be removed first, resulting in a
                        // PendingChange::RemoveThenInsert.
                        assert_gt!(**pc_key, entry.key);
                        new_index.append(entry);
                    }
                    Some((pc_key, PendingChange::RemoveThenInsert(pc_value))) => {
                        if **pc_key == entry.key {
                            // This key must have been removed (evicted) and then re-inserted.
                            // Add the pending change to the next generation instead of the current index's entry
                            assert_eq!(pc_value.size, entry.value.size);
                            new_index.append(IndexEntry {
                                key: **pc_key,
                                value: *pc_value,
                            });

                            // this pending change is consumed
                            pending_changes_iter.next();
                        } else {
                            // We shouldn't have skipped any, because there has to be a corresponding Index entry
                            assert_gt!(**pc_key, entry.key);
                            new_index.append(entry);
                        }
                    }
                    Some((pc_key, PendingChange::UpdateAtime(pc_value))) => {
                        if **pc_key == entry.key {
                            // Add the pending entry to the next generation instead of the current index's entry
                            assert_eq!(pc_value.location, entry.value.location);
                            assert_eq!(pc_value.size, entry.value.size);
                            new_index.append(IndexEntry {
                                key: **pc_key,
                                value: *pc_value,
                            });

                            // this pending change is consumed
                            pending_changes_iter.next();
                        } else {
                            // We shouldn't have skipped any, because there has to be a corresponding Index entry
                            assert_gt!(**pc_key, entry.key);
                            new_index.append(entry);
                        }
                    }
                    None => {
                        // no more pending changes
                        new_index.append(entry);
                    }
                }
                future::ready(())
            })
            .await;
        while let Some((pc_key, PendingChange::Insert(pc_value))) = pending_changes_iter.peek() {
            // Add this new entry to the index
            trace!(
                "remaining pending change, appending to new index: {:?} {:?}",
                pc_key,
                pc_value
            );
            new_index.append(IndexEntry {
                key: **pc_key,
                value: *pc_value,
            });
            // Consume pending change.  We don't do that in the `while let`
            // because we want to leave any unmatched items in the iterator so
            // that we can print them out when failing below.
            pending_changes_iter.next();
        }
        // Other pending changes refer to existing index entries and therefore should have been processed above
        assert!(
            pending_changes_iter.peek().is_none(),
            "next={:?}",
            pending_changes_iter.peek().unwrap()
        );

        // Note: the caller is about to flush as well, but we want to count the time in the below info!
        new_index.flush().await;

        self.pending_changes.clear();
        old_index.clear();
        drop(old_index);

        self.operation_log.clear();

        info!(
            "wrote new index with {} entries ({} MB) in {:.1}s ({:.1}MB/s)",
            new_index.len(),
            new_index.num_bytes() / 1024 / 1024,
            begin.elapsed().as_secs_f64(),
            (new_index.num_bytes() as f64 / 1024f64 / 1024f64) / begin.elapsed().as_secs_f64(),
        );
        self.index = Arc::new(tokio::sync::RwLock::new(new_index));
    }
}
