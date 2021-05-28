use crate::base_types::*;
use crate::object_access::ObjectAccess;
use crate::object_based_log::*;
use crate::object_block_map::ObjectBlockMap;
use anyhow::{Context, Result};
use core::future::Future;
use futures::future;
use futures::stream::*;
use log::*;
use more_asserts::*;
use nvpair::NvList;
use serde::{Deserialize, Serialize};
use serde_bytes::ByteBuf;
use std::cmp::{max, min};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fmt;
use std::mem;
use std::ops::Bound::*;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Instant, SystemTime};
use stream_reduce::Reduce;
use tokio::sync::oneshot;

// XXX need a real tunables infrastructure.  Probably should use toml.
// start freeing when the pending frees are this % of the entire pool
const FREE_HIGHWATER_PCT: f64 = 10.0;
// stop freeing when the pending frees are this % of the entire pool
const FREE_LOWWATER_PCT: f64 = 9.0;
// don't bother freeing unless there are at least this number of free blocks
const FREE_MIN_BLOCKS: u64 = 1000;
const MAX_BYTES_PER_OBJECT: u32 = 1024 * 1024;

// minimum number of chunks before we consider condensing
const LOG_CONDENSE_MIN_CHUNKS: usize = 30;
// when log is 5x as large as the condensed version
const LOG_CONDENSE_MULTIPLE: usize = 5;

#[derive(Serialize, Deserialize, Debug)]
struct PoolPhys {
    guid: PoolGUID, // redundant with key, for verification
    name: String,
    last_txg: TXG,
}
impl OnDisk for PoolPhys {}

#[derive(Serialize, Deserialize, Debug)]
pub struct UberblockPhys {
    guid: PoolGUID,   // redundant with key, for verification
    txg: TXG,         // redundant with key, for verification
    date: SystemTime, // for debugging
    storage_object_log: ObjectBasedLogPhys,
    pending_frees_log: ObjectBasedLogPhys,
    object_size_log: ObjectBasedLogPhys,
    next_block: BlockID, // next BlockID that can be allocated
    stats: PoolStatsPhys,
    zfs_uberblock: TerseVec<u8>,
    zfs_config: TerseVec<u8>,
}
impl OnDisk for UberblockPhys {}

/// exists just to reduce Debug output on fields we don't really care about
#[derive(Serialize, Deserialize)]
pub struct TerseVec<T>(Vec<T>);
impl<T> fmt::Debug for TerseVec<T> {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.write_fmt(format_args!("[...{} elements...]", self.0.len()))
    }
}

#[derive(Serialize, Deserialize, Debug, Default, Clone, Copy)]
struct PoolStatsPhys {
    blocks_count: u64, // Note: does not include the pending_object
    blocks_bytes: u64, // Note: does not include the pending_object
    pending_frees_count: u64,
    pending_frees_bytes: u64,
    objects_count: u64, // XXX shouldn't really be needed since we always have the storage_object_log loaded into the `objects` field
}
impl OnDisk for PoolStatsPhys {}

#[derive(Serialize, Deserialize, Debug)]
struct DataObjectPhys {
    guid: PoolGUID,      // redundant with key, for verification
    object: ObjectID,    // redundant with key, for verification
    blocks_size: u32,    // sum of blocks.values().len()
    min_block: BlockID,  // inclusive (all blocks are >= min_block)
    next_block: BlockID, // exclusive (all blocks are < next_block)

    // Note: if this object was rewritten to consolidate adjacent objects, the
    // blocks in this object may have been originally written over a range of
    // TXG's.
    min_txg: TXG,
    max_txg: TXG, // inclusive

    blocks: HashMap<BlockID, ByteBuf>,
}
impl OnDisk for DataObjectPhys {}

#[derive(Debug, Serialize, Deserialize, Copy, Clone)]
enum StorageObjectLogEntry {
    Alloc {
        object: ObjectID,
        min_block: BlockID,
    },
    Free {
        object: ObjectID,
    },
}
impl OnDisk for StorageObjectLogEntry {}
impl ObjectBasedLogEntry for StorageObjectLogEntry {}

#[derive(Debug, Serialize, Deserialize, Copy, Clone)]
enum ObjectSizeLogEntry {
    Exists {
        object: ObjectID,
        num_blocks: u32,
        num_bytes: u32, // bytes in blocks; does not include Agent metadata
    },
    Freed {
        object: ObjectID,
    },
}
impl OnDisk for ObjectSizeLogEntry {}
impl ObjectBasedLogEntry for ObjectSizeLogEntry {}

#[derive(Debug, Serialize, Deserialize, Copy, Clone, PartialEq)]
struct PendingFreesLogEntry {
    block: BlockID,
    size: u32, // in bytes
}
impl OnDisk for PendingFreesLogEntry {}
impl ObjectBasedLogEntry for PendingFreesLogEntry {}

/*
 * Accessors for on-disk structures
 */

impl PoolPhys {
    fn key(guid: PoolGUID) -> String {
        format!("zfs/{}/super", guid)
    }

    async fn exists(object_access: &ObjectAccess, guid: PoolGUID) -> bool {
        object_access.object_exists(&Self::key(guid)).await
    }

    async fn get(object_access: &ObjectAccess, guid: PoolGUID) -> Result<Self> {
        let key = Self::key(guid);
        let buf = object_access.get_object(&key).await?;
        let this: Self = serde_json::from_slice(&buf)
            .context(format!("Failed to decode contents of {}", key))?;
        debug!("got {:#?}", this);
        assert_eq!(this.guid, guid);
        Ok(this)
    }

    async fn put(&self, object_access: &ObjectAccess) {
        debug!("putting {:#?}", self);
        let buf = serde_json::to_vec(&self).unwrap();
        object_access.put_object(&Self::key(self.guid), buf).await;
    }
}

impl UberblockPhys {
    fn key(guid: PoolGUID, txg: TXG) -> String {
        format!("zfs/{}/txg/{}", guid, txg)
    }

    pub fn get_zfs_uberblock(&self) -> &Vec<u8> {
        &self.zfs_uberblock.0
    }

    pub fn get_zfs_config(&self) -> &Vec<u8> {
        &self.zfs_config.0
    }

    async fn get(object_access: &ObjectAccess, guid: PoolGUID, txg: TXG) -> Result<Self> {
        let key = Self::key(guid, txg);
        let buf = object_access.get_object(&key).await?;
        let this: Self = serde_json::from_slice(&buf)
            .context(format!("Failed to decode contents of {}", key))?;
        debug!("got {:#?}", this);
        assert_eq!(this.guid, guid);
        assert_eq!(this.txg, txg);
        Ok(this)
    }

    async fn put(&self, object_access: &ObjectAccess) {
        debug!("putting {:#?}", self);
        let buf = serde_json::to_vec(&self).unwrap();
        object_access
            .put_object(&Self::key(self.guid, self.txg), buf)
            .await;
    }
}

const NUM_DATA_PREFIXES: i32 = 64;

impl DataObjectPhys {
    fn key(guid: PoolGUID, object: ObjectID) -> String {
        format!(
            "zfs/{}/data/{:03}/{}",
            guid,
            object.0 % NUM_DATA_PREFIXES as u64,
            object
        )
    }

    // Could change this to return an Iterator
    fn prefixes(guid: PoolGUID) -> Vec<String> {
        let mut vec = Vec::new();
        for x in 0..NUM_DATA_PREFIXES {
            vec.push(format!("zfs/{}/data/{:03}/", guid, x));
        }
        vec
    }

    fn verify(&self) {
        assert_eq!(
            self.blocks_size as usize,
            self.blocks.values().map(|x| x.len()).sum::<usize>()
        );
        assert_le!(self.min_txg, self.max_txg);
        assert_le!(self.min_block, self.next_block);
        if !self.blocks.is_empty() {
            assert_le!(self.min_block, self.blocks.keys().min().unwrap());
            assert_gt!(self.next_block, self.blocks.keys().max().unwrap());
        }
    }

    async fn get(object_access: &ObjectAccess, guid: PoolGUID, obj: ObjectID) -> Result<Self> {
        let this = Self::get_from_key(object_access, &Self::key(guid, obj)).await?;
        assert_eq!(this.guid, guid);
        assert_eq!(this.object, obj);
        Ok(this)
    }

    async fn get_from_key(object_access: &ObjectAccess, key: &str) -> Result<Self> {
        let buf = object_access.get_object(key).await?;
        let begin = Instant::now();
        let this: Self =
            bincode::deserialize(&buf).context(format!("Failed to decode contents of {}", key))?;
        debug!(
            "{:?}: deserialized {} blocks from {} bytes in {}ms",
            this.object,
            this.blocks.len(),
            buf.len(),
            begin.elapsed().as_millis()
        );
        this.verify();
        Ok(this)
    }

    async fn put(&self, object_access: &ObjectAccess) {
        let begin = Instant::now();
        let contents = bincode::serialize(&self).unwrap();
        debug!(
            "{:?}: serialized {} blocks in {} bytes in {}ms",
            self.object,
            self.blocks.len(),
            contents.len(),
            begin.elapsed().as_millis()
        );
        self.verify();
        object_access
            .put_object(&Self::key(self.guid, self.object), contents)
            .await;
    }
}

/*
 * Main storage pool interface
 */

//#[derive(Debug)]
pub struct Pool {
    pub state: Arc<PoolState>,
}

//#[derive(Debug)]
pub struct PoolState {
    syncing_state: std::sync::Mutex<Option<PoolSyncingState>>,
    object_block_map: ObjectBlockMap,
    pub shared_state: Arc<PoolSharedState>,
}

/// state that's modified while syncing a txg
//#[derive(Debug)]
struct PoolSyncingState {
    // Note: some objects may contain additional (adjacent) blocks, if they have
    // been consolidated but this fact is not yet represented in the log.  A
    // consolidated object won't be removed until after the log reflects that.
    storage_object_log: ObjectBasedLog<StorageObjectLogEntry>,

    // Note: the object_size_log may not have the most up-to-date size info for
    // every object, because it's updated after the object is overwritten, when
    // processing pending frees.
    object_size_log: ObjectBasedLog<ObjectSizeLogEntry>,

    // Note: the pending_frees_log may contain frees that were already applied,
    // if we crashed while processing pending frees.
    pending_frees_log: ObjectBasedLog<PendingFreesLogEntry>,

    pending_object: PendingObjectState,
    pending_unordered_writes: HashMap<BlockID, (ByteBuf, oneshot::Sender<()>)>,
    pub last_txg: TXG,
    pub syncing_txg: Option<TXG>,
    stats: PoolStatsPhys,
    reclaim_done: Option<oneshot::Receiver<SyncTask>>,
    // Protects objects that are being overwritten for sync-to-convergence
    rewriting_objects: HashMap<ObjectID, Arc<tokio::sync::Mutex<()>>>,
    // objects to delete at the end of this txg
    objects_to_delete: Vec<ObjectID>,
    // Flush immediately once we have one of these blocks (and all previous blocks)
    pending_flushes: BTreeSet<BlockID>,
}

type SyncTask =
    Box<dyn FnOnce(&mut PoolSyncingState) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> + Send>;

#[derive(Debug)]
enum PendingObjectState {
    Pending(DataObjectPhys, Vec<oneshot::Sender<()>>), // available to write
    NotPending(BlockID), // not available to write; this is the next blockID to use
}

impl PendingObjectState {
    fn as_mut_pending(&mut self) -> (&mut DataObjectPhys, &mut Vec<oneshot::Sender<()>>) {
        match self {
            PendingObjectState::Pending(phys, done) => (phys, done),
            _ => panic!("invalid {:?}", self),
        }
    }

    fn unwrap_pending(self) -> (DataObjectPhys, Vec<oneshot::Sender<()>>) {
        match self {
            PendingObjectState::Pending(phys, done) => (phys, done),
            _ => panic!("invalid {:?}", self),
        }
    }

    fn is_pending(&self) -> bool {
        match self {
            PendingObjectState::Pending(..) => true,
            PendingObjectState::NotPending(..) => false,
        }
    }

    fn next_block(&self) -> BlockID {
        match self {
            PendingObjectState::Pending(phys, _) => phys.next_block,
            PendingObjectState::NotPending(next_block) => *next_block,
        }
    }

    fn new_pending(guid: PoolGUID, object: ObjectID, next_block: BlockID, txg: TXG) -> Self {
        PendingObjectState::Pending(
            DataObjectPhys {
                guid,
                object,
                min_block: next_block,
                next_block,
                min_txg: txg,
                max_txg: txg,
                blocks_size: 0,
                blocks: HashMap::new(),
            },
            Vec::new(),
        )
    }
}

/*
 * Note: this struct is passed to the OBL code.  It needs to be a separate struct from Pool,
 * because it can't refer back to the OBL itself, which would create a circular reference.
 */
#[derive(Clone)]
pub struct PoolSharedState {
    pub object_access: ObjectAccess,
    pub guid: PoolGUID,
    pub name: String,
}

impl PoolSyncingState {
    fn next_block(&self) -> BlockID {
        self.pending_object.next_block()
    }

    fn log_free(&mut self, ent: PendingFreesLogEntry) {
        let txg = self.syncing_txg.unwrap();
        assert_lt!(ent.block, self.next_block());
        self.pending_frees_log.append(txg, ent);
        self.stats.pending_frees_count += 1;
        self.stats.pending_frees_bytes += ent.size as u64;
    }
}

impl PoolState {
    // XXX change this to return a Result.  If the syncing_state is None, return
    // an error which means that we're in the middle of end_txg.
    fn with_syncing_state<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&mut PoolSyncingState) -> R,
    {
        let mut guard = self.syncing_state.lock().unwrap();
        f(guard.as_mut().unwrap())
    }
}

impl Pool {
    pub async fn exists(object_access: &ObjectAccess, guid: PoolGUID) -> bool {
        PoolPhys::exists(object_access, guid).await
    }

    pub async fn get_config(object_access: &ObjectAccess, guid: PoolGUID) -> Result<NvList> {
        let pool_phys = PoolPhys::get(object_access, guid).await?;
        let uberblock_phys =
            UberblockPhys::get(object_access, pool_phys.guid, pool_phys.last_txg).await?;
        let nvl = NvList::try_unpack(&uberblock_phys.zfs_config.0)?;
        Ok(nvl)
    }

    pub async fn create(object_access: &ObjectAccess, name: &str, guid: PoolGUID) {
        let phys = PoolPhys {
            guid,
            name: name.to_string(),
            last_txg: TXG(0),
        };
        // XXX make sure it doesn't already exist
        phys.put(object_access).await;
    }

    async fn open_from_txg(
        object_access: &ObjectAccess,
        pool_phys: &PoolPhys,
        txg: TXG,
    ) -> (Pool, Option<UberblockPhys>, BlockID) {
        let phys = UberblockPhys::get(object_access, pool_phys.guid, txg)
            .await
            .unwrap();

        let shared_state = Arc::new(PoolSharedState {
            object_access: object_access.clone(),
            guid: pool_phys.guid,
            name: pool_phys.name.clone(),
        });
        let pool = Pool {
            state: Arc::new(PoolState {
                shared_state: shared_state.clone(),
                syncing_state: std::sync::Mutex::new(Some(PoolSyncingState {
                    last_txg: phys.txg,
                    syncing_txg: None,
                    storage_object_log: ObjectBasedLog::open_by_phys(
                        shared_state.clone(),
                        &format!("zfs/{}/StorageObjectLog", pool_phys.guid),
                        &phys.storage_object_log,
                    ),
                    object_size_log: ObjectBasedLog::open_by_phys(
                        shared_state.clone(),
                        &format!("zfs/{}/ObjectSizeLog", pool_phys.guid),
                        &phys.object_size_log,
                    ),
                    pending_frees_log: ObjectBasedLog::open_by_phys(
                        shared_state.clone(),
                        &format!("zfs/{}/PendingFreesLog", pool_phys.guid),
                        &phys.pending_frees_log,
                    ),
                    pending_object: PendingObjectState::NotPending(phys.next_block),
                    pending_unordered_writes: HashMap::new(),
                    stats: phys.stats,
                    reclaim_done: None,
                    rewriting_objects: HashMap::new(),
                    objects_to_delete: Vec::new(),
                    pending_flushes: BTreeSet::new(),
                })),
                object_block_map: ObjectBlockMap::new(),
            }),
        };

        let mut syncing_state = {
            let mut guard = pool.state.syncing_state.lock().unwrap();
            guard.take().unwrap()
        };

        syncing_state.storage_object_log.recover().await;
        syncing_state.object_size_log.recover().await;
        syncing_state.pending_frees_log.recover().await;

        // load block -> object mapping
        let begin = Instant::now();
        let mut num_alloc_entries: u64 = 0;
        let mut num_free_entries: u64 = 0;
        syncing_state
            .storage_object_log
            .iterate()
            .for_each(|ent| {
                match ent {
                    StorageObjectLogEntry::Alloc {
                        object,
                        min_block: first_possible_block,
                    } => {
                        pool.state
                            .object_block_map
                            .insert(object, first_possible_block);
                        num_alloc_entries += 1;
                    }
                    StorageObjectLogEntry::Free { object } => {
                        pool.state.object_block_map.remove(object);
                        num_free_entries += 1;
                    }
                }

                future::ready(())
            })
            .await;
        info!(
            "loaded mapping from {} objects with {} allocs and {} frees in {}ms",
            syncing_state.storage_object_log.num_chunks,
            num_alloc_entries,
            num_free_entries,
            begin.elapsed().as_millis()
        );

        pool.state.object_block_map.verify();

        assert_eq!(
            pool.state.object_block_map.len() as u64,
            syncing_state.stats.objects_count
        );

        let next_block = syncing_state.next_block();

        //println!("opened {:#?}", pool);

        *pool.state.syncing_state.lock().unwrap() = Some(syncing_state);
        (pool, Some(phys), next_block)
    }

    pub async fn open(
        object_access: &ObjectAccess,
        guid: PoolGUID,
    ) -> (Pool, Option<UberblockPhys>, BlockID) {
        let phys = PoolPhys::get(object_access, guid).await.unwrap();
        if phys.last_txg.0 == 0 {
            let shared_state = Arc::new(PoolSharedState {
                object_access: object_access.clone(),
                guid,
                name: phys.name,
            });
            let pool = Pool {
                state: Arc::new(PoolState {
                    shared_state: shared_state.clone(),
                    syncing_state: std::sync::Mutex::new(Some(PoolSyncingState {
                        last_txg: TXG(0),
                        syncing_txg: None,
                        storage_object_log: ObjectBasedLog::create(
                            shared_state.clone(),
                            &format!("zfs/{}/StorageObjectLog", guid),
                        ),
                        object_size_log: ObjectBasedLog::create(
                            shared_state.clone(),
                            &format!("zfs/{}/ObjectSizeLog", guid),
                        ),
                        pending_frees_log: ObjectBasedLog::create(
                            shared_state.clone(),
                            &format!("zfs/{}/PendingFreesLog", guid),
                        ),
                        pending_object: PendingObjectState::NotPending(BlockID(0)),
                        pending_unordered_writes: HashMap::new(),
                        stats: PoolStatsPhys::default(),
                        reclaim_done: None,
                        rewriting_objects: HashMap::new(),
                        objects_to_delete: Vec::new(),
                        pending_flushes: BTreeSet::new(),
                    })),
                    object_block_map: ObjectBlockMap::new(),
                }),
            };

            let next_block = pool
                .state
                .with_syncing_state(|syncing_state| syncing_state.next_block());
            (pool, None, next_block)
        } else {
            Pool::open_from_txg(object_access, &phys, phys.last_txg).await
        }
    }

    pub fn get_prop(&self, name: &str) -> u64 {
        self.state.with_syncing_state(|syncing_state| {
            let stats = syncing_state.stats;
            match name {
                "zoa_allocated" => stats.pending_frees_bytes,
                "zoa_freeing" => stats.pending_frees_bytes,
                "zoa_objects" => stats.objects_count,
                _ => panic!("invalid prop name: {}", name),
            }
        })
    }

    pub fn resume_txg(&self, txg: TXG) {
        // The syncing_state is only held while a txg is open (begun).  It's not
        // allowed to call begin_txg() while a txg is already open, so the lock
        // must not be held.
        // XXX change this to return an error to the client
        self.state.with_syncing_state(|syncing_state| {
            assert!(syncing_state.syncing_txg.is_none());
            assert_gt!(txg.0, syncing_state.last_txg.0);
            syncing_state.syncing_txg = Some(txg);

            // Resuming state is indicated by pending_object = NotPending
            assert!(!syncing_state.pending_object.is_pending());
        })
    }

    async fn get_recovered_objects(
        state: &Arc<PoolState>,
        shared_state: &Arc<PoolSharedState>,
        txg: TXG,
    ) -> BTreeMap<ObjectID, DataObjectPhys> {
        let begin = Instant::now();
        let last_obj = state.object_block_map.last_object();
        let list_stream = FuturesUnordered::new();
        for prefix in DataObjectPhys::prefixes(shared_state.guid) {
            let shared_state = shared_state.clone();
            list_stream.push(async move {
                shared_state
                    .object_access
                    .collect_objects(&prefix, Some(format!("{}{}", prefix, last_obj)))
                    .await
            });
        }

        let recovered = list_stream
            .flat_map(|vec| {
                let sub_stream = FuturesUnordered::new();
                for key in vec {
                    let shared_state = shared_state.clone();
                    sub_stream.push(async move {
                        async move {
                            DataObjectPhys::get_from_key(&shared_state.object_access, &key).await
                        }
                    });
                }
                sub_stream
            })
            .buffer_unordered(50)
            .fold(BTreeMap::new(), |mut map, data_res| async move {
                let data = data_res.unwrap();
                assert_eq!(data.guid, shared_state.guid);
                assert_eq!(data.min_txg, txg);
                assert_eq!(data.max_txg, txg);
                debug!(
                    "resume: found {:?}, min={:?} next={:?}",
                    data.object, data.min_block, data.next_block
                );
                map.insert(data.object, data);
                map
            })
            .await;
        info!(
            "resume: listed and read {} objects in {}ms",
            recovered.len(),
            begin.elapsed().as_millis()
        );
        recovered
    }

    pub async fn resume_complete(&self) {
        // XXX we need to wait for all writes to be added to
        // pending_unordered_writes, but there's no way to do that
        // XXX should no longer be needed since write_block() is called
        // synchronously from the main Server task, so all writes have already been added to pending_unordered_writes
        //sleep(Duration::from_secs(1)).await;

        let state = &self.state;
        let txg = self.state.with_syncing_state(|syncing_state| {
            // verify that we're in resuming state
            assert!(!syncing_state.pending_object.is_pending());
            syncing_state.syncing_txg.unwrap()
        });
        let shared_state = &state.shared_state;

        let recovered_objects = Self::get_recovered_objects(state, shared_state, txg).await;

        self.state.with_syncing_state(|syncing_state| {
            let ordered_writes: BTreeSet<BlockID> = syncing_state
                .pending_unordered_writes
                .keys()
                .map(|x| *x)
                .collect();

            let mut recovered_objects_iter = recovered_objects.into_iter().peekable();
            let mut ordered_writes_iter = ordered_writes.into_iter().peekable();

            while let Some((_, next_recovered_object)) = recovered_objects_iter.peek() {
                match ordered_writes_iter.peek() {
                    Some(next_ordered_write)
                        if next_ordered_write < &next_recovered_object.min_block =>
                    {
                        // writes are next, and there are objects after this

                        assert!(!syncing_state.pending_object.is_pending());
                        syncing_state.pending_object = PendingObjectState::new_pending(
                            self.state.shared_state.guid,
                            state.object_block_map.last_object().next(),
                            syncing_state.pending_object.next_block(),
                            txg,
                        );

                        // XXX Unless there is already an object at object.next(), we
                        // should limit the object size as normal.
                        // XXX we need to limit the unordered writes to be only those before the next object
                        Self::write_unordered_to_pending_object(state, syncing_state, None);

                        let (phys, _) = syncing_state.pending_object.as_mut_pending();
                        debug!(
                            "resume: writes are next; creating {:?}, min={:?} next={:?}",
                            phys.object, phys.min_block, phys.next_block
                        );

                        Self::initiate_flush_object_impl(state, syncing_state);
                        let next_block = syncing_state.pending_object.next_block();
                        syncing_state.pending_object = PendingObjectState::NotPending(next_block);

                        // skip over writes that were moved to pending_object and written out
                        while let Some(_) =
                            peekable_next_if(&mut ordered_writes_iter, |b| b < &next_block)
                        {
                        }
                    }
                    _ => {
                        // already-written object is next

                        let (_, recovered_obj) = recovered_objects_iter.next().unwrap();
                        debug!(
                            "resume: next is {:?}, min={:?} next={:?}",
                            recovered_obj.object, recovered_obj.min_block, recovered_obj.next_block
                        );

                        Self::account_new_object(state, syncing_state, &recovered_obj);

                        // The kernel may not have known that this was already
                        // written (e.g. we didn't quite get to sending the "write
                        // done" response), so it sent us the write again.  In this
                        // case we will not create a object, since the blocks are
                        // already persistent, so we need to notify the waiter now.
                        while let Some(obsolete_write) =
                            peekable_next_if(&mut ordered_writes_iter, |b| {
                                b < &recovered_obj.next_block
                            })
                        {
                            trace!(
                                "resume: {:?} is obsoleted by existing {:?}",
                                obsolete_write,
                                recovered_obj.object,
                            );
                            let (_, sender) = syncing_state
                                .pending_unordered_writes
                                .remove(&obsolete_write)
                                .unwrap();
                            sender.send(()).unwrap();
                        }
                        assert!(!syncing_state.pending_object.is_pending());
                        syncing_state.pending_object =
                            PendingObjectState::NotPending(recovered_obj.next_block);
                    }
                }
            }

            // no recovered objects left; move ordered portion of writes to pending_object
            assert!(!syncing_state.pending_object.is_pending());
            syncing_state.pending_object = PendingObjectState::new_pending(
                self.state.shared_state.guid,
                state.object_block_map.last_object().next(),
                syncing_state.pending_object.next_block(),
                txg,
            );

            debug!("resume: moving last writes to pending_object");
            Self::write_unordered_to_pending_object(
                state,
                syncing_state,
                Some(MAX_BYTES_PER_OBJECT),
            );
            info!("resume: completed");
        })
    }

    pub fn begin_txg(&self, txg: TXG) {
        self.state.with_syncing_state(|syncing_state| {
            // XXX change this to return an error to the client
            assert!(syncing_state.syncing_txg.is_none());
            assert_gt!(txg, syncing_state.last_txg);
            syncing_state.syncing_txg = Some(txg);

            assert!(!syncing_state.pending_object.is_pending());
            syncing_state.pending_object = PendingObjectState::new_pending(
                self.state.shared_state.guid,
                self.state.object_block_map.last_object().next(),
                syncing_state.pending_object.next_block(),
                txg,
            );
        })
    }

    pub async fn end_txg(&self, uberblock: Vec<u8>, config: Vec<u8>) {
        let state = &self.state;

        let mut syncing_state = {
            let mut guard = state.syncing_state.lock().unwrap();
            guard.take().unwrap()
        };

        // should have already been flushed; no pending writes
        assert!(syncing_state.pending_unordered_writes.is_empty());
        {
            let (phys, senders) = syncing_state.pending_object.as_mut_pending();
            assert!(phys.blocks.is_empty());
            assert!(senders.is_empty());

            syncing_state.pending_object = PendingObjectState::NotPending(phys.next_block);
        }

        try_reclaim_frees(state.clone(), &mut syncing_state);
        try_condense_object_log(state.clone(), &mut syncing_state).await;

        syncing_state.rewriting_objects.clear();

        let txg = syncing_state.syncing_txg.unwrap();

        // Should only be adding to this during end_txg.
        // XXX change to an Option?
        assert!(syncing_state.objects_to_delete.is_empty());

        if let Some(rt) = syncing_state.reclaim_done.as_mut() {
            if let Ok(cb) = rt.try_recv() {
                cb(&mut syncing_state).await;
            }
        }

        future::join3(
            syncing_state.storage_object_log.flush(txg),
            syncing_state.object_size_log.flush(txg),
            syncing_state.pending_frees_log.flush(txg),
        )
        .await;

        syncing_state.storage_object_log.flush(txg).await;
        syncing_state.object_size_log.flush(txg).await;
        syncing_state.pending_frees_log.flush(txg).await;

        // write uberblock
        let u = UberblockPhys {
            guid: state.shared_state.guid,
            txg,
            date: SystemTime::now(),
            storage_object_log: syncing_state.storage_object_log.to_phys(),
            object_size_log: syncing_state.object_size_log.to_phys(),
            pending_frees_log: syncing_state.pending_frees_log.to_phys(),
            next_block: syncing_state.next_block(),
            zfs_uberblock: TerseVec(uberblock),
            stats: syncing_state.stats,
            zfs_config: TerseVec(config),
        };
        u.put(&state.shared_state.object_access).await;

        // write super
        PoolPhys {
            guid: state.shared_state.guid,
            name: state.shared_state.name.clone(),
            last_txg: txg,
        }
        .put(&state.shared_state.object_access)
        .await;

        // Now that the metadata state has been atomically moved forward, we
        // can delete objects that are no longer needed
        // Note: we don't care about waiting for the frees to complete.
        // XXX need some mechanism to clean up these objects if we crash Maybe
        // we can look at the object log and look at the last txg's FREE
        // records.  We would need to make sure the frees complete before the
        // next txg.  We would need to make sure that those FREE records were
        // not consolidated away.
        // Note: we intentionally issue the delete calls serially because
        // AWS doesn't like getting a lot of them at the same time (it
        // returns HTTP 503 "Please reduce your request rate.")
        // XXX move this code to its own function?
        let objects_to_delete = syncing_state.objects_to_delete.split_off(0);
        let shared_state = state.shared_state.clone();
        tokio::spawn(async move {
            let begin = Instant::now();
            let len = objects_to_delete.len();
            for objects in objects_to_delete.chunks(900) {
                shared_state
                    .object_access
                    .delete_objects(
                        objects
                            .iter()
                            .map(|o| DataObjectPhys::key(shared_state.guid, *o))
                            .collect(),
                    )
                    .await;
            }
            if len != 0 {
                info!(
                    "deleted {} objects in {}ms",
                    len,
                    begin.elapsed().as_millis()
                );
            }
        });

        // update txg
        syncing_state.last_txg = txg;
        syncing_state.syncing_txg = None;

        // put syncing_state back in the Option
        assert!(state.syncing_state.lock().unwrap().is_none());
        *state.syncing_state.lock().unwrap() = Some(syncing_state);
    }

    fn check_pending_flushes(state: &PoolState, syncing_state: &mut PoolSyncingState) {
        let mut do_flush = false;
        let next_block = syncing_state.pending_object.as_mut_pending().0.next_block;
        while let Some(flush_block_ref) = syncing_state.pending_flushes.iter().next() {
            let flush_block = *flush_block_ref;
            if flush_block < next_block {
                do_flush = true;
                syncing_state.pending_flushes.remove(&flush_block);
            }
        }
        if do_flush {
            Self::initiate_flush_object_impl(state, syncing_state);
        }
    }

    // Begin writing out all blocks up to and including the given BlockID.  We
    // may not have called write_block() on all these blocks yet, but we will
    // soon.
    // Basically, as soon as we have this blockID and all the previous ones,
    // start writing that pending object immediately.
    pub fn initiate_flush(&self, block: BlockID) {
        self.state.with_syncing_state(|syncing_state| {
            // XXX because called when server times out waiting for request
            if syncing_state.syncing_txg.is_none() {
                return;
            }
            if !syncing_state.pending_object.is_pending() {
                return;
            }

            syncing_state.pending_flushes.insert(block);
            Self::check_pending_flushes(&self.state, syncing_state);
        })
    }

    fn account_new_object(
        state: &PoolState,
        syncing_state: &mut PoolSyncingState,
        phys: &DataObjectPhys,
    ) {
        let txg = syncing_state.syncing_txg.unwrap();
        let object = phys.object;
        assert_eq!(phys.guid, state.shared_state.guid);
        assert_eq!(phys.min_txg, txg);
        assert_eq!(phys.max_txg, txg);
        assert_ge!(object, state.object_block_map.last_object().next());
        syncing_state.stats.objects_count += 1;
        syncing_state.stats.blocks_bytes += phys.blocks_size as u64;
        syncing_state.stats.blocks_count += phys.blocks.len() as u64;
        state.object_block_map.insert(object, phys.min_block);
        syncing_state.storage_object_log.append(
            txg,
            StorageObjectLogEntry::Alloc {
                min_block: phys.min_block,
                object,
            },
        );
        syncing_state.object_size_log.append(
            txg,
            ObjectSizeLogEntry::Exists {
                object,
                num_blocks: phys.blocks.len() as u32,
                num_bytes: phys.blocks_size,
            },
        );
    }

    // completes when we've initiated the PUT to the object store.
    // callers should wait on the semaphore to ensure it's completed
    fn initiate_flush_object_impl(state: &PoolState, syncing_state: &mut PoolSyncingState) {
        let txg = syncing_state.syncing_txg.unwrap();

        let (object, next_block) = {
            let (phys, _) = syncing_state.pending_object.as_mut_pending();
            if phys.blocks.is_empty() {
                return;
            } else {
                (phys.object, phys.next_block)
            }
        };

        let (phys, senders) = mem::replace(
            &mut syncing_state.pending_object,
            PendingObjectState::new_pending(
                state.shared_state.guid,
                object.next(),
                next_block,
                txg,
            ),
        )
        .unwrap_pending();

        assert_eq!(object, phys.object);

        Self::account_new_object(state, syncing_state, &phys);

        debug!(
            "{:?}: writing {:?}: blocks={} bytes={} min={:?}",
            txg,
            object,
            phys.blocks.len(),
            phys.blocks_size,
            phys.min_block
        );

        // write to object store and wake up waiters
        let shared_state = state.shared_state.clone();
        tokio::spawn(async move {
            phys.put(&shared_state.object_access).await;
            for sender in senders {
                sender.send(()).unwrap();
            }
        });
    }

    fn do_overwrite_impl(
        state: &PoolState,
        syncing_state: &mut PoolSyncingState,
        id: BlockID,
        data: Vec<u8>,
    ) -> oneshot::Receiver<()> {
        let object = state.object_block_map.block_to_object(id);
        let shared_state = state.shared_state.clone();
        let txg = syncing_state.syncing_txg.unwrap();
        let (sender, receiver) = oneshot::channel();

        // lock is needed because client could concurrently overwrite 2
        // blocks in the same object. If the get/put's from the object store
        // could run concurrently, the last put could clobber the earlier
        // ones.
        let mtx = syncing_state
            .rewriting_objects
            .entry(object)
            .or_default()
            .clone();

        tokio::spawn(async move {
            let _guard = mtx.lock().await;
            debug!("rewriting {:?} to overwrite {:?}", object, id);
            let mut phys =
                DataObjectPhys::get(&shared_state.object_access, shared_state.guid, object)
                    .await
                    .unwrap();
            // must have been written this txg
            assert_eq!(phys.min_txg, txg);
            assert_eq!(phys.max_txg, txg);
            let removed = phys.blocks.remove(&id);
            // this blockID must have been written
            assert!(removed.is_some());

            // Size must not change.  This way we don't have to change the
            // accounting, which would require writing a new entry to the
            // ObjectSizeLog, which is not allowed in this (async) context.
            // XXX this may be problematic if we switch to ashift=0
            assert_eq!(removed.unwrap().len(), data.len());

            phys.blocks.insert(id, ByteBuf::from(data));
            phys.put(&shared_state.object_access).await;
            sender.send(()).unwrap();
        });
        receiver
    }

    fn write_unordered_to_pending_object(
        state: &PoolState,
        syncing_state: &mut PoolSyncingState,
        size_limit_opt: Option<u32>,
    ) {
        // If we're in the middle of resuming, we aren't building the pending object, so skip this
        if !syncing_state.pending_object.is_pending() {
            return;
        }

        let mut next_block = syncing_state.next_block();
        while let Some((buf, sender)) = syncing_state.pending_unordered_writes.remove(&next_block) {
            trace!(
                "found next {:?} in unordered pending writes; transferring to pending object",
                next_block
            );
            let (phys, senders) = syncing_state.pending_object.as_mut_pending();
            phys.blocks_size += buf.len() as u32;
            phys.blocks.insert(phys.next_block, buf);
            next_block = next_block.next();
            phys.next_block = next_block;
            senders.push(sender);
            if let Some(size_limit) = size_limit_opt {
                if phys.blocks_size >= size_limit {
                    Self::initiate_flush_object_impl(state, syncing_state);
                }
            }
        }
        Self::check_pending_flushes(state, syncing_state);
    }

    pub fn write_block(&self, block: BlockID, data: Vec<u8>) -> impl Future<Output = ()> {
        let receiver = self.state.with_syncing_state(|syncing_state| {
            // XXX change to return error
            assert!(syncing_state.syncing_txg.is_some());

            if block < syncing_state.next_block() {
                // XXX the design is for this to not happen. Writes must be received
                // in blockID-order. However, for now we allow overwrites during
                // sync to convergence via this slow path.
                Self::do_overwrite_impl(&self.state, syncing_state, block, data)
            } else {
                let (sender, receiver) = oneshot::channel();
                trace!("inserting {:?} to unordered pending writes", block);
                syncing_state
                    .pending_unordered_writes
                    .insert(block, (ByteBuf::from(data), sender));

                Self::write_unordered_to_pending_object(
                    &self.state,
                    syncing_state,
                    Some(MAX_BYTES_PER_OBJECT),
                );
                receiver
            }
        });
        async move {
            receiver.await.unwrap();
        }
    }

    pub async fn read_block(&self, block: BlockID) -> Vec<u8> {
        let object = self.state.object_block_map.block_to_object(block);
        let shared_state = self.state.shared_state.clone();

        debug!("reading {:?} for {:?}", object, block);
        let phys = DataObjectPhys::get(&shared_state.object_access, shared_state.guid, object)
            .await
            .unwrap();
        // XXX consider using debug_assert_eq
        assert_eq!(
            phys.blocks_size as usize,
            phys.blocks.values().map(|x| x.len()).sum::<usize>()
        );
        if phys.blocks.get(&block).is_none() {
            //println!("{:#?}", self.objects);
            error!("{:#?}", phys);
        }
        // XXX to_owned() copies the data; would be nice to return a reference
        phys.blocks.get(&block).unwrap().to_owned().into_vec()
    }

    pub fn free_block(&self, block: BlockID, size: u32) {
        // the syncing_state is only held from the thread that owns the Pool
        // (i.e. this thread) and from end_txg(). It's not allowed to call this
        // function while in the middle of an end_txg(), so the lock must not be
        // held. XXX change this to return an error to the client
        self.state.with_syncing_state(|syncing_state| {
            syncing_state.log_free(PendingFreesLogEntry { block, size });
        })
    }
}

//
// Following routines deal with reclaiming free space
//

fn log_new_sizes(
    syncing_state: &mut PoolSyncingState,
    rewritten_object_sizes: Vec<(ObjectID, u32)>,
) {
    let txg = syncing_state.syncing_txg.unwrap();
    for (object, size) in rewritten_object_sizes {
        // log to on-disk size
        syncing_state.object_size_log.append(
            txg,
            ObjectSizeLogEntry::Exists {
                object,
                num_blocks: 0, // XXX need num_blocks
                num_bytes: size,
            },
        );
    }
}

fn log_deleted_objects(
    state: Arc<PoolState>,
    syncing_state: &mut PoolSyncingState,
    deleted_objects: Vec<ObjectID>,
) {
    let txg = syncing_state.syncing_txg.unwrap();
    let begin = Instant::now();
    for object in deleted_objects {
        syncing_state
            .storage_object_log
            .append(txg, StorageObjectLogEntry::Free { object });
        state.object_block_map.remove(object);
        syncing_state
            .object_size_log
            .append(txg, ObjectSizeLogEntry::Freed { object });
        syncing_state.stats.objects_count -= 1;
        // XXX maybe use mem::replace to move our whole vector, since there
        // aren't any other users of objects_to_delete?
        syncing_state.objects_to_delete.push(object);
    }
    info!(
        "reclaim: {:?} logged {} deleted objects in {}ms",
        txg,
        syncing_state.objects_to_delete.len(),
        begin.elapsed().as_millis()
    );
}

async fn build_new_frees<'a, I>(
    syncing_state: &mut PoolSyncingState,
    remaining_frees: I,
    remainder: ObjectBasedLogRemainder,
) where
    I: IntoIterator<Item = &'a PendingFreesLogEntry>,
{
    let txg = syncing_state.syncing_txg.unwrap();
    let begin = Instant::now();

    // We need to call .iterate_after() before .clear(), otherwise we'd be
    // iterating the new, empty generation.
    let stream = syncing_state
        .pending_frees_log
        .iter_remainder(txg, remainder)
        .await;
    syncing_state.pending_frees_log.clear(txg).await;

    syncing_state.stats.pending_frees_count = 0;
    syncing_state.stats.pending_frees_bytes = 0;
    for ent in remaining_frees {
        syncing_state.log_free(*ent);
    }
    stream
        .for_each(|ent| {
            syncing_state.log_free(ent);
            future::ready(())
        })
        .await;
    // Note: the caller (end_txg_cb()) is about to call flush(), but doing it
    // here ensures that the time to PUT these objects is accounted for in the
    // info!() below.
    syncing_state.pending_frees_log.flush(txg).await;
    info!(
        "reclaim: {:?} transferred {} freed blocks in {}ms",
        txg,
        syncing_state.stats.pending_frees_count,
        begin.elapsed().as_millis()
    );
}

async fn get_object_sizes(
    object_size_log_stream: impl Stream<Item = ObjectSizeLogEntry>,
) -> BTreeMap<ObjectID, u32> {
    let mut object_sizes: BTreeMap<ObjectID, u32> = BTreeMap::new();
    let begin = Instant::now();
    object_size_log_stream
        .for_each(|ent| {
            match ent {
                ObjectSizeLogEntry::Exists {
                    object: obj,
                    num_blocks: _,
                    num_bytes,
                } => {
                    // overwrite existing value, if any
                    object_sizes.insert(obj, num_bytes);
                }
                ObjectSizeLogEntry::Freed { object: obj } => {
                    // value must already exist
                    object_sizes.remove(&obj).unwrap();
                }
            }
            future::ready(())
        })
        .await;
    info!(
        "reclaim: loaded sizes for {} objects in {}ms",
        object_sizes.len(),
        begin.elapsed().as_millis()
    );
    object_sizes
}

async fn get_frees_per_obj(
    state: &PoolState,
    pending_frees_log_stream: impl Stream<Item = PendingFreesLogEntry>,
) -> HashMap<ObjectID, Vec<PendingFreesLogEntry>> {
    // XXX The Vecs will grow by doubling, thus wasting ~1/4 of the
    // memory used by it.  It would be better if we gathered the
    // BlockID's into a single big Vec with the exact required size,
    // then in-place sort, and then have this map to a slice of the one
    // big Vec.
    let mut frees_per_obj: HashMap<ObjectID, Vec<PendingFreesLogEntry>> = HashMap::new();
    let mut count: u64 = 0;
    let begin = Instant::now();
    pending_frees_log_stream
        .for_each(|ent| {
            let obj = state.object_block_map.block_to_object(ent.block);
            // XXX change to debug-only assert?
            assert!(!frees_per_obj.entry(obj).or_default().contains(&ent));
            frees_per_obj.entry(obj).or_default().push(ent);
            count += 1;
            future::ready(())
        })
        .await;
    info!(
        "reclaim: loaded {} freed blocks in {}ms",
        count,
        begin.elapsed().as_millis()
    );
    frees_per_obj
}

async fn reclaim_frees_object(
    state: Arc<PoolState>,
    objects: Vec<(ObjectID, u32, Vec<PendingFreesLogEntry>)>,
) -> (ObjectID, u32) {
    let first_object = objects[0].0;
    let shared_state = state.shared_state.clone();
    debug!(
        "reclaim: consolidating {} objects into {:?} to free {} blocks",
        objects.len(),
        first_object,
        objects.iter().map(|x| x.2.len()).sum::<usize>()
    );

    let stream = FuturesUnordered::new();
    let mut to_delete = Vec::new();
    let mut first = None;
    for (object, new_object_size, frees) in objects {
        // All but the first object need to be deleted.
        match first {
            None => first = Some(object),
            Some(first_obj) => {
                assert_gt!(object, first_obj);
                to_delete.push(object);
            }
        }

        let min_block = state.object_block_map.object_to_min_block(object);
        let next_block = state.object_block_map.object_to_next_block(object);
        let my_shared_state = shared_state.clone();
        stream.push(async move {
            async move {
                let mut phys =
                    DataObjectPhys::get(&my_shared_state.object_access, my_shared_state.guid, object)
                        .await
                        .unwrap();
                for ent in frees {
                    // If we crashed in the middle of this operation last time, the
                    // block may already have been removed (and the object
                    // rewritten), however the stats were not yet updated (since
                    // that happens as part of txg_end, atomically with the updates
                    // to the PendingFreesLog).  In this case we ignore the fact
                    // that it isn't present, but count this block as removed for
                    // stats purposes.
                    if let Some(v) = phys.blocks.remove(&ent.block) {
                        assert_eq!(v.len() as u32, ent.size);
                        phys.blocks_size -= v.len() as u32;
                    }
                }

                // The object could have been rewritten as part of a previous
                // reclaim that we crashed in the middle of.  In that case, the
                // object may have additional blocks which we do not expect
                // (past next_block).  However, the expected size
                // (new_object_size) must match the size of the blocks within
                // the expected range (up to next_block).  Additionally, any
                // blocks outside the expected range are also represented in
                // their expected objects.  So, we can correctly remove them
                // from this object, undoing the previous, uncommitted
                // consolidation.  Therefore, if the expected size is zero, we
                // can remove this object without reading it because it doesn't
                // have any required blocks.  Instead we fabricate an empty
                // DataObjectPhys with the same metadata as what we expect.

                if phys.min_block != min_block || phys.next_block != next_block {
                    debug!("reclaim: {:?} expected range BlockID[{},{}), found BlockID[{},{}), trimming uncommitted consolidation",
                        object, min_block, next_block, phys.min_block, phys.next_block);
                    phys
                        .blocks
                        .retain(|block, _| block >= &min_block && block < &next_block);
                    assert_eq!(
                        new_object_size,
                        phys
                            .blocks
                            .iter()
                            .map(|(_, data)| data.len() as u32)
                            .sum::<u32>()
                    );
                    assert_ge!(phys.blocks_size, new_object_size);
                    phys.blocks_size = new_object_size;

                    assert_le!(phys.min_block, min_block);
                    phys.min_block = min_block;
                    assert_ge!(phys.next_block, next_block);
                    phys.next_block = next_block;
                } else {
                    assert_eq!(
                        new_object_size,
                        phys
                            .blocks
                            .iter()
                            .map(|(_, data)| data.len() as u32)
                            .sum::<u32>()
                    );
                    assert_eq!(phys.blocks_size, new_object_size);
                }

                phys
            }
        });
    }
    let new_phys = stream
        .buffered(10)
        .reduce(|mut a, mut b| async move {
            assert_eq!(a.guid, b.guid);
            debug!(
                "reclaim: moving {} blocks from {:?} (TXG[{},{}] BlockID[{},{})) to {:?} (TXG[{},{}] BlockID[{},{}))",
                b.blocks.len(),
                b.object,
                b.min_txg,
                b.max_txg,
                b.min_block,
                b.next_block,
                a.object,
                a.min_txg,
                a.max_txg,
                a.min_block,
                a.next_block,
            );
            a.object = min(a.object, b.object);
            a.min_txg = min(a.min_txg, b.min_txg);
            a.max_txg = max(a.max_txg, b.max_txg);
            a.min_block = min(a.min_block, b.min_block);
            a.next_block = max(a.next_block, b.next_block);
            let mut already_moved = 0;
            for (k, v) in b.blocks.drain() {
                let len = v.len() as u32;
                match a.blocks.insert(k, v) {
                    Some(old_vec) => {
                        // May have already been transferred in a previous job
                        // during which we crashed before updating the metadata.
                        assert_eq!(&old_vec, a.blocks.get(&k).unwrap());
                        already_moved += 1;
                    }
                    None => {
                        a.blocks_size += len;
                    }
                }
            }
            if already_moved > 0 {
                debug!(
                    "reclaim: while moving blocks from {:?} to {:?} found {} blocks already moved",
                    b.object, a.object, already_moved
                );
            }
            a
        })
        .await
        .unwrap();

    assert_eq!(new_phys.object, first_object);
    // XXX would be nice to skip this if we didn't actually make any change
    // (because we already did it all before crashing)
    new_phys.put(&shared_state.object_access).await;

    (new_phys.object, new_phys.blocks_size)
}

fn try_reclaim_frees(state: Arc<PoolState>, syncing_state: &mut PoolSyncingState) {
    if syncing_state.reclaim_done.is_some() {
        return;
    }

    // XXX change this to be based on bytes, once those stats are working?
    // XXX make this tunable?
    if syncing_state.stats.pending_frees_count
        < (syncing_state.stats.blocks_count as f64 * FREE_HIGHWATER_PCT / 100f64) as u64
        || syncing_state.stats.pending_frees_count < FREE_MIN_BLOCKS
    {
        return;
    }
    info!(
        "reclaim: {:?} starting; pending_frees_count={} blocks_count={}",
        syncing_state.syncing_txg.unwrap(),
        syncing_state.stats.pending_frees_count,
        syncing_state.stats.blocks_count,
    );

    // Note: the object size stream may or may not include entries added this
    // txg.  Fortunately, the frees stream can't have any frees within object
    // created this txg, so this is not a problem.
    let (pending_frees_log_stream, frees_remainder) = syncing_state.pending_frees_log.iter_most();

    let (object_size_log_stream, sizes_remainder) = syncing_state.object_size_log.iter_most();

    let required_frees = syncing_state.stats.pending_frees_count
        - (syncing_state.stats.blocks_count as f64 * FREE_LOWWATER_PCT / 100f64) as u64;

    let (sender, receiver) = oneshot::channel();
    syncing_state.reclaim_done = Some(receiver);

    let state = state.clone();
    tokio::spawn(async move {
        // load pending frees
        let mut frees_per_object = get_frees_per_obj(&state, pending_frees_log_stream).await;

        // sort objects by number of free blocks
        // XXX should be based on free space (bytes)?  And perhaps objects that
        // will be entirely freed should always be processed?
        let mut objects_by_frees: BTreeSet<(usize, ObjectID)> = BTreeSet::new();
        for (obj, hs) in frees_per_object.iter() {
            // MAX-len because we want to sort by which has the most to
            // free, (high to low) and then by object ID (low to high)
            // because we consolidate forward
            objects_by_frees.insert((usize::MAX - hs.len(), *obj));
        }

        // load object sizes
        let object_sizes = get_object_sizes(object_size_log_stream).await;

        let begin = Instant::now();

        let mut join_handles = Vec::new();
        let mut freed_blocks_count: u64 = 0;
        let mut freed_blocks_bytes: u64 = 0;
        let mut rewritten_object_sizes: Vec<(ObjectID, u32)> = Vec::new();
        let mut deleted_objects: Vec<ObjectID> = Vec::new();
        let mut writing: HashSet<ObjectID> = HashSet::new();
        let outstanding = Arc::new(tokio::sync::Semaphore::new(30));
        for (_, object) in objects_by_frees {
            if !frees_per_object.contains_key(&object) {
                // this object is being removed by a multi-object consolidation
                continue;
            }
            // XXX limit amount of outstanding get/put requests?
            let mut objects_to_consolidate: Vec<(ObjectID, u32, Vec<PendingFreesLogEntry>)> =
                Vec::new();
            let mut new_size: u32 = 0;
            assert!(object_sizes.contains_key(&object));
            let mut first = true;
            for (later_obj, later_size) in object_sizes.range((Included(object), Unbounded)) {
                let later_bytes_freed: u32 = frees_per_object
                    .get(later_obj)
                    .unwrap_or(&Vec::new())
                    .iter()
                    .map(|e| e.size)
                    .sum();
                let later_new_size = later_size - later_bytes_freed;
                if first {
                    assert_eq!(object, *later_obj);
                    assert!(!writing.contains(later_obj));
                    first = false;
                } else {
                    // If we run into an object that we're already writing, we
                    // can't consolidate with it.
                    if writing.contains(later_obj) {
                        break;
                    }
                    if new_size + later_new_size > MAX_BYTES_PER_OBJECT {
                        break;
                    }
                }
                new_size += later_new_size;
                let frees = frees_per_object.remove(later_obj).unwrap_or_default();
                freed_blocks_count += frees.len() as u64;
                freed_blocks_bytes += later_bytes_freed as u64;
                objects_to_consolidate.push((*later_obj, later_new_size, frees));
            }
            // XXX look for earlier objects too?

            // Must include at least the target object
            assert_eq!(objects_to_consolidate[0].0, object);

            writing.insert(object);

            // all but the first object need to be deleted by syncing context
            for (later_object, _, _) in objects_to_consolidate.iter().skip(1) {
                //complete.rewritten_object_sizes.push((*obj, 0));
                deleted_objects.push(*later_object);
            }
            // Note: we could calculate the new object's size here as well,
            // but that would be based on the object_sizes map/log, which
            // may have inaccuracies if we crashed during reclaim.  Instead
            // we calculate the size based on the object contents, and
            // return it from the spawned task.

            let sem2 = outstanding.clone();
            let state2 = state.clone();
            join_handles.push(tokio::spawn(async move {
                // limits the amount of outstanding get/put requests (roughly).
                // XXX would be nice to do this based on number of objs to consolidate
                let _permit = sem2.acquire().await.unwrap();
                reclaim_frees_object(state2, objects_to_consolidate).await
            }));
            if freed_blocks_count > required_frees {
                break;
            }
        }
        let num_handles = join_handles.len();
        for join_handle in join_handles {
            let (object, size) = join_handle.await.unwrap();
            rewritten_object_sizes.push((object, size));
        }

        info!(
            "reclaim: rewrote {} objects in {:.1}sec, freeing {} MiB from {} blocks ({:.1}MiB/s)",
            num_handles,
            begin.elapsed().as_secs_f64(),
            freed_blocks_bytes / 1024 / 1024,
            freed_blocks_count,
            ((freed_blocks_bytes as f64 / 1024f64 / 1024f64) / begin.elapsed().as_secs_f64()),
        );

        let r = sender.send(Box::new(move |syncing_state| {
            Box::pin(async move {
                syncing_state.stats.blocks_count -= freed_blocks_count;
                syncing_state.stats.blocks_bytes -= freed_blocks_bytes;

                let remaining_frees = frees_per_object.values().flatten();
                build_new_frees(syncing_state, remaining_frees, frees_remainder).await;
                log_deleted_objects(state, syncing_state, deleted_objects);
                try_condense_object_sizes(syncing_state, object_sizes, sizes_remainder).await;
                log_new_sizes(syncing_state, rewritten_object_sizes);

                syncing_state.reclaim_done = None;
            })
        }));
        assert!(r.is_ok()); // can not use .unwrap() because the type is not Debug
    });
}

//
// following routines deal with condensing other ObjectBasedLogs
//

async fn try_condense_object_log(state: Arc<PoolState>, syncing_state: &mut PoolSyncingState) {
    // XXX change this to be based on bytes, once those stats are working?
    let len = state.object_block_map.len();
    if syncing_state.storage_object_log.num_chunks
        < (LOG_CONDENSE_MIN_CHUNKS
            + LOG_CONDENSE_MULTIPLE * (len + ENTRIES_PER_OBJECT) / ENTRIES_PER_OBJECT)
            as u64
    {
        return;
    }
    let txg = syncing_state.syncing_txg.unwrap();
    info!(
        "{:?} storage_object_log condense: starting; objects={} entries={} len={}",
        txg,
        syncing_state.storage_object_log.num_chunks,
        syncing_state.storage_object_log.num_entries,
        len
    );

    let begin = Instant::now();
    syncing_state.storage_object_log.clear(txg).await;
    {
        state.object_block_map.for_each(|ent| {
            syncing_state.storage_object_log.append(
                txg,
                StorageObjectLogEntry::Alloc {
                    object: ent.object,
                    min_block: ent.block,
                },
            )
        });
    }
    // Note: the caller (end_txg_cb()) is about to call flush(), but doing it
    // here ensures that the time to PUT these objects is accounted for in the
    // info!() below.
    syncing_state.storage_object_log.flush(txg).await;

    info!(
        "{:?} storage_object_log condense: wrote {} entries to {} objects in {}ms",
        txg,
        syncing_state.storage_object_log.num_entries,
        syncing_state.storage_object_log.num_chunks,
        begin.elapsed().as_millis()
    );
}

async fn try_condense_object_sizes(
    syncing_state: &mut PoolSyncingState,
    object_sizes: BTreeMap<ObjectID, u32>,
    remainder: ObjectBasedLogRemainder,
) {
    // XXX change this to be based on bytes, once those stats are working?
    let len = object_sizes.len();
    if syncing_state.object_size_log.num_chunks
        < (LOG_CONDENSE_MIN_CHUNKS
            + LOG_CONDENSE_MULTIPLE * (len + ENTRIES_PER_OBJECT) / ENTRIES_PER_OBJECT)
            as u64
    {
        return;
    }
    let txg = syncing_state.syncing_txg.unwrap();
    info!(
        "{:?} object_size_log condense: starting; objects={} entries={} len={}",
        txg,
        syncing_state.object_size_log.num_chunks,
        syncing_state.object_size_log.num_entries,
        len
    );

    let begin = Instant::now();
    // We need to call .iterate_after() before .clear(), otherwise we'd be
    // iterating the new, empty generation.
    let stream = syncing_state
        .object_size_log
        .iter_remainder(txg, remainder)
        .await;
    syncing_state.object_size_log.clear(txg).await;
    {
        for (object, num_bytes) in object_sizes.iter() {
            syncing_state.object_size_log.append(
                txg,
                ObjectSizeLogEntry::Exists {
                    object: *object,
                    num_blocks: 0, // XXX need num blocks
                    num_bytes: *num_bytes,
                },
            );
        }
    }

    stream
        .for_each(|ent| {
            syncing_state.object_size_log.append(txg, ent);
            future::ready(())
        })
        .await;
    // Note: the caller (end_txg_cb()) is about to call flush(), but doing it
    // here ensures that the time to PUT these objects is accounted for in the
    // info!() below.
    syncing_state.object_size_log.flush(txg).await;

    info!(
        "{:?} object_size_log condense: wrote {} entries to {} objects in {}ms",
        txg,
        syncing_state.object_size_log.num_entries,
        syncing_state.object_size_log.num_chunks,
        begin.elapsed().as_millis()
    );
}

// This works like Peekable::next_if(), which isn't available in the version of Rust that we use.
fn peekable_next_if<I: Iterator>(
    this: &mut std::iter::Peekable<I>,
    func: impl FnOnce(&I::Item) -> bool,
) -> Option<I::Item> {
    match this.peek() {
        Some(matched) if func(&matched) => this.next(),
        _ => None,
    }
}
