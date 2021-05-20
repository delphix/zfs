use crate::object_based_log::*;
use crate::object_block_map::ObjectBlockMap;
use crate::{base_types::*, object_access::ObjectAccess};
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
use tokio::sync::*;

// XXX need a real tunables infrastructure
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
        obj: ObjectID,
        first_possible_block: BlockID,
    },
    Free {
        obj: ObjectID,
    },
}
impl OnDisk for StorageObjectLogEntry {}
impl ObjectBasedLogEntry for StorageObjectLogEntry {}

#[derive(Debug, Serialize, Deserialize, Copy, Clone)]
enum ObjectSizeLogEntry {
    Exists {
        obj: ObjectID,
        num_blocks: u32,
        num_bytes: u32, // bytes in blocks; does not include Agent metadata
    },
    Freed {
        obj: ObjectID,
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

    async fn get(object_access: &ObjectAccess, guid: PoolGUID) -> Self {
        let buf = object_access.get_object(&Self::key(guid)).await;
        let this: Self = bincode::deserialize(&buf).unwrap();
        debug!("got {:#?}", this);
        assert_eq!(this.guid, guid);
        this
    }

    async fn put(&self, object_access: &ObjectAccess) {
        debug!("putting {:#?}", self);
        let buf = bincode::serialize(&self).unwrap();
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

    async fn get(object_access: &ObjectAccess, guid: PoolGUID, txg: TXG) -> Self {
        let buf = object_access.get_object(&Self::key(guid, txg)).await;
        let this: Self = bincode::deserialize(&buf).unwrap();
        debug!("got {:#?}", this);
        assert_eq!(this.guid, guid);
        assert_eq!(this.txg, txg);
        this
    }

    async fn put(&self, object_access: &ObjectAccess) {
        debug!("putting {:#?}", self);
        let buf = bincode::serialize(&self).unwrap();
        object_access
            .put_object(&Self::key(self.guid, self.txg), buf)
            .await;
    }
}

impl DataObjectPhys {
    fn key(guid: PoolGUID, obj: ObjectID) -> String {
        format!("zfs/{}/data/{}/{}", guid, obj.0 % 64, obj)
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

    async fn get(object_access: &ObjectAccess, guid: PoolGUID, obj: ObjectID) -> Self {
        let buf = object_access.get_object(&Self::key(guid, obj)).await;
        let begin = Instant::now();
        let this: Self = bincode::deserialize(&buf).unwrap();
        debug!(
            "{:?}: deserialized {} blocks from {} bytes in {}ms",
            obj,
            this.blocks.len(),
            buf.len(),
            begin.elapsed().as_millis()
        );
        assert_eq!(this.guid, guid);
        assert_eq!(this.object, obj);
        this.verify();
        this
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
    syncing_state: tokio::sync::Mutex<PoolSyncingState>,
    block_to_obj: std::sync::RwLock<ObjectBlockMap>,
    pub readonly_state: Arc<PoolSharedState>,
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
    reclaim_cb: Option<oneshot::Receiver<SyncTask>>,
    // Protects objects that are being overwritten for sync-to-convergence
    rewriting_objects: HashMap<ObjectID, Arc<tokio::sync::Mutex<()>>>,
    // objects to delete at the end of this txg
    objects_to_delete: Vec<ObjectID>,
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
        match &self.pending_object {
            PendingObjectState::Pending(phys, _) => phys.next_block,
            PendingObjectState::NotPending(next_block) => *next_block,
        }
    }

    fn log_free(&mut self, ent: PendingFreesLogEntry) {
        let txg = self.syncing_txg.unwrap();
        assert_lt!(ent.block, self.next_block());
        self.pending_frees_log.append(txg, ent);
        self.stats.pending_frees_count += 1;
        self.stats.pending_frees_bytes += ent.size as u64;
    }
}

impl Pool {
    pub async fn get_config(object_access: &ObjectAccess, guid: PoolGUID) -> NvList {
        let pool_phys = PoolPhys::get(object_access, guid).await;
        let ubphys = UberblockPhys::get(object_access, pool_phys.guid, pool_phys.last_txg).await;
        NvList::try_unpack(&ubphys.zfs_config.0).unwrap()
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
        let phys = UberblockPhys::get(object_access, pool_phys.guid, txg).await;

        let readonly_state = Arc::new(PoolSharedState {
            object_access: object_access.clone(),
            guid: pool_phys.guid,
            name: pool_phys.name.clone(),
        });
        let pool = Pool {
            state: Arc::new(PoolState {
                readonly_state: readonly_state.clone(),
                syncing_state: tokio::sync::Mutex::new(PoolSyncingState {
                    last_txg: phys.txg,
                    syncing_txg: None,
                    storage_object_log: ObjectBasedLog::open_by_phys(
                        readonly_state.clone(),
                        &format!("zfs/{}/StorageObjectLog", pool_phys.guid),
                        &phys.storage_object_log,
                    ),
                    object_size_log: ObjectBasedLog::open_by_phys(
                        readonly_state.clone(),
                        &format!("zfs/{}/ObjectSizeLog", pool_phys.guid),
                        &phys.object_size_log,
                    ),
                    pending_frees_log: ObjectBasedLog::open_by_phys(
                        readonly_state.clone(),
                        &format!("zfs/{}/PendingFreesLog", pool_phys.guid),
                        &phys.pending_frees_log,
                    ),
                    pending_object: PendingObjectState::NotPending(phys.next_block),
                    pending_unordered_writes: HashMap::new(),
                    stats: phys.stats,
                    reclaim_cb: None,
                    rewriting_objects: HashMap::new(),
                    objects_to_delete: Vec::new(),
                }),
                block_to_obj: std::sync::RwLock::new(ObjectBlockMap::new()),
            }),
        };

        let mut syncing_state = pool.state.syncing_state.try_lock().unwrap();

        syncing_state.storage_object_log.recover().await;
        syncing_state.object_size_log.recover().await;
        syncing_state.pending_frees_log.recover().await;

        // load block -> object mapping
        let begin = Instant::now();
        let objects_rwlock = &pool.state.block_to_obj;
        let mut num_alloc_entries: u64 = 0;
        let mut num_free_entries: u64 = 0;
        syncing_state
            .storage_object_log
            .iterate()
            .for_each(|ent| {
                let mut objects = objects_rwlock.write().unwrap();
                match ent {
                    StorageObjectLogEntry::Alloc {
                        obj,
                        first_possible_block,
                    } => {
                        objects.insert(obj, first_possible_block);
                        num_alloc_entries += 1;
                    }
                    StorageObjectLogEntry::Free { obj } => {
                        objects.remove(obj);
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

        objects_rwlock.read().unwrap().verify();

        assert_eq!(
            objects_rwlock.read().unwrap().len() as u64,
            syncing_state.stats.objects_count
        );

        let next_block = syncing_state.next_block();
        drop(syncing_state);

        //println!("opened {:#?}", pool);

        (pool, Some(phys), next_block)
    }

    pub async fn open(
        object_access: &ObjectAccess,
        guid: PoolGUID,
    ) -> (Pool, Option<UberblockPhys>, BlockID) {
        let phys = PoolPhys::get(object_access, guid).await;
        if phys.last_txg.0 == 0 {
            let readonly_state = Arc::new(PoolSharedState {
                object_access: object_access.clone(),
                guid,
                name: phys.name,
            });
            let pool = Pool {
                state: Arc::new(PoolState {
                    readonly_state: readonly_state.clone(),
                    syncing_state: tokio::sync::Mutex::new(PoolSyncingState {
                        last_txg: TXG(0),
                        syncing_txg: None,
                        storage_object_log: ObjectBasedLog::create(
                            readonly_state.clone(),
                            &format!("zfs/{}/StorageObjectLog", guid),
                        ),
                        object_size_log: ObjectBasedLog::create(
                            readonly_state.clone(),
                            &format!("zfs/{}/ObjectSizeLog", guid),
                        ),
                        pending_frees_log: ObjectBasedLog::create(
                            readonly_state.clone(),
                            &format!("zfs/{}/PendingFreesLog", guid),
                        ),
                        pending_object: PendingObjectState::NotPending(BlockID(0)),
                        pending_unordered_writes: HashMap::new(),
                        stats: PoolStatsPhys::default(),
                        reclaim_cb: None,
                        rewriting_objects: HashMap::new(),
                        objects_to_delete: Vec::new(),
                    }),
                    block_to_obj: std::sync::RwLock::new(ObjectBlockMap::new()),
                }),
            };
            let syncing_state = pool.state.syncing_state.try_lock().unwrap();
            let next_block = syncing_state.next_block();
            drop(syncing_state);
            (pool, None, next_block)
        } else {
            Pool::open_from_txg(object_access, &phys, phys.last_txg).await
        }
    }

    pub async fn get_prop(&self, name: &str) -> u64 {
        /*
         * XXX find another way to get stats that doesn't invlove getting the
         * syncing state lock. The problem is that we need benig_txg() to get the
         * lock without waiting, which is otherwise OK since there can't be
         * writes, frees, or end_txg's going on while we try to start a txg.
         */
        let stats = self.state.syncing_state.lock().await.stats;
        match name {
            "zoa_allocated" => stats.pending_frees_bytes,
            "zoa_freeing" => stats.pending_frees_bytes,
            "zoa_objects" => stats.objects_count,
            _ => panic!("invalid prop name: {}", name),
        }
    }

    pub fn begin_txg(&self, txg: TXG) {
        // The syncing_state is only held while a txg is open (begun).  It's not
        // allowed to call begin_txg() while a txg is already open, so the lock
        // must not be held.
        // XXX change this to return an error to the client
        let mut syncing_state = self.state.syncing_state.try_lock().unwrap();

        assert!(syncing_state.syncing_txg.is_none());
        assert_gt!(txg.0, syncing_state.last_txg.0);
        syncing_state.syncing_txg = Some(txg);
        let last_obj = self.state.block_to_obj.read().unwrap().last_obj();

        match syncing_state.pending_object {
            PendingObjectState::NotPending(next_block) => {
                syncing_state.pending_object = PendingObjectState::Pending(
                    DataObjectPhys {
                        guid: self.state.readonly_state.guid,
                        object: last_obj.next(),
                        min_block: next_block,
                        next_block,
                        min_txg: txg,
                        max_txg: txg,
                        blocks_size: 0,
                        blocks: HashMap::new(),
                    },
                    Vec::new(),
                );
            }
            _ => {
                panic!("invalid {:?}", syncing_state.pending_object);
            }
        }
    }

    pub async fn end_txg(&self, uberblock: Vec<u8>, config: Vec<u8>) {
        let state = &self.state;
        let mut syncing_state = state.syncing_state.lock().await;

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

        if let Some(rt) = syncing_state.reclaim_cb.as_mut() {
            if let Ok(cb) = rt.try_recv() {
                cb(&mut syncing_state).await;
            }
        }

        // XXX await these 3 at the same time?
        syncing_state.storage_object_log.flush(txg).await;
        syncing_state.object_size_log.flush(txg).await;
        syncing_state.pending_frees_log.flush(txg).await;

        // write uberblock
        let u = UberblockPhys {
            guid: state.readonly_state.guid,
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
        u.put(&state.readonly_state.object_access).await;

        // write super
        let s = PoolPhys {
            guid: state.readonly_state.guid,
            name: state.readonly_state.name.clone(),
            last_txg: txg,
        };
        s.put(&state.readonly_state.object_access).await;

        // Now that the metadata state has been atomically moved forward, we
        // can delete objects that are no longer needed
        // Note: we don't care about waiting for the frees to complete.
        // XXX need some mechanism to clean up these objects if we crash
        // Note: we intentionally issue the delete calls serially because
        // AWS doesn't like getting a lot of them at the same time (it
        // returns HTTP 503 "Please reduce your request rate.")
        // XXX move this code to its own function?
        let objects_to_delete = syncing_state.objects_to_delete.split_off(0);
        let readonly_state = state.readonly_state.clone();
        tokio::spawn(async move {
            let begin = Instant::now();
            let len = objects_to_delete.len();
            for objs in objects_to_delete.chunks(900) {
                let mut v = Vec::new();
                for obj in objs {
                    let key = DataObjectPhys::key(readonly_state.guid, *obj);
                    v.push(key);
                }
                readonly_state.object_access.delete_objects(v).await;
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
    }

    /*
    pub async fn flush_writes(&mut self) {
        self.initiate_flush_object();
        Self::wait_for_pending_flushes(&mut self.syncing_state.try_lock().unwrap());
    }

    pub async fn flush_up_to(&mut self, block: BlockID) {}
    */

    // Note: only flushes writes that we've received, that are in-order.
    // pending_unordered_writes are not flushed.
    pub async fn initiate_flush_object(&self) {
        let mut syncing_state = self.state.syncing_state.lock().await;
        // XXX because called when server times out waiting for request
        if syncing_state.syncing_txg.is_none() {
            return;
        }

        Self::initiate_flush_object_impl(&self.state, &mut *syncing_state);
    }

    // completes when we've initiated the PUT to the object store.
    // callers should wait on the semaphore to ensure it's completed
    fn initiate_flush_object_impl(state: &PoolState, syncing_state: &mut PoolSyncingState) {
        let txg = syncing_state.syncing_txg.unwrap();

        let (obj, next_block) = {
            let (phys, _) = syncing_state.pending_object.as_mut_pending();
            if phys.blocks.is_empty() {
                return;
            } else {
                (phys.object, phys.next_block)
            }
        };

        let (phys, senders) = mem::replace(
            &mut syncing_state.pending_object,
            PendingObjectState::Pending(
                DataObjectPhys {
                    guid: state.readonly_state.guid,
                    object: obj.next(),
                    min_txg: txg,
                    max_txg: txg,
                    min_block: next_block,
                    next_block,
                    blocks_size: 0,
                    blocks: HashMap::new(),
                },
                Vec::new(),
            ),
        )
        .unwrap_pending();

        assert_eq!(phys.guid, state.readonly_state.guid);
        assert_eq!(phys.min_txg, txg);
        assert_eq!(phys.max_txg, txg);

        assert_eq!(obj, state.block_to_obj.read().unwrap().last_obj().next());

        // increment stats
        syncing_state.stats.objects_count += 1;
        syncing_state.stats.blocks_bytes += phys.blocks_size as u64;
        syncing_state.stats.blocks_count += phys.blocks.len() as u64;

        // add to in-memory block->object map
        state
            .block_to_obj
            .write()
            .unwrap()
            .insert(obj, phys.min_block);

        // log to on-disk block->object map
        syncing_state.storage_object_log.append(
            txg,
            StorageObjectLogEntry::Alloc {
                first_possible_block: phys.min_block,
                obj,
            },
        );

        // log to on-disk size
        syncing_state.object_size_log.append(
            txg,
            ObjectSizeLogEntry::Exists {
                obj,
                num_blocks: phys.blocks.len() as u32,
                num_bytes: phys.blocks_size,
            },
        );

        debug!(
            "{:?}: writing {:?}: blocks={} bytes={} min={:?}",
            txg,
            obj,
            phys.blocks.len(),
            phys.blocks_size,
            phys.min_block
        );

        // write to object store and wake up waiters
        let readonly_state = state.readonly_state.clone();
        tokio::spawn(async move {
            phys.put(&readonly_state.object_access).await;
            for s in senders {
                s.send(()).unwrap();
            }
        });
    }

    fn do_overwrite_impl(
        state: &PoolState,
        syncing_state: &mut PoolSyncingState,
        id: BlockID,
        data: Vec<u8>,
    ) -> oneshot::Receiver<()> {
        let obj = state.block_to_obj.read().unwrap().block_to_obj(id);
        let shared_state = state.readonly_state.clone();
        let txg = syncing_state.syncing_txg.unwrap();
        let (s, r) = oneshot::channel();

        // lock is needed because client could concurrently overwrite 2
        // blocks in the same object. If the get/put's from the object store
        // could run concurrently, the last put could clobber the earlier
        // ones.
        let mtx = syncing_state
            .rewriting_objects
            .entry(obj)
            .or_default()
            .clone();

        tokio::spawn(async move {
            let _guard = mtx.lock().await;
            debug!("rewriting {:?} to overwrite {:?}", obj, id);
            let mut obj_phys =
                DataObjectPhys::get(&shared_state.object_access, shared_state.guid, obj).await;
            // must have been written this txg
            assert_eq!(obj_phys.min_txg, txg);
            assert_eq!(obj_phys.max_txg, txg);
            let removed = obj_phys.blocks.remove(&id);
            // this blockID must have been written
            assert!(removed.is_some());

            // Size must not change.  This way we don't have to change the
            // accounting, which would require writing a new entry to the
            // ObjectSizeLog, which is not allowed in this (async) context.
            // XXX this may be problematic if we switch to ashift=0
            assert_eq!(removed.unwrap().len(), data.len());

            obj_phys.blocks.insert(id, ByteBuf::from(data));
            obj_phys.put(&shared_state.object_access).await;
            s.send(()).unwrap();
        });
        r
    }

    fn write_unordered_to_ordered(
        state: &PoolState,
        mut syncing_state: MutexGuard<PoolSyncingState>,
    ) {
        let mut nb = syncing_state.next_block();
        while let Some((buf, s)) = syncing_state.pending_unordered_writes.remove(&nb) {
            trace!(
                "found next {:?} in unordered pending writes; transferring to pending object",
                nb
            );
            let (phys, senders) = syncing_state.pending_object.as_mut_pending();
            phys.blocks_size += buf.len() as u32;
            phys.blocks.insert(phys.next_block, buf);
            nb = nb.next();
            phys.next_block = nb;
            //let (_, senders) = syncing_state.pending_object.unwrap_pending();
            senders.push(s);
            if phys.blocks_size >= MAX_BYTES_PER_OBJECT {
                Self::initiate_flush_object_impl(state, &mut syncing_state);
            }
        }
    }

    pub async fn write_block(&self, id: BlockID, data: Vec<u8>) {
        let r;
        // ensure that the syncig_state lock is dropped before we wait on `r`.
        {
            let mut syncing_state = self.state.syncing_state.lock().await;
            // XXX change to return error
            assert!(syncing_state.syncing_txg.is_some());

            if id < syncing_state.next_block() {
                // XXX the design is for this to not happen. Writes must be received
                // in blockID-order. However, for now we allow overwrites during
                // sync to convergence via this slow path.
                r = Self::do_overwrite_impl(&self.state, &mut syncing_state, id, data);
            } else {
                let (s, myr) = oneshot::channel();
                trace!("inserting {:?} to unordered pending writes", id);
                syncing_state
                    .pending_unordered_writes
                    .insert(id, (ByteBuf::from(data), s));

                Self::write_unordered_to_ordered(&self.state, syncing_state);
                r = myr;
            };
        }

        r.await.unwrap();
    }

    pub async fn read_block(&self, id: BlockID) -> Vec<u8> {
        let obj = self.state.block_to_obj.read().unwrap().block_to_obj(id);
        let readonly_state = self.state.readonly_state.clone();
        //let state = self.state.clone();

        debug!("reading {:?} for {:?}", obj, id);
        let block =
            DataObjectPhys::get(&readonly_state.object_access, readonly_state.guid, obj).await;
        // XXX consider using debug_assert_eq
        assert_eq!(
            block.blocks_size as usize,
            block.blocks.values().map(|x| x.len()).sum::<usize>()
        );
        if block.blocks.get(&id).is_none() {
            //println!("{:#?}", self.objects);
            error!("{:#?}", block);
        }
        // XXX to_owned() copies the data; would be nice to pass a reference to the callback
        block.blocks.get(&id).unwrap().to_owned().into_vec()
    }

    pub async fn free_block(&self, block: BlockID, size: u32) {
        // the syncing_state is only held from the thread that owns the Pool
        // (i.e. this thread) and from end_txg(). It's not allowed to call this
        // function while in the middle of an end_txg(), so the lock must not be
        // held. XXX change this to return an error to the client
        let mut syncing_state = self.state.syncing_state.lock().await;
        syncing_state.log_free(PendingFreesLogEntry { block, size });
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
    for (obj, size) in rewritten_object_sizes {
        // log to on-disk size
        syncing_state.object_size_log.append(
            txg,
            ObjectSizeLogEntry::Exists {
                obj,
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
    for obj in deleted_objects {
        syncing_state
            .storage_object_log
            .append(txg, StorageObjectLogEntry::Free { obj });
        state.block_to_obj.write().unwrap().remove(obj);
        syncing_state
            .object_size_log
            .append(txg, ObjectSizeLogEntry::Freed { obj });
        syncing_state.stats.objects_count -= 1;
        // XXX maybe use mem::replace to move our whole vector, since there
        // aren't any other users of objects_to_delete?
        syncing_state.objects_to_delete.push(obj);
    }
    info!(
        "reclaim: {:?} logged {} deleted objects in {}ms",
        txg,
        syncing_state.objects_to_delete.len(),
        begin.elapsed().as_millis()
    );
}

async fn build_new_frees(
    syncing_state: &mut PoolSyncingState,
    remaining_frees: Vec<PendingFreesLogEntry>,
    remainder: ObjectBasedLogRemainder,
) {
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
        // XXX could we build this in open context and then just add the new entries from syncing context?
        syncing_state.log_free(ent);
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
                    obj,
                    num_blocks: _,
                    num_bytes,
                } => {
                    // overwrite existing value, if any
                    object_sizes.insert(obj, num_bytes);
                }
                ObjectSizeLogEntry::Freed { obj } => {
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
            let obj = state.block_to_obj.read().unwrap().block_to_obj(ent.block);
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
    shared_state: Arc<PoolSharedState>,
    objs: Vec<(ObjectID, u32, Vec<PendingFreesLogEntry>)>,
) -> (ObjectID, u32) {
    let first_obj = objs[0].0;
    debug!(
        "reclaim: consolidating {} objects into {:?} to free {} blocks",
        objs.len(),
        first_obj,
        objs.iter().map(|x| x.2.len()).sum::<usize>()
    );

    let stream = FuturesUnordered::new();
    let mut to_delete = Vec::new();
    let mut first = true;
    for (obj, _, frees) in objs {
        if !first {
            to_delete.push(obj);
        }
        first = false;

        let my_shared_state = shared_state.clone();
        stream.push(async move {
            async move {
                let mut obj_phys =
                    DataObjectPhys::get(&my_shared_state.object_access, my_shared_state.guid, obj)
                        .await;
                // XXX This is not true, because the object could have been
                // rewritten as part of a previous reclaim that we crashed in
                // the middle of.  In this case the actual size may be larger or
                // smaller than the ObjectSizeLog Entry, because this object may
                // have been compacted on its own (shrinking it), or
                // consolidated with subsequent objects (it could grow or
                // shrink).
                /*
                assert_ge!(
                    obj_size,
                    obj_phys.blocks_size,
                    "{} ObjectSizeLogEntry should be at least as large as actual size",
                    obj,
                );
                */
                for pfle in frees {
                    let removed = obj_phys.blocks.remove(&pfle.block);
                    // If we crashed in the middle of this operation last time, the
                    // block may already have been removed (and the object
                    // rewritten), however the stats were not yet updated (since
                    // that happens as part of txg_end, atomically with the updates
                    // to the PendingFreesLog).  In this case we ignore the fact
                    // that it isn't present, but count this block as removed for
                    // stats purposes.
                    if let Some(v) = removed {
                        assert_eq!(v.len() as u32, pfle.size);
                        obj_phys.blocks_size -= v.len() as u32;
                    }
                }
                obj_phys
            }
        });
    }
    let new_obj = stream
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

    assert_eq!(new_obj.object, first_obj);
    new_obj.put(&shared_state.object_access).await;

    (new_obj.object, new_obj.blocks_size)
}

fn try_reclaim_frees(state: Arc<PoolState>, syncing_state: &mut PoolSyncingState) {
    if syncing_state.reclaim_cb.is_some() {
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

    let (s, r) = oneshot::channel();
    syncing_state.reclaim_cb = Some(r);

    let state = state.clone();
    tokio::spawn(async move {
        let shared_state = &state.readonly_state;

        // load pending frees
        let mut frees_per_obj = get_frees_per_obj(&state, pending_frees_log_stream).await;

        // sort objects by number of free blocks
        // XXX should be based on free space (bytes)?
        let mut objs_by_frees: BTreeSet<(usize, ObjectID)> = BTreeSet::new();
        for (obj, hs) in frees_per_obj.iter() {
            // MAX-len because we want to sort by which has the most to
            // free, (high to low) and then by object ID (low to high)
            // because we consolidate forward
            objs_by_frees.insert((usize::MAX - hs.len(), *obj));
        }

        // load object sizes
        let object_sizes = get_object_sizes(object_size_log_stream).await;

        let begin = Instant::now();

        let mut join_handles = Vec::new();
        let mut freed_blocks_count: u64 = 0;
        let mut freed_blocks_bytes: u64 = 0;
        let mut remaining_frees: Vec<PendingFreesLogEntry> = Vec::new();
        let mut rewritten_object_sizes: Vec<(ObjectID, u32)> = Vec::new();
        let mut deleted_objects: Vec<ObjectID> = Vec::new();
        let mut writing: HashSet<ObjectID> = HashSet::new();
        let outstanding = Arc::new(tokio::sync::Semaphore::new(30));
        for (_, obj) in objs_by_frees {
            if !frees_per_obj.contains_key(&obj) {
                // this object is being removed by a multi-object consolidation
                continue;
            }
            // XXX limit amount of outstanding get/put requests?
            let mut objs_to_consolidate: Vec<(ObjectID, u32, Vec<PendingFreesLogEntry>)> =
                Vec::new();
            let mut new_size: u32 = 0;
            assert!(object_sizes.contains_key(&obj));
            let mut first = true;
            for (later_obj, later_size) in object_sizes.range((Included(obj), Unbounded)) {
                let later_bytes_freed: u32 = frees_per_obj
                    .get(later_obj)
                    .unwrap_or(&Vec::new())
                    .iter()
                    .map(|e| e.size)
                    .sum();
                let later_new_size = later_size - later_bytes_freed;
                if first {
                    assert_eq!(obj, *later_obj);
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
                let frees = frees_per_obj.remove(later_obj).unwrap_or_default();
                freed_blocks_count += frees.len() as u64;
                freed_blocks_bytes += later_bytes_freed as u64;
                objs_to_consolidate.push((*later_obj, *later_size, frees));
            }
            // XXX look for earlier objects too?

            // Must include at least the target object
            assert_eq!(objs_to_consolidate[0].0, obj);

            writing.insert(obj);

            // all but the first object need to be deleted by syncing context
            for (later_obj, _, _) in objs_to_consolidate.iter().skip(1) {
                //complete.rewritten_object_sizes.push((*obj, 0));
                deleted_objects.push(*later_obj);
            }
            // Note: we could calculate the new object's size here as well,
            // but that would be based on the object_sizes map/log, which
            // may have inaccuracies if we crashed during reclaim.  Instead
            // we calculate the size based on the object contents, and
            // return it from the spawned task.

            // XXX would be nice to know if we are freeing the entire object
            // in which case we wouldn't need to read it.  Would have to
            // keep a count of blocks per object in RAM?
            let sem2 = outstanding.clone();
            let ss2 = shared_state.clone();
            join_handles.push(tokio::spawn(async move {
                // limits the amount of outstanding get/put requests (roughly).
                // XXX would be nice to do this based on number of objs to consolidate
                let p = sem2.acquire().await;
                assert!(p.is_ok());
                reclaim_frees_object(ss2, objs_to_consolidate).await
            }));
            if freed_blocks_count > required_frees {
                break;
            }
        }
        for (_, mut frees) in frees_per_obj.drain() {
            // XXX copying around the blocks, although maybe this is not huge???
            // XXX could simply give the whole frees_per_obj and have syncing context iterate
            remaining_frees.append(&mut frees);
        }

        let num_handles = join_handles.len();
        for jh in join_handles {
            let (obj, size) = jh.await.unwrap();
            rewritten_object_sizes.push((obj, size));
        }

        info!(
            "reclaim: rewrote {} objects in {:.1}sec, freeing {} MiB from {} blocks ({:.1}MiB/s)",
            num_handles,
            begin.elapsed().as_secs_f64(),
            freed_blocks_bytes / 1024 / 1024,
            freed_blocks_count,
            ((freed_blocks_bytes as f64 / 1024f64 / 1024f64) / begin.elapsed().as_secs_f64()),
        );

        let r = s.send(Box::new(move |syncing_state| {
            Box::pin(async move {
                syncing_state.stats.blocks_count -= freed_blocks_count;
                syncing_state.stats.blocks_bytes -= freed_blocks_bytes;

                build_new_frees(syncing_state, remaining_frees, frees_remainder).await;
                log_deleted_objects(state, syncing_state, deleted_objects);
                try_condense_object_sizes(syncing_state, object_sizes, sizes_remainder).await;
                log_new_sizes(syncing_state, rewritten_object_sizes);

                syncing_state.reclaim_cb = None;
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
    let len = state.block_to_obj.read().unwrap().len();
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
        let block_to_obj = state.block_to_obj.read().unwrap();
        for ent in block_to_obj.iter() {
            syncing_state.storage_object_log.append(
                txg,
                StorageObjectLogEntry::Alloc {
                    obj: ent.obj,
                    first_possible_block: ent.block,
                },
            );
        }
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
        for (obj, num_bytes) in object_sizes.iter() {
            syncing_state.object_size_log.append(
                txg,
                ObjectSizeLogEntry::Exists {
                    obj: *obj,
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
