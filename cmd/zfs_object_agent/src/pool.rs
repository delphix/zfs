use crate::base_types::*;
use crate::object_access;
use crate::object_based_log::*;
use crate::object_block_map::ObjectBlockMap;
use core::future::Future;
use futures::future;
use futures::future::*;
use futures::stream::*;
use s3::bucket::Bucket;
use serde::{Deserialize, Serialize};
use std::cmp::min;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::mem;
use std::ops::Bound::*;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Instant, SystemTime};
use stream_reduce::Reduce;
use tokio::sync::*;
use tokio::task::JoinHandle;

// XXX need a real tunables infrastructure
// start freeing when the pending frees are this % of the entire pool
const FREE_HIGHWATER_PCT: f64 = 2.0;
// stop freeing when the pending frees are this % of the entire pool
const FREE_LOWWATER_PCT: f64 = 1.5;
// don't bother freeing unless there are at least this number of free blocks
const FREE_MIN_BLOCKS: u64 = 1000;
// XXX change this to bytes
const MAX_BLOCKS_PER_OBJECT: usize = 100;
// XXX increase
const MAX_BYTES_PER_OBJECT: u32 = 128 * 1024;

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
    highest_block: BlockID, // highest blockID in use
    stats: PoolStatsPhys,
    zfs_uberblock: Vec<u8>,
}
impl OnDisk for UberblockPhys {}

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
    guid: PoolGUID,   // redundant with key, for verification
    object: ObjectID, // redundant with key, for verification
    blocks_size: u32, // sum of blocks.values().len()
    // XXX add min/max block ID ?
    txg: TXG, // for debugging
    blocks: HashMap<BlockID, Vec<u8>>,
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

    async fn get(bucket: &Bucket, guid: PoolGUID) -> Self {
        let buf = object_access::get_object(bucket, &Self::key(guid)).await;
        let this: Self = bincode::deserialize(&buf).unwrap();
        println!("got {:#?}", this);
        assert_eq!(this.guid, guid);
        this
    }

    async fn put(&self, bucket: &Bucket) {
        println!("putting {:#?}", self);
        let buf = &bincode::serialize(&self).unwrap();
        object_access::put_object(bucket, &Self::key(self.guid), buf).await;
    }
}

impl UberblockPhys {
    fn key(guid: PoolGUID, txg: TXG) -> String {
        format!("zfs/{}/txg/{}", guid, txg)
    }

    pub fn get_zfs_uberblock(&self) -> &Vec<u8> {
        &self.zfs_uberblock
    }

    async fn get(bucket: &Bucket, guid: PoolGUID, txg: TXG) -> Self {
        let buf = object_access::get_object(bucket, &Self::key(guid, txg)).await;
        let this: Self = bincode::deserialize(&buf).unwrap();
        println!("got {:#?}", this);
        assert_eq!(this.guid, guid);
        assert_eq!(this.txg, txg);
        this
    }

    async fn put(&self, bucket: &Bucket) {
        println!("putting {:#?}", self);
        let buf = &bincode::serialize(&self).unwrap();
        object_access::put_object(bucket, &Self::key(self.guid, self.txg), buf).await;
    }
}

impl DataObjectPhys {
    fn key(guid: PoolGUID, obj: ObjectID) -> String {
        format!("zfs/{}/data/{}", guid, obj)
    }

    async fn get(bucket: &Bucket, guid: PoolGUID, obj: ObjectID) -> Self {
        let buf = object_access::get_object(bucket, &Self::key(guid, obj)).await;
        let begin = Instant::now();
        let this: Self = bincode::deserialize(&buf).unwrap();
        assert_eq!(this.guid, guid);
        assert_eq!(this.object, obj);
        println!(
            "{:?}: deserialized {} blocks from {} bytes in {}ms",
            obj,
            this.blocks.len(),
            buf.len(),
            begin.elapsed().as_millis()
        );
        this
    }

    async fn put(&self, bucket: &Bucket) {
        let begin = Instant::now();
        let contents = bincode::serialize(&self).unwrap();
        println!(
            "{:?}: serialized {} blocks in {} bytes in {}ms",
            self.object,
            self.blocks.len(),
            contents.len(),
            begin.elapsed().as_millis()
        );
        object_access::put_object(bucket, &Self::key(self.guid, self.object), &contents).await;
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
    // The syncing_state mutex is either owned by the syncing task (spawned by
    // end_txg_cb()) or by the owner of the containing Pool. When acquired by
    // the containing pool, end_txg_cb() must not be running. In other words,
    // the pool's logical contents can not be mutated (by writing/freeing a
    // block) while end_txg_cb() is running. Given this access pattern, there is
    // never any contention on the mutex and therefore we can always use
    // try_lock() then return an error to the caller if the lock can't be
    // acquired, which only happens due to incorrect usage as mentioned above.
    // In other words, the Mutex is only used to pass ownership of the syncing
    // state between the one "open context" thread and the one "syncing context"
    // thread.
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

    pending_object: Option<PendingObject>,
    pending_object_min_block: BlockID,
    pending_object_max_block: Option<BlockID>,
    pending_flushes: Vec<JoinHandle<()>>,
    pub last_txg: TXG,
    pub syncing_txg: Option<TXG>,
    stats: PoolStatsPhys,
    reclaim_cb: Option<oneshot::Receiver<SyncTask>>,
    // Protects objects that are being overwritten for sync-to-convergence
    rewriting_objects: HashMap<ObjectID, Arc<tokio::sync::Mutex<()>>>,
}

type SyncTask =
    Box<dyn FnOnce(&mut PoolSyncingState) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> + Send>;

#[derive(Debug)]
struct PendingObject {
    done: Arc<Semaphore>,
    phys: DataObjectPhys,
}

/*
 * Note: this struct is passed to the OBL code.  It needs to be a separate struct from Pool,
 * because it can't refer back to the OBL itself, which would create a circular reference.
 */
#[derive(Debug, Clone)]
pub struct PoolSharedState {
    pub bucket: Bucket,
    pub guid: PoolGUID,
    pub name: String,
}

impl PoolSyncingState {
    fn log_free(&mut self, ent: PendingFreesLogEntry) {
        let txg = self.syncing_txg.unwrap();
        assert!(ent.block < self.pending_object_min_block);
        self.pending_frees_log.append(txg, ent);
        self.stats.pending_frees_count += 1;
        self.stats.pending_frees_bytes += ent.size as u64;
    }
}

impl Pool {
    pub async fn create(bucket: &Bucket, name: &str, guid: PoolGUID) {
        let phys = PoolPhys {
            guid,
            name: name.to_string(),
            last_txg: TXG(0),
        };
        // XXX make sure it doesn't already exist
        phys.put(bucket).await;
    }

    async fn open_from_txg(
        bucket: &Bucket,
        pool_phys: &PoolPhys,
        txg: TXG,
    ) -> (Pool, Option<UberblockPhys>, BlockID) {
        let phys = UberblockPhys::get(bucket, pool_phys.guid, txg).await;

        let readonly_state = Arc::new(PoolSharedState {
            bucket: bucket.clone(),
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
                    pending_object: None,
                    pending_object_min_block: phys.highest_block.next(),
                    pending_object_max_block: None,
                    pending_flushes: Vec::new(),
                    stats: phys.stats,
                    reclaim_cb: None,
                    rewriting_objects: HashMap::new(),
                }),
                block_to_obj: std::sync::RwLock::new(ObjectBlockMap::new()),
            }),
        };

        let mut syncing_state = pool.state.syncing_state.lock().await;

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
        println!(
            "loaded mapping with {} allocs and {} frees in {}ms",
            num_alloc_entries,
            num_free_entries,
            begin.elapsed().as_millis()
        );

        objects_rwlock.read().unwrap().verify();

        assert_eq!(
            objects_rwlock.read().unwrap().len() as u64,
            syncing_state.stats.objects_count
        );

        // load free map just to verify
        let begin = Instant::now();
        let mut frees: HashSet<BlockID> = HashSet::new();
        syncing_state
            .pending_frees_log
            .iterate()
            .for_each(|ent| {
                let inserted = frees.insert(ent.block);
                if !inserted {
                    println!("duplicate free entry {:?}", ent.block);
                }
                assert!(inserted);
                future::ready(())
            })
            .await;
        println!(
            "loaded {} freed blocks in {}ms",
            frees.len(),
            begin.elapsed().as_millis()
        );
        //println!("{:#?}", frees);
        let next_block = Self::next_block_locked(&syncing_state);
        drop(syncing_state);

        //println!("opened {:#?}", pool);

        (pool, Some(phys), next_block)
    }

    pub async fn open(bucket: &Bucket, guid: PoolGUID) -> (Pool, Option<UberblockPhys>, BlockID) {
        let phys = PoolPhys::get(bucket, guid).await;
        if phys.last_txg.0 == 0 {
            let readonly_state = Arc::new(PoolSharedState {
                bucket: bucket.clone(),
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
                        pending_object: None,
                        pending_object_min_block: BlockID(0),
                        pending_object_max_block: None,
                        pending_flushes: Vec::new(),
                        stats: PoolStatsPhys::default(),
                        reclaim_cb: None,
                        rewriting_objects: HashMap::new(),
                    }),
                    block_to_obj: std::sync::RwLock::new(ObjectBlockMap::new()),
                }),
            };
            let syncing_state = pool.state.syncing_state.try_lock().unwrap();
            let next_block = Self::next_block_locked(&syncing_state);
            drop(syncing_state);
            (pool, None, next_block)
        } else {
            Pool::open_from_txg(bucket, &phys, phys.last_txg).await
        }
    }

    pub fn begin_txg(&mut self, txg: TXG) {
        // the syncing_state is only held from the thread that owns the Pool
        // (i.e. this thread) and from end_txg(). It's not allowed to call this
        // function while in the middle of an end_txg(), so the lock must not be
        // held. XXX change this to return an error to the client
        let syncing_state = &mut self.state.syncing_state.try_lock().unwrap();

        assert!(syncing_state.syncing_txg.is_none());
        assert!(txg.0 > syncing_state.last_txg.0);
        syncing_state.syncing_txg = Some(txg);
        let last_obj = self.state.block_to_obj.read().unwrap().last_obj();

        syncing_state.pending_object = Some(PendingObject {
            done: Arc::new(Semaphore::new(0)),
            phys: DataObjectPhys {
                guid: self.state.readonly_state.guid,
                object: last_obj.next(),
                txg,
                blocks_size: 0,
                blocks: HashMap::new(),
            },
        });
    }

    pub fn end_txg_cb<F>(&mut self, uberblock: Vec<u8>, cb: F)
    where
        F: Future + Send + 'static,
    {
        self.initiate_flush_object_impl(false);

        try_reclaim_frees(self.state.clone());

        let state = self.state.clone();
        tokio::spawn(async move {
            let mut syncing_state = state.syncing_state.try_lock().unwrap();
            let txg = syncing_state.syncing_txg.unwrap();
            Self::wait_for_pending_flushes(&mut syncing_state).await;
            syncing_state.rewriting_objects.clear();

            if let Some(rt) = syncing_state.reclaim_cb.as_mut() {
                if let Ok(cb) = rt.try_recv() {
                    cb(&mut syncing_state).await;
                }
            }

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
                highest_block: BlockID(syncing_state.pending_object_min_block.0 - 1),
                zfs_uberblock: uberblock,
                stats: syncing_state.stats.clone(),
            };
            u.put(&state.readonly_state.bucket).await;

            // write super
            let s = PoolPhys {
                guid: state.readonly_state.guid,
                name: state.readonly_state.name.clone(),
                last_txg: txg,
            };
            s.put(&state.readonly_state.bucket).await;

            // update txg
            syncing_state.last_txg = txg;
            syncing_state.syncing_txg = None;
            assert!(syncing_state.pending_object.is_none());

            // We need to drop the mutex before sending response (in callback).
            // Otherwise we could send the response, and get another request
            // which needs the mutex before we drop it. Since we are using
            // try_enter().unwrap(), that would panic if we are still holding
            // the mutex.
            drop(syncing_state);
            cb.await;
        });
    }

    async fn wait_for_pending_flushes(syncing_state: &mut PoolSyncingState) {
        // these should be equivalent
        //let wait_for = self.pending_flushes.split_off(0);
        let wait_for = mem::take(&mut syncing_state.pending_flushes);
        let join_result = join_all(wait_for).await;
        for r in join_result {
            r.unwrap();
        }
    }

    /*
    pub async fn flush_writes(&mut self) {
        self.initiate_flush_object();
        Self::wait_for_pending_flushes(&mut self.syncing_state.try_lock().unwrap());
    }

    pub async fn flush_up_to(&mut self, block: BlockID) {}
    */

    pub fn initiate_flush_object(&mut self) {
        self.initiate_flush_object_impl(true);
    }

    // completes when we've initiated the PUT to the object store.
    // callers should wait on the semaphore to ensure it's completed
    fn initiate_flush_object_impl(&mut self, new_pending: bool) {
        // the syncing_state is only held from the thread that owns the Pool
        // (i.e. this thread) and from end_txg(). It's not allowed to call this
        // function while in the middle of an end_txg(), so the lock must not be
        // held. XXX change this to return an error to the client
        let syncing_state = &mut self.state.syncing_state.try_lock().unwrap();

        // XXX because called when server times out waiting for request
        if syncing_state.syncing_txg.is_none() {
            return;
        }

        let txg = syncing_state.syncing_txg.unwrap();

        if syncing_state
            .pending_object
            .as_ref()
            .unwrap()
            .phys
            .blocks
            .is_empty()
        {
            if !new_pending {
                syncing_state.pending_object = None;
            }
            return;
        }
        let min_block = syncing_state.pending_object_min_block;
        let max_block = syncing_state.pending_object_max_block.unwrap();
        {
            let pending_object = syncing_state.pending_object.as_mut().unwrap();

            // verify BlockID's are in expected range
            for b in pending_object.phys.blocks.keys() {
                assert!(*b >= min_block);
                assert!(*b <= max_block);
            }
            assert_eq!(pending_object.phys.guid, self.state.readonly_state.guid);
            assert_eq!(pending_object.phys.txg, txg);
        }

        let obj = syncing_state.pending_object.as_mut().unwrap().phys.object;
        let po = match new_pending {
            true => syncing_state.pending_object.replace(PendingObject {
                done: Arc::new(Semaphore::new(0)),
                phys: DataObjectPhys {
                    guid: self.state.readonly_state.guid,
                    object: obj.next(),
                    txg,
                    blocks_size: 0,
                    blocks: HashMap::new(),
                },
            }),
            false => syncing_state.pending_object.take(),
        }
        .unwrap();
        assert_eq!(obj, po.phys.object);
        assert_eq!(
            obj,
            self.state.block_to_obj.read().unwrap().last_obj().next()
        );
        // reset pending_object for next use
        syncing_state.pending_object_min_block = max_block.next();
        syncing_state.pending_object_max_block = None;

        // increment stats
        let num_blocks = po.phys.blocks.len() as u32;
        let num_bytes = po.phys.blocks_size;
        // XXX consider using debug_assert_eq
        assert_eq!(
            num_bytes as usize,
            po.phys.blocks.values().map(|x| x.len()).sum::<usize>()
        );
        syncing_state.stats.objects_count += 1;
        syncing_state.stats.blocks_bytes += num_bytes as u64;
        syncing_state.stats.blocks_count += num_blocks as u64;

        // add to in-memory block->object map
        self.state
            .block_to_obj
            .write()
            .unwrap()
            .insert(obj, min_block);

        // log to on-disk block->object map
        syncing_state.storage_object_log.append(
            txg,
            StorageObjectLogEntry::Alloc {
                first_possible_block: min_block,
                obj,
            },
        );

        // log to on-disk size
        syncing_state.object_size_log.append(
            txg,
            ObjectSizeLogEntry::Exists {
                obj,
                num_blocks,
                num_bytes,
            },
        );

        println!(
            "{:?}: writing {:?}: blocks={} bytes={} min={:?}",
            txg, obj, num_blocks, num_bytes, min_block
        );

        // write to object store
        let readonly_state = self.state.readonly_state.clone();
        syncing_state.pending_flushes.push(tokio::spawn(async move {
            po.phys.put(&readonly_state.bucket).await;
            po.done.close();
        }));
    }

    fn next_block_locked(syncing_state: &PoolSyncingState) -> BlockID {
        match syncing_state.pending_object_max_block {
            Some(max) => max.next(),
            None => syncing_state.pending_object_min_block,
        }
    }

    // XXX change to return a Future rather than Arc<Sem>?
    fn do_write_impl(&mut self, id: BlockID, data: Vec<u8>) -> (Arc<Semaphore>, bool) {
        // the syncing_state is only held from the thread that owns the Pool
        // (i.e. this thread) and from end_txg(). It's not allowed to call
        // this function while in the middle of an end_txg(), so the lock
        // must not be held. XXX change this to return an error to the
        // client
        let mut syncing_state = self.state.syncing_state.try_lock().unwrap();
        assert!(syncing_state.syncing_txg.is_some());
        //assert!(id >= Self::next_block_locked(&syncing_state));
        if id < Self::next_block_locked(&syncing_state) {
            // XXX the design is for this to not happen. Writes must be received
            // in blockID-order. However, for now we allow overwrites during
            // sync to convergence via this slow path.
            let obj = self.state.block_to_obj.read().unwrap().block_to_obj(id);
            let shared_state = self.state.readonly_state.clone();
            let txg = syncing_state.syncing_txg.unwrap();
            let sem = Arc::new(Semaphore::new(0));
            let sem2 = sem.clone();

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
                println!("rewriting {:?} to overwrite {:?}", obj, id);
                let mut obj_phys =
                    DataObjectPhys::get(&shared_state.bucket, shared_state.guid, obj).await;
                // must have been written this txg
                assert_eq!(obj_phys.txg, txg);
                let removed = obj_phys.blocks.remove(&id);
                // this blockID must have been written
                assert!(removed.is_some());

                // Size must not change.  This way we don't have to change the
                // accounting, which would require writing a new entry to the
                // ObjectSizeLog, which is not allowed in this (async) context.
                // XXX this may be problematic if we switch to ashift=0
                assert_eq!(removed.unwrap().len(), data.len());

                obj_phys.blocks.insert(id, data);
                obj_phys.put(&shared_state.bucket).await;
                sem2.close();
            });
            return (sem, false);
        }

        assert_eq!(
            syncing_state.pending_object_max_block.is_none(),
            syncing_state
                .pending_object
                .as_ref()
                .unwrap()
                .phys
                .blocks
                .is_empty()
        );
        syncing_state.pending_object_max_block = Some(id);
        let pending_object = syncing_state.pending_object.as_mut().unwrap();
        pending_object.phys.blocks_size += data.len() as u32;
        pending_object.phys.blocks.insert(id, data);
        let sem = pending_object.done.clone();
        let do_flush = pending_object.phys.blocks.len() >= MAX_BLOCKS_PER_OBJECT;
        (sem, do_flush)
    }

    pub fn write_block_cb<F>(&mut self, id: BlockID, data: Vec<u8>, cb: F)
    where
        F: Future + Send + 'static,
    {
        // since initiate_flush_object() gets the syncing_state mutex, we need
        // to drop the mutex before calling it
        let (sem, do_flush) = self.do_write_impl(id, data);

        if do_flush {
            self.initiate_flush_object();
        }

        tokio::spawn(async move {
            let res = sem.acquire().await;
            assert!(res.is_err());
            cb.await;
        });
    }

    pub fn read_block_cb<F>(&self, id: BlockID, cb: impl FnOnce(Vec<u8>) -> F + Send + 'static)
    where
        F: Future + Send + 'static,
    {
        let obj = self.state.block_to_obj.read().unwrap().block_to_obj(id);
        let readonly_state = self.state.readonly_state.clone();
        //let state = self.state.clone();

        tokio::spawn(async move {
            println!("reading {:?} for {:?}", obj, id);
            let block = DataObjectPhys::get(&readonly_state.bucket, readonly_state.guid, obj).await;
            // XXX consider using debug_assert_eq
            assert_eq!(
                block.blocks_size as usize,
                block.blocks.values().map(|x| x.len()).sum::<usize>()
            );
            if block.blocks.get(&id).is_none() {
                //println!("{:#?}", self.objects);
                println!("{:#?}", block);
            }
            cb(block.blocks.get(&id).unwrap().to_owned()).await;
        });
    }

    pub fn free_block(&mut self, block: BlockID, size: u32) {
        // the syncing_state is only held from the thread that owns the Pool
        // (i.e. this thread) and from end_txg(). It's not allowed to call this
        // function while in the middle of an end_txg(), so the lock must not be
        // held. XXX change this to return an error to the client
        let mut syncing_state = self.state.syncing_state.try_lock().unwrap();
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
    for obj in deleted_objects {
        syncing_state
            .storage_object_log
            .append(txg, StorageObjectLogEntry::Free { obj });
        state.block_to_obj.write().unwrap().remove(obj);
        syncing_state
            .object_size_log
            .append(txg, ObjectSizeLogEntry::Freed { obj });
        syncing_state.stats.objects_count -= 1;
    }
}

async fn build_new_frees(
    syncing_state: &mut PoolSyncingState,
    remaining_frees: Vec<PendingFreesLogEntry>,
    num_chunks: u64,
) {
    let txg = syncing_state.syncing_txg.unwrap();
    let begin = Instant::now();

    // We need to call .iterate_after() before .clear(), otherwise we'd be
    // iterating the new, empty generation.
    let (stream, _) = syncing_state.pending_frees_log.iterate_after(num_chunks);
    syncing_state.pending_frees_log.clear(txg).await;

    syncing_state.stats.pending_frees_count = 0;
    syncing_state.stats.pending_frees_bytes = 0;
    for ent in remaining_frees {
        syncing_state.log_free(ent);
    }
    stream
        .for_each(|ent| {
            syncing_state.log_free(ent);
            future::ready(())
        })
        .await;
    println!(
        "transferred {} freed blocks in {}ms",
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
                    object_sizes.remove(&obj);
                }
            }
            future::ready(())
        })
        .await;
    println!(
        "loaded sizes for {} objects in {}ms",
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
    println!(
        "loaded {} freed blocks in {}ms",
        count,
        begin.elapsed().as_millis()
    );
    frees_per_obj
}

async fn reclaim_frees_object(
    shared_state: Arc<PoolSharedState>,
    objs: Vec<(ObjectID, Vec<PendingFreesLogEntry>)>,
) -> (ObjectID, u32) {
    let first_obj = objs[0].0;
    println!(
        "rewriting {} objects starting with {:?} to free {} blocks",
        objs.len(),
        first_obj,
        objs.iter().map(|x| x.1.len()).sum::<usize>()
    );

    let stream = FuturesUnordered::new();
    let mut to_delete = Vec::new();
    let mut first = true;
    for (obj, frees) in objs {
        if !first {
            to_delete.push(obj);
        }
        first = false;

        let my_shared_state = shared_state.clone();
        stream.push(async move {
            let mut obj_phys =
                DataObjectPhys::get(&my_shared_state.bucket, my_shared_state.guid, obj).await;
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
        });
    }
    let new_obj = stream
        .reduce(|mut a, mut b| async move {
            assert_eq!(a.guid, b.guid);
            a.object = min(a.object, b.object);
            a.txg = min(a.txg, b.txg); // XXX maybe should track min & max txg?
            println!(
                "moving {} blocks from {:?} to {:?}",
                b.blocks.len(),
                b.object,
                a.object
            );
            let mut already_moved = 0;
            for (k, v) in b.blocks.drain() {
                let k2 = k.clone();
                let len = v.len() as u32;
                match a.blocks.insert(k, v) {
                    Some(old_vec) => {
                        // May have already been transferred in a previous job
                        // during which we crashed before updating the metadata.
                        assert_eq!(&old_vec, a.blocks.get(&k2).unwrap());
                        already_moved += 1;
                    }
                    None => {
                        a.blocks_size += len;
                    }
                }
            }
            if already_moved > 0 {
                println!(
                    "while moving blocks from {:?} to {:?} found {} blocks already moved",
                    b.object, a.object, already_moved
                );
            }
            a
        })
        .await
        .unwrap();

    assert_eq!(new_obj.object, first_obj);
    new_obj.put(&shared_state.bucket).await;

    (new_obj.object, new_obj.blocks_size)
}

fn try_reclaim_frees(state: Arc<PoolState>) {
    let mut syncing_state = state.syncing_state.try_lock().unwrap();
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
    println!(
        "starting to reclaim space (process pending frees) pending_frees_count={} blocks_count={}",
        syncing_state.stats.pending_frees_count, syncing_state.stats.blocks_count
    );

    let (pending_frees_log_stream, num_chunks) = syncing_state.pending_frees_log.iterate_after(0);

    let object_size_log_stream = syncing_state.object_size_log.iterate();

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
        for (_, obj) in objs_by_frees.iter() {
            if !frees_per_obj.contains_key(obj) {
                // this object is being removed by a multi-object consolidation
                continue;
            }
            // XXX limit amount of outstanding get/put requests?
            let mut objs_to_consolidate: Vec<(ObjectID, Vec<PendingFreesLogEntry>)> = Vec::new();
            let mut new_size: u32 = 0;
            for (later_obj, later_size) in object_sizes.range((Included(obj), Unbounded)) {
                // If we run into an object that we're already writing, we
                // can't consolidate with it.
                if writing.contains(later_obj) {
                    break;
                }
                let later_bytes_freed: u32 = frees_per_obj
                    .get(later_obj)
                    .unwrap_or(&Vec::new())
                    .iter()
                    .map(|e| e.size)
                    .sum();
                let later_new_size = later_size - later_bytes_freed;

                if new_size > 0 && new_size + later_new_size > MAX_BYTES_PER_OBJECT {
                    break;
                }
                new_size += later_new_size;
                let frees = frees_per_obj.remove(later_obj).unwrap_or_default();
                freed_blocks_count += frees.len() as u64;
                freed_blocks_bytes += later_bytes_freed as u64;
                objs_to_consolidate.push((*later_obj, frees));
            }
            // XXX look for earlier objects too?

            // Must include at least the target object
            assert_eq!(objs_to_consolidate[0].0, *obj);

            writing.insert(*obj);

            // all but the first object need to be deleted by syncing context
            for (obj, _) in objs_to_consolidate.iter().skip(1) {
                //complete.rewritten_object_sizes.push((*obj, 0));
                deleted_objects.push(*obj);
            }
            // Note: we could calculate the new object's size here as well,
            // but that would be based on the object_sizes map/log, which
            // may have inaccuracies if we crashed during reclaim.  Instead
            // we calculate the size based on the object contents, and
            // return it from the spawned task.

            // XXX would be nice to know if we are freeing the entire object
            // in which case we wouldn't need to read it.  Would have to
            // keep a count of blocks per object in RAM?
            join_handles.push(tokio::spawn(reclaim_frees_object(
                shared_state.clone(),
                objs_to_consolidate,
            )));
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

        println!(
            "rewrote {} objects in {}ms",
            num_handles,
            begin.elapsed().as_millis(),
        );

        let r = s.send(Box::new(move |syncing_state| {
            Box::pin(async move {
                println!("reclaim complete; transferring tail of frees to new generation");

                syncing_state.stats.blocks_count -= freed_blocks_count;
                syncing_state.stats.blocks_bytes -= freed_blocks_bytes;

                build_new_frees(syncing_state, remaining_frees, num_chunks).await;
                log_deleted_objects(state, syncing_state, deleted_objects);
                log_new_sizes(syncing_state, rewritten_object_sizes);

                syncing_state.reclaim_cb = None;
            })
        }));
        assert!(r.is_ok()); // can not use .unwrap() because the type is not Debug
    });
}
