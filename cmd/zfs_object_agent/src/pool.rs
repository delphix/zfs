use crate::object_access;
use crate::object_based_log::*;
use core::future::Future;
use futures::future;
use futures::future::*;
use futures::stream::*;
use futures::task;
use s3::bucket::Bucket;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fmt;
use std::mem;
use std::ops::Bound::*;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;
use std::time::{Instant, SystemTime};
use tokio::sync::*;
use tokio::task::JoinHandle;

/*
 * Things that are stored on disk.
 */
pub trait OnDisk: Serialize + DeserializeOwned {}

#[derive(Serialize, Deserialize, Debug, Copy, Clone, PartialEq)]
pub struct TXG(pub u64);
impl OnDisk for TXG {}
impl fmt::Display for TXG {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Serialize, Deserialize, Debug, Copy, Clone, PartialEq, Eq, Ord, PartialOrd, Hash)]
pub struct PoolGUID(pub u64);
impl OnDisk for PoolGUID {}
impl fmt::Display for PoolGUID {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Serialize, Deserialize, Debug, Copy, Clone, PartialEq, Eq, Ord, PartialOrd)]
pub struct ObjectID(pub u64);
impl OnDisk for ObjectID {}
impl fmt::Display for ObjectID {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Serialize, Deserialize, Debug, Copy, Clone, PartialEq, Eq, Ord, PartialOrd, Hash)]
pub struct BlockID(pub u64);
impl OnDisk for BlockID {}
impl fmt::Display for BlockID {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Serialize, Deserialize, Debug)]
struct PoolPhys {
    guid: PoolGUID, // redundant with key, for verification
    name: String,
    last_txg: TXG,
}
impl OnDisk for PoolPhys {}

#[derive(Serialize, Deserialize, Debug)]
struct UberblockPhys {
    guid: PoolGUID,   // redundant with key, for verification
    txg: TXG,         // redundant with key, for verification
    date: SystemTime, // for debugging
    storage_object_log: ObjectBasedLogPhys,
    pending_frees_log: ObjectBasedLogPhys,
    highest_block: BlockID, // highest blockID in use
    stats: PoolStatsPhys,
    zfs_uberblock: Vec<u8>,
}
impl OnDisk for UberblockPhys {}

#[derive(Serialize, Deserialize, Debug, Default, Clone)]
struct PoolStatsPhys {
    blocks_count: u64,
    blocks_bytes: u64,
    pending_frees_count: u64,
    pending_frees_bytes: u64,
    objects_count: u64, // XXX shouldn't really be needed since we always have the storage_object_log loaded into the `objects` field
    objects_bytes: u64,
}
impl OnDisk for PoolStatsPhys {}

#[derive(Serialize, Deserialize, Debug)]
struct DataObjectPhys {
    guid: PoolGUID,   // redundant with key, for verification
    object: ObjectID, // redundant with key, for verification
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
        first_possible_block: BlockID,
    },
}
impl OnDisk for StorageObjectLogEntry {}
impl ObjectBasedLogEntry for StorageObjectLogEntry {}

#[derive(Debug, Serialize, Deserialize, Copy, Clone)]
struct PendingFreesLogEntry {
    block: BlockID,
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

#[derive(Debug)]
pub struct Pool {
    pub state: Arc<PoolState>,
}

#[derive(Debug)]
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

    objects: std::sync::RwLock<BTreeMap<BlockID, ObjectID>>,
    pub readonly_state: Arc<PoolSharedState>,
}

/// state that's modified while syncing a txg
#[derive(Debug)]
struct PoolSyncingState {
    storage_object_log: ObjectBasedLog<StorageObjectLogEntry>,
    pending_frees_log: ObjectBasedLog<PendingFreesLogEntry>,
    pending_object: Option<PendingObject>, // XXX maybe this and syncing_txg should be under the same Option
    pending_object_min_block: BlockID,
    pending_object_max_block: Option<BlockID>,
    pending_flushes: Vec<JoinHandle<()>>,
    pub last_txg: TXG,
    pub syncing_txg: Option<TXG>,
    stats: PoolStatsPhys,
    reclaim_task: Option<JoinHandle<ReclaimFreesComplete>>,
}

#[derive(Debug)]
struct PendingObject {
    done: Arc<Semaphore>,
    phys: DataObjectPhys,
}

//pub type RequestID = u64;

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

#[derive(Debug)]
struct ReclaimFreesComplete {
    freed_blocks_count: u64,
    freed_blocks_bytes: u64,
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
    ) -> (Pool, TXG, BlockID) {
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
                    pending_frees_log: ObjectBasedLog::open_by_phys(
                        readonly_state.clone(),
                        &format!("zfs/{}/PendingFreesLog", pool_phys.guid),
                        &phys.pending_frees_log,
                    ),
                    pending_object: None,
                    pending_object_min_block: BlockID(phys.highest_block.0 + 1),
                    pending_object_max_block: None,
                    pending_flushes: Vec::new(),
                    stats: phys.stats,
                    reclaim_task: None,
                }),
                objects: std::sync::RwLock::new(BTreeMap::new()),
            }),
        };

        let mut syncing_state = pool.state.syncing_state.lock().await;

        syncing_state.storage_object_log.recover().await;
        syncing_state.pending_frees_log.recover().await;

        // load block -> object mapping
        let begin = Instant::now();
        let objects_rwlock = &pool.state.objects;
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
                        objects.insert(first_possible_block, obj);
                    }
                    StorageObjectLogEntry::Free {
                        obj,
                        first_possible_block,
                    } => {
                        let (_, removed_obj) = objects.remove_entry(&first_possible_block).unwrap();
                        assert_eq!(removed_obj, obj);
                    }
                }

                future::ready(())
            })
            .await;
        println!(
            "loaded mapping for {} objects in {}ms",
            objects_rwlock.write().unwrap().len(),
            begin.elapsed().as_millis()
        );

        // verify that object ID's are increasing as block ID's are increasing
        let mut max_obj = ObjectID(0);
        for v in objects_rwlock.write().unwrap().values() {
            assert!(*v > max_obj);
            max_obj = *v;
        }

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
        let last_txg = syncing_state.last_txg;
        let next_block = Self::next_block_locked(&syncing_state);
        drop(syncing_state);

        println!("opened {:#?}", pool);

        (pool, last_txg, next_block)
    }

    pub async fn open(bucket: &Bucket, guid: PoolGUID) -> (Pool, TXG, BlockID) {
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
                        pending_frees_log: ObjectBasedLog::create(
                            readonly_state.clone(),
                            &format!("zfs/{}/PendingFreesLog", guid),
                        ),
                        pending_object: None,
                        pending_object_min_block: BlockID(0),
                        pending_object_max_block: None,
                        pending_flushes: Vec::new(),
                        stats: PoolStatsPhys::default(),
                        reclaim_task: None,
                    }),
                    objects: std::sync::RwLock::new(BTreeMap::new()),
                }),
            };
            let syncing_state = pool.state.syncing_state.try_lock().unwrap();
            let last_txg = syncing_state.last_txg;
            let next_block = Self::next_block_locked(&syncing_state);
            drop(syncing_state);
            (pool, last_txg, next_block)
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
        let last_obj = *self
            .state
            .objects
            .read()
            .unwrap()
            .values()
            .next_back()
            .unwrap_or(&ObjectID(0));

        syncing_state.pending_object = Some(PendingObject {
            done: Arc::new(Semaphore::new(0)),
            phys: DataObjectPhys {
                guid: self.state.readonly_state.guid,
                object: ObjectID(last_obj.0 + 1),
                txg,
                blocks: HashMap::new(),
            },
        });
    }

    fn reclaim_frees_object(
        shared_state: Arc<PoolSharedState>,
        obj: ObjectID,
        blocks: Vec<BlockID>,
    ) -> JoinHandle<u64> {
        println!("rewriting {:?} to free {} blocks", obj, blocks.len());
        tokio::spawn(async move {
            let mut bytes_freed = 0u64;
            let mut obj_phys =
                DataObjectPhys::get(&shared_state.bucket, shared_state.guid, obj).await;
            for b in blocks {
                let removed = obj_phys.blocks.remove(&b);
                // XXX if we crashed in the middle of this operation last time,
                // the block may already have been removed (and the object
                // rewritten). In this case we should ignore the fact that it
                // isn't present, and count this block as removed for stats
                // purposes, since the stats were not yet updated when we
                // crashed. However, unclear how we would know how many bytes
                // were freed.
                assert!(removed.is_some());
                bytes_freed += removed.unwrap().len() as u64;
            }
            obj_phys.put(&shared_state.bucket).await;
            bytes_freed
        })
    }

    fn try_reclaim_frees_async(&mut self) {
        let mut syncing_state = self.state.syncing_state.try_lock().unwrap();

        if syncing_state.reclaim_task.is_some() {
            return;
        }

        // XXX change this to be based on bytes, once those stats are working?
        // XXX make this tunable?
        if syncing_state.stats.pending_frees_count < syncing_state.stats.blocks_count / 100
            && syncing_state.stats.pending_frees_count < 10_000
        {
            return;
        }

        let state = self.state.clone();

        // XXX to save RAM, change to a Vec that we sort in-place
        let stream = syncing_state.pending_frees_log.iterate();

        syncing_state.reclaim_task = Some(tokio::spawn(async move {
            let shared_state = &state.readonly_state;
            let mut frees: BTreeSet<BlockID> = BTreeSet::new();

            // load pending frees
            let begin = Instant::now();
            stream
                .for_each(|ent| {
                    let inserted = frees.insert(ent.block);
                    if !inserted {
                        panic!("duplicate free entry {:?}", ent.block);
                    }
                    future::ready(())
                })
                .await;
            println!(
                "loaded {} freed blocks in {}ms",
                frees.len(),
                begin.elapsed().as_millis()
            );

            // rewrite objects, omitting freed blocks
            // XXX want to consolidate adjacent small objects
            let mut complete = ReclaimFreesComplete {
                freed_blocks_count: 0,
                freed_blocks_bytes: 0,
            };
            let mut current_obj_state: Option<(ObjectID, Vec<BlockID>)> = None;
            let mut join_handles = Vec::new();

            let begin = Instant::now();
            for free_block in frees {
                let obj = Self::block_to_object(&state.objects.read().unwrap(), free_block);

                if current_obj_state.is_some() && current_obj_state.as_ref().unwrap().0 != obj {
                    let (myobj, myblocks) = current_obj_state.take().unwrap();
                    join_handles.push(Self::reclaim_frees_object(
                        shared_state.clone(),
                        myobj,
                        myblocks,
                    ));
                }
                if current_obj_state.is_none() {
                    current_obj_state = Some((obj, Vec::new()))
                }

                let (myobj, myblocks) = current_obj_state.as_mut().unwrap();
                assert_eq!(*myobj, obj);
                myblocks.push(free_block);
                complete.freed_blocks_count += 1;
            }
            if current_obj_state.is_some() {
                let (myobj, myblocks) = current_obj_state.unwrap();
                join_handles.push(Self::reclaim_frees_object(
                    shared_state.clone(),
                    myobj,
                    myblocks,
                ));
            }

            let num_handles = join_handles.len();
            for jh in join_handles {
                complete.freed_blocks_bytes += jh.await.unwrap();
            }

            println!(
                "rewrote {} objects in {}ms: {:?}",
                num_handles,
                begin.elapsed().as_millis(),
                complete
            );

            complete
        }));
    }

    pub fn end_txg_cb<F>(&mut self, uberblock: Vec<u8>, cb: F)
    where
        F: Future + Send + 'static,
    {
        self.initiate_flush_object();

        self.try_reclaim_frees_async();

        let state = self.state.clone();

        tokio::spawn(async move {
            let mut syncing_state = state.syncing_state.try_lock().unwrap();
            let txg = syncing_state.syncing_txg.unwrap();
            Self::wait_for_pending_flushes(&mut syncing_state).await;
            syncing_state.storage_object_log.flush(txg).await;
            syncing_state.pending_frees_log.flush(txg).await;

            if let Some(reclaim_task) = syncing_state.reclaim_task.as_mut() {
                let waker = task::noop_waker();
                let mut cx = Context::from_waker(&waker);
                match Future::poll(Pin::new(reclaim_task), &mut cx) {
                    Poll::Ready(out) => {
                        let rfc = out.unwrap();
                        syncing_state.stats.blocks_count -= rfc.freed_blocks_count;
                        syncing_state.stats.blocks_bytes -= rfc.freed_blocks_bytes;

                        // XXX other frees that occurred in the meantime need to
                        // be preserved. rfc needs to tell us which frees need
                        // to be transferred from current generation to next.
                        syncing_state.stats.pending_frees_count = 0;
                        syncing_state.stats.pending_frees_bytes = 0;

                        // clear log of frees
                        let txg = syncing_state.syncing_txg.unwrap();
                        syncing_state.pending_frees_log.clear(txg).await;
                    }
                    Poll::Pending => {}
                }
            }

            // write uberblock
            let u = UberblockPhys {
                guid: state.readonly_state.guid,
                txg,
                date: SystemTime::now(),
                storage_object_log: syncing_state.storage_object_log.to_phys(),
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
            syncing_state.pending_object = None;

            // We need to drop the mutex before sending respons (in callback).
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

    // completes when we've initiated the PUT to the object store.
    // callers should wait on the semaphore to ensure it's completed
    pub fn initiate_flush_object(&mut self) {
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

        let po = syncing_state.pending_object.as_mut().unwrap();
        let old_po = mem::replace(
            po,
            PendingObject {
                done: Arc::new(Semaphore::new(0)),
                phys: DataObjectPhys {
                    guid: self.state.readonly_state.guid,
                    object: ObjectID(po.phys.object.0 + 1),
                    txg: txg,
                    blocks: HashMap::new(),
                },
            },
        );
        let last_obj = *self
            .state
            .objects
            .read()
            .unwrap()
            .values()
            .next_back()
            .unwrap_or(&ObjectID(0));
        let obj = ObjectID(last_obj.0 + 1);
        assert_eq!(obj, old_po.phys.object);

        // increment stats
        syncing_state.stats.objects_count += 1;
        // XXX need to encode to get size before spawning??
        //self.stats.objects_bytes += XXX;

        // write to object store
        let readonly_state = self.state.readonly_state.clone();

        syncing_state.pending_flushes.push(tokio::spawn(async move {
            old_po.phys.put(&readonly_state.bucket).await;
            old_po.done.close();
        }));

        // add to in-memory block->object map
        self.state.objects.write().unwrap().insert(min_block, obj);

        // log to on-disk block->object map
        syncing_state.storage_object_log.append(
            txg,
            StorageObjectLogEntry::Alloc {
                first_possible_block: min_block,
                obj: obj,
            },
        );

        // reset pending_object for next use
        syncing_state.pending_object_min_block = BlockID(max_block.0 + 1);
        syncing_state.pending_object_max_block = None;
    }

    fn next_block_locked(syncing_state: &PoolSyncingState) -> BlockID {
        match syncing_state.pending_object_max_block {
            Some(max) => BlockID(max.0 + 1),
            None => syncing_state.pending_object_min_block,
        }
    }

    fn do_write_impl(&mut self, id: BlockID, data: Vec<u8>) -> (Arc<Semaphore>, bool) {
        // the syncing_state is only held from the thread that owns the Pool
        // (i.e. this thread) and from end_txg(). It's not allowed to call
        // this function while in the middle of an end_txg(), so the lock
        // must not be held. XXX change this to return an error to the
        // client
        let mut syncing_state = self.state.syncing_state.try_lock().unwrap();
        assert!(syncing_state.syncing_txg.is_some());
        assert!(id >= Self::next_block_locked(&syncing_state));
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
        syncing_state.stats.blocks_count += 1;
        syncing_state.stats.blocks_bytes += data.len() as u64;
        let pending_object = syncing_state.pending_object.as_mut().unwrap();
        pending_object.phys.blocks.insert(id, data);
        let sem = pending_object.done.clone();
        let do_flush = pending_object.phys.blocks.len() >= 1000;
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

    fn block_to_object(map: &BTreeMap<BlockID, ObjectID>, block: BlockID) -> ObjectID {
        // find entry equal or less than this blockID
        let (_, o) = map.range((Unbounded, Included(block))).next_back().unwrap();
        *o
    }

    pub fn read_block_cb<F>(&self, id: BlockID, cb: impl FnOnce(Vec<u8>) -> F + Send + 'static)
    where
        F: Future + Send + 'static,
    {
        let obj = Self::block_to_object(&self.state.objects.read().unwrap(), id);
        let readonly_state = self.state.readonly_state.clone();

        tokio::spawn(async move {
            println!("reading {:?} for {:?}", obj, id);
            let block = DataObjectPhys::get(&readonly_state.bucket, readonly_state.guid, obj).await;
            // XXX add block to a small cache
            if block.blocks.get(&id).is_none() {
                //println!("{:#?}", self.objects);
                println!("{:#?}", block);
            }
            cb(block.blocks.get(&id).unwrap().to_owned()).await;
        });
    }

    pub fn free_block(&mut self, id: BlockID) {
        // the syncing_state is only held from the thread that owns the Pool
        // (i.e. this thread) and from end_txg(). It's not allowed to call this
        // function while in the middle of an end_txg(), so the lock must not be
        // held. XXX change this to return an error to the client
        let mut syncing_state = self.state.syncing_state.try_lock().unwrap();

        let txg = syncing_state.syncing_txg.unwrap();
        assert!(id < syncing_state.pending_object_min_block);
        syncing_state
            .pending_frees_log
            .append(txg, PendingFreesLogEntry { block: id });
        syncing_state.stats.pending_frees_count += 1;
        // XXX make caller pass in size of block?
        //self.stats.pending_frees_bytes += size;
    }
}
