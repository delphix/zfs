use crate::object_access;
use crate::object_based_log::*;
use futures::future;
use futures::future::*;
use futures::stream::*;
use s3::bucket::Bucket;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt;
use std::mem;
use std::ops::Bound::*;
use std::sync::Arc;
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
    zfs_uberblock: Vec<u8>,
}
impl OnDisk for UberblockPhys {}

#[derive(Serialize, Deserialize, Debug)]
struct DataObjectPhys {
    guid: PoolGUID,   // redundant with key, for verification
    object: ObjectID, // redundant with key, for verification
    txg: TXG,         // for debugging
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

    async fn put(&self, bucket: &Bucket, guid: PoolGUID) {
        println!("putting {:#?}", self);
        assert_eq!(self.guid, guid);
        let buf = &bincode::serialize(&self).unwrap();
        object_access::put_object(bucket, &Self::key(guid), buf).await;
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

    async fn put(&self, bucket: &Bucket, guid: PoolGUID, txg: TXG) {
        println!("putting {:#?}", self);
        assert_eq!(self.guid, guid);
        assert_eq!(self.txg, txg);
        let buf = &bincode::serialize(&self).unwrap();
        object_access::put_object(bucket, &Self::key(guid, txg), buf).await;
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
        println!(
            "{:?}: deserialized {} blocks from {} bytes in {}ms",
            obj,
            this.blocks.len(),
            buf.len(),
            begin.elapsed().as_millis()
        );
        this
    }

    async fn put(&self, bucket: &Bucket, guid: PoolGUID, obj: ObjectID) {
        let begin = Instant::now();
        let contents = bincode::serialize(&self).unwrap();
        println!(
            "{:?}: serialized {} blocks in {} bytes in {}ms",
            obj,
            self.blocks.len(),
            contents.len(),
            begin.elapsed().as_millis()
        );
        object_access::put_object(bucket, &Self::key(guid, obj), &contents).await;
    }
}

/*
 * Main storage pool interface
 */

#[derive(Debug)]
pub struct Pool {
    // XXX use accessor rather than pub?
    pub state: PoolSharedState,
    // XXX use accessor rather than pub?
    syncing_state: Arc<tokio::sync::Mutex<PoolSyncingState>>,
    objects: BTreeMap<BlockID, ObjectID>,
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
    pub last_txg: TXG,
    pub syncing_txg: Option<TXG>,
}

// XXX no longer need explicit lifetime, since bucket is copied
impl Pool {
    pub async fn create(bucket: &Bucket, name: &str, guid: PoolGUID) {
        let phys = PoolPhys {
            guid: guid,
            name: name.to_string(),
            last_txg: TXG(0),
        };
        // XXX make sure it doesn't already exist
        phys.put(bucket, guid).await;
    }

    async fn open_from_txg(bucket: &Bucket, pool_phys: &PoolPhys, txg: TXG) -> Pool {
        let phys = UberblockPhys::get(bucket, pool_phys.guid, txg).await;

        let mut pool = Pool {
            state: PoolSharedState {
                bucket: bucket.clone(),
                guid: pool_phys.guid,
                name: pool_phys.name.clone(),
                last_txg: phys.txg,
                syncing_txg: None,
            },
            syncing_state: Arc::new(tokio::sync::Mutex::new(PoolSyncingState {
                storage_object_log: ObjectBasedLog::open_by_phys(
                    &bucket,
                    &format!("zfs/{}/StorageObjectLog", pool_phys.guid),
                    &phys.storage_object_log,
                ),
                pending_frees_log: ObjectBasedLog::open_by_phys(
                    &bucket,
                    &format!("zfs/{}/PendingFreesLog", pool_phys.guid),
                    &phys.pending_frees_log,
                ),
                pending_object: None,
                pending_object_min_block: BlockID(phys.highest_block.0 + 1),
                pending_object_max_block: None,
                pending_flushes: Vec::new(),
            })),
            objects: BTreeMap::new(),
        };

        let arc = pool.syncing_state.clone();
        let mut syncing_state = arc.lock().await;

        syncing_state.storage_object_log.recover().await;
        syncing_state.pending_frees_log.recover().await;

        // load block -> object mapping
        let begin = Instant::now();
        syncing_state
            .storage_object_log
            .iterate()
            .for_each(|ent| {
                match ent {
                    StorageObjectLogEntry::Alloc {
                        obj,
                        first_possible_block,
                    } => {
                        pool.objects.insert(first_possible_block, obj);
                    }
                    StorageObjectLogEntry::Free {
                        obj,
                        first_possible_block,
                    } => {
                        let (_, removed_obj) =
                            pool.objects.remove_entry(&first_possible_block).unwrap();
                        assert_eq!(removed_obj, obj);
                    }
                }

                future::ready(())
            })
            .await;
        println!(
            "loaded mapping for {} objects in {}ms",
            pool.objects.len(),
            begin.elapsed().as_millis()
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

        println!("opened {:#?}", pool);

        // verify that object ID's are increasing as block ID's are increasing
        let mut max_obj = ObjectID(0);
        for v in pool.objects.values() {
            assert!(*v > max_obj);
            max_obj = *v;
        }

        pool
    }

    pub async fn open(bucket: &Bucket, guid: PoolGUID) -> Pool {
        let phys = PoolPhys::get(bucket, guid).await;
        if phys.last_txg.0 == 0 {
            // XXX maybe last_txg should be Option<TXG>
            Pool {
                state: PoolSharedState {
                    bucket: bucket.clone(),
                    guid,
                    name: phys.name,
                    last_txg: TXG(0),
                    syncing_txg: None,
                },
                syncing_state: Arc::new(tokio::sync::Mutex::new(PoolSyncingState {
                    storage_object_log: ObjectBasedLog::create(
                        bucket,
                        &format!("zfs/{}/StorageObjectLog", guid),
                    ),
                    pending_frees_log: ObjectBasedLog::create(
                        bucket,
                        &format!("zfs/{}/PendingFreesLog", guid),
                    ),
                    pending_object: None,
                    pending_object_min_block: BlockID(1),
                    pending_object_max_block: None,
                    pending_flushes: Vec::new(),
                })),
                objects: BTreeMap::new(),
            }
        } else {
            Pool::open_from_txg(bucket, &phys, phys.last_txg).await
        }
    }

    pub fn begin_txg(&mut self, txg: TXG) {
        assert!(self.state.syncing_txg.is_none());
        assert!(txg.0 > self.state.last_txg.0);
        self.state.syncing_txg = Some(txg);
        let last_obj = self.objects.values().next_back().unwrap_or(&ObjectID(0));

        // the syncing_state is only held from the thread that owns the Pool
        // (i.e. this thread) and from end_txg(). It's not allowed to call this
        // function while in the middle of an end_txg(), so the lock must not be
        // held. XXX change this to return an error to the client
        let mut syncing_state = self.syncing_state.try_lock().unwrap();
        syncing_state.pending_object = Some(PendingObject {
            done: Arc::new(Semaphore::new(0)),
            phys: DataObjectPhys {
                guid: self.state.guid,
                object: ObjectID(last_obj.0 + 1),
                txg,
                blocks: HashMap::new(),
            },
        });
    }

    /*
    pub async fn end_txg(&mut self, uberblock: Vec<u8>) {
        let txg = self.state.syncing_txg.unwrap();
        self.initiate_flush_object();

        self.flush_blocks().await;
        self.storage_object_log.flush(&self.state).await;
        self.pending_frees_log.flush(&self.state).await;

        // write uberblock
        let u = UberblockPhys {
            guid: self.state.guid,
            txg: txg,
            date: SystemTime::now(),
            storage_object_log: self.storage_object_log.to_phys(),
            pending_frees_log: self.pending_frees_log.to_phys(),
            highest_block: BlockID(self.pending_object_min_block.0 - 1),
            zfs_uberblock: uberblock,
        };
        u.put(&self.state.bucket, self.state.guid, txg).await;

        // write super
        let s = PoolPhys {
            guid: self.state.guid,
            name: self.state.name.clone(),
            last_txg: txg,
        };
        s.put(&self.state.bucket, self.state.guid).await;

        // update txg
        self.state.last_txg = txg;
        self.state.syncing_txg = None;
        self.pending_object = None;
    }
    */

    pub fn end_txg_cb<F>(&mut self, uberblock: Vec<u8>, cb: F)
    where
        F: Future + Send + 'static,
    {
        let txg = self.state.syncing_txg.unwrap();
        self.initiate_flush_object();

        let arc = self.syncing_state.clone();
        let guid = self.state.guid;
        let state = self.state.clone();

        // XXX this change makes sense after the spawned task completes
        self.state.last_txg = txg;
        self.state.syncing_txg = None;

        tokio::spawn(async move {
            {
                let mut syncing_state = arc.lock().await;
                Self::wait_for_pending_flushes(&mut syncing_state).await;
                syncing_state.storage_object_log.flush(&state).await;
                syncing_state.pending_frees_log.flush(&state).await;

                // write uberblock
                let u = UberblockPhys {
                    guid: guid,
                    txg: txg,
                    date: SystemTime::now(),
                    storage_object_log: syncing_state.storage_object_log.to_phys(),
                    pending_frees_log: syncing_state.pending_frees_log.to_phys(),
                    highest_block: BlockID(syncing_state.pending_object_min_block.0 - 1),
                    zfs_uberblock: uberblock,
                };
                u.put(&state.bucket, state.guid, txg).await;

                // write super
                let s = PoolPhys {
                    guid: guid,
                    name: state.name.clone(),
                    last_txg: txg,
                };
                s.put(&state.bucket, guid).await;

                // update txg
                syncing_state.pending_object = None;
            }

            cb.await;
        });
    }

    /*
    pub async fn end_txg(&mut self, uberblock: Vec<u8>) {
        let notify = Arc::new(Notify::new());
        let notify2 = notify.clone();
        self.end_txg_cb(uberblock, async move {
            notify.notify_one();
        });
        notify2.notified().await;
    }
    */

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
        assert!(self.state.syncing_txg.is_some());

        // the syncing_state is only held from the thread that owns the Pool
        // (i.e. this thread) and from end_txg(). It's not allowed to call this
        // function while in the middle of an end_txg(), so the lock must not be
        // held. XXX change this to return an error to the client
        let mut syncing_state = self.syncing_state.try_lock().unwrap();

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
        let last_obj = self.objects.values().next_back().unwrap_or(&ObjectID(0));
        let obj = ObjectID(last_obj.0 + 1);
        let min_block = syncing_state.pending_object_min_block;
        let max_block = syncing_state.pending_object_max_block.unwrap();
        {
            let pending_object = syncing_state.pending_object.as_mut().unwrap();

            // verify BlockID's are in expected range
            for b in pending_object.phys.blocks.keys() {
                assert!(*b >= min_block);
                assert!(*b <= max_block);
            }
            assert_eq!(pending_object.phys.guid, self.state.guid);
            assert_eq!(pending_object.phys.txg, self.state.syncing_txg.unwrap());
        }

        let arc: &mut PendingObject = syncing_state.pending_object.as_mut().unwrap();
        let old_po = mem::replace(
            arc,
            PendingObject {
                done: Arc::new(Semaphore::new(0)),
                phys: DataObjectPhys {
                    guid: self.state.guid,
                    object: ObjectID(last_obj.0 + 1),
                    txg: self.state.syncing_txg.unwrap(),
                    blocks: HashMap::new(),
                },
            },
        );

        // write to object store
        let bucket = self.state.bucket.clone();
        let guid = self.state.guid;

        let handle = tokio::spawn(async move {
            old_po.phys.put(&bucket, guid, obj).await;
            old_po.done.close();
        });
        syncing_state.pending_flushes.push(handle);

        // add to in-memory block->object map
        self.objects.insert(min_block, obj);

        // log to on-disk block->object map
        syncing_state.storage_object_log.append(
            &self.state,
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

    pub fn next_block(&self) -> BlockID {
        // the syncing_state is only held from the thread that owns the Pool
        // (i.e. this thread) and from end_txg(). It's not allowed to call
        // this function while in the middle of an end_txg(), so the lock
        // must not be held. XXX change this to return an error to the
        // client
        let syncing_state = self.syncing_state.try_lock().unwrap();
        Self::next_block_locked(&syncing_state)
    }

    fn do_write_impl(&mut self, id: BlockID, data: Vec<u8>) -> (Arc<Semaphore>, bool) {
        let mut syncing_state = self.syncing_state.try_lock().unwrap();
        assert!(self.state.syncing_txg.is_some());
        assert!(syncing_state.pending_object.is_some());
        assert!(id >= Self::next_block_locked(&syncing_state));
        let mut pending_object = syncing_state.pending_object.take().unwrap();
        assert_eq!(
            syncing_state.pending_object_max_block.is_none(),
            pending_object.phys.blocks.is_empty()
        );
        syncing_state.pending_object_max_block = Some(id);
        pending_object.phys.blocks.insert(id, data);
        let sem = pending_object.done.clone();
        let do_flush = pending_object.phys.blocks.len() >= 1000;
        syncing_state.pending_object = Some(pending_object);
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

    /*
    pub async fn read_block(&self, id: BlockID) -> Vec<u8> {
        // find entry equal or less than this blockID
        let (_, obj) = self
            .objects
            .range((Unbounded, Included(id)))
            .next_back()
            .unwrap();

        println!("reading {:?} for {:?}", *obj, id);
        let block = DataObjectPhys::get(&self.state.bucket, self.state.guid, *obj).await;
        // XXX add block to a small cache
        if block.blocks.get(&id).is_none() {
            println!("{:#?}", self.objects);
            println!("{:#?}", block);
        }
        block.blocks.get(&id).unwrap().to_owned()
    }
    */

    pub fn read_block_cb<F>(&self, id: BlockID, cb: impl FnOnce(Vec<u8>) -> F + Send + 'static)
    where
        F: Future + Send + 'static,
    {
        // find entry equal or less than this blockID
        let (_, o) = self
            .objects
            .range((Unbounded, Included(id)))
            .next_back()
            .unwrap();
        let obj = *o;
        let bucket = self.state.bucket.clone();
        let guid = self.state.guid;

        tokio::spawn(async move {
            println!("reading {:?} for {:?}", obj, id);
            let block = DataObjectPhys::get(&bucket, guid, obj).await;
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
        let mut syncing_state = self.syncing_state.try_lock().unwrap();

        assert!(self.state.syncing_txg.is_some());
        assert!(id < syncing_state.pending_object_min_block);
        syncing_state
            .pending_frees_log
            .append(&self.state, PendingFreesLogEntry { block: id });
    }
}
