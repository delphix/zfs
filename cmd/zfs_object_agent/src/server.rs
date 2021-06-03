use crate::object_access::ObjectAccess;
use crate::pool::*;
use crate::zettacache::ZettaCache;
use crate::{base_types::*, object_access::OAError};
use anyhow::Context;
use lazy_static::lazy_static;
use log::*;
use nvpair::{NvData, NvEncoding, NvList, NvListRef};
use rusoto_s3::S3;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::{cmp::max, sync::Arc};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use serde::{Deserialize, Serialize};
use std::{
    cmp::max,
    collections::HashMap,
    ffi::CString,
    fs::File,
    io::{Read, Write},
    net::{IpAddr, Ipv4Addr},
    path::Path,
    sync::Arc,
    time::{Instant, SystemTime},
};
use tokio::net::unix::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::UnixListener;
use tokio::net::UnixStream;
use tokio::sync::Mutex;
use tokio::time::Duration;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    time::sleep,
};
use uuid::Uuid;

const LEASE_DURATION: Duration = Duration::from_secs(10);
const CLAIM_DURATION: Duration = Duration::from_secs(2);
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(1);

enum OwnResult {
    SUCCESS,
    FAILURE(HeartbeatImpl),
    RETRY,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
struct PoolOwnerImpl {
    id: PoolGUID,
    owner: Uuid,
}

impl PoolOwnerImpl {
    fn key(id: PoolGUID) -> String {
        format!("zfs/{}/owner", id.to_string())
    }

    async fn get(object_access: &ObjectAccess, id: PoolGUID) -> anyhow::Result<Self> {
        let key = Self::key(id);
        let buf = object_access.get_object_impl(&key, None).await?;
        let this: Self = serde_json::from_slice(&buf)
            .context(format!("Failed to decode contents of {}", key))?;
        debug!("got {:#?}", this);
        assert_eq!(this.id, id);
        Ok(this)
    }

    async fn put_timeout(
        &self,
        object_access: &ObjectAccess,
        timeout: Option<Duration>,
    ) -> Result<
        rusoto_s3::PutObjectOutput,
        OAError<rusoto_core::RusotoError<rusoto_s3::PutObjectError>>,
    > {
        debug!("putting {:#?}", self);
        let buf = serde_json::to_vec(&self).unwrap();
        object_access
            .put_object_timed(&Self::key(self.id), buf, timeout)
            .await
    }

    async fn delete(object_access: &ObjectAccess, id: PoolGUID) {
        object_access.delete_object(&Self::key(id)).await;
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
struct HeartbeatImpl {
    timestamp: SystemTime,
    hostname: String,
    addr: IpAddr,
    lease_duration: Duration,
    id: Uuid,
}

impl HeartbeatImpl {
    fn key(id: Uuid) -> String {
        format!("zfs/agents/{}", id.to_string())
    }

    async fn get(object_access: &ObjectAccess, id: Uuid) -> anyhow::Result<Self> {
        let key = Self::key(id);
        let buf = object_access.get_object_impl(&key, None).await?;
        let this: Self = serde_json::from_slice(&buf)
            .context(format!("Failed to decode contents of {}", key))?;
        debug!("got {:#?}", this);
        assert_eq!(this.id, id);
        Ok(this)
    }

    async fn put_timeout(
        &self,
        object_access: &ObjectAccess,
        timeout: Option<Duration>,
    ) -> Result<
        rusoto_s3::PutObjectOutput,
        OAError<rusoto_core::RusotoError<rusoto_s3::PutObjectError>>,
    > {
        debug!("putting {:#?}", self);
        let buf = serde_json::to_vec(&self).unwrap();
        object_access
            .put_object_timed(&Self::key(self.id), buf, timeout)
            .await
    }

    async fn delete(object_access: &ObjectAccess, id: Uuid) {
        object_access.delete_object(&Self::key(id)).await;
    }
}

pub struct Server {
    input: OwnedReadHalf,
    output: Arc<tokio::sync::Mutex<OwnedWriteHalf>>,
    // Pool is Some once we get a "open pool" request
    pool: Option<Arc<Pool>>,
    num_outstanding_writes: Arc<AtomicUsize>,
    // XXX make Option?
    max_blockid: BlockId, // Maximum blockID that we've received a write for
    readonly: bool,
}

impl Server {
    fn suspend_all_pools(cause: &str) {
        error!("Suspending pools due to {}", cause);
        todo!("Clean pool suspending not implemented");
    }

    fn start_heartbeat(
        heartbeat_container: &'static Arc<
            std::sync::Mutex<HashMap<(CString, CString, CString), u64>>,
        >,
        nvl: &NvList,
        id: Uuid,
    ) -> (CString, CString, CString) {
        let endpoint = nvl.lookup_string("endpoint").unwrap();
        let region = nvl.lookup_string("region").unwrap();
        let bucket = nvl.lookup_string("bucket").unwrap();
        let key = (endpoint, region, bucket);
        {
            let mut heartbeats = heartbeat_container.lock().unwrap();

            let value = heartbeats.get(&key);
            let existing = value.is_some();
            let i = *value.unwrap_or(&0);
            heartbeats.insert(key.clone(), i + 1);
            if existing {
                debug!("{} existing references found", i);
                return key;
            }
        }
        let object_access = Self::get_object_access(nvl);
        let key_copy = key.clone();
        tokio::spawn(async move {
            let mut last_heartbeat = HeartbeatImpl::get(&object_access, id)
                .await
                .map_or(SystemTime::UNIX_EPOCH, |x| x.timestamp);
            info!("Starting heartbeat with id {}", id);
            loop {
                let count = {
                    let heartbeats = heartbeat_container.lock().unwrap();
                    *heartbeats.get(&key_copy).unwrap()
                };
                if count == 0 as u64 {
                    info!("Stopping heartbeat with id {}", id);
                    {
                        let mut heartbeats = heartbeat_container.lock().unwrap();
                        heartbeats.remove(&key_copy);
                    }
                    HeartbeatImpl::delete(&object_access, id).await;
                    return;
                }
                let heartbeat = HeartbeatImpl {
                    timestamp: SystemTime::now(),
                    hostname: hostname::get().unwrap().into_string().unwrap(),
                    addr: IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), // test
                    lease_duration: LEASE_DURATION,
                    id,
                };
                let result = heartbeat
                    .put_timeout(&object_access, Some(Duration::from_secs(2)))
                    .await;
                if last_heartbeat != SystemTime::UNIX_EPOCH
                    && SystemTime::now()
                        .duration_since(last_heartbeat)
                        .map_or(false, |dur| dur > LEASE_DURATION)
                {
                    Self::suspend_all_pools("lease timeout");
                    break;
                }
                if let Ok(_) = result {
                    last_heartbeat = heartbeat.timestamp;
                }
                sleep(HEARTBEAT_INTERVAL).await;
            }
        });
        key
    }

    async fn get_next_request(pipe: &mut OwnedReadHalf) -> tokio::io::Result<NvList> {
        // XXX kernel sends this as host byte order
        let len64 = pipe.read_u64_le().await?;
        //println!("got request len: {}", len64);
        if len64 > 20_000_000 {
            // max zfs block size is 16MB
            panic!("got unreasonable request length {} ({:#x})", len64, len64);
        }

        let mut v = Vec::new();
        // XXX would be nice if we didn't have to zero it out.  Should be able
        // to do that using read_buf(), treating the Vec as a BufMut, but will
        // require multiple calls to do the equivalent of read_exact().
        v.resize(len64 as usize, 0);
        pipe.read_exact(v.as_mut()).await?;
        let nvl = NvList::try_unpack(v.as_ref()).unwrap();
        Ok(nvl)
    }

    pub fn ustart(connection: UnixStream) {
        let (r, w) = connection.into_split();
        let mut server = Server {
            input: r,
            output: Arc::new(Mutex::new(w)),
            pool: None,
            num_outstanding_writes: Arc::new(AtomicUsize::new(0)),
            max_blockid: BlockId(0),
            readonly: true,
        };
        tokio::spawn(async move {
            loop {
                let nvl = match Self::get_next_request(&mut server.input).await {
                    Err(e) => {
                        info!("got error reading from user connection: {:?}", e);
                        return;
                    }
                    Ok(nvl) => nvl,
                };
                match nvl.lookup_string("Type").unwrap().to_str().unwrap() {
                    "get pools" => {
                        // XXX nvl includes credentials; need to redact?
                        info!("got request: {:?}", nvl);
                        server.get_pools(&nvl).await;
                    }
                    other => {
                        panic!("bad type {:?} in request {:?}", other, nvl);
                    }
                }
            }
        });
    }

    pub fn start(
        connection: UnixStream,
        cache: Option<ZettaCache>,
        id: Uuid,
        mut heartbeats: &'static Arc<std::sync::Mutex<HashMap<(CString, CString, CString), u64>>>,
    ) {
        let (r, w) = connection.into_split();
        let mut server = Server {
            input: r,
            output: Arc::new(Mutex::new(w)),
            pool: None,
            num_outstanding_writes: Arc::new(AtomicUsize::new(0)),
            max_blockid: BlockId(0),
            readonly: true,
        };
        tokio::spawn(async move {
            let mut heartbeat_key = None;
            loop {
                let nvl = match tokio::time::timeout(
                    Duration::from_millis(100),
                    Self::get_next_request(&mut server.input),
                )
                .await
                {
                    Err(_) => {
                        // timed out. Note that we can not call flush_writes()
                        // while in the middle of a end_txg(). So we only do it
                        // while there are writes in progress, which can't be
                        // the case during an end_txg().
                        // XXX we should also be able to time out and flush even
                        // if we are getting lots of reads.
                        if server.pool.is_some()
                            && server.num_outstanding_writes.load(Ordering::Acquire) > 0
                        {
                            trace!("timeout; flushing writes");
                            server.flush_writes();
                        }
                        continue;
                    }
                    Ok(getreq_result) => match getreq_result {
                        Err(_) => {
                            info!(
                                "got error reading from kernel connection: {:?}",
                                getreq_result
                            );
                            return;
                        }
                        Ok(mynvl) => mynvl,
                    },
                };
                match nvl.lookup_string("Type").unwrap().to_str().unwrap() {
                    "create pool" => {
                        // XXX nvl includes credentials; need to redact?
                        info!("got request: {:?}", nvl);
                        let guid = PoolGuid(nvl.lookup_uint64("GUID").unwrap());
                        let name = nvl.lookup_string("name").unwrap();
                        let object_access = Self::get_object_access(nvl.as_ref());
                        heartbeat_key = Some(Self::start_heartbeat(heartbeats, &nvl, id));

                        server
                            .create_pool(&object_access, guid, name.to_str().unwrap())
                            .await;
                    }
                    "open pool" => {
                        // XXX nvl includes credentials; need to redact?
                        info!("got request: {:?}", nvl);
                        let guid = PoolGuid(nvl.lookup_uint64("GUID").unwrap());
                        let object_access = Self::get_object_access(nvl.as_ref());
                        let readonly = nvl.lookup("readonly").is_ok();
                        heartbeat_key = Some(Self::start_heartbeat(&mut heartbeats, &nvl, id));
                        server
                            .open_pool(
                                &object_access,
                                guid,
                                id,
                                readonly,
                                cache.as_ref().cloned()),
                            )
                            .await;
                    }
                    "begin txg" => {
                        debug!("got request: {:?}", nvl);
                        let txg = Txg(nvl.lookup_uint64("TXG").unwrap());
                        server.begin_txg(txg);
                    }
                    "resume txg" => {
                        info!("got request: {:?}", nvl);
                        let txg = Txg(nvl.lookup_uint64("TXG").unwrap());
                        server.resume_txg(txg);
                    }
                    "resume complete" => {
                        info!("got request: {:?}", nvl);
                        server.resume_complete().await;
                    }
                    "flush writes" => {
                        trace!("got request: {:?}", nvl);
                        server.flush_writes();
                    }
                    "end txg" => {
                        debug!("got request: {:?}", nvl);
                        let uberblock = nvl.lookup("uberblock").unwrap().data();
                        let config = nvl.lookup("config").unwrap().data();
                        if let NvData::Uint8Array(slice) = uberblock {
                            if let NvData::Uint8Array(slice2) = config {
                                server.end_txg(slice.to_vec(), slice2.to_vec());
                            } else {
                                panic!("config not expected type")
                            }
                        } else {
                            panic!("uberblock not expected type")
                        }
                    }
                    "write block" => {
                        let block = BlockId(nvl.lookup_uint64("block").unwrap());
                        let data = nvl.lookup("data").unwrap().data();
                        let id = nvl.lookup_uint64("request_id").unwrap();
                        let token = nvl.lookup_uint64("token").unwrap();
                        if let NvData::Uint8Array(slice) = data {
                            trace!(
                                "got write request id={}: {:?} len={}",
                                id,
                                block,
                                slice.len()
                            );
                            server.write_block(block, slice.to_vec(), id, token);
                        } else {
                            panic!("data not expected type")
                        }
                    }
                    "free block" => {
                        trace!("got request: {:?}", nvl);
                        let block = BlockId(nvl.lookup_uint64("block").unwrap());
                        let size = nvl.lookup_uint64("size").unwrap();
                        server.free_block(block, size as u32);
                    }
                    "read block" => {
                        trace!("got request: {:?}", nvl);
                        let block = BlockId(nvl.lookup_uint64("block").unwrap());
                        let id = nvl.lookup_uint64("request_id").unwrap();
                        let token = nvl.lookup_uint64("token").unwrap();
                        server.read_block(block, id, token);
                    }
                    "exit agent" => {
                        info!("Receiving agent shutdown request");
                        server.close_pool().await;
                        if let Some(key) = heartbeat_key {
                            let mut map = heartbeats.lock().unwrap();
                            let val = map.get_mut(&key).unwrap();
                            *val = *val - 1;
                        }
                        return;
                    }
                    other => {
                        panic!("bad type {:?} in request {:?}", other, nvl);
                    }
                }
            }
        });
    }

    async fn send_response(output: &Mutex<OwnedWriteHalf>, nvl: NvList) {
        //println!("sending response: {:?}", nvl);
        let buf = nvl.pack(NvEncoding::Native).unwrap();
        drop(nvl);
        let len64 = buf.len() as u64;
        let mut w = output.lock().await;
        // XXX kernel expects this as host byte order
        //println!("sending response of {} bytes", len64);
        w.write_u64_le(len64).await.unwrap();
        w.write_all(buf.as_slice()).await.unwrap();
    }

    fn get_object_access(nvl: &NvListRef) -> ObjectAccess {
        let bucket_name = nvl.lookup_string("bucket").unwrap();
        let region_str = nvl.lookup_string("region").unwrap();
        let endpoint = nvl.lookup_string("endpoint").unwrap();
        ObjectAccess::new(
            endpoint.to_str().unwrap(),
            region_str.to_str().unwrap(),
            bucket_name.to_str().unwrap(),
        )
    }

    async fn get_pools(&mut self, nvl: &NvList) {
        let region_str = nvl.lookup_string("region").unwrap();
        let endpoint = nvl.lookup_string("endpoint").unwrap();
        let mut client =
            ObjectAccess::get_client(endpoint.to_str().unwrap(), region_str.to_str().unwrap());
        let mut resp = NvList::new_unique_names();
        let mut buckets = vec![];
        if let Ok(bucket) = nvl.lookup_string("bucket") {
            buckets.push(bucket.into_string().unwrap());
        } else {
            buckets.append(
                &mut client
                    .list_buckets()
                    .await
                    .unwrap()
                    .buckets
                    .unwrap()
                    .into_iter()
                    .map(|b| b.name.unwrap())
                    .collect(),
            );
        }

        for buck in buckets {
            let object_access = ObjectAccess::from_client(client, buck.as_str());
            if let Ok(guid) = nvl.lookup_uint64("guid") {
                if !Pool::exists(&object_access, PoolGuid(guid)).await {
                    client = object_access.release_client();
                    continue;
                }
                let pool_config = Pool::get_config(&object_access, PoolGuid(guid)).await;
                if pool_config.is_err() {
                    client = object_access.release_client();
                    continue;
                }
                resp.insert(format!("{}", guid), pool_config.unwrap().as_ref())
                    .unwrap();
                debug!("sending response: {:?}", resp);
                Self::send_response(&self.output, resp).await;
                return;
            }
            for prefix in object_access.collect_prefixes("zfs/").await {
                debug!("prefix: {}", prefix);
                let split: Vec<&str> = prefix.rsplitn(3, '/').collect();
                let guid_str: &str = split[1];
                if let Ok(guid64) = str::parse::<u64>(guid_str) {
                    let guid = PoolGuid(guid64);
                    // XXX do this in parallel for all guids?
                    match Pool::get_config(&object_access, guid).await {
                        Ok(pool_config) => resp.insert(guid_str, pool_config.as_ref()).unwrap(),
                        Err(e) => {
                            error!("skipping {:?}: {:?}", guid, e);
                        }
                    }
                }
            }
            client = object_access.release_client();
        }
        debug!("sending response: {:?}", resp);
        Self::send_response(&self.output, resp).await;
    }

    async fn claim_pool(
        &self,
        object_access: &ObjectAccess,
        guid: PoolGUID,
        id: Uuid,
    ) -> OwnResult {
        let start = Instant::now();
        let owner_res = PoolOwnerImpl::get(object_access, guid).await;
        let mut duration = Instant::now().duration_since(start);
        if let Ok(owner) = owner_res {
            info!("Owner found: {:?}", owner);
            let heartbeat_res = HeartbeatImpl::get(object_access, owner.owner).await;
            duration = Instant::now().duration_since(start);
            if let Ok(heartbeat) = heartbeat_res {
                info!("Heartbeat found: {:?}", heartbeat);
                if owner.owner == id {
                    info!("Self heartbeat found");
                    return OwnResult::SUCCESS;
                }
                /*
                 * We do this twice, because in the normal case we'll find an updated heartbeat within
                 * a couple seconds. If the case where there are issues, we wait for the full duration.
                 */
                let short_duration = HEARTBEAT_INTERVAL.checked_mul(2).unwrap();
                let long_duration = LEASE_DURATION
                    .checked_mul(2)
                    .unwrap()
                    .checked_sub(short_duration)
                    .unwrap();
                sleep(short_duration).await;
                let new_heartbeat_res = HeartbeatImpl::get(object_access, owner.owner).await;
                if let Ok(new_heartbeat) = new_heartbeat_res {
                    if heartbeat.timestamp != new_heartbeat.timestamp {
                        return OwnResult::FAILURE(new_heartbeat);
                    }
                }
                sleep(long_duration).await;
                let new_heartbeat_res = HeartbeatImpl::get(object_access, owner.owner).await;
                if let Ok(new_heartbeat) = new_heartbeat_res {
                    if heartbeat.timestamp != new_heartbeat.timestamp {
                        return OwnResult::FAILURE(new_heartbeat);
                    }
                }
                let time = Instant::now();
                let new_owner_res = PoolOwnerImpl::get(object_access, guid).await;
                if let Ok(new_owner) = new_owner_res {
                    if new_owner.owner != owner.owner {
                        return OwnResult::FAILURE(
                            HeartbeatImpl::get(object_access, new_owner.owner)
                                .await
                                .unwrap(),
                        );
                    }
                }
                duration = Instant::now().duration_since(time);
            }
        }

        if duration > CLAIM_DURATION {
            return OwnResult::RETRY;
        }

        let owner = PoolOwnerImpl {
            id: guid,
            owner: id,
        };

        let put_result = owner
            .put_timeout(
                object_access,
                Some(CLAIM_DURATION.checked_sub(duration).unwrap()),
            )
            .await;

        if let Err(OAError::TimeoutError(_)) = put_result {
            return OwnResult::RETRY;
        }
        sleep(CLAIM_DURATION.checked_mul(3).unwrap()).await;

        let final_owner_res = PoolOwnerImpl::get(object_access, guid).await;
        if let Ok(final_owner) = final_owner_res {
            if final_owner.owner != id {
                return OwnResult::FAILURE(
                    HeartbeatImpl::get(object_access, final_owner.owner)
                        .await
                        .unwrap_or(HeartbeatImpl {
                            timestamp: SystemTime::now(),
                            hostname: "unknown".to_string(),
                            addr: IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
                            lease_duration: LEASE_DURATION,
                            id: final_owner.owner,
                        }),
                );
            }
        }
        return OwnResult::SUCCESS;
    }

    async fn unclaim_pool(&self, object_access: &ObjectAccess, guid: PoolGUID) {
        PoolOwnerImpl::delete(object_access, guid).await;
    }

    async fn create_pool(&mut self, object_access: &ObjectAccess, guid: PoolGuid, name: &str) {
        Pool::create(object_access, name, guid).await;
        let mut nvl = NvList::new_unique_names();
        nvl.insert("Type", "pool create done").unwrap();
        nvl.insert("GUID", &guid.0).unwrap();
        debug!("sending response: {:?}", nvl);
        Self::send_response(&self.output, nvl).await;
    }

    /// initiate pool opening.  Responds when pool is open
    async fn open_pool(
        &mut self,
        object_access: &ObjectAccess,
        guid: PoolGuid,
        id: Uuid,
        readonly: bool,
        cache: Option<ZettaCache>,
    ) {
        let (pool, phys_opt, next_block) = Pool::open(object_access, guid, cache).await;
        while readonly == false {
            match self.claim_pool(object_access, guid, id).await {
                OwnResult::SUCCESS => {
                    break;
                }
                OwnResult::FAILURE(heartbeat) => {
                    let mut nvl = NvList::new_unique_names();
                    nvl.insert("Type", "pool open failed").unwrap();
                    nvl.insert("cause", "MMP").unwrap();
                    nvl.insert("hostname", heartbeat.hostname.as_str()).unwrap();
                    debug!("sending response: {:?}", nvl);
                    Self::send_response(&self.output, nvl).await;
                    return;
                }
                OwnResult::RETRY => {
                    continue;
                }
            }
        }
        self.pool = Some(Arc::new(pool));
        self.readonly = readonly;
        let mut nvl = NvList::new_unique_names();
        nvl.insert("Type", "pool open done").unwrap();
        nvl.insert("GUID", &guid.0).unwrap();
        if let Some(phys) = phys_opt {
            nvl.insert("uberblock", &phys.get_zfs_uberblock()[..])
                .unwrap();
            nvl.insert("config", &phys.get_zfs_config()[..]).unwrap();
        }

        nvl.insert("next_block", &next_block.0).unwrap();
        debug!("sending response: {:?}", nvl);
        Self::send_response(&self.output, nvl).await;
    }

    // no response
    fn begin_txg(&mut self, txg: Txg) {
        let pool = self.pool.as_ref().unwrap().clone();
        pool.begin_txg(txg);
    }

    // no response
    fn resume_txg(&mut self, txg: Txg) {
        let pool = self.pool.as_ref().unwrap().clone();
        pool.resume_txg(txg);
    }

    // no response
    // This is .await'ed by the server's thread, so we can't see any new writes
    // come in while it's in progress.
    async fn resume_complete(&mut self) {
        let pool = self.pool.as_ref().unwrap().clone();
        pool.resume_complete().await;
    }

    // no response
    fn flush_writes(&mut self) {
        let pool = self.pool.as_ref().unwrap().clone();
        let max_blockid = self.max_blockid;
        pool.initiate_flush(max_blockid);
    }

    // sends response when completed
    fn end_txg(&mut self, uberblock: Vec<u8>, config: Vec<u8>) {
        let pool = self.pool.as_ref().unwrap().clone();
        let output = self.output.clone();
        // client should have already flushed all writes
        // XXX change to an error return
        assert_eq!(self.num_outstanding_writes.load(Ordering::Acquire), 0);
        tokio::spawn(async move {
            let stats = pool.end_txg(uberblock, config).await;
            let mut nvl = NvList::new_unique_names();
            nvl.insert("Type", "end txg done").unwrap();
            nvl.insert("blocks_count", &stats.blocks_count).unwrap();
            nvl.insert("blocks_bytes", &stats.blocks_bytes).unwrap();
            nvl.insert("pending_frees_count", &stats.pending_frees_count)
                .unwrap();
            nvl.insert("pending_frees_bytes", &stats.pending_frees_bytes)
                .unwrap();
            nvl.insert("objects_count", &stats.objects_count).unwrap();
            debug!("sending response: {:?}", nvl);
            Self::send_response(&output, nvl).await;
        });
    }

    /// queue write, sends response when completed (persistent).  Does not block.
    /// completion may not happen until flush_pool() is called
    fn write_block(&mut self, block: BlockId, data: Vec<u8>, request_id: u64, token: u64) {
        self.max_blockid = max(block, self.max_blockid);
        let pool = self.pool.as_ref().unwrap().clone();
        let output = self.output.clone();
        self.num_outstanding_writes.fetch_add(1, Ordering::Release);
        // Need to write_block() before spawning, so that the Pool knows what's been written before resume_complete()
        let fut = pool.write_block(block, data);
        let now = self.num_outstanding_writes.clone();
        tokio::spawn(async move {
            fut.await;
            now.fetch_sub(1, Ordering::Release);
            let mut nvl = NvList::new_unique_names();
            nvl.insert("Type", "write done").unwrap();
            nvl.insert("block", &block.0).unwrap();
            nvl.insert("request_id", &request_id).unwrap();
            nvl.insert("token", &token).unwrap();
            trace!("sending response: {:?}", nvl);
            Self::send_response(&output, nvl).await;
        });
    }

    /// initiate free.  No response.  Does not block.  Completes when the current txg is ended.
    fn free_block(&mut self, block: BlockId, size: u32) {
        let pool = self.pool.as_ref().unwrap().clone();
        pool.free_block(block, size);
    }

    /// initiate read, sends response when completed.  Does not block.
    fn read_block(&mut self, block: BlockId, request_id: u64, token: u64) {
        let pool = self.pool.as_ref().unwrap().clone();
        let output = self.output.clone();
        tokio::spawn(async move {
            let data = pool.read_block(block).await;
            let mut nvl = NvList::new_unique_names();
            nvl.insert("Type", "read done").unwrap();
            nvl.insert("block", &block.0).unwrap();
            nvl.insert("request_id", &request_id).unwrap();
            nvl.insert("token", &token).unwrap();
            nvl.insert("data", data.as_slice()).unwrap();
            trace!(
                "sending read done response: block={} req={} data=[{} bytes]",
                block,
                request_id,
                data.len()
            );
            Self::send_response(&output, nvl).await;
        });
    }

    async fn close_pool(&mut self) {
        if self.pool.is_none() || self.readonly {
            return;
        }
        let pool_shared_state = &self.pool.as_ref().unwrap().state.shared_state;
        self.unclaim_pool(&pool_shared_state.object_access, pool_shared_state.guid)
            .await;
    }
}

fn create_listener(path: String) -> UnixListener {
    let _ = std::fs::remove_file(&path);
    info!("Listening on: {}", path);
    UnixListener::bind(&path).unwrap()
}

lazy_static! {
    static ref HEARTBEAT: Arc<std::sync::Mutex<HashMap<(CString, CString, CString), u64>>> =
        Arc::new(std::sync::Mutex::new(HashMap::new()));
}

pub async fn do_server(socket_dir: &str, cache_path: Option<&str>) {
    let ksocket_name = format!("{}/zfs_kernel_socket", socket_dir);
    let usocket_name = format!("{}/zfs_user_socket", socket_dir);
    let id_path_str = format!("{}/zfs_agent_id", socket_dir);
    let id_path = Path::new(&id_path_str);

    let id = match File::open(id_path) {
        Ok(mut f) => {
            let mut bytes = [0; 16];
            assert!(f.read(&mut bytes).unwrap() == 16);
            Uuid::from_bytes(bytes)
        }
        Err(_) => {
            let mut file = File::create(id_path).unwrap();
            let uuid = Uuid::new_v4();
            file.write_all(uuid.as_bytes()).unwrap();
            uuid
        }
    };

    let klistener = create_listener(ksocket_name.clone());
    let ulistener = create_listener(usocket_name.clone());

    let ujh = tokio::spawn(async move {
        loop {
            match ulistener.accept().await {
                Ok((socket, _)) => {
                    info!("accepted connection on {}", usocket_name);
                    self::Server::ustart(socket);
                }
                Err(e) => {
                    warn!("accept() on {} failed: {}", usocket_name, e);
                }
            }
        }
    });

    let cache = match cache_path {
        Some(path) => Some(ZettaCache::open(path).await),
        None => None,
    };
    let kjh = tokio::spawn(async move {
        loop {
            match klistener.accept().await {
                Ok((socket, _)) => {
                    info!("accepted connection on {}", ksocket_name);
                    self::Server::start(
                        socket,
                        cache.as_ref().cloned(),
                        id,
                        &HEARTBEAT,
                    );
                }
                Err(e) => {
                    warn!("accept() on {} failed: {}", ksocket_name, e);
                }
            }
        }
    });

    ujh.await.unwrap();
    kjh.await.unwrap();
}
