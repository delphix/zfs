use crate::base_types::*;
use crate::connection::Connection;
use crate::object_access::ObjectAccess;
use crate::pool::*;
use crate::zettacache::ZettaCache;
use futures::future;
use futures::Future;
use log::*;
use nvpair::{NvData, NvList, NvListRef};
use rusoto_s3::S3;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;
use std::{cmp::max, sync::Arc};
use tokio::net::UnixListener;
use tokio::net::UnixStream;

struct KernelConnectionState {
    pool: Option<Arc<Pool>>,
    num_outstanding_writes: Arc<AtomicUsize>,
    // XXX make Option?
    max_blockid: BlockId, // Maximum blockID that we've received a write for
    cache: Option<ZettaCache>,
}

impl KernelConnectionState {
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

    async fn create_pool_impl(object_access: &ObjectAccess, guid: PoolGuid, name: &str) -> NvList {
        Pool::create(object_access, name, guid).await;
        let mut nvl = NvList::new_unique_names();
        nvl.insert("Type", "pool create done").unwrap();
        nvl.insert("GUID", &guid.0).unwrap();
        debug!("sending response: {:?}", nvl);
        nvl
    }

    fn create_pool(self, nvl: NvList) -> impl Future<Output = (Self, Option<NvList>)> {
        info!("got request: {:?}", nvl);
        let guid = PoolGuid(nvl.lookup_uint64("GUID").unwrap());
        let name = nvl.lookup_string("name").unwrap();
        let object_access = Self::get_object_access(nvl.as_ref());
        async move {
            let response =
                Self::create_pool_impl(&object_access, guid, name.to_str().unwrap()).await;
            (self, Some(response))
        }
    }

    async fn open_pool_impl(
        object_access: &ObjectAccess,
        guid: PoolGuid,
        cache: Option<ZettaCache>,
    ) -> (Pool, NvList) {
        let (pool, phys_opt, next_block) = Pool::open(object_access, guid, cache).await;
        //self.pool = Some(Arc::new(pool));
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
        (pool, nvl)
    }

    fn open_pool(mut self, nvl: NvList) -> impl Future<Output = (Self, Option<NvList>)> {
        info!("got request: {:?}", nvl);
        let guid = PoolGuid(nvl.lookup_uint64("GUID").unwrap());
        let object_access = Self::get_object_access(nvl.as_ref());
        let cache = self.cache.as_ref().cloned();
        async move {
            let (pool, response) = Self::open_pool_impl(&object_access, guid, cache).await;
            self.pool = Some(Arc::new(pool));
            (self, Some(response))
        }
    }

    fn begin_txg(&mut self, nvl: NvList) -> impl Future<Output = Option<NvList>> {
        debug!("got request: {:?}", nvl);
        let txg = Txg(nvl.lookup_uint64("TXG").unwrap());

        let pool = self.pool.as_ref().unwrap();
        pool.begin_txg(txg);

        future::ready(None)
    }

    fn resume_txg(&mut self, nvl: NvList) -> impl Future<Output = Option<NvList>> {
        info!("got request: {:?}", nvl);
        let txg = Txg(nvl.lookup_uint64("TXG").unwrap());

        let pool = self.pool.as_ref().unwrap();
        pool.resume_txg(txg);

        future::ready(None)
    }

    fn resume_complete(self, nvl: NvList) -> impl Future<Output = (Self, Option<NvList>)> {
        info!("got request: {:?}", nvl);

        // This is .await'ed by the server's thread, so we can't see any new writes
        // come in while it's in progress.
        async move {
            let pool = self.pool.as_ref().unwrap();
            pool.resume_complete().await;
            (self, None)
        }
    }

    fn flush_writes(&mut self, nvl: NvList) -> impl Future<Output = Option<NvList>> {
        trace!("got request: {:?}", nvl);
        let pool = self.pool.as_ref().unwrap();
        pool.initiate_flush(self.max_blockid);
        future::ready(None)
    }

    // sends response when completed
    async fn end_txg_impl(pool: Arc<Pool>, uberblock: Vec<u8>, config: Vec<u8>) -> NvList {
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
        nvl
    }

    fn end_txg(&mut self, nvl: NvList) -> impl Future<Output = Option<NvList>> {
        debug!("got request: {:?}", nvl);

        // client should have already flushed all writes
        // XXX change to an error return
        assert_eq!(self.num_outstanding_writes.load(Ordering::Acquire), 0);

        let pool = self.pool.as_ref().unwrap().clone();
        async move {
            let uberblock = nvl.lookup("uberblock").unwrap().data();
            let config = nvl.lookup("config").unwrap().data();
            if let NvData::Uint8Array(slice) = uberblock {
                if let NvData::Uint8Array(slice2) = config {
                    Some(Self::end_txg_impl(pool, slice.to_vec(), slice2.to_vec()).await)
                } else {
                    panic!("config not expected type")
                }
            } else {
                panic!("uberblock not expected type")
            }
        }
    }

    /// queue write, sends response when completed (persistent).
    /// completion may not happen until flush_pool() is called
    fn write_block(&mut self, nvl: NvList) -> impl Future<Output = Option<NvList>> {
        let block = BlockId(nvl.lookup_uint64("block").unwrap());
        let data = nvl.lookup("data").unwrap().data();
        let request_id = nvl.lookup_uint64("request_id").unwrap();
        if let NvData::Uint8Array(slice) = data {
            trace!(
                "got write request id={}: {:?} len={}",
                request_id,
                block,
                slice.len()
            );

            self.max_blockid = max(block, self.max_blockid);
            let pool = self.pool.as_ref().unwrap().clone();
            self.num_outstanding_writes.fetch_add(1, Ordering::Release);
            // Need to write_block() before spawning, so that the Pool knows what's been written before resume_complete()
            let fut = pool.write_block(block, slice.to_vec());
            let now = self.num_outstanding_writes.clone();
            async move {
                fut.await;
                now.fetch_sub(1, Ordering::Release);
                let mut nvl = NvList::new_unique_names();
                nvl.insert("Type", "write done").unwrap();
                nvl.insert("block", &block.0).unwrap();
                nvl.insert("request_id", &request_id).unwrap();
                trace!("sending response: {:?}", nvl);
                //Self::send_response(&output, nvl).await;
                Some(nvl)
            }
        } else {
            panic!("data not expected type")
        }
    }

    fn free_block(&mut self, nvl: NvList) -> impl Future<Output = Option<NvList>> {
        trace!("got request: {:?}", nvl);
        let block = BlockId(nvl.lookup_uint64("block").unwrap());
        let size = nvl.lookup_uint64("size").unwrap();

        let pool = self.pool.as_ref().unwrap();
        pool.free_block(block, size as u32);

        future::ready(None)
    }

    fn read_block(&mut self, nvl: NvList) -> impl Future<Output = Option<NvList>> {
        trace!("got request: {:?}", nvl);
        let block = BlockId(nvl.lookup_uint64("block").unwrap());
        let request_id = nvl.lookup_uint64("request_id").unwrap();

        let pool = self.pool.as_ref().unwrap().clone();
        async move {
            let data = pool.read_block(block).await;
            let mut nvl = NvList::new_unique_names();
            nvl.insert("Type", "read done").unwrap();
            nvl.insert("block", &block.0).unwrap();
            nvl.insert("request_id", &request_id).unwrap();
            nvl.insert("data", data.as_slice()).unwrap();
            trace!(
                "sending read done response: block={} req={} data=[{} bytes]",
                block,
                request_id,
                data.len()
            );
            Some(nvl)
        }
    }

    fn timeout(&mut self) {
        if let Some(pool) = &self.pool {
            if self.num_outstanding_writes.load(Ordering::Acquire) > 0 {
                trace!("timeout; flushing writes");
                pool.initiate_flush(self.max_blockid);
            }
        }
    }

    fn start(stream: UnixStream, cache: Option<ZettaCache>) {
        let mut connection = Connection::new(
            stream,
            KernelConnectionState {
                pool: None,
                num_outstanding_writes: Default::default(),
                max_blockid: BlockId(0),
                cache,
            },
        );

        connection
            .register_serial_handler("create pool", Box::new(KernelConnectionState::create_pool));
        connection.register_serial_handler("open pool", Box::new(KernelConnectionState::open_pool));
        connection.register_serial_handler(
            "resume complete",
            Box::new(KernelConnectionState::resume_complete),
        );

        connection.register_timeout_handler(
            Duration::from_millis(100),
            Box::new(KernelConnectionState::timeout),
        );

        connection.register_handler("begin txg", Box::new(KernelConnectionState::begin_txg));
        connection.register_handler("resume txg", Box::new(KernelConnectionState::resume_txg));
        connection.register_handler(
            "flush writes",
            Box::new(KernelConnectionState::flush_writes),
        );
        connection.register_handler("end txg", Box::new(KernelConnectionState::end_txg));
        connection.register_handler("write block", Box::new(KernelConnectionState::write_block));
        connection.register_handler("free block", Box::new(KernelConnectionState::free_block));
        connection.register_handler("read block", Box::new(KernelConnectionState::read_block));

        connection.start();
    }
}

struct UserConnectionState {}

impl UserConnectionState {
    fn get_pools(&mut self, nvl: NvList) -> impl Future<Output = Option<NvList>> {
        async move { Self::get_pools_impl(nvl).await }
    }

    async fn get_pools_impl(nvl: NvList) -> Option<NvList> {
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
                return Some(resp);
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
        Some(resp)
    }
}

struct Server {}

impl Server {
    pub fn ustart(stream: UnixStream) {
        let mut connection = Connection::new(stream, UserConnectionState {});

        connection.register_handler("get pools", Box::new(UserConnectionState::get_pools));
        connection.start();
    }

    pub fn start(connection: UnixStream, cache: Option<ZettaCache>) {
        KernelConnectionState::start(connection, cache);
        // XXX remove all below
        /*
        tokio::spawn(async move {
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
            }
        });
        */
    }
}

fn create_listener(path: String) -> UnixListener {
    let _ = std::fs::remove_file(&path);
    info!("Listening on: {}", path);
    UnixListener::bind(&path).unwrap()
}

pub async fn do_server(socket_dir: &str, cache_path: Option<&str>) {
    let ksocket_name = format!("{}/zfs_kernel_socket", socket_dir);
    let usocket_name = format!("{}/zfs_user_socket", socket_dir);

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
                    self::Server::start(socket, cache.as_ref().cloned());
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
