use crate::base_types::*;
use crate::object_access::ObjectAccess;
use crate::pool::*;
use crate::server::handler_return_ok;
use crate::server::HandlerReturn;
use crate::server::SerialHandlerReturn;
use crate::server::Server;
use crate::zettacache::ZettaCache;
use anyhow::anyhow;
use anyhow::Result;
use log::*;
use nvpair::{NvData, NvList, NvListRef};
use std::cmp::max;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

pub struct KernelServerState {
    cache: Option<ZettaCache>,
}

impl KernelServerState {
    fn connection_handler(&self) -> KernelConnectionState {
        KernelConnectionState {
            cache: self.cache.as_ref().cloned(),
            ..Default::default()
        }
    }

    pub fn start(socket_dir: &str, cache: Option<ZettaCache>) {
        let socket_path = format!("{}/zfs_kernel_socket", socket_dir);
        let mut server = Server::new(
            &socket_path,
            KernelServerState { cache },
            Box::new(Self::connection_handler),
        );

        KernelConnectionState::register(&mut server);
        server.start();
    }
}

#[derive(Default)]
struct KernelConnectionState {
    pool: Option<Arc<Pool>>,
    num_outstanding_writes: Arc<AtomicUsize>,
    max_blockid: Option<BlockId>, // Maximum blockID that we've received a write for
    cache: Option<ZettaCache>,
}

impl KernelConnectionState {
    fn get_object_access(nvl: &NvListRef) -> Result<ObjectAccess> {
        let bucket_name = nvl.lookup_string("bucket")?;
        let region_str = nvl.lookup_string("region")?;
        let endpoint = nvl.lookup_string("endpoint")?;
        Ok(ObjectAccess::new(
            endpoint.to_str().unwrap(),
            region_str.to_str().unwrap(),
            bucket_name.to_str().unwrap(),
        ))
    }

    async fn create_pool_impl(object_access: &ObjectAccess, guid: PoolGuid, name: &str) -> NvList {
        Pool::create(object_access, name, guid).await;
        let mut nvl = NvList::new_unique_names();
        nvl.insert("Type", "pool create done").unwrap();
        nvl.insert("GUID", &guid.0).unwrap();
        debug!("sending response: {:?}", nvl);
        nvl
    }

    fn create_pool(self, nvl: NvList) -> SerialHandlerReturn<Self> {
        info!("got request: {:?}", nvl);
        let guid = PoolGuid(nvl.lookup_uint64("GUID").unwrap());
        let name = nvl.lookup_string("name").unwrap();
        let object_access = Self::get_object_access(nvl.as_ref()).unwrap();
        Box::pin(async move {
            let response =
                Self::create_pool_impl(&object_access, guid, name.to_str().unwrap()).await;
            (self, Some(response))
        })
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

    fn open_pool(mut self, nvl: NvList) -> SerialHandlerReturn<Self> {
        info!("got request: {:?}", nvl);
        let guid = PoolGuid(nvl.lookup_uint64("GUID").unwrap());
        let object_access = Self::get_object_access(nvl.as_ref()).unwrap();
        let cache = self.cache.as_ref().cloned();
        Box::pin(async move {
            let (pool, response) = Self::open_pool_impl(&object_access, guid, cache).await;
            self.pool = Some(Arc::new(pool));
            (self, Some(response))
        })
    }

    fn begin_txg(&mut self, nvl: NvList) -> HandlerReturn {
        debug!("got request: {:?}", nvl);
        let txg = Txg(nvl.lookup_uint64("TXG")?);
        let pool = self.pool.as_ref().ok_or_else(|| anyhow!("no pool open"))?;
        pool.begin_txg(txg);

        handler_return_ok(None)
    }

    fn resume_txg(&mut self, nvl: NvList) -> HandlerReturn {
        info!("got request: {:?}", nvl);
        let txg = Txg(nvl.lookup_uint64("TXG")?);
        let pool = self.pool.as_ref().ok_or_else(|| anyhow!("no pool open"))?;
        pool.resume_txg(txg);

        handler_return_ok(None)
    }

    fn resume_complete(self, nvl: NvList) -> SerialHandlerReturn<Self> {
        info!("got request: {:?}", nvl);

        // This is .await'ed by the server's thread, so we can't see any new writes
        // come in while it's in progress.
        Box::pin(async move {
            let pool = self.pool.as_ref().unwrap();
            pool.resume_complete().await;
            (self, None)
        })
    }

    fn flush_writes(&mut self, nvl: NvList) -> HandlerReturn {
        trace!("got request: {:?}", nvl);
        let pool = self.pool.as_ref().ok_or_else(|| anyhow!("no pool open"))?;
        if let Some(max_blockid) = self.max_blockid {
            pool.initiate_flush(max_blockid);
        }
        handler_return_ok(None)
    }

    // sends response when completed
    async fn end_txg_impl(
        pool: Arc<Pool>,
        uberblock: Vec<u8>,
        config: Vec<u8>,
    ) -> Result<Option<NvList>> {
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
        Ok(Some(nvl))
    }

    fn end_txg(&mut self, nvl: NvList) -> HandlerReturn {
        debug!("got request: {:?}", nvl);

        // client should have already flushed all writes
        if self.num_outstanding_writes.load(Ordering::Acquire) != 0 {
            return Err(anyhow!("outstanding writes while trying to end txg"));
        }

        let pool = self
            .pool
            .as_ref()
            .ok_or_else(|| anyhow!("no pool open"))?
            .clone();
        Ok(Box::pin(async move {
            let uberblock = nvl.lookup("uberblock").unwrap().data();
            let config = nvl.lookup("config").unwrap().data();
            if let NvData::Uint8Array(slice) = uberblock {
                if let NvData::Uint8Array(slice2) = config {
                    Self::end_txg_impl(pool, slice.to_vec(), slice2.to_vec()).await
                } else {
                    panic!("config not expected type")
                }
            } else {
                panic!("uberblock not expected type")
            }
        }))
    }

    /// queue write, sends response when completed (persistent).
    /// completion may not happen until flush_pool() is called
    fn write_block(&mut self, nvl: NvList) -> HandlerReturn {
        let block = BlockId(nvl.lookup_uint64("block")?);
        let data = nvl.lookup("data")?.data();
        let request_id = nvl.lookup_uint64("request_id")?;
        if let NvData::Uint8Array(slice) = data {
            trace!(
                "got write request id={}: {:?} len={}",
                request_id,
                block,
                slice.len()
            );

            self.max_blockid = Some(match self.max_blockid {
                Some(max_blockid) => max(block, max_blockid),
                None => block,
            });

            let pool = self
                .pool
                .as_ref()
                .ok_or_else(|| anyhow!("no pool open"))?
                .clone();
            self.num_outstanding_writes.fetch_add(1, Ordering::Release);
            // Need to write_block() before spawning, so that the Pool knows what's been written before resume_complete()
            let fut = pool.write_block(block, slice.to_vec());
            let now = self.num_outstanding_writes.clone();
            Ok(Box::pin(async move {
                fut.await;
                now.fetch_sub(1, Ordering::Release);
                let mut nvl = NvList::new_unique_names();
                nvl.insert("Type", "write done").unwrap();
                nvl.insert("block", &block.0).unwrap();
                nvl.insert("request_id", &request_id).unwrap();
                trace!("sending response: {:?}", nvl);
                Ok(Some(nvl))
            }))
        } else {
            Err(anyhow!("data {:?} not expected type", data))
        }
    }

    fn free_block(&mut self, nvl: NvList) -> HandlerReturn {
        trace!("got request: {:?}", nvl);
        let block = BlockId(nvl.lookup_uint64("block")?);
        let size = nvl.lookup_uint64("size")? as u32;

        let pool = self.pool.as_ref().ok_or_else(|| anyhow!("no pool open"))?;
        pool.free_block(block, size);

        handler_return_ok(None)
    }

    fn read_block(&mut self, nvl: NvList) -> HandlerReturn {
        trace!("got request: {:?}", nvl);
        let block = BlockId(nvl.lookup_uint64("block")?);
        let request_id = nvl.lookup_uint64("request_id")?;

        let pool = self
            .pool
            .as_ref()
            .ok_or_else(|| anyhow!("no pool open"))?
            .clone();
        Ok(Box::pin(async move {
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
            Ok(Some(nvl))
        }))
    }

    fn timeout(&mut self) {
        if let Some(pool) = &self.pool {
            if self.num_outstanding_writes.load(Ordering::Acquire) > 0 {
                if let Some(max_blockid) = self.max_blockid {
                    trace!("timeout; flushing writes");
                    pool.initiate_flush(max_blockid);
                }
            }
        }
    }

    fn register(server: &mut Server<KernelServerState, KernelConnectionState>) {
        server.register_serial_handler("create pool", Box::new(Self::create_pool));
        server.register_serial_handler("open pool", Box::new(Self::open_pool));
        server.register_serial_handler("resume complete", Box::new(Self::resume_complete));

        server.register_timeout_handler(Duration::from_millis(100), Box::new(Self::timeout));

        server.register_handler("begin txg", Box::new(Self::begin_txg));
        server.register_handler("resume txg", Box::new(Self::resume_txg));
        server.register_handler("flush writes", Box::new(Self::flush_writes));
        server.register_handler("end txg", Box::new(Self::end_txg));
        server.register_handler("write block", Box::new(Self::write_block));
        server.register_handler("free block", Box::new(Self::free_block));
        server.register_handler("read block", Box::new(Self::read_block));
    }
}
