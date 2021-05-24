use crate::base_types::*;
use crate::object_access::ObjectAccess;
use crate::pool::*;
use log::*;
use nvpair::{NvData, NvEncoding, NvList};
use rusoto_s3::S3;
use std::{cmp::max, sync::Arc};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::unix::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::UnixListener;
use tokio::net::UnixStream;
use tokio::sync::Mutex;
use tokio::time::Duration;

pub struct Server {
    input: OwnedReadHalf,
    output: Arc<tokio::sync::Mutex<OwnedWriteHalf>>,
    // Pool is Some once we get a "open pool" request
    pool: Option<Arc<Pool>>,
    num_outstanding_writes: Arc<std::sync::Mutex<usize>>,
    // XXX make Option?
    max_blockid: BlockID, // Maximum blockID that we've received a write for
}

impl Server {
    async fn get_next_request(pipe: &mut OwnedReadHalf) -> tokio::io::Result<NvList> {
        // XXX kernel sends this as host byte order
        let len64 = pipe.read_u64_le().await?;
        //println!("got request len: {}", len64);
        if len64 > 20_000_000 {
            // max zfs block size is 16MB
            panic!("got unreasonable request length {} ({:#x})", len64, len64);
        }

        let mut v = Vec::new();
        // XXX would be nice if we didn't have to zero it out
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
            num_outstanding_writes: Arc::new(std::sync::Mutex::new(0)),
            max_blockid: BlockID(0),
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

    pub fn start(connection: UnixStream) {
        let (r, w) = connection.into_split();
        let mut server = Server {
            input: r,
            output: Arc::new(Mutex::new(w)),
            pool: None,
            num_outstanding_writes: Arc::new(std::sync::Mutex::new(0)),
            max_blockid: BlockID(0),
        };
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
                            && *server.num_outstanding_writes.lock().unwrap() > 0
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
                        let guid = PoolGUID(nvl.lookup_uint64("GUID").unwrap());
                        let name = nvl.lookup_string("name").unwrap();
                        let object_access = Self::get_object_access(nvl.as_ref());
                        server
                            .create_pool(&object_access, guid, name.to_str().unwrap())
                            .await;
                    }
                    "open pool" => {
                        // XXX nvl includes credentials; need to redact?
                        info!("got request: {:?}", nvl);
                        let guid = PoolGUID(nvl.lookup_uint64("GUID").unwrap());
                        let object_access = Self::get_object_access(nvl.as_ref());
                        server.open_pool(&object_access, guid).await;
                    }
                    "begin txg" => {
                        debug!("got request: {:?}", nvl);
                        let txg = TXG(nvl.lookup_uint64("TXG").unwrap());
                        server.begin_txg(txg);
                    }
                    "resume txg" => {
                        debug!("got request: {:?}", nvl);
                        let txg = TXG(nvl.lookup_uint64("TXG").unwrap());
                        server.resume_txg(txg);
                    }
                    "resume complete" => {
                        debug!("got request: {:?}", nvl);
                        server.resume_complete().await;
                    }
                    "flush writes" => {
                        trace!("got request: {:?}", nvl);
                        server.flush_writes();
                    }
                    "get props" => {
                        debug!("got request: {:?}", nvl);
                        let props = nvl.lookup_nvlist("props").unwrap();
                        server.get_props(props);
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
                        let block = BlockID(nvl.lookup_uint64("block").unwrap());
                        let data = nvl.lookup("data").unwrap().data();
                        let id = nvl.lookup_uint64("request_id").unwrap();
                        if let NvData::Uint8Array(slice) = data {
                            trace!(
                                "got write request id={}: {:?} len={}",
                                id,
                                block,
                                slice.len()
                            );
                            server.write_block(block, slice.to_vec(), id);
                        } else {
                            panic!("data not expected type")
                        }
                    }
                    "free block" => {
                        trace!("got request: {:?}", nvl);
                        let block = BlockID(nvl.lookup_uint64("block").unwrap());
                        let size = nvl.lookup_uint64("size").unwrap();
                        server.free_block(block, size as u32);
                    }
                    "read block" => {
                        trace!("got request: {:?}", nvl);
                        let block = BlockID(nvl.lookup_uint64("block").unwrap());
                        let id = nvl.lookup_uint64("request_id").unwrap();
                        server.read_block(block, id);
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

    fn get_object_access(nvl: &nvpair::NvListRef) -> ObjectAccess {
        let bucket_name = nvl.lookup_string("bucket").unwrap();
        let region_str = nvl.lookup_string("region").unwrap();
        let endpoint = nvl.lookup_string("endpoint").unwrap();
        let credential_str = nvl.lookup_string("credentials").unwrap();

        ObjectAccess::new(
            endpoint.to_str().unwrap(),
            region_str.to_str().unwrap(),
            bucket_name.to_str().unwrap(),
            credential_str.to_str().unwrap(),
        )
    }

    async fn get_pools(&mut self, nvl: &NvList) {
        let region_str = nvl.lookup_string("region").unwrap();
        let endpoint = nvl.lookup_string("endpoint").unwrap();
        let credential_str = nvl.lookup_string("credentials").unwrap();
        let mut client = ObjectAccess::get_client(
            endpoint.to_str().unwrap(),
            region_str.to_str().unwrap(),
            credential_str.to_str().unwrap(),
        );
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
            let objs = object_access
                .list_objects("zfs/", Some("/".to_string()), None)
                .await;
            for res in objs {
                if let Some(prefixes) = res.common_prefixes {
                    for prefix in prefixes {
                        let pfx = prefix.prefix.unwrap();
                        debug!("prefix: {}", pfx);
                        let vector: Vec<&str> = pfx.rsplitn(3, '/').collect();
                        let guid_str: &str = vector[1];
                        if let Ok(guid64) = str::parse::<u64>(guid_str) {
                            let guid = PoolGUID(guid64);
                            match Pool::get_config(&object_access, guid).await {
                                Ok(pool_config) => {
                                    resp.insert(guid_str, pool_config.as_ref()).unwrap()
                                }
                                Err(e) => {
                                    error!("skipping {:?}: {:?}", guid, e);
                                }
                            }
                        }
                    }
                }
            }
            client = object_access.release_client();
        }
        debug!("sending response: {:?}", resp);
        Self::send_response(&self.output, resp).await;
    }

    async fn create_pool(&mut self, object_access: &ObjectAccess, guid: PoolGUID, name: &str) {
        Pool::create(object_access, name, guid).await;
        let mut nvl = NvList::new_unique_names();
        nvl.insert("Type", "pool create done").unwrap();
        nvl.insert("GUID", &guid.0).unwrap();
        debug!("sending response: {:?}", nvl);
        Self::send_response(&self.output, nvl).await;
    }

    /// initiate pool opening.  Responds when pool is open
    async fn open_pool(&mut self, object_access: &ObjectAccess, guid: PoolGUID) {
        let (pool, phys_opt, next_block) = Pool::open(object_access, guid).await;
        self.pool = Some(Arc::new(pool));
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
    fn begin_txg(&mut self, txg: TXG) {
        let pool = self.pool.as_ref().unwrap().clone();
        pool.begin_txg(txg);
    }

    // no response
    fn resume_txg(&mut self, txg: TXG) {
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
        tokio::spawn(async move {
            pool.initiate_flush(max_blockid).await;
        });
    }

    // sends response
    fn get_props(&self, props: NvList) {
        let pool = self.pool.as_ref().unwrap().clone();
        let mut props_vec = Vec::new();
        for p in props.iter() {
            props_vec.push(p.name().to_str().unwrap().to_owned());
        }
        let output = self.output.clone();

        tokio::spawn(async move {
            let mut response = NvList::new_unique_names();
            for n in props_vec {
                let value = pool.get_prop(n.as_ref()).await;
                response.insert(n, &value).unwrap();
            }
            debug!("sending response: {:?}", response);
            Self::send_response(&output, response).await;
        });
    }

    // sends response when completed
    fn end_txg(&mut self, uberblock: Vec<u8>, config: Vec<u8>) {
        let pool = self.pool.as_ref().unwrap().clone();
        let output = self.output.clone();
        // client should have already flushed all writes
        // XXX change to an error return
        assert_eq!(*self.num_outstanding_writes.lock().unwrap(), 0);
        tokio::spawn(async move {
            pool.end_txg(uberblock, config).await;
            let mut nvl = NvList::new_unique_names();
            nvl.insert("Type", "end txg done").unwrap();
            debug!("sending response: {:?}", nvl);
            Self::send_response(&output, nvl).await;
        });
    }

    /// queue write, sends response when completed (persistent).  Does not block.
    /// completion may not happen until flush_pool() is called
    fn write_block(&mut self, block: BlockID, data: Vec<u8>, request_id: u64) {
        self.max_blockid = max(block, self.max_blockid);
        let pool = self.pool.as_ref().unwrap().clone();
        let output = self.output.clone();
        let now = self.num_outstanding_writes.clone();
        let mut count = now.lock().unwrap();
        *count += 1;
        drop(count);
        tokio::spawn(async move {
            pool.write_block(block, data).await;
            // Note: {braces} needed so that lock is dropped before the .await
            {
                let mut count = now.lock().unwrap();
                *count -= 1;
            }
            let mut nvl = NvList::new_unique_names();
            nvl.insert("Type", "write done").unwrap();
            nvl.insert("block", &block.0).unwrap();
            nvl.insert("request_id", &request_id).unwrap();
            trace!("sending response: {:?}", nvl);
            Self::send_response(&output, nvl).await;
        });
    }

    /// initiate free.  No response.  Does not block.  Completes when the current txg is ended.
    fn free_block(&mut self, block: BlockID, size: u32) {
        let pool = self.pool.as_ref().unwrap().clone();
        tokio::spawn(async move {
            pool.free_block(block, size).await;
        });
    }

    /// initiate read, sends response when completed.  Does not block.
    fn read_block(&mut self, block: BlockID, request_id: u64) {
        let pool = self.pool.as_ref().unwrap().clone();
        let output = self.output.clone();
        tokio::spawn(async move {
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
            Self::send_response(&output, nvl).await;
        });
    }
}

fn create_listener(path: String) -> UnixListener {
    let _ = std::fs::remove_file(&path);
    info!("Listening on: {}", path);
    UnixListener::bind(&path).unwrap()
}

pub async fn do_server(socket_dir: &str) {
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

    let kjh = tokio::spawn(async move {
        loop {
            match klistener.accept().await {
                Ok((socket, _)) => {
                    info!("accepted connection on {}", ksocket_name);
                    self::Server::start(socket);
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
