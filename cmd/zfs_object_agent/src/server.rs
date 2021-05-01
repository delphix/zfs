use crate::pool::*;
use nvpair::NvData;
use nvpair::NvEncoding;
use nvpair::NvList;
use s3::bucket::Bucket;
use s3::creds::Credentials;
use s3::Region;
use std::sync::Arc;
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
    pool: Option<Pool>,
    num_outstanding_writes: Arc<std::sync::Mutex<usize>>,
}

impl Server {
    async fn get_next_request(pipe: &mut OwnedReadHalf) -> tokio::io::Result<NvList> {
        // XXX kernel sends this as host byte order
        let len64 = pipe.read_u64_le().await?;
        println!("got request len: {}", len64);
        if len64 > 20_000_000 {
            // max zfs block size is 16MB
            panic!("got unreasonable request length {} ({:#x})", len64, len64);
        }

        let mut v = Vec::new();
        v.resize(len64 as usize, 0);
        pipe.read_exact(v.as_mut()).await?;
        let nvl = NvList::try_unpack(v.as_ref()).unwrap();
        Ok(nvl)
    }

    pub fn start(connection: UnixStream) {
        let (r, w) = connection.into_split();
        let mut server = Server {
            input: r,
            output: Arc::new(Mutex::new(w)),
            pool: None,
            num_outstanding_writes: Arc::new(std::sync::Mutex::new(0)),
        };
        tokio::spawn(async move {
            loop {
                let nvl = match tokio::time::timeout(
                    Duration::from_millis(1000),
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
                            server.flush_writes();
                        }
                        continue;
                    }
                    Ok(getreq_result) => match getreq_result {
                        Err(_) => {
                            println!("got error reading from connection: {:?}", getreq_result);
                            return;
                        }
                        Ok(mynvl) => mynvl,
                    },
                };
                match nvl.lookup_string("Type").unwrap().to_str().unwrap() {
                    "create pool" => {
                        println!("got request: {:?}", nvl);
                        let guid = PoolGUID(nvl.lookup_uint64("GUID").unwrap());
                        let name = nvl.lookup_string("name").unwrap();
                        let bucket = Self::get_bucket(nvl.as_ref());
                        server
                            .create_pool(&bucket, guid, name.to_str().unwrap())
                            .await;
                    }
                    "open pool" => {
                        println!("got request: {:?}", nvl);
                        let guid = PoolGUID(nvl.lookup_uint64("GUID").unwrap());
                        let bucket = Self::get_bucket(nvl.as_ref());
                        server.open_pool(&bucket, guid).await;
                    }
                    "begin txg" => {
                        println!("got request: {:?}", nvl);
                        let txg = TXG(nvl.lookup_uint64("TXG").unwrap());
                        server.begin_txg(txg);
                    }
                    "flush writes" => {
                        println!("got request: {:?}", nvl);
                        server.flush_writes();
                    }
                    "end txg" => {
                        println!("got request: {:?}", nvl);
                        let data = nvl.lookup("data").unwrap().data();
                        if let NvData::Uint8Array(slice) = data {
                            server.end_txg(slice.to_vec());
                        } else {
                            panic!("data not expected type")
                        }
                    }
                    "write block" => {
                        let block = BlockID(nvl.lookup_uint64("block").unwrap());
                        let data = nvl.lookup("data").unwrap().data();
                        let id = nvl.lookup_uint64("request_id").unwrap();
                        if let NvData::Uint8Array(slice) = data {
                            println!(
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
                        println!("got request: {:?}", nvl);
                        let block = BlockID(nvl.lookup_uint64("block").unwrap());
                        let _size = nvl.lookup_uint64("size").unwrap();
                        server.free_block(block);
                    }
                    "read block" => {
                        println!("got request: {:?}", nvl);
                        let block = BlockID(nvl.lookup_uint64("block").unwrap());
                        let id = nvl.lookup_uint64("request_id").unwrap();
                        server.read_block(block, id);
                    }
                    other => {
                        println!("got request: {:?}", nvl);
                        panic!("bad type {:?}", other);
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
        println!("sending response of {} bytes", len64);
        w.write_u64_le(len64).await.unwrap();
        w.write(buf.as_slice()).await.unwrap();
    }

    // Construct a custom Region.
    fn get_region(region_str: &str, endpoint: &str) -> Region {
        let region = Region::Custom {
            region: region_str.to_owned(),
            endpoint: endpoint.to_owned(),
        };

        region
    }

    fn get_credentials(nvl: &nvpair::NvListRef) -> Credentials {
        // credentials should be <access-key-id>:<secret-access-key>
        let credential_str = nvl.lookup_string("credentials").unwrap();
        let mut iter = credential_str.to_str().unwrap().split(":");
        let access_key_id = iter.next().unwrap().trim();
        let secret_access_key = iter.next().unwrap().trim();

        let credentials = Credentials::new(
            Some(access_key_id),
            Some(secret_access_key),
            None,
            None,
            None,
        )
        .unwrap();

        credentials
    }

    fn get_bucket(nvl: &nvpair::NvListRef) -> s3::bucket::Bucket {
        let bucket_name = nvl.lookup_string("bucket").unwrap();
        let region_str = nvl.lookup_string("region").unwrap();
        let endpoint = nvl.lookup_string("endpoint").unwrap();
        let region: Region =
            Self::get_region(region_str.to_str().unwrap(), endpoint.to_str().unwrap());
        println!("region: {:?}", region);
        println!("Endpoint: {}", region.endpoint());

        let credentials = Self::get_credentials(nvl);

        Bucket::new(bucket_name.to_str().unwrap(), region, credentials).unwrap()
    }

    async fn create_pool(&mut self, bucket: &Bucket, guid: PoolGUID, name: &str) {
        Pool::create(bucket, name, guid).await;
        let mut nvl = NvList::new_unique_names();
        nvl.insert("Type", "pool create done").unwrap();
        nvl.insert("GUID", &guid.0).unwrap();
        println!("sending response: {:?}", nvl);
        Self::send_response(&self.output, nvl).await;
    }

    /// initiate pool opening.  Responds when pool is open
    async fn open_pool(&mut self, bucket: &Bucket, guid: PoolGUID) {
        let (pool, last_txg, next_block) = Pool::open(bucket, guid).await;
        self.pool = Some(pool);
        let mut nvl = NvList::new_unique_names();
        nvl.insert("Type", "pool open done").unwrap();
        nvl.insert("GUID", &guid.0).unwrap();
        nvl.insert("next txg", &(last_txg.0 + 1)).unwrap();
        nvl.insert("next block", &next_block.0).unwrap();
        println!("sending response: {:?}", nvl);
        Self::send_response(&self.output, nvl).await;
    }

    // no response
    fn begin_txg(&mut self, txg: TXG) {
        self.pool.as_mut().unwrap().begin_txg(txg);
    }

    // no response
    fn flush_writes(&mut self) {
        self.pool.as_mut().unwrap().initiate_flush_object();
    }

    // sends response when completed
    fn end_txg(&mut self, uberblock: Vec<u8>) {
        let pool = self.pool.as_mut().unwrap();
        let output = self.output.clone();
        pool.end_txg_cb(uberblock, async move {
            let mut nvl = NvList::new_unique_names();
            nvl.insert("Type", "end txg done").unwrap();
            println!("sending response: {:?}", nvl);
            Self::send_response(&output, nvl).await;
        });
    }

    /// queue write, sends response when completed (persistent).  Does not block.
    /// completion may not happen until flush_pool() is called
    fn write_block(&mut self, block: BlockID, data: Vec<u8>, request_id: u64) {
        let pool = self.pool.as_mut().unwrap();
        let output = self.output.clone();
        let now = self.num_outstanding_writes.clone();
        let mut count = now.lock().unwrap();
        *count += 1;
        drop(count);
        pool.write_block_cb(block, data, async move {
            // Note: {braces} needed so that count goes away before the .await
            {
                let mut count = now.lock().unwrap();
                *count -= 1;
            }
            let mut nvl = NvList::new_unique_names();
            nvl.insert("Type", "write done").unwrap();
            nvl.insert("block", &block.0).unwrap();
            nvl.insert("request_id", &request_id).unwrap();
            println!("sending response: {:?}", nvl);
            Self::send_response(&output, nvl).await;
        });
    }

    /// initiate free.  No response.  Does not block.  Completes when the current txg is ended.
    fn free_block(&mut self, block: BlockID) {
        self.pool.as_mut().unwrap().free_block(block);
    }

    /// initiate read, sends response when completed.  Does not block.
    fn read_block(&mut self, block: BlockID, request_id: u64) {
        let pool = self.pool.as_mut().unwrap();
        let output = self.output.clone();
        pool.read_block_cb(block, move |data| async move {
            let mut nvl = NvList::new_unique_names();
            nvl.insert("Type", "read done").unwrap();
            nvl.insert("block", &block.0).unwrap();
            nvl.insert("data", data.as_slice()).unwrap();
            nvl.insert("request_id", &request_id).unwrap();
            println!(
                "sending read done response: block={} req={} data=[{} bytes]",
                block,
                request_id,
                data.len()
            );
            Self::send_response(&output, nvl).await;
        });
    }
}

pub async fn do_server() {
    let socket_name = std::env::args()
        .nth(2)
        .unwrap_or("/run/zfs_socket".to_string());

    let _ = std::fs::remove_file(&socket_name);
    let listener = UnixListener::bind(&socket_name).unwrap();
    println!("Listening on: {}", socket_name);

    loop {
        match listener.accept().await {
            Ok((socket, _)) => {
                self::Server::start(socket);
            }
            Err(e) => {
                println!("accept() failed: {}", e);
            }
        }
    }
}
