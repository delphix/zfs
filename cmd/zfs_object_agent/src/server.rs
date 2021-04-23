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
use tokio::net::UnixStream;
use tokio::sync::Mutex;
use tokio::time::Duration;

pub struct Server {
    input: OwnedReadHalf,
    output: Arc<tokio::sync::Mutex<OwnedWriteHalf>>,
    // Pool is Some once we get a "open pool" request
    pool: Option<Pool>,
}

impl Server {
    async fn get_next_request(pipe: &mut OwnedReadHalf) -> tokio::io::Result<NvList> {
        // XXX kernel sends this as host byte order
        let len64 = pipe.read_u64_le().await?;
        println!("got request len: {}", len64);

        let mut v = Vec::new();
        v.resize(len64 as usize, 0);
        pipe.read_exact(v.as_mut()).await?;
        let nvl = NvList::try_unpack(v.as_ref()).unwrap();
        println!("got request: {:?}", nvl);
        Ok(nvl)
    }

    pub fn start(connection: UnixStream) {
        let (r, w) = connection.into_split();
        let mut server = Server {
            input: r,
            output: Arc::new(Mutex::new(w)),
            pool: None,
        };
        tokio::spawn(async move {
            loop {
                let req = Self::get_next_request(&mut server.input);
                let result = tokio::time::timeout(Duration::from_millis(1000), req).await;
                let nvl;
                match result {
                    Err(_) => {
                        // timed out
                        if server.pool.is_some() {
                            server.flush_writes();
                        }
                        continue;
                    }
                    Ok(real_result) => {
                        if real_result.is_err() {
                            println!("got error reading from connection: {:?}", real_result);
                            return;
                        }
                        nvl = real_result.unwrap();
                    }
                }
                //let nvl = result.unwrap();
                let type_name = nvl.lookup_string("Type").unwrap();
                match type_name.to_str().unwrap() {
                    "create pool" => {
                        let guid = PoolGUID(nvl.lookup_uint64("GUID").unwrap());
                        let name = nvl.lookup_string("name").unwrap();
                        let bucket_name = nvl.lookup_string("bucket").unwrap();
                        let region = Region::UsWest2;
                        let credentials = Credentials::default().unwrap();
                        let bucket =
                            Bucket::new(bucket_name.to_str().unwrap(), region, credentials)
                                .unwrap();
                        server
                            .create_pool(&bucket, guid, name.to_str().unwrap())
                            .await;
                    }
                    "open pool" => {
                        let guid = PoolGUID(nvl.lookup_uint64("GUID").unwrap());
                        let bucket_name = nvl.lookup_string("bucket").unwrap();
                        let region = Region::UsWest2;
                        let credentials = Credentials::default().unwrap();
                        let bucket =
                            Bucket::new(bucket_name.to_str().unwrap(), region, credentials)
                                .unwrap();
                        server.open_pool(&bucket, guid).await;
                    }
                    "begin txg" => {
                        let txg = TXG(nvl.lookup_uint64("TXG").unwrap());
                        server.begin_txg(txg);
                    }
                    "flush writes" => {
                        server.flush_writes();
                    }
                    "end txg" => {
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
                            server.write_block(block, slice.to_vec(), id);
                        } else {
                            panic!("data not expected type")
                        }
                    }
                    "free block" => {
                        let block = BlockID(nvl.lookup_uint64("block").unwrap());
                        server.free_block(block);
                    }
                    "read block" => {
                        let block = BlockID(nvl.lookup_uint64("block").unwrap());
                        let id = nvl.lookup_uint64("request_id").unwrap();
                        server.read_block(block, id);
                    }
                    _ => panic!("bad type {:?}", type_name),
                }
            }
        });
    }

    async fn send_response(output: &Mutex<OwnedWriteHalf>, nvl: NvList) {
        println!("sending response: {:?}", nvl);
        let buf = nvl.pack(NvEncoding::Native).unwrap();
        drop(nvl);
        let len64 = buf.len() as u64;
        let mut w = output.lock().await;
        // XXX kernel expects this as host byte order
        w.write_u64_le(len64).await.unwrap();
        w.write(buf.as_slice()).await.unwrap();
    }

    async fn create_pool(&mut self, bucket: &Bucket, guid: PoolGUID, name: &str) {
        Pool::create(bucket, name, guid).await;
        let mut nvl = NvList::new_unique_names();
        nvl.insert("Type", "pool create done").unwrap();
        nvl.insert("GUID", &guid.0).unwrap();
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
            Self::send_response(&output, nvl).await;
        });
    }

    /// queue write, sends response when completed (persistent).  Does not block.
    /// completion may not happen until flush_pool() is called
    fn write_block(&mut self, block: BlockID, data: Vec<u8>, request_id: u64) {
        let pool = self.pool.as_mut().unwrap();
        let output = self.output.clone();
        pool.write_block_cb(block, data, async move {
            let mut nvl = NvList::new_unique_names();
            nvl.insert("Type", "write done").unwrap();
            nvl.insert("BlockID", &block.0).unwrap();
            nvl.insert("request_id", &request_id).unwrap();
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
            nvl.insert("BlockID", &block.0).unwrap();
            nvl.insert("data", data.as_slice()).unwrap();
            nvl.insert("request_id", &request_id).unwrap();
            Self::send_response(&output, nvl).await;
        });
    }
}
