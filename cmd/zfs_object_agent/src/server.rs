use crate::pool::*;
use nvpair::NvData;
use nvpair::NvEncoding;
use nvpair::NvList;
use s3::bucket::Bucket;
use s3::creds::Credentials;
use s3::Region;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::unix::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::UnixStream;
use tokio::sync::Mutex;

pub struct Server {
    input: OwnedReadHalf,
    output: Arc<tokio::sync::Mutex<OwnedWriteHalf>>,
    pools: Arc<std::sync::Mutex<HashMap<PoolGUID, Pool>>>,
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
            pools: Arc::new(std::sync::Mutex::new(HashMap::new())),
        };
        tokio::spawn(async move {
            loop {
                let result = Self::get_next_request(&mut server.input).await;
                if result.is_err() {
                    println!("got error reading from connection: {:?}", result);
                    return;
                }
                let nvl = result.unwrap();
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
                        server.create_pool(&bucket, guid, name.to_str().unwrap());
                    }
                    "open pool" => {
                        let guid = PoolGUID(nvl.lookup_uint64("GUID").unwrap());
                        let bucket_name = nvl.lookup_string("bucket").unwrap();
                        let region = Region::UsWest2;
                        let credentials = Credentials::default().unwrap();
                        let bucket =
                            Bucket::new(bucket_name.to_str().unwrap(), region, credentials)
                                .unwrap();
                        server.open_pool(&bucket, guid);
                    }
                    "begin txg" => {
                        let guid = PoolGUID(nvl.lookup_uint64("GUID").unwrap());
                        let txg = TXG(nvl.lookup_uint64("TXG").unwrap());
                        server.begin_txg(guid, txg);
                    }
                    "flush writes" => {
                        let guid = PoolGUID(nvl.lookup_uint64("GUID").unwrap());
                        server.flush_writes(guid);
                    }
                    "end txg" => {
                        let guid = PoolGUID(nvl.lookup_uint64("GUID").unwrap());
                        let data = nvl.lookup("data").unwrap().data();
                        if let NvData::Uint8Array(slice) = data {
                            server.end_txg(guid, slice.to_vec());
                        } else {
                            panic!("data not expected type")
                        }
                    }
                    "write block" => {
                        let guid = PoolGUID(nvl.lookup_uint64("GUID").unwrap());
                        let block = BlockID(nvl.lookup_uint64("block").unwrap());
                        let data = nvl.lookup("data").unwrap().data();
                        if let NvData::Uint8Array(slice) = data {
                            server.write_block(guid, block, slice.to_vec());
                        } else {
                            panic!("data not expected type")
                        }
                    }
                    "free block" => {
                        let guid = PoolGUID(nvl.lookup_uint64("GUID").unwrap());
                        let block = BlockID(nvl.lookup_uint64("block").unwrap());
                        server.free_block(guid, block);
                    }
                    "read block" => {
                        let guid = PoolGUID(nvl.lookup_uint64("GUID").unwrap());
                        let block = BlockID(nvl.lookup_uint64("block").unwrap());
                        let id = nvl.lookup_uint64("request_id").unwrap();
                        server.read_block(guid, block, id);
                    }
                    _ => panic!("bad type {:?}", type_name),
                }
            }
        });
    }

    async fn send_response(output: Arc<Mutex<OwnedWriteHalf>>, nvl: NvList) {
        println!("sending response: {:?}", nvl);
        let buf = nvl.pack(NvEncoding::Native).unwrap();
        drop(nvl);
        let len64 = buf.len() as u64;
        let mut w = output.lock().await;
        // XXX kernel expects this as host byte order
        w.write_u64_le(len64).await.unwrap();
        w.write(buf.as_slice()).await.unwrap();
    }

    fn create_pool(&mut self, bucket: &Bucket, guid: PoolGUID, name: &str) {
        let mybuck = bucket.clone();
        let output = self.output.clone();
        let my_name = name.to_owned();
        tokio::spawn(async move {
            Pool::create(&mybuck, &my_name, guid).await;
            let mut nvl = NvList::new_unique_names();
            nvl.insert("Type", "pool create done").unwrap();
            nvl.insert("GUID", &guid.0).unwrap();
            Self::send_response(output, nvl).await;
        });
    }

    /// initiate pool opening.  Responds when pool is open
    fn open_pool(&mut self, bucket: &Bucket, guid: PoolGUID) {
        let mybuck = bucket.clone();
        let pools = self.pools.clone();
        let output = self.output.clone();
        tokio::spawn(async move {
            let pool = Pool::open(&mybuck, guid).await;
            let last_txg = pool.state.last_txg;
            let next_block = pool.next_block();
            pools.lock().unwrap().insert(guid, pool);
            let mut nvl = NvList::new_unique_names();
            nvl.insert("Type", "pool open done").unwrap();
            nvl.insert("GUID", &guid.0).unwrap();
            nvl.insert("next txg", &(last_txg.0 + 1)).unwrap();
            nvl.insert("next block", &next_block.0).unwrap();
            Self::send_response(output, nvl).await;
        });
    }

    // no response
    fn begin_txg(&mut self, guid: PoolGUID, txg: TXG) {
        let mut pools = self.pools.lock().unwrap();
        let pool = pools.get_mut(&guid).unwrap();
        pool.begin_txg(txg);
    }

    // no response
    fn flush_writes(&mut self, guid: PoolGUID) {
        let mut pools = self.pools.lock().unwrap();
        let pool = pools.get_mut(&guid).unwrap();
        pool.initiate_flush_object();
    }

    // sends response when completed
    fn end_txg(&mut self, guid: PoolGUID, uberblock: Vec<u8>) {
        let mut pools = self.pools.lock().unwrap();
        let pool = pools.get_mut(&guid).unwrap();
        let output = self.output.clone();
        pool.end_txg_cb(uberblock, async move {
            let mut nvl = NvList::new_unique_names();
            nvl.insert("Type", "end txg done").unwrap();
            nvl.insert("GUID", &guid.0).unwrap();
            Self::send_response(output, nvl).await;
        });
    }

    /// queue write, sends response when completed (persistent).  Does not block.
    /// completion may not happen until flush_pool() is called
    fn write_block(&mut self, guid: PoolGUID, block: BlockID, data: Vec<u8>) {
        let mut pools = self.pools.lock().unwrap();
        let pool = pools.get_mut(&guid).unwrap();
        let arc = self.output.clone();
        pool.write_block_cb(block, data, async move {
            let mut nvl = NvList::new_unique_names();
            nvl.insert("Type", "write done").unwrap();
            nvl.insert("GUID", &guid.0).unwrap();
            nvl.insert("BlockID", &block.0).unwrap();
            Self::send_response(arc, nvl).await;
        });
    }

    /// initiate free.  No response.  Does not block.  Completes when the current txg is ended.
    fn free_block(&mut self, guid: PoolGUID, block: BlockID) {
        let mut pools = self.pools.lock().unwrap();
        let pool = pools.get_mut(&guid).unwrap();
        pool.free_block(block);
    }

    /// initiate read, sends response when completed.  Does not block.
    fn read_block(&mut self, guid: PoolGUID, block: BlockID, request_id: u64) {
        let mut pools = self.pools.lock().unwrap();
        let pool = pools.get_mut(&guid).unwrap();
        let arc = self.output.clone();
        pool.read_block_cb(block, move |data| {
            let mut nvl = NvList::new_unique_names();
            nvl.insert("Type", "read done").unwrap();
            nvl.insert("GUID", &guid.0).unwrap();
            nvl.insert("BlockID", &block.0).unwrap();
            nvl.insert("data", data.as_slice()).unwrap();
            nvl.insert("request_id", &request_id).unwrap();
            Self::send_response(arc, nvl)
        });
    }
}
