use crate::object_access;
use crate::pool::{OnDisk, PoolGUID, PoolSharedState, TXG};
use async_stream::stream;
use futures::future;
use futures::future::*;
use futures::stream::*;
use futures_core::Stream;
use s3::bucket::Bucket;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Instant;
use tokio::task::JoinHandle;

/*
 * Note: The OBLIterator returns a struct, not a reference. That way it doesn't
 * have to manage the reference lifetime.  It also means that the ObjectBasedLog
 * needs to contain a Copy/Clone type so that we can copy it to return from the
 * OBLIterator.
 */
pub trait ObjectBasedLogEntry: 'static + OnDisk + Copy + Clone + Unpin + Send + Sync {}

#[derive(Serialize, Deserialize, Debug)]
pub struct ObjectBasedLogPhys {
    generation: u64,
    num_chunks: u64,
}

#[derive(Serialize, Deserialize, Debug)]
struct ObjectBasedLogChunk<T: ObjectBasedLogEntry> {
    guid: PoolGUID,
    generation: u64,
    chunk: u64,
    txg: TXG,
    #[serde(bound(deserialize = "Vec<T>: DeserializeOwned"))]
    entries: Vec<T>,
}
impl<T: ObjectBasedLogEntry> OnDisk for ObjectBasedLogChunk<T> {}

impl<T: ObjectBasedLogEntry> ObjectBasedLogChunk<T> {
    fn key(name: &str, generation: u64, chunk: u64) -> String {
        format!("{}/{}/{}", name, generation, chunk)
    }

    async fn get(bucket: &Bucket, name: &str, generation: u64, chunk: u64) -> Self {
        let buf = object_access::get_object(bucket, &Self::key(name, generation, chunk)).await;
        let begin = Instant::now();
        let this: Self = bincode::deserialize(&buf).unwrap();
        println!(
            "deserialized {} log entries in {}ms",
            this.entries.len(),
            begin.elapsed().as_millis()
        );
        this
    }

    async fn put(&self, bucket: &Bucket, name: &str, generation: u64, chunk: u64) {
        let begin = Instant::now();
        let buf = &bincode::serialize(&self).unwrap();
        println!(
            "serialized {} log entries in {}ms",
            self.entries.len(),
            begin.elapsed().as_millis()
        );
        object_access::put_object(bucket, &Self::key(name, generation, chunk), buf).await;
    }
}

#[derive(Debug)]
pub struct ObjectBasedLog<T: ObjectBasedLogEntry> {
    pool: Arc<PoolSharedState>,
    name: String,
    generation: u64,
    num_chunks: u64,
    pending_entries: Vec<T>,
    recovered: bool,
    pending_flushes: Vec<JoinHandle<()>>,
}

impl<T: ObjectBasedLogEntry> ObjectBasedLog<T> {
    pub fn create(pool: Arc<PoolSharedState>, name: &str) -> ObjectBasedLog<T> {
        ObjectBasedLog {
            pool,
            name: name.to_string(),
            generation: 0,
            num_chunks: 0,
            recovered: true,
            pending_entries: Vec::new(),
            pending_flushes: Vec::new(),
        }
    }

    pub fn open_by_phys(
        pool: Arc<PoolSharedState>,
        name: &str,
        phys: &ObjectBasedLogPhys,
    ) -> ObjectBasedLog<T> {
        ObjectBasedLog {
            pool,
            name: name.to_string(),
            generation: phys.generation,
            num_chunks: phys.num_chunks,
            recovered: false,
            pending_entries: Vec::new(),
            pending_flushes: Vec::new(),
        }
    }

    /*
    pub fn verify_clean_shutdown(&mut self) {
        // Make sure there are no objects past the logical end of the log
        self.recovered = true;
    }
    */

    /// Recover after a system crash, where the kernel also crashed and we are discarding
    /// any changes after the current txg.
    pub async fn recover(&mut self) {
        // XXX now that we are flushing async, there could be gaps in written
        // but not needed chunkID's.  Probably want to change keys to use padded numbers so that
        // we can easily find any after the last chunk.

        // Delete any chunks past the logical end of the log
        for c in self.num_chunks.. {
            let key = &format!("{}/{}/{}", self.name, self.generation, c);
            if object_access::object_exists(&self.pool.bucket, &key).await {
                object_access::delete_object(&self.pool.bucket, &key).await;
            } else {
                break;
            }
        }

        // Delete the partially-complete generation (if present)
        for c in 0.. {
            let key = &format!("{}/{}/{}", self.name, self.generation + 1, c);
            if object_access::object_exists(&self.pool.bucket, key).await {
                object_access::delete_object(&self.pool.bucket, key).await;
            } else {
                break;
            }
        }

        // XXX verify that there are no chunks/generations past what we deleted

        self.recovered = true;
    }

    pub fn to_phys(&self) -> ObjectBasedLogPhys {
        ObjectBasedLogPhys {
            generation: self.generation,
            num_chunks: self.num_chunks,
        }
    }

    pub fn append(&mut self, txg: TXG, value: T) {
        assert!(self.recovered);
        // XXX assert that txg is the same as the txg for the other pending entries?
        self.pending_entries.push(value);
        // XXX should be based on chunk size (bytes)
        if self.pending_entries.len() > 1000 {
            self.initiate_flush(txg);
        }
    }

    pub fn initiate_flush(&mut self, txg: TXG) {
        assert!(self.recovered);

        let chunk = ObjectBasedLogChunk {
            guid: self.pool.guid,
            txg,
            generation: self.generation,
            chunk: self.num_chunks,
            entries: self.pending_entries.split_off(0),
        };

        // XXX cloning name, would be nice if we could find a way to
        // reference them from the spawned task (use Arc)
        let pool = self.pool.clone();
        let name = self.name.clone();
        let generation = self.generation;
        let num_chunks = self.num_chunks;
        let handle = tokio::spawn(async move {
            chunk.put(&pool.bucket, &name, generation, num_chunks).await;
        });
        self.pending_flushes.push(handle);

        assert!(self.pending_entries.is_empty());
        self.num_chunks += 1;
    }

    pub async fn flush(&mut self, txg: TXG) {
        if !self.pending_entries.is_empty() {
            self.initiate_flush(txg);
        }
        let wait_for = self.pending_flushes.split_off(0);
        let join_result = join_all(wait_for).await;
        for r in join_result {
            r.unwrap();
        }
    }

    pub async fn clear(&mut self, txg: TXG) {
        self.flush(txg).await;
        self.generation += 1;
        self.num_chunks = 0;
    }

    /*
    pub async fn read_serial(&self) -> Vec<T> {
        let mut entries = Vec::new();
        for chunk in 0..self.num_chunks {
            let mut chunk =
                ObjectBasedLogChunk::get(&self.bucket, &self.name, self.generation, chunk).await;
            let begin = Instant::now();
            entries.append(&mut chunk.entries);
            println!("appended entries in {}ms", begin.elapsed().as_millis());
        }
        println!("got {} entries total", entries.len());
        entries
    }
    */

    pub async fn read(&self) -> Vec<T> {
        let mut stream = FuturesOrdered::new();
        for chunk in 0..self.num_chunks {
            let fut =
                ObjectBasedLogChunk::get(&self.pool.bucket, &self.name, self.generation, chunk);
            stream.push(fut);
        }
        let mut entries = Vec::new();
        // XXX may need to use stream.buffered() so we don't have too many outstanding fd's / connections
        // XXX or retire this in favor of the iterate() interface
        stream
            .for_each(|mut chunk| {
                println!(
                    "appending {} entries of chunk {}",
                    chunk.entries.len(),
                    chunk.chunk
                );
                entries.append(&mut chunk.entries);
                future::ready(())
            })
            .await;

        println!("got {} entries total", entries.len());
        entries
    }

    pub fn iterate(&self) -> impl Stream<Item = T> {
        let mut stream = FuturesOrdered::new();
        let generation = self.generation;
        for chunk in 0..self.num_chunks {
            let pool = self.pool.clone();
            let n = self.name.clone();
            let fut = async move {
                async move { ObjectBasedLogChunk::get(&pool.bucket, &n, generation, chunk).await }
            };
            stream.push(fut);
        }
        // Note: buffered() is needed because rust-s3 creates one connection for
        // each request, rather than using a connection pool. If we created 1000
        // connections we'd run into the open file descriptor limit.
        let mut buffered_stream = stream.buffered(50);
        stream! {
            while let Some(fut) = buffered_stream.next().await {
                let chunk = fut;
                println!("yielding entries of chunk {}", chunk.chunk);
                for ent in chunk.entries {
                    yield ent;
                }
            }
        }
    }
}
