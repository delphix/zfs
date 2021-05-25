use anyhow::{Context, Result};
use async_stream::stream;
use bytes::Bytes;
use core::time::Duration;
use futures::{Future, StreamExt};
use http::StatusCode;
use lazy_static::lazy_static;
use log::*;
use lru::LruCache;
use rand::prelude::*;
use rusoto_core::{ByteStream, RusotoError};
use rusoto_s3::*;
use std::collections::HashMap;
use std::env;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::Semaphore;

struct ObjectCache {
    // XXX cache key should include Bucket
    cache: LruCache<String, Arc<Vec<u8>>>,
    reading: HashMap<String, Arc<Semaphore>>,
}

lazy_static! {
    static ref CACHE: std::sync::Mutex<ObjectCache> = std::sync::Mutex::new(ObjectCache {
        cache: LruCache::new(100),
        reading: HashMap::new(),
    });
    static ref PREFIX: String = match env::var("AWS_PREFIX") {
        Ok(val) => format!("{}/", val),
        Err(_) => "".to_string(),
    };
}

// log operations that take longer than this with info!()
const LONG_OPERATION_DURATION: Duration = Duration::from_secs(2);

#[derive(Clone)]
pub struct ObjectAccess {
    client: rusoto_s3::S3Client,
    bucket_str: String,
}

/// For testing, prefix all object keys with this string.
pub fn prefixed(key: &str) -> String {
    format!("{}{}", *PREFIX, key)
}

async fn retry<F, O, E>(msg: &str, f: impl Fn() -> F) -> Result<O, RusotoError<E>>
where
    E: core::fmt::Debug + core::fmt::Display + std::error::Error,
    F: Future<Output = (bool, Result<O, RusotoError<E>>)>,
{
    debug!("{}: begin", msg);
    let begin = Instant::now();
    let mut delay = Duration::from_secs_f64(thread_rng().gen_range(0.001..0.2));
    let result = loop {
        match f().await {
            (true, Err(e)) => {
                // XXX why can't we use {} with `e`?  lifetime error???
                debug!(
                    "{} returned: {:?}; retrying in {}ms",
                    msg,
                    e,
                    delay.as_millis()
                );
                if delay > LONG_OPERATION_DURATION {
                    info!(
                        "long retry: {} returned: {:?}; retrying in {:?}",
                        msg, e, delay
                    );
                }
            }
            (_, res) => {
                break res;
            }
        }
        tokio::time::sleep(delay).await;
        delay = delay.mul_f64(thread_rng().gen_range(1.5..2.5));
    };
    let elapsed = begin.elapsed();
    debug!("{}: returned in {}ms", msg, elapsed.as_millis());
    if elapsed > LONG_OPERATION_DURATION {
        info!("long completion: {}: returned in {:?}", msg, elapsed);
    }
    result
}

impl ObjectAccess {
    pub fn get_client(endpoint: &str, region_str: &str, creds: &str) -> S3Client {
        info!("region: {:?}", region_str);
        info!("Endpoint: {}", endpoint);

        let mut iter = creds.split(':');
        let access_key_id = iter.next().unwrap().trim();
        let secret_access_key = iter.next().unwrap().trim();

        let http_client = rusoto_core::HttpClient::new().unwrap();
        let creds = rusoto_core::credential::StaticProvider::new(
            access_key_id.to_string(),
            secret_access_key.to_string(),
            None,
            None,
        );
        rusoto_s3::S3Client::new_with(http_client, creds, rusoto_core::Region::UsWest2)
    }

    pub fn from_client(client: rusoto_s3::S3Client, bucket: &str) -> Self {
        ObjectAccess {
            client,
            bucket_str: bucket.to_string(),
        }
    }

    pub fn new(endpoint: &str, region_str: &str, bucket: &str, creds: &str) -> Self {
        let client = ObjectAccess::get_client(endpoint, region_str, creds);

        ObjectAccess {
            client,
            bucket_str: bucket.to_string(),
        }
    }

    pub fn release_client(self) -> S3Client {
        self.client
    }

    async fn get_object_impl(&self, key: &str) -> Result<Vec<u8>> {
        let msg = format!("get {}", prefixed(key));
        let output = retry(&msg, || async {
            let req = GetObjectRequest {
                bucket: self.bucket_str.clone(),
                key: prefixed(key),
                ..Default::default()
            };
            let res = self.client.get_object(req).await;
            match res {
                Ok(_) => (true, res),
                Err(RusotoError::Service(GetObjectError::NoSuchKey(_))) => (false, res),
                Err(_) => (true, res),
            }
        })
        .await
        .with_context(|| format!("Failed to {}", msg))?;
        let begin = Instant::now();
        let mut v = match output.content_length {
            None => Vec::new(),
            Some(len) => Vec::with_capacity(len as usize),
        };
        output
            .body
            .unwrap() // XXX could the connection die while we're reading the bytestream?
            .for_each(|x| {
                let b = x.unwrap();
                v.extend_from_slice(&b);
                async {}
            })
            .await;
        debug!(
            "{}: got {} bytes of data in additional {}ms",
            msg,
            v.len(),
            begin.elapsed().as_millis()
        );

        Ok(v)
    }

    pub async fn get_object(&self, key: &str) -> Result<Arc<Vec<u8>>> {
        // XXX restructure so that this block "returns" an async func that does the
        // 2nd half?
        loop {
            let mysem;
            let reader;
            // need this block separate so that we can drop the mutex before the .await
            // note: the compiler doesn't realize that drop(c) actually drops it
            {
                let mut c = CACHE.lock().unwrap();
                let mykey = key.to_string();
                match c.cache.get(&mykey) {
                    Some(v) => {
                        debug!("found {} in cache", key);
                        return Ok(v.clone());
                    }
                    None => match c.reading.get(key) {
                        None => {
                            mysem = Arc::new(Semaphore::new(0));
                            c.reading.insert(mykey, mysem.clone());
                            reader = true;
                        }
                        Some(sem) => {
                            debug!("found {} read in progress", key);
                            mysem = sem.clone();
                            reader = false;
                        }
                    },
                }
            }
            if reader {
                let v = Arc::new(self.get_object_impl(key).await?);
                let mut c = CACHE.lock().unwrap();
                mysem.close();
                c.cache.put(key.to_string(), v.clone());
                c.reading.remove(key);
                return Ok(v);
            } else {
                let res = mysem.acquire().await;
                assert!(res.is_err());
                // XXX restructure so that new value is sent via tokio::sync::watch,
                // so we don't have to look up in the cache again?  although this
                // case is uncommon
            }
        }
    }

    pub async fn list_objects(
        &self,
        prefix: &str,
        delimiter: Option<String>,
        start_after: Option<String>,
    ) -> Vec<ListObjectsV2Output> {
        let full_prefix = prefixed(prefix);
        let full_start_after = match start_after {
            Some(sa) => Some(prefixed(&sa)),
            None => None,
        };
        let mut results = Vec::new();
        let mut continuation_token = None;
        loop {
            continuation_token = match retry(
                &format!(
                    "list {} (delim {:?}, after {:?})",
                    full_prefix, delimiter, full_start_after
                ),
                || async {
                    let req = ListObjectsV2Request {
                        bucket: self.bucket_str.clone(),
                        continuation_token: continuation_token.clone(),
                        delimiter: delimiter.clone(),
                        fetch_owner: Some(false),
                        prefix: Some(full_prefix.clone()),
                        start_after: full_start_after.clone(),
                        ..Default::default()
                    };
                    (true, self.client.list_objects_v2(req).await)
                },
            )
            .await
            .unwrap()
            {
                #[rustfmt::skip]
                output @ ListObjectsV2Output {next_continuation_token: Some(_), ..} => {
                    let next_token = output.next_continuation_token.clone();
                    results.push(output);
                    next_token
                }
                output => {
                    results.push(output);
                    break;
                }
            }
        }
        results
    }

    pub async fn object_exists(&self, key: &str) -> bool {
        let res = retry(&format!("head {}", prefixed(key)), || async {
            let req = HeadObjectRequest {
                bucket: self.bucket_str.clone(),
                key: prefixed(key),
                ..Default::default()
            };
            let res = self.client.head_object(req).await;
            match res {
                Err(RusotoError::Service(HeadObjectError::NoSuchKey(_))) => (false, res),
                Err(RusotoError::Unknown(rusoto_core::request::BufferedHttpResponse {
                    status: StatusCode::NOT_FOUND,
                    body: _,
                    headers: _,
                })) => (false, res),
                _ => (true, res),
            }
        })
        .await;
        res.err() == None
    }

    async fn put_object_impl(&self, key: &str, data: Vec<u8>) {
        let len = data.len();
        let a = Arc::new(Bytes::from(data));
        retry(
            &format!("put {} ({} bytes)", prefixed(key), len),
            || async {
                let my_b = (*a).clone();
                let stream = ByteStream::new_with_size(stream! { yield Ok(my_b)}, len);

                let req = PutObjectRequest {
                    bucket: self.bucket_str.clone(),
                    key: prefixed(key),
                    body: Some(stream),
                    ..Default::default()
                };
                (true, self.client.put_object(req).await)
            },
        )
        .await
        .unwrap();
    }

    pub async fn put_object(&self, key: &str, data: Vec<u8>) {
        {
            // invalidate cache.  don't hold lock across .await below
            let mut c = CACHE.lock().unwrap();
            let mykey = key.to_string();
            if c.cache.contains(&mykey) {
                debug!("found {} in cache when putting - invalidating", key);
                // XXX unfortuate to be copying; this happens every time when
                // freeing (we get/modify/put the object)
                c.cache.put(mykey, Arc::new(data.clone()));
            }
        }

        self.put_object_impl(key, data).await;
    }

    pub async fn delete_object(&self, key: &str) {
        self.delete_objects(vec![key.to_string()]).await;
    }

    // return if retry needed
    // XXX unfortunate that this level of retry can't be handled by retry()
    // XXX ideally we would only retry the failed deletions, not all of them
    async fn delete_objects_impl(&self, keys: &Vec<String>) -> bool {
        let msg = format!(
            "delete {} objects including {}",
            keys.len(),
            prefixed(&keys[0])
        );
        let output = retry(&msg, || async {
            let v: Vec<_> = keys
                .iter()
                .map(|x| ObjectIdentifier {
                    key: prefixed(x),
                    ..Default::default()
                })
                .collect();
            let req = DeleteObjectsRequest {
                bucket: self.bucket_str.clone(),
                delete: Delete {
                    objects: v,
                    quiet: Some(true),
                },
                ..Default::default()
            };
            (true, self.client.delete_objects(req).await)
        })
        .await
        .unwrap();
        if let Some(errs) = output.errors {
            if !errs.is_empty() {}
            for err in errs {
                debug!("delete: error from s3; retrying in 100ms: {:?}", err);
            }

            tokio::time::sleep(Duration::from_millis(100)).await;
            return true;
        }
        return false;
    }

    // XXX just have it take ObjectIdentifiers? but they would need to be
    // prefixed already, to avoid creating new ObjectIdentifiers
    // Note: AWS supports up to 1000 keys per delete_objects request.
    // XXX should we split them up here?
    pub async fn delete_objects(&self, keys: Vec<String>) {
        while self.delete_objects_impl(&keys).await {}
    }
}
