use anyhow::Result;
use async_stream::stream;
use bytes::Bytes;
use core::time::Duration;
use futures::Future;
use lazy_static::lazy_static;
use lru::LruCache;
use rand::prelude::*;
use rusoto_core::{ByteStream, RusotoError};
use rusoto_s3::*;
use std::collections::HashMap;
use std::env;
use std::sync::Arc;
use std::time::Instant;
use tokio::{io::AsyncReadExt, sync::Semaphore};

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

#[derive(Clone)]
pub struct ObjectAccess {
    bucket: s3::Bucket,
    client: rusoto_s3::S3Client,
    bucket_str: String,
}

/// For testing, prefix all object keys with this string.
pub fn prefixed(key: &str) -> String {
    format!("{}{}", *PREFIX, key)
}

async fn retry<F, O, E>(msg: &str, f: impl Fn() -> F) -> O
where
    E: core::fmt::Debug + core::fmt::Display + std::error::Error,
    F: Future<Output = Result<O, RusotoError<E>>>,
{
    println!("{}: begin", msg);
    let begin = Instant::now();
    let mut delay = Duration::from_secs_f64(thread_rng().gen_range(0.001..0.2));
    let result = loop {
        match f().await {
            Err(e) => {
                // XXX why can't we use {} with `e`?  lifetime error???
                println!("{} returned: {:?}; retrying in {:?}", msg, e, delay);
            }
            Ok(result) => {
                break result;
            }
        }
        tokio::time::sleep(delay).await;
        delay = delay.mul_f64(thread_rng().gen_range(1.5..2.5));
    };
    println!(
        "{}: returned success in {}ms",
        msg,
        begin.elapsed().as_millis()
    );
    result
}

impl ObjectAccess {
    pub fn new(endpoint: &str, region: &str, bucket: &str, creds: &str) -> Self {
        let region = s3::Region::Custom {
            region: region.to_owned(),
            endpoint: endpoint.to_owned(),
        };
        println!("region: {:?}", region);
        println!("Endpoint: {}", region.endpoint());

        let mut iter = creds.split(":");
        let access_key_id = iter.next().unwrap().trim();
        let secret_access_key = iter.next().unwrap().trim();
        let credentials = s3::creds::Credentials::new(
            Some(access_key_id),
            Some(secret_access_key),
            None,
            None,
            None,
        )
        .unwrap();

        let http_client = rusoto_core::HttpClient::new().unwrap();
        let creds = rusoto_core::credential::StaticProvider::new(
            access_key_id.to_string(),
            secret_access_key.to_string(),
            None,
            None,
        );
        let client =
            rusoto_s3::S3Client::new_with(http_client, creds, rusoto_core::Region::UsWest2);

        ObjectAccess {
            bucket: s3::Bucket::new(bucket, region, credentials).unwrap(),
            client,
            bucket_str: bucket.to_string(),
        }
    }

    async fn get_object_impl(&self, key: &str) -> Vec<u8> {
        let msg = format!("get {}", prefixed(key));
        let output = retry(&msg, || async {
            let req = GetObjectRequest {
                bucket: self.bucket_str.clone(),
                key: prefixed(key),
                ..Default::default()
            };
            self.client.get_object(req).await
        })
        .await;
        let begin = Instant::now();
        let mut v = Vec::new();
        output
            .body
            .unwrap()
            .into_async_read()
            .read_to_end(&mut v)
            .await
            .unwrap();
        println!(
            "{}: got {} bytes of data in additional {}ms",
            msg,
            v.len(),
            begin.elapsed().as_millis()
        );

        v
    }

    pub async fn get_object(&self, key: &str) -> Arc<Vec<u8>> {
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
                        println!("found {} in cache", key);
                        return v.clone();
                    }
                    None => match c.reading.get(key) {
                        None => {
                            mysem = Arc::new(Semaphore::new(0));
                            c.reading.insert(mykey, mysem.clone());
                            reader = true;
                        }
                        Some(sem) => {
                            println!("found {} read in progress", key);
                            mysem = sem.clone();
                            reader = false;
                        }
                    },
                }
            }
            if reader {
                let v = Arc::new(self.get_object_impl(key).await);
                let mut c = CACHE.lock().unwrap();
                mysem.close();
                c.cache.put(key.to_string(), v.clone());
                c.reading.remove(key);
                return v;
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
    ) -> Vec<s3::serde_types::ListBucketResult> {
        let full_prefix = prefixed(prefix);
        self.bucket.list(full_prefix, delimiter).await.unwrap()
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
                self.client.put_object(req).await
            },
        )
        .await;
    }

    pub async fn put_object(&self, key: &str, data: Vec<u8>) {
        {
            // invalidate cache.  don't hold lock across .await below
            let mut c = CACHE.lock().unwrap();
            let mykey = key.to_string();
            if c.cache.contains(&mykey) {
                println!("found {} in cache when putting - invalidating", key);
                c.cache.put(mykey, Arc::new(data.clone()));
            }
        }

        self.put_object_impl(key, data).await;
    }

    pub async fn delete_object(&self, key: &str) {
        let mut v = Vec::new();
        v.push(key.to_string());
        self.delete_objects(v).await;
    }

    // XXX just have it take ObjectIdentifiers? but they would need to be
    // prefixed already, to avoid creating new ObjectIdentifiers
    // Note: AWS supports up to 1000 keys per delete_objects request.
    // XXX should we split them up here?
    pub async fn delete_objects(&self, keys: Vec<String>) {
        let msg = format!(
            "delete {} objects including {}",
            keys.len(),
            prefixed(&keys[0])
        );
        let x = retry(&msg, || async {
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
            self.client.delete_objects(req).await
        })
        .await;
        if let Some(errs) = x.errors {
            for err in errs {
                panic!("error from s3: {:?}", err);
            }
        }
    }

    pub async fn object_exists(&self, key: &str) -> bool {
        let prefixed_key = prefixed(key);
        println!("looking for {}", prefixed_key);
        let begin = Instant::now();
        match self.bucket.list(prefixed_key, None).await {
            Ok(results) => {
                assert!(results.len() == 1);
                let list = &results[0];
                println!("list completed in {}ms", begin.elapsed().as_millis());
                // Note need to check if this exact name is in the results. If we are looking
                // for "x/y" and there is "x/y" and "x/yz", both will be returned.
                list.contents.iter().find(|o| o.key == key).is_some()
            }
            Err(_) => {
                return false;
            }
        }
    }
}
