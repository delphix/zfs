use anyhow::Result;
use core::time::Duration;
use futures::Future;
use lazy_static::lazy_static;
use lru::LruCache;
use s3::bucket::Bucket;
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

/// For testing, prefix all object keys with this string.
pub fn prefixed(key: &str) -> String {
    format!("{}{}", *PREFIX, key)
}

async fn retry<Fut>(msg: &str, f: impl Fn() -> Fut) -> Vec<u8>
where
    Fut: Future<Output = Result<(Vec<u8>, u16)>>,
{
    println!("{}: begin", msg);
    let begin = Instant::now();
    let mut delay = Duration::from_millis(100);
    let data = loop {
        match f().await {
            Err(e) => {
                println!("{} returned: {}; retrying in {:?}", msg, e, delay);
            }
            Ok((mydata, code)) => {
                if code >= 200 && code < 300 {
                    break mydata;
                } else if code >= 500 && code < 600 {
                    println!(
                        "{}: returned http code {}; retrying in {:?}",
                        msg, code, delay
                    );
                } else {
                    panic!(
                        "{}: returned invalid http code {}; response: {}",
                        msg,
                        code,
                        String::from_utf8(mydata).unwrap_or("<Invalid UTF8>".to_string())
                    );
                }
            }
        }
        tokio::time::sleep(delay).await;
        delay *= 2;
    };
    println!(
        "{}: returned {} bytes in {}ms",
        msg,
        data.len(),
        begin.elapsed().as_millis()
    );
    data
}

async fn get_object_impl(bucket: &Bucket, key: &str) -> Vec<u8> {
    let prefixed_key = &prefixed(key);
    retry(&format!("get {}", prefixed_key), || async {
        bucket.get_object(prefixed_key).await
    })
    .await
}

pub async fn get_object(bucket: &Bucket, key: &str) -> Arc<Vec<u8>> {
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
            let v = Arc::new(get_object_impl(bucket, key).await);
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
    bucket: &Bucket,
    prefix: &str,
    delimiter: Option<String>,
) -> Vec<s3::serde_types::ListBucketResult> {
    let full_prefix = prefixed(prefix);
    bucket.list(full_prefix, delimiter).await.unwrap()
}

// XXX update to take the actual Vec so that we can cache it?  Although that
// should be the uncommon case.
pub async fn put_object(bucket: &Bucket, key: &str, data: &[u8]) {
    {
        // invalidate cache.  don't hold lock across .await below
        let mut c = CACHE.lock().unwrap();
        let mykey = key.to_string();
        if c.cache.contains(&mykey) {
            println!("found {} in cache when putting - invalidating", key);
            c.cache.put(mykey, Arc::new(data.to_vec()));
        }
    }

    let prefixed_key = &prefixed(key);
    retry(
        &format!("put {} ({} bytes)", prefixed_key, data.len()),
        || async { bucket.put_object(prefixed_key, data).await },
    )
    .await;
}

pub async fn delete_object(bucket: &Bucket, key: &str) {
    let prefixed_key = &prefixed(key);
    retry(&format!("delete {}", prefixed_key), || async {
        bucket.delete_object(prefixed_key).await
    })
    .await;
}

pub async fn object_exists(bucket: &Bucket, key: &str) -> bool {
    let prefixed_key = prefixed(key);
    println!("looking for {}", prefixed_key);
    let begin = Instant::now();
    match bucket.list(prefixed_key, None).await {
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
