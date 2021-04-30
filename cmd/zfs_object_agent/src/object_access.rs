use core::time::Duration;
use lazy_static::lazy_static;
use lru::LruCache;
use s3::bucket::Bucket;
use std::collections::HashMap;
use std::env;
use std::fs;
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
}

/// For testing, prefix all object keys with this string.
/// Should be something like "username-vmname"
pub fn prefixed(key: &str) -> String {
    let prefix = match env::var("AWS_PREFIX") {
        Ok(val) => val,
        Err(_) => {
            let raw_id = match fs::read_to_string("/etc/machine-id") {
                Ok(machineid) => machineid,
                Err(_) => env::var("USER").expect(
                    "environment variable AWS_PREFIX or USER must be set \
                    or file /etc/machine-id should be present",
                ),
            };
            raw_id[..std::cmp::min(10, raw_id.len())].to_string()
        }
    };
    format!("{}/{}", prefix, key)
}

async fn get_object_impl(bucket: &Bucket, key: &str) -> Vec<u8> {
    let prefixed_key = prefixed(key);
    println!("getting {}", prefixed_key);
    let begin = Instant::now();
    let data = loop {
        let (mydata, code) = bucket.get_object(&prefixed_key).await.unwrap();
        if code == 200 {
            break mydata;
        } else if code >= 500 && code < 600 {
            println!(
                "{}: get returned http code {}; retrying in 1 second",
                prefixed_key, code
            );
            tokio::time::sleep(Duration::from_millis(1000)).await;
            continue;
        } else {
            panic!(
                "{}: get returned invalid http code {} while getting",
                prefixed_key, code
            );
        }
    };

    println!(
        "{}: got {} bytes in {}ms",
        prefixed_key,
        data.len(),
        begin.elapsed().as_millis()
    );
    data
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

// XXX update to take the actual Vec so that we can cache it?  Although that
// should be the uncommon case.
pub async fn put_object(bucket: &Bucket, key: &str, data: &[u8]) {
    let prefixed_key = prefixed(key);
    println!("putting {}", prefixed_key);

    // invalidate cache
    {
        let mut c = CACHE.lock().unwrap();
        let mykey = key.to_string();
        if c.cache.contains(&mykey) {
            println!("found {} in cache when putting - invalidating", key);
            c.cache.put(mykey, Arc::new(data.to_vec()));
        }
    }

    let begin = Instant::now();
    loop {
        let (_, code) = bucket.put_object(&prefixed_key, data).await.unwrap();
        if code == 200 {
            break;
        } else if code >= 500 && code < 600 {
            println!(
                "{}: put returned http code {}; retrying in 1 second",
                prefixed_key, code
            );
            tokio::time::sleep(Duration::from_millis(1000)).await;
            continue;
        } else {
            panic!("{}: put returned invalid http code {}", prefixed_key, code);
        }
    }

    println!(
        "{}: put {} bytes in {}ms",
        prefixed_key,
        data.len(),
        begin.elapsed().as_millis()
    );
}

pub async fn delete_object(bucket: &Bucket, key: &str) {
    let prefixed_key = prefixed(key);
    println!("deleting {}", prefixed_key);
    let begin = Instant::now();
    loop {
        let (_, code) = bucket.delete_object(&prefixed_key).await.unwrap();
        if code == 200 {
            break;
        } else if code >= 500 && code < 600 {
            println!(
                "{}: put returned http code {}; retrying in 1 second",
                prefixed_key, code
            );
            tokio::time::sleep(Duration::from_millis(1000)).await;
            continue;
        } else {
            panic!("{}: put returned invalid http code {}", prefixed_key, code);
        }
    }
    println!(
        "{}: deleted in {}ms",
        prefixed_key,
        begin.elapsed().as_millis()
    );
}

pub async fn object_exists(bucket: &Bucket, key: &str) -> bool {
    let prefixed_key = prefixed(key);
    println!("looking for {}", prefixed_key);
    let begin = Instant::now();
    let results = bucket.list(prefixed_key, None).await.unwrap();
    assert!(results.len() == 1);
    let list = &results[0];
    println!("list completed in {}ms", begin.elapsed().as_millis());
    // Note need to check if this exact name is in the results. If we are looking
    // for "x/y" and there is "x/y" and "x/yz", both will be returned.
    list.contents.iter().find(|o| o.key == key).is_some()
}
