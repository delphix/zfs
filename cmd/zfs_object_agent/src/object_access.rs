use s3::bucket::Bucket;
use std::env;
use std::fs;
use std::time::Instant;

/// For testing, prefix all object keys with this string.
/// Should be something like "username-vmname"
pub fn prefixed(key: &str) -> String {
    let prefix = match env::var("AWS_PREFIX") {
        Ok(val) => val,
        Err(_) => {
            let raw_id = fs::read_to_string("/etc/machine-id").unwrap_or(env::var("USER").unwrap());
            raw_id[..std::cmp::min(10, raw_id.len())].to_string()
        }
    };
    format!("{}/{}", prefix, key)
}

pub async fn get_object(bucket: &Bucket, key: &str) -> Vec<u8> {
    let prefixed_key = prefixed(key);
    println!("getting {}", prefixed_key);
    let begin = Instant::now();
    let (data, code) = bucket.get_object(&prefixed_key).await.unwrap();

    println!(
        "{}: got {} bytes in {}ms",
        prefixed_key,
        data.len(),
        begin.elapsed().as_millis()
    );
    assert_eq!(code, 200);
    data
}

pub async fn put_object(bucket: &Bucket, key: &str, data: &[u8]) {
    let prefixed_key = prefixed(key);
    println!("putting {}", prefixed_key);
    let begin = Instant::now();
    let (_, code) = bucket.put_object(&prefixed_key, data).await.unwrap();
    println!(
        "{}: put {} bytes in {}ms",
        prefixed_key,
        data.len(),
        begin.elapsed().as_millis()
    );
    assert_eq!(code, 200);
}

pub async fn delete_object(bucket: &Bucket, key: &str) {
    let prefixed_key = prefixed(key);
    println!("deleting {}", prefixed_key);
    let begin = Instant::now();
    let (_, code) = bucket.delete_object(&prefixed_key).await.unwrap();
    println!(
        "{}: deletion got code={} in {}ms",
        prefixed_key,
        code,
        begin.elapsed().as_millis()
    );
    assert_eq!(code, 204);
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
