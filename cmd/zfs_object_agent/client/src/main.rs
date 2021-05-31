//use crate::object_access::ObjectAccess;
use async_recursion::async_recursion;
use chrono::prelude::*;
use chrono::DateTime;
use client::Client;
use futures::future::*;
use lazy_static::lazy_static;
use libzoa::base_types::*;
use libzoa::object_access;
use libzoa::object_access::ObjectAccess;
use libzoa::pool::*;
use nvpair::*;
use rand::prelude::*;
use rusoto_core::ByteStream;
use rusoto_s3::*;
use s3::creds::Credentials;
use s3::Region;
use s3::{bucket::Bucket, serde_types::ListBucketResult};
use std::env;
use std::error::Error;
use std::fs;
use std::fs::File;
use std::io::prelude::*;
use std::io::Read;
use std::time::{Duration, Instant};
use std::{collections::BTreeSet, i64};
use tokio::io::AsyncReadExt;
mod client;

const ENDPOINT: &str = "https://s3-us-west-2.amazonaws.com";
const REGION: &str = "us-west-2";
const BUCKET_NAME: &str = "cloudburst-data-2";
const POOL_NAME: &str = "testpool";
const POOL_GUID: u64 = 1234;
const AWS_DELETION_BATCH_SIZE: usize = 1000;

lazy_static! {
    static ref AWS_PREFIX: String = match env::var("AWS_PREFIX") {
        Ok(val) => format!("{}/", val),
        Err(_) => "".to_string(),
    };
    static ref AWS_ACCESS_KEY_ID: String = env::var("AWS_ACCESS_KEY_ID")
        .expect("the AWS_ACCESS_KEY_ID environment variable must be set");
    static ref AWS_SECRET_ACCESS_KEY: String = env::var("AWS_SECRET_ACCESS_KEY")
        .expect("the AWS_SECRET_ACCESS_KEY environment variable must be set");
    static ref AWS_CREDENTIALS: String = format!(
        "{}:{}",
        AWS_ACCESS_KEY_ID.clone(),
        AWS_SECRET_ACCESS_KEY.clone()
    );
}

async fn do_s3(bucket: &Bucket) -> Result<(), Box<dyn Error>> {
    let key = "mahrens/test.file2";
    println!("getting {}", key);
    let (data, code) = bucket.get_object(key).await?;
    println!("HTTP return code = {}", code);
    println!("object contents = {}", String::from_utf8(data)?);

    let content = "I want to go to S3".as_bytes();
    println!("putting {}", key);
    let (data, code) = bucket.put_object(key, content).await?;
    println!("HTTP return code = {}", code);
    println!("response contents = {}", String::from_utf8(data)?);

    let results = bucket.list("mahrens".to_string(), None).await?;
    for list_results in results {
        assert_eq!(code, 200);
        for res in list_results.contents {
            println!("found object {}", res.key);
        }
    }

    return std::result::Result::Ok(());
}

async fn do_s3_rusoto() -> Result<(), Box<dyn Error>> {
    let client = S3Client::new(rusoto_core::Region::UsWest2);

    let key = "mahrens/test.file2";

    println!("getting {}", key);
    let req = GetObjectRequest {
        bucket: BUCKET_NAME.to_string(),
        key: key.to_string(),
        ..Default::default()
    };
    let res = client.get_object(req).await?;
    let mut s = String::new();
    res.body
        .unwrap()
        .into_async_read()
        .read_to_string(&mut s)
        .await
        .unwrap();
    println!("object contents = {}", s);

    let content = "I want to go to S3".as_bytes().to_vec();
    println!("putting {}", key);
    let req = PutObjectRequest {
        bucket: BUCKET_NAME.to_string(),
        key: key.to_string(),
        body: Some(ByteStream::from(content)),
        ..Default::default()
    };
    client.put_object(req).await?;

    return std::result::Result::Ok(());
}

fn do_btree() {
    let mut bt: BTreeSet<[u64; 8]> = BTreeSet::new();
    let mut rng = rand::thread_rng();
    let n = 10000000;
    for _ in 0..n {
        bt.insert(rng.gen());
    }
    println!("added {} items to btree", n);
    std::thread::sleep(Duration::from_secs(1000));
}

async fn do_create() -> Result<(), Box<dyn Error>> {
    let mut client = Client::connect().await;
    let aws_key_id = env::var("AWS_ACCESS_KEY_ID")
        .expect("the AWS_ACCESS_KEY_ID environment variable must be set");
    let secret_key = env::var("AWS_SECRET_ACCESS_KEY")
        .expect("the AWS_SECRET_ACCESS_KEY environment variable must be set");
    let endpoint = ENDPOINT;
    let region = REGION;
    let bucket_name = BUCKET_NAME;
    let pool_name = POOL_NAME;
    let guid = PoolGUID(POOL_GUID);

    client
        .create_pool(
            &aws_key_id,
            &secret_key,
            &region,
            &endpoint,
            &bucket_name,
            guid,
            &pool_name,
        )
        .await;
    client.get_next_response().await;

    Ok(())
}

async fn do_write() -> Result<(), Box<dyn Error>> {
    let guid = PoolGUID(1234);
    let (mut client, next_txg, mut next_block) = setup_client(guid).await;

    let begin = Instant::now();
    let n = 5200;

    client.begin_txg(guid, next_txg).await;

    let task = client.get_responses_initiate(n);

    for _ in 0..n {
        let mut data: Vec<u8> = Vec::new();
        let mut rng = thread_rng();
        let len = rng.gen::<u32>() % 100;
        for _ in 0..len {
            data.push(rng.gen());
        }
        //println!("requesting write of {}B", len);
        client.write_block(guid, next_block, &data).await;
        //println!("writing {}B to {:?}...", len, id);
        next_block = BlockID(next_block.0 + 1);
    }
    client.flush_writes(guid).await;
    client.get_responses_join(task).await;

    client.end_txg(guid, &[]).await;
    client.get_next_response().await;

    println!("wrote {} blocks in {}ms", n, begin.elapsed().as_millis());

    Ok(())
}

async fn setup_client(guid: PoolGUID) -> (Client, TXG, BlockID) {
    let mut client = Client::connect().await;

    let bucket_name = BUCKET_NAME;
    let aws_key_id = env::var("AWS_ACCESS_KEY_ID")
        .expect("the AWS_ACCESS_KEY_ID environment variable must be set");
    let secret_key = env::var("AWS_SECRET_ACCESS_KEY")
        .expect("the AWS_SECRET_ACCESS_KEY environment variable must be set");
    let endpoint = ENDPOINT;
    let region = REGION;

    client
        .open_pool(
            &aws_key_id,
            &secret_key,
            &region,
            endpoint,
            &bucket_name,
            guid,
        )
        .await;

    let nvl = client.get_next_response().await;
    let txg = TXG(nvl.lookup_uint64("next txg").unwrap());
    let block = BlockID(nvl.lookup_uint64("next block").unwrap());

    (client, txg, block)
}

async fn do_read() -> Result<(), Box<dyn Error>> {
    let guid = PoolGUID(1234);
    let (mut client, _, _) = setup_client(guid).await;

    let max = 1000;
    let begin = Instant::now();
    let n = 50;

    let task = client.get_responses_initiate(n);

    for _ in 0..n {
        let id = BlockID((thread_rng().gen::<u64>() + 1) % max);
        client.read_block(guid, id).await;
    }

    client.get_responses_join(task).await;

    println!("read {} blocks in {}ms", n, begin.elapsed().as_millis());

    Ok(())
}

async fn do_free() -> Result<(), Box<dyn Error>> {
    let guid = PoolGUID(1234);
    let (mut client, mut next_txg, mut next_block) = setup_client(guid).await;

    // write some blocks, which we will then free some of

    client.begin_txg(guid, next_txg).await;
    next_txg = TXG(next_txg.0 + 1);

    let num_writes: usize = 10000;
    let task = client.get_responses_initiate(num_writes);
    let mut ids = Vec::new();

    for _ in 0..num_writes {
        let mut data: Vec<u8> = Vec::new();
        let mut rng = thread_rng();
        let len = rng.gen::<u32>() % 100;
        for _ in 0..len {
            data.push(rng.gen());
        }
        //println!("requesting write of {}B", len);
        client.write_block(guid, next_block, &data).await;
        //println!("writing {}B to {:?}...", len, id);
        ids.push(next_block);
        next_block = BlockID(next_block.0 + 1);
    }
    client.flush_writes(guid).await;
    client.get_responses_join(task).await;

    client.end_txg(guid, &[]).await;
    client.get_next_response().await;

    // free half the blocks, randomly selected
    client.begin_txg(guid, next_txg).await;

    for i in rand::seq::index::sample(&mut thread_rng(), ids.len(), ids.len() / 2) {
        client.free_block(guid, ids[i]).await;
    }
    client.end_txg(guid, &[]).await;
    client.get_next_response().await;

    Ok(())
}

fn get_int_param(args: &Vec<String>, offset: usize, default: i64) -> i64 {
    let mut val: i64 = default;
    if args.len() > offset {
        val = args[offset].parse::<i64>().unwrap();
    }

    val
}

fn has_expired(last_modified: &str, min_age_days: i64) -> bool {
    let mod_time = DateTime::parse_from_rfc3339(last_modified).unwrap();
    let num_days = mod_time.signed_duration_since(Local::now()).num_days();

    min_age_days == 0 || (-1 * num_days) > min_age_days
}

fn print_list(list_results: ListBucketResult, min_age_days: i64) {
    for res in list_results.contents {
        if has_expired(res.last_modified.as_str(), min_age_days) {
            let mod_time = DateTime::parse_from_rfc3339(res.last_modified.as_str()).unwrap();
            println!("{:30}  {}", mod_time.to_string(), res.key);
        }
    }
}

async fn list_and_process(
    bucket: &Bucket,
    min_age_days: i64,
    process: fn(ListBucketResult, i64),
) -> Result<(), Box<dyn Error>> {
    let mut continuation_token = None;
    loop {
        let (list_results, _) = bucket
            .list_page(
                object_access::prefixed(""),
                None,
                continuation_token,
                None,
                None,
            )
            .await?;
        continuation_token = list_results.next_continuation_token.clone();

        process(list_results, min_age_days);

        if continuation_token.is_none() {
            break;
        }
    }

    Ok(())
}

async fn do_list(bucket: &Bucket, args: &Vec<String>) -> Result<(), Box<dyn Error>> {
    let min_age_days: i64 = get_int_param(args, 2, 0);
    list_and_process(bucket, min_age_days, print_list)
        .await
        .unwrap();

    Ok(())
}

async fn delete_list(
    bucket: &Bucket,
    list_results: ListBucketResult,
    min_age_days: i64,
) -> (i64, i64) {
    let mut futures = Vec::new();
    let mut deleted: i64 = 0;
    let mut skipped: i64 = 0;

    for res in list_results.contents {
        if has_expired(res.last_modified.as_str(), min_age_days) {
            println!("deleting object {}...", res.key);
            deleted = deleted + 1;
            let begin = Instant::now();
            let fut = async move {
                bucket.delete_object(&res.key).await.unwrap();
                println!(
                    "finished deleting object {} in {}ms",
                    res.key,
                    begin.elapsed().as_millis()
                );
            };
            futures.push(fut);
        } else {
            skipped = skipped + 1;
        }
    }
    join_all(futures).await;

    (deleted, skipped)
}

async fn do_delete_impl(bucket: &Bucket, min_age_days: i64) -> Result<(), Box<dyn Error>> {
    let begin = Instant::now();

    let mut total_deleted: i64 = 0;
    let mut total_skipped: i64 = 0;
    let mut continuation_token = None;
    loop {
        let (list_results, _) = bucket
            .list_page(
                object_access::prefixed(""),
                None,
                continuation_token,
                None,
                None,
            )
            .await?;
        continuation_token = list_results.next_continuation_token.clone();

        let (deleted, skipped) = delete_list(bucket, list_results, min_age_days).await;
        total_deleted = total_deleted + deleted;
        total_skipped = total_skipped + skipped;

        if continuation_token.is_none() {
            break;
        }
    }

    println!(
        "deleted {}, skipped {} objects in {}ms; .",
        total_deleted,
        total_skipped,
        begin.elapsed().as_millis()
    );

    Ok(())
}

async fn do_delete(bucket: &Bucket, args: &Vec<String>) -> Result<(), Box<dyn Error>> {
    let min_age_days: i64 = get_int_param(args, 2, -1);
    assert!(min_age_days < 0, "Usage: zoa_test delete <number-of-days>");

    do_delete_impl(&bucket, min_age_days).await.unwrap();

    Ok(())
}

fn get_file_as_byte_vec(filename: &str) -> Vec<u8> {
    let mut f = File::open(filename).expect("no file found");
    let metadata = fs::metadata(filename).expect("unable to read metadata");
    let mut buffer = vec![0; metadata.len() as usize];
    f.read(&mut buffer).expect("buffer overflow");

    buffer
}

fn write_file_as_bytes(filename: &str, contents: &[u8]) {
    let mut f = File::create(filename).unwrap();
    f.write_all(contents).unwrap();
}

fn do_nvpair() {
    let buf = get_file_as_byte_vec("/etc/zfs/zpool.cache");
    let nvp = &mut NvList::try_unpack(buf.as_slice()).unwrap();
    //let nvp = &mut NvListRef::unpack(&buf[..]).unwrap();

    nvp.insert("new int", &5).unwrap();

    let mut vec: Vec<u8> = Vec::new();
    vec.push(1);
    vec.push(2);
    vec.push(3);
    nvp.insert("new uint8 array", vec.as_slice()).unwrap();

    println!("{:#?}", nvp);

    let newbuf = nvp.pack(NvEncoding::Native).unwrap();
    write_file_as_bytes("./zpool.cache.rust", &newbuf);
}

fn get_object_access() -> ObjectAccess {
    ObjectAccess::new(ENDPOINT, REGION, BUCKET_NAME, &AWS_CREDENTIALS.clone())
}

fn strip_prefix(prefix: &str) -> &str {
    if prefix.starts_with(AWS_PREFIX.as_str()) {
        &prefix[AWS_PREFIX.len()..]
    } else {
        prefix
    }
}
struct ListObject {
    key: String,
    last_modified: String,
}

#[async_recursion]
async fn list_objects(
    object_access: &ObjectAccess,
    prefix: &str,
    recursive: bool,
) -> Vec<ListObject> {
    // Strip aws_prefix as object_access adds it back automatically.
    let stripped_prefix = strip_prefix(prefix);
    let mut vec: Vec<ListObject> = Vec::new();

    for output in object_access.list_objects(&stripped_prefix, None).await {
        for objects in output.contents {
            for object in objects {
                let o = ListObject {
                    key: object.key.unwrap(),
                    last_modified: object.last_modified.unwrap(),
                };
                vec.push(o);
            }
        }

        if !recursive {
            continue;
        }

        // Recursively call list_objects for each prefix in common_prefixes.
        for object_prefixs in output.common_prefixes {
            for object_prefix in object_prefixs {
                let objects =
                    list_objects(&object_access, &object_prefix.prefix.unwrap(), true).await;
                for object in objects {
                    let o = ListObject {
                        key: object.key,
                        last_modified: object.last_modified,
                    };
                    vec.push(o);
                }
            }
        }
    }

    vec
}

fn print_list_objects(objects: Vec<ListObject>) {
    for object in objects {
        let mod_time = DateTime::parse_from_rfc3339(&object.last_modified).unwrap();
        println!("{:30}  {}", mod_time, object.key);
    }
}

async fn get_prefixes(object_access: &ObjectAccess, prefix: &str) -> Vec<String> {
    let mut vec: Vec<String> = Vec::new();

    for prefix in object_access.collect_prefixes(strip_prefix(prefix)).await {
        vec.push(prefix);
    }

    vec
}

fn get_super_object(objects: &Vec<ListObject>) -> String {
    let mut super_object: String = "-".to_string();

    for object in objects {
        if object.key.ends_with("/super") {
            super_object = object.key.clone();
            break;
        }
    }

    super_object
}

fn get_super_timestamp(objects: &Vec<ListObject>) -> String {
    let mut mod_time: String = "-".to_string();

    for object in objects {
        if object.key.ends_with("/super") {
            mod_time = object.last_modified.clone();
            break;
        }
    }

    mod_time
}

async fn decode_super(object_access: &ObjectAccess, super_object: &str, guid: PoolGUID) -> String {
    let pool: String;
    match Pool::get_config(&object_access, guid).await {
        Ok(pool_config) => {
            let name = pool_config.lookup_string("name").unwrap();
            let hostname = pool_config.lookup_string("hostname").unwrap();
            pool = format!("guid:{:?} name:{:?} host:{:?}", guid, name, hostname);
        }
        Err(_e) => {
            /*
             * XXX Pool::get_config() only works for pools under the AWS_PREFIX because it assumes the
             * path to the "super" object.
             */
            pool = if AWS_PREFIX.len() == 0 && !super_object.starts_with("zfs/") {
                format!("guid:{:?} (pool inside an alt AWS_PREFIX).", guid)
            } else {
                format!("guid:{:?} unknown format.", guid)
            };
        }
    }

    format!("{}\n\tsuper:{}", pool, super_object)
}

async fn get_pool_prefixes(object_access: &ObjectAccess) -> Vec<String> {
    let mut vec: Vec<String> = Vec::new();
    let prefix_list = get_prefixes(&object_access, "").await;
    println!("prefix_list {:?}", prefix_list);

    for prefix in prefix_list {
        if prefix.eq("zfs/") || prefix.ends_with("/zfs/") {
            vec.push(prefix);
        } else if prefix.ne("/") {
            vec.push(format!("{}{}", prefix, "zfs/"));
        }
    }

    vec
}

enum PoolProcessOp {
    ListSuperOnly,
    ListAllObjects,
    DeletePool,
}

impl PoolProcessOp {
    fn is_delete(&self) -> bool {
        match self {
            PoolProcessOp::ListAllObjects => false,
            PoolProcessOp::ListSuperOnly => false,
            PoolProcessOp::DeletePool => true,
        }
    }

    fn is_list_all_objects(&self) -> bool {
        match self {
            PoolProcessOp::ListAllObjects => true,
            PoolProcessOp::ListSuperOnly => false,
            _ => false,
        }
    }
}

/*
 * Common routine for deleting and listing pools and objects in a pool.
 * all_objects: if false only "super" objects are listed; else all objects are listed.
 * destroy: if true, instead of listing, the pool objects are destroyed.
 * min_age_days: minimum age for destroying a pool.
 */
async fn find_and_process_pools(
    op: PoolProcessOp,
    min_age_days: i64,
) -> Result<(), Box<dyn Error>> {
    let object_access = get_object_access();

    println!("Looking for pools...");
    for pool_prefix in get_pool_prefixes(&object_access).await {
        println!("Looking for pools UNDER {}...", pool_prefix);

        for prefix in get_prefixes(&object_access, &pool_prefix).await {
            let split: Vec<&str> = prefix.rsplitn(3, '/').collect();
            let guid_str: &str = split[1];
            if let Ok(guid64) = str::parse::<u64>(guid_str) {
                let guid = PoolGUID(guid64);

                // Get only super from the pool.
                let objects = list_objects(&object_access, &prefix, false).await;
                let timestamp = get_super_timestamp(&objects);
                let super_object = get_super_object(&objects);
                println!(
                    "\n{:30} {}",
                    timestamp,
                    decode_super(&object_access, super_object.as_str(), guid).await,
                );

                if op.is_delete() {
                    if !has_expired(timestamp.as_str(), min_age_days) {
                        println!(
                            "Skipping  pool deletion as it is not {} days old.",
                            min_age_days
                        );
                        continue;
                    }

                    let all_objects = list_objects(&object_access, &prefix, true).await;
                    let mut keys: Vec<String> = Vec::new();

                    for object in all_objects {
                        keys.push(strip_prefix(object.key.as_str()).to_string());
                    }
                    assert!(keys.len() > 0);
                    println!("Deleting {} objects in the pool.", keys.len());

                    /*
                     * XXX: Is there a better way than orming an array of chunks and reconstrucing a vector for each
                     * chunk?
                     */
                    for chunk in keys.chunks(AWS_DELETION_BATCH_SIZE) {
                        object_access.delete_objects(chunk.to_vec()).await;
                    }
                } else if op.is_list_all_objects() {
                    // Lookup all objects in the pool.
                    let all_objects = list_objects(&object_access, &prefix, true).await;
                    print_list_objects(all_objects);
                }
            }
        }
    }

    Ok(())
}

async fn list_pools(op: PoolProcessOp) -> Result<(), Box<dyn Error>> {
    find_and_process_pools(op, 0).await
}

async fn destroy_old_pools(args: &Vec<String>) -> Result<(), Box<dyn Error>> {
    let min_age_days: i64 = get_int_param(args, 2, -1);
    assert!(min_age_days >= 0, "Usage: zoa_test destroy_old_pools <number-of-days>");

    find_and_process_pools(PoolProcessOp::DeletePool, min_age_days).await
}

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();

    // assumes that AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY environment
    // variables are set
    let region_str = REGION;
    let bucket_name = BUCKET_NAME;

    let region: Region = region_str.parse().unwrap();
    let credentials = Credentials::default().unwrap();
    let bucket = Bucket::new(bucket_name, region, credentials).unwrap();
    println!("bucket: {:?}", bucket);

    match &args[1][..] {
        "s3" => do_s3(&bucket).await.unwrap(),
        "s3_rusoto" => do_s3_rusoto().await.unwrap(),
        "list" => do_list(&bucket, &args).await.unwrap(),
        "delete" => do_delete(&bucket, &args).await.unwrap(),
        "list_pools" => list_pools(PoolProcessOp::ListSuperOnly).await.unwrap(),
        "list_pool_objects" => list_pools(PoolProcessOp::ListAllObjects).await.unwrap(),
        "destroy_old_pools" => destroy_old_pools(&args).await.unwrap(),
        "create" => do_create().await.unwrap(),
        "write" => do_write().await.unwrap(),
        "read" => do_read().await.unwrap(),
        "free" => do_free().await.unwrap(),
        "btree" => do_btree(),
        "nvpair" => do_nvpair(),

        _ => {
            println!("invalid argument: {}", args[1]);
        }
    }
}
