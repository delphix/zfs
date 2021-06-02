use chrono::prelude::*;
use chrono::DateTime;
use client::Client;
use lazy_static::lazy_static;
use libzoa::base_types::*;
use libzoa::object_access::ObjectAccess;
use libzoa::pool::*;
use nvpair::*;
use rand::prelude::*;
use rusoto_core::ByteStream;
use rusoto_s3::*;
use s3::bucket::Bucket;
use s3::creds::Credentials;
use s3::Region;
use std::collections::BTreeSet;
use std::env;
use std::error::Error;
use std::fs;
use std::fs::File;
use std::io::prelude::*;
use std::io::Read;
use std::time::{Duration, Instant};
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
    static ref OBJECT_ACCESS: ObjectAccess =
        ObjectAccess::new(ENDPOINT, REGION, BUCKET_NAME, &AWS_CREDENTIALS.clone());
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

fn has_expired(last_modified: &str, min_age: Duration) -> bool {
    let mod_time = DateTime::parse_from_rfc2822(last_modified).unwrap();
    let age = Local::now().signed_duration_since(mod_time);

    min_age == Duration::from_secs(0) || age > chrono::Duration::from_std(min_age).unwrap()
}

async fn print_super(object_access: &ObjectAccess, pool_key: &str, output: &HeadObjectOutput) {
    print!(
        "{:30} {:40}",
        DateTime::parse_from_rfc2822(output.last_modified.as_ref().unwrap()).unwrap(),
        pool_key
    );
    let split: Vec<&str> = pool_key.rsplitn(3, '/').collect();
    let guid_str: &str = split[1];
    if let Ok(guid64) = str::parse::<u64>(guid_str) {
        let guid = PoolGUID(guid64);
        match Pool::get_config(&object_access, guid).await {
            Ok(pool_config) => {
                let name = pool_config.lookup_string("name").unwrap();
                let hostname = pool_config.lookup_string("hostname").unwrap();
                println!(
                    "\t{:20} {}",
                    name.to_str().unwrap(),
                    hostname.to_str().unwrap()
                );
            }
            Err(_e) => {
                /*
                 * XXX Pool::get_config() only works for pools under the AWS_PREFIX because it assumes the
                 * path to the "super" object.
                 */
                if AWS_PREFIX.len() == 0 && !pool_key.starts_with("zfs/") {
                    println!("\t(pool inside an alt AWS_PREFIX)");
                } else {
                    println!("\t-unknown format-");
                };
            }
        }
    }
}

fn strip_prefix(prefix: &str) -> &str {
    if prefix.starts_with(AWS_PREFIX.as_str()) {
        &prefix[AWS_PREFIX.len()..]
    } else {
        prefix
    }
}

async fn find_old_pools(min_age: Duration) -> Vec<String> {
    let object_access = &OBJECT_ACCESS;
    let mut pool_keys = object_access.collect_prefixes("zfs/").await;

    let aws_prefix: &String = &AWS_PREFIX;
    if aws_prefix == "" {
        for prefix in object_access.collect_prefixes("").await {
            pool_keys.append(
                &mut object_access
                    .collect_prefixes(&format!("{}zfs/", prefix))
                    .await,
            );
        }
    }

    let mut vec = Vec::new();
    for pool_key in pool_keys {
        match object_access
            .head_object(strip_prefix(&format!("{}super", pool_key)))
            .await
        {
            Some(output) => {
                print_super(&object_access, &pool_key, &output).await;
                if has_expired(output.last_modified.as_ref().unwrap(), min_age) {
                    vec.push(pool_key);
                } else {
                    println!(
                        "Skipping pool as it is not {} days old.",
                        min_age.as_secs() / (60 * 60 * 24)
                    );
                }
            }
            None => {
                println!("didn't find super object for {}", pool_key);
            }
        }
    }
    vec
}

async fn do_list_pools(list_all_objects: bool) -> Result<(), Box<dyn Error>> {
    let object_access = &OBJECT_ACCESS;
    for prefix in find_old_pools(Duration::from_secs(0)).await {
        // Lookup all objects in the pool.
        if list_all_objects {
            for object in object_access
                .collect_all_objects(strip_prefix(&prefix))
                .await
            {
                println!("    {}", object);
            }
        }
    }
    Ok(())
}

async fn do_destroy_old_pools(args: &Vec<String>) -> Result<(), Box<dyn Error>> {
    let min_age = if args.len() > 2 {
        Duration::from_secs(args[2].parse::<u64>().unwrap() * 60 * 60 * 24)
    } else {
        panic!("Usage: zoa_test destroy_old_pools <number-of-days>");
    };

    let object_access = &OBJECT_ACCESS;

    for prefix in find_old_pools(min_age).await {
        for chunk in object_access
            .collect_all_objects(strip_prefix(&prefix))
            .await
            .chunks(AWS_DELETION_BATCH_SIZE)
        {
            object_access
                .delete_objects(
                    &chunk
                        .iter()
                        .map(|o| strip_prefix(&o).to_string())
                        .collect::<Vec<_>>(),
                )
                .await;
        }
    }
    Ok(())
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
        "list_pools" => do_list_pools(false).await.unwrap(),
        "list_pool_objects" => do_list_pools(true).await.unwrap(),
        "destroy_old_pools" => do_destroy_old_pools(&args).await.unwrap(),
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
