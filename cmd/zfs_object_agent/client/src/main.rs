use client::Client;
use futures::future::*;
use libzoa::object_access;
use libzoa::object_based_log::*;
use libzoa::pool::*;
use nvpair::*;
use rand::prelude::*;
use s3::bucket::Bucket;
use s3::creds::Credentials;
use s3::Region;
use serde::{Deserialize, Serialize};
use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};
use std::convert::TryInto;
use std::env;
use std::error::Error;
use std::fs;
use std::fs::File;
use std::io::prelude::*;
use std::io::Read;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
mod client;

//#[macro_use]
//extern crate more_asserts;

const ENDPOINT: &str = "https://s3-us-west-2.amazonaws.com";
const REGION: &str = "us-west-2";
const BUCKET_NAME: &str = "cloudburst-data-2";
const POOL_NAME: &str = "testpool";
const POOL_GUID: u64 = 1234;

thread_local!(static RNG: RefCell<ThreadRng> = RefCell::new(rand::thread_rng()));

async fn do_s3(bucket: &Bucket) -> Result<(), Box<dyn Error>> {
    let key = "mahrens/test.file2";
    println!("getting {}", key);
    let (data, code) = bucket.get_object(key).await?;
    println!("HTTP return code = {}", code);
    println!("object contents = {}", String::from_utf8(data)?);

    let content = "I want to go to S3".as_bytes();
    println!("putting {}", key);
    let (_, code) = bucket.put_object(key, content).await?;
    println!("HTTP return code = {}", code);

    let results = bucket.list("mahrens".to_string(), None).await?;
    for list_results in results {
        assert_eq!(code, 200);
        for res in list_results.contents {
            println!("found object {}", res.key);
        }
    }

    return std::result::Result::Ok(());
}

fn subsec_nanos() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .subsec_nanos()
        .into()
}

#[derive(Serialize, Deserialize, Debug, Ord, Eq, PartialEq, PartialOrd, Copy, Clone)]
pub struct MyEntry(u128);
impl OnDisk for MyEntry {}
impl ObjectBasedLogEntry for MyEntry {}

async fn do_obl(bucket: &Bucket) -> Result<(), Box<dyn Error>> {
    let obl_name = "mahrens/obl";
    let fake_guid = PoolGUID(1234);
    let fake_state = PoolSharedState {
        bucket: bucket.clone(),
        guid: fake_guid,
        name: "obltest".to_string(),
    };
    let mut obl: ObjectBasedLog<MyEntry> = ObjectBasedLog::create(Arc::new(fake_state), obl_name);
    let fake_txg = TXG(1);

    println!("{:#?}", obl);

    let entries = obl.read().await;
    /*
    let mut i = 0;
    for e in entries {
        println!("entry {}: {}", i, e);
        i = i + 1;
    }
    */
    println!("read {} entries", entries.len());

    let mut bt = BTreeMap::new();
    let mut i: i32 = 0;
    let begin = Instant::now();
    //for ent in &obl {
    for ent in obl.read().await {
        let e = bt.entry(ent).or_insert(0);
        *e += 1;
        i += 1;
    }
    println!(
        "iterated {} entries in {}ms, btree has {} entries",
        i,
        begin.elapsed().as_millis(),
        bt.len()
    );

    //obl.append(5u32);
    for _ in 1..1500000 {
        obl.append(fake_txg, MyEntry(subsec_nanos().try_into().unwrap()));
    }
    obl.flush(fake_txg).await;

    Ok(())
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

async fn do_delete(bucket: &Bucket) -> Result<(), Box<dyn Error>> {
    let begin = Instant::now();
    let results = bucket.list(object_access::prefixed(""), None).await?;
    println!(
        "listed {} objects in {}ms",
        results.len(),
        begin.elapsed().as_millis()
    );
    let mut futures = Vec::new();
    let begin = Instant::now();
    for list_results in results {
        for res in list_results.contents {
            println!("deleting object {}...", res.key);
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
        }
    }
    join_all(futures).await;
    println!("deleted all objects in {}ms", begin.elapsed().as_millis());
    /*
    loop {
        let (res, idx, remaining_futures) = select_all(futures).await;
        futures = remaining_futures;
    }
    */
    Ok(())
}

async fn do_list(bucket: &Bucket) -> Result<(), Box<dyn Error>> {
    let results = bucket.list(object_access::prefixed(""), None).await?;
    for list_results in results {
        for res in list_results.contents {
            println!("found object {}", res.key);
        }
    }
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

fn main() {
    let args: Vec<String> = std::env::args().collect();

    // assumes that AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY environment
    // variables are set
    let region_str = REGION;
    let bucket_name = BUCKET_NAME;

    let region: Region = region_str.parse().unwrap();
    let credentials = Credentials::default().unwrap();
    let bucket = Bucket::new(bucket_name, region, credentials).unwrap();
    println!("bucket: {:?}", bucket);

    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_name("zoa")
        .build()
        .unwrap()
        .block_on(async move {
            match &args[1][..] {
                "s3" => do_s3(&bucket).await.unwrap(),
                "list" => do_list(&bucket).await.unwrap(),
                "delete" => do_delete(&bucket).await.unwrap(),
                "obl" => do_obl(&bucket).await.unwrap(),
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
        });
}
