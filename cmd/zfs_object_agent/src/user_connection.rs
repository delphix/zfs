use crate::base_types::*;
use crate::object_access::ObjectAccess;
use crate::pool::*;
use crate::server::{HandlerReturn, Server};
use anyhow::Result;
use log::*;
use nvpair::NvList;
use rusoto_s3::S3;

pub struct UserServerState {}

impl UserServerState {
    fn connection_handler(&self) -> UserConnectionState {
        UserConnectionState {}
    }

    pub fn start(socket_dir: &str) {
        let socket_path = format!("{}/zfs_user_socket", socket_dir);
        let mut server = Server::new(
            &socket_path,
            UserServerState {},
            Box::new(Self::connection_handler),
        );

        UserConnectionState::register(&mut server);

        server.start();
    }
}

#[derive(Default)]
struct UserConnectionState {}

impl UserConnectionState {
    fn get_pools(&mut self, nvl: NvList) -> HandlerReturn {
        Ok(Box::pin(async move { Self::get_pools_impl(nvl).await }))
    }

    async fn get_pools_impl(nvl: NvList) -> Result<Option<NvList>> {
        let region_str = nvl.lookup_string("region")?;
        let endpoint = nvl.lookup_string("endpoint")?;
        let mut client = ObjectAccess::get_client(endpoint.to_str()?, region_str.to_str()?);
        let mut response = NvList::new_unique_names();
        let mut buckets = vec![];
        if let Ok(bucket) = nvl.lookup_string("bucket") {
            buckets.push(bucket.into_string()?);
        } else {
            buckets.append(
                &mut client
                    .list_buckets()
                    .await?
                    .buckets
                    .unwrap()
                    .into_iter()
                    .map(|b| b.name.unwrap())
                    .collect(),
            );
        }

        for buck in buckets {
            let object_access = ObjectAccess::from_client(client, buck.as_str());
            if let Ok(guid) = nvl.lookup_uint64("guid") {
                if !Pool::exists(&object_access, PoolGuid(guid)).await {
                    client = object_access.release_client();
                    continue;
                }
                match Pool::get_config(&object_access, PoolGuid(guid)).await {
                    Ok(pool_config) => {
                        response
                            .insert(format!("{}", guid), pool_config.as_ref())
                            .unwrap();
                        debug!("sending response: {:?}", response);
                        return Ok(Some(response));
                    }
                    Err(_) => {
                        client = object_access.release_client();
                        continue;
                    }
                }
            }
            for prefix in object_access.collect_prefixes("zfs/").await {
                debug!("prefix: {}", prefix);
                let split: Vec<&str> = prefix.rsplitn(3, '/').collect();
                let guid_str = split[1];
                if let Ok(guid64) = str::parse::<u64>(guid_str) {
                    let guid = PoolGuid(guid64);
                    // XXX do this in parallel for all guids?
                    match Pool::get_config(&object_access, guid).await {
                        Ok(pool_config) => response.insert(guid_str, pool_config.as_ref()).unwrap(),
                        Err(e) => {
                            error!("skipping {:?}: {:?}", guid, e);
                        }
                    }
                }
            }
            client = object_access.release_client();
        }
        debug!("sending response: {:?}", response);
        Ok(Some(response))
    }

    fn register(server: &mut Server<UserServerState, UserConnectionState>) {
        server.register_handler("get pools", Box::new(Self::get_pools));
    }
}
