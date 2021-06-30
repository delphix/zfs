use std::{
    collections::HashMap,
    net::{IpAddr, Ipv4Addr},
    sync::{Arc, Weak},
    time::{Duration, SystemTime},
};

use anyhow::Context;
use lazy_static::lazy_static;
use log::{debug, error, info};
use serde::{Deserialize, Serialize};
use tokio::time::sleep;
use uuid::Uuid;

use crate::object_access::{OAError, ObjectAccess};

pub const LEASE_DURATION: Duration = Duration::from_secs(10);
pub const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(1);

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct HeartbeatPhys {
    pub timestamp: SystemTime,
    pub hostname: String,
    pub addr: IpAddr,
    pub lease_duration: Duration,
    pub id: Uuid,
}

impl HeartbeatPhys {
    fn key(id: Uuid) -> String {
        format!("zfs/agents/{}", id.to_string())
    }

    pub async fn get(object_access: &ObjectAccess, id: Uuid) -> anyhow::Result<Self> {
        let key = Self::key(id);
        let buf = object_access.get_object_impl(&key, None).await?;
        let this: Self = serde_json::from_slice(&buf)
            .context(format!("Failed to decode contents of {}", key))?;
        debug!("got {:#?}", this);
        assert_eq!(this.id, id);
        Ok(this)
    }

    pub async fn put_timeout(
        &self,
        object_access: &ObjectAccess,
        timeout: Option<Duration>,
    ) -> Result<
        rusoto_s3::PutObjectOutput,
        OAError<rusoto_core::RusotoError<rusoto_s3::PutObjectError>>,
    > {
        debug!("putting {:#?}", self);
        let buf = serde_json::to_vec(&self).unwrap();
        object_access
            .put_object_timed(&Self::key(self.id), buf, timeout)
            .await
    }

    pub async fn delete(object_access: &ObjectAccess, id: Uuid) {
        object_access.delete_object(&Self::key(id)).await;
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct HeartbeatImpl {
    endpoint: String,
    region: String,
    bucket: String,
}
pub struct HeartbeatGuard {
    key: HeartbeatImpl,
}

impl Drop for HeartbeatGuard {
    fn drop(&mut self) {
        let mut heartbeats = HEARTBEAT.lock().unwrap();
        heartbeats.remove(&self.key);
    }
}

lazy_static! {
    static ref HEARTBEAT: Arc<std::sync::Mutex<HashMap<HeartbeatImpl, Weak<HeartbeatGuard>>>> =
        Default::default();
}

pub fn start_heartbeat(object_access: ObjectAccess, id: Uuid) -> Arc<HeartbeatGuard> {
    let key = HeartbeatImpl {
        endpoint: object_access.get_endpoint(),
        region: object_access.get_region(),
        bucket: object_access.get_bucket(),
    };
    let guard;
    {
        let mut heartbeats = HEARTBEAT.lock().unwrap();
        match heartbeats.get(&key) {
            None => {
                guard = Arc::new(HeartbeatGuard { key: key.clone() });
                heartbeats.insert(key.clone(), Arc::downgrade(&guard));
            }
            Some(val) => {
                debug!("existing references found");
                return val.upgrade().unwrap();
            }
        }
    }
    tokio::spawn(async move {
        let mut last_heartbeat = HeartbeatPhys::get(&object_access, id)
            .await
            .map_or(SystemTime::UNIX_EPOCH, |x| x.timestamp);
        info!("Starting heartbeat with id {}", id);
        loop {
            let existing;
            {
                let mut heartbeats = HEARTBEAT.lock().unwrap();
                existing = !heartbeats.get(&key).is_none();
                if !existing {
                    info!("Stopping heartbeat with id {}", id);
                    heartbeats.remove(&key);
                }
            }
            if !existing {
                HeartbeatPhys::delete(&object_access, id).await;
                return;
            }
            let heartbeat = HeartbeatPhys {
                timestamp: SystemTime::now(),
                hostname: hostname::get().unwrap().into_string().unwrap(),
                addr: IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), // test
                lease_duration: LEASE_DURATION,
                id,
            };
            let result = heartbeat
                .put_timeout(&object_access, Some(Duration::from_secs(2)))
                .await;
            if last_heartbeat != SystemTime::UNIX_EPOCH
                && SystemTime::now()
                    .duration_since(last_heartbeat)
                    .map_or(false, |dur| dur > LEASE_DURATION)
            {
                suspend_all_pools("lease timeout");
                break;
            }
            if let Ok(_) = result {
                last_heartbeat = heartbeat.timestamp;
            }
            sleep(HEARTBEAT_INTERVAL).await;
        }
    });
    guard
}

fn suspend_all_pools(cause: &str) {
    error!("Suspending pools due to {}", cause);
    todo!("Clean pool suspending not implemented");
}
