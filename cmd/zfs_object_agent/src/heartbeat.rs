use std::{
    collections::HashMap,
    sync::{Arc, Weak},
    time::{Duration, SystemTime},
};

use anyhow::Context;
use lazy_static::lazy_static;
use log::{debug, error, info};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use uuid::Uuid;

use crate::object_access::{OAError, ObjectAccess};

pub const LEASE_DURATION: Duration = Duration::from_secs(10);
pub const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(1);

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct HeartbeatPhys {
    pub timestamp: SystemTime,
    pub hostname: String,
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
    _key: Arc<HeartbeatImpl>,
}

lazy_static! {
    static ref HEARTBEAT: Arc<std::sync::Mutex<HashMap<HeartbeatImpl, Weak<HeartbeatImpl>>>> =
        Default::default();
}

pub async fn start_heartbeat(object_access: ObjectAccess, id: Uuid) -> HeartbeatGuard {
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
                let value = Arc::new(key.clone());
                heartbeats.insert(key.clone(), Arc::downgrade(&value));
                guard = HeartbeatGuard { _key: value };
            }
            Some(val_ref) => {
                debug!("existing entry found");
                match val_ref.upgrade() {
                    Some(val) => {
                        /*
                         * There is an existing heartbeat with references, simply add a reference
                         * and return.
                         */
                        debug!("upgrade succeeded, using existing heartbeat");
                        return HeartbeatGuard { _key: val };
                    }
                    None => {
                        /*
                         * In this case, there is already a heartbeat thread that would terminate
                         * on its next iteration. Replace the existing weak ref with a new one, and
                         * let it keep running.
                         */
                        let value = Arc::new(key.clone());
                        heartbeats.insert(key.clone(), Arc::downgrade(&value));
                        return HeartbeatGuard { _key: value };
                    }
                }
            }
        }
    }
    let (tx, mut rx) = mpsc::channel(1);
    tokio::spawn(async move {
        let mut last_heartbeat = HeartbeatPhys::get(&object_access, id)
            .await
            .ok()
            .map(|x| x.timestamp);
        if lease_timed_out(last_heartbeat) {
            tx.send(true).await.unwrap();
            suspend_all_pools("lease timeout");
            return;
        }
        info!("Starting heartbeat with id {}", id);
        let mut interval = tokio::time::interval(HEARTBEAT_INTERVAL);
        loop {
            interval.tick().await;
            let existing;
            {
                let mut heartbeats = HEARTBEAT.lock().unwrap();
                let value = heartbeats.get(&key);
                existing = value.unwrap().upgrade().is_some();
                if !existing {
                    heartbeats.remove(&key);
                    info!("Stopping heartbeat with id {}", id);
                }
            }
            if !existing {
                HeartbeatPhys::delete(&object_access, id).await;
                return;
            }
            let heartbeat = HeartbeatPhys {
                timestamp: SystemTime::now(),
                hostname: hostname::get().unwrap().into_string().unwrap(),
                lease_duration: LEASE_DURATION,
                id,
            };
            let result = heartbeat
                .put_timeout(&object_access, Some(Duration::from_secs(2)))
                .await;
            if lease_timed_out(last_heartbeat) {
                if last_heartbeat.is_none() {
                    tx.send(true).await.unwrap();
                }
                suspend_all_pools("lease timeout");
                return;
            }
            if result.is_ok() {
                if last_heartbeat.is_none() {
                    tx.send(true).await.unwrap();
                }
                last_heartbeat = Some(heartbeat.timestamp);
            }
        }
    });
    rx.recv().await;
    rx.close();
    guard
}

fn lease_timed_out(last_heartbeat: Option<SystemTime>) -> bool {
    if let Some(time) = last_heartbeat {
        if SystemTime::now()
            .duration_since(time)
            .map_or(false, |dur| dur > LEASE_DURATION)
        {
            return true;
        }
    }
    false
}

fn suspend_all_pools(cause: &str) {
    error!("Suspending pools due to {}", cause);
    todo!("Clean pool suspending not implemented");
}
