//! Utility functions and wrappers for working with NVMEoF devices in SPDK.

use std::fmt;

use snafu::Snafu;

use crate::{
    core::Bdev,
    target,
    target::nvmf::{share, unshare},
};

#[derive(Debug, Snafu)]
pub enum NexusNvmfError {
    #[snafu(display("Bdev not found {}", dev))]
    BdevNotFound { dev: String },
    #[snafu(display(
        "Failed to create iscsi target for bdev uuid {}, error {}",
        dev,
        err
    ))]
    CreateTargetFailed { dev: String, err: String },
}

/// Nvmf target representation.
pub struct NexusNvmfTarget {
    uuid: String,
    //bdev_name: String, // TOMTODO Needed?
}

impl NexusNvmfTarget {
    pub async fn create(my_uuid: &str) -> Result<Self, NexusNvmfError> {
        println!("TM22 Handling nvmf: {}", my_uuid);
        let bdev = match Bdev::lookup_by_name(&my_uuid) {
            None => {
                return Err(NexusNvmfError::CreateTargetFailed {
                    // TOMTODO Not right, but whatever.
                    dev: my_uuid.to_string(),
                    err: "failed to find".to_string(),
                });
            }
            Some(bd) => bd,
        };

        match share(&my_uuid, &bdev).await {
            Ok(_) => Ok(Self {
                uuid: my_uuid.to_string(),
            }),
            Err(e) => Err(NexusNvmfError::CreateTargetFailed {
                dev: my_uuid.to_string(),
                err: e.to_string(),
            }),
        }
    }

    pub async fn destroy(self) {
        info!("Destroying iscsi frontend target");
        match unshare(&self.uuid).await {
            Ok(()) => (),
            Err(e) => {
                error!("Failed to destroy nvmf frontend target, error {}", e)
            }
        }
    }

    pub fn as_uri(&self) -> String {
        match target::nvmf::get_uri(&self.uuid) {
            Some(uri) => uri,
            None => "HAHA NO".to_string(), // TOMTODO Errors...
        }
    }
}

// TOMTODO Why are there two of these??
impl fmt::Debug for NexusNvmfTarget {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}@{:?}", self.as_uri(), self.uuid)
    }
}

// TOMTODO Why are there two of these??
impl fmt::Display for NexusNvmfTarget {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_uri())
    }
}
