#![allow(non_snake_case)]

use quint_connect::{Driver, Result, State, Step, quint_run, switch};
use serde::Deserialize;

use crate::ItemStreamVersion;

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
struct StreamCase {
    #[serde(rename = "key")]
    _key: String,
    operation: String,
    #[serde(rename = "pointerVersion")]
    pointer_version: i64,
}

#[derive(Debug, Deserialize, PartialEq, Eq)]
struct ItemVersionedStreamState {
    #[serde(rename = "lastCase")]
    last_case: StreamCase,
    #[serde(rename = "lastOutcome")]
    last_outcome: String,
    #[serde(rename = "lastVersion")]
    last_version: i64,
}

impl State<ItemVersionedStreamDriver> for ItemVersionedStreamState {
    fn from_driver(driver: &ItemVersionedStreamDriver) -> Result<Self> {
        Ok(Self {
            last_case: driver.last_case.clone(),
            last_outcome: driver.last_outcome.clone(),
            last_version: i64::try_from(driver.last_version.get())?,
        })
    }
}

#[derive(Debug)]
struct ItemVersionedStreamDriver {
    last_case: StreamCase,
    last_outcome: String,
    last_version: ItemStreamVersion,
}

impl Default for ItemVersionedStreamDriver {
    fn default() -> Self {
        Self {
            last_case: StreamCase {
                _key: "a".to_string(),
                operation: "none".to_string(),
                pointer_version: 0,
            },
            last_outcome: "not_checked".to_string(),
            last_version: ItemStreamVersion::new(0),
        }
    }
}

impl Driver for ItemVersionedStreamDriver {
    type State = ItemVersionedStreamState;

    fn step(&mut self, step: &Step) -> Result {
        switch!(step {
            init => {
                *self = Self::default();
            },
            Check(key: String, operation: String, pointerVersion: i64) => {
                self.check(key, operation, pointerVersion)?;
            },
            step(key: String?, operation: String?, pointerVersion: i64?) => {
                if let (Some(key), Some(operation), Some(pointer_version)) =
                    (key, operation, pointerVersion)
                {
                    self.check(key, operation, pointer_version)?;
                }
            },
        })
    }
}

impl ItemVersionedStreamDriver {
    fn check(&mut self, key: String, operation: String, pointer_version: i64) -> Result {
        self.last_case = StreamCase {
            _key: key,
            operation,
            pointer_version,
        };

        self.last_outcome = if ItemStreamVersion::try_from(pointer_version).is_err() {
            "rejected_old_format".to_string()
        } else if self.last_case.operation == "put" || self.last_case.operation == "delete" {
            let Some(next_version) = self.last_version.checked_increment() else {
                return Err(std::io::Error::other("version overflow").into());
            };
            self.last_version = next_version;
            "accepted_versioned_mutation".to_string()
        } else {
            "accepted_pointer".to_string()
        };
        Ok(())
    }
}

#[quint_run(
    spec = "../../quint/item_versioned_stream_mbt.qnt",
    max_samples = 64,
    max_steps = 12,
    seed = "0x51f15e1"
)]
fn item_versioned_stream_mbt_matches_rust_boundary() -> impl Driver {
    ItemVersionedStreamDriver::default()
}
