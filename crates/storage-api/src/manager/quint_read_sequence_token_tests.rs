#![allow(non_snake_case)]

use std::collections::BTreeSet;

use quint_connect::{Driver, Result, State, Step, quint_run, switch};
use serde::Deserialize;
use storage_types::{ExclusiveStartKey, ReadSequenceConsistency};

use super::storage_manager_impl_read_sequence_token::{
    READ_SEQUENCE_TOKEN_VERSION, ReadSequenceQueryContinuation, ReadSequenceToken,
    decode_read_sequence_token, encode_read_sequence_token,
};

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
struct TokenCase {
    #[serde(rename = "completedNodes")]
    completed_nodes: BTreeSet<i64>,
    #[serde(rename = "continuationNodes")]
    continuation_nodes: BTreeSet<i64>,
    tampered: bool,
}

#[derive(Debug, Deserialize, PartialEq, Eq)]
struct ReadSequenceTokenMbtState {
    #[serde(rename = "lastCase")]
    last_case: TokenCase,
    #[serde(rename = "lastOutcome")]
    last_outcome: String,
    #[serde(rename = "decodedCompletedNodes")]
    decoded_completed_nodes: BTreeSet<i64>,
    #[serde(rename = "decodedContinuationNodes")]
    decoded_continuation_nodes: BTreeSet<i64>,
}

impl State<ReadSequenceTokenMbtDriver> for ReadSequenceTokenMbtState {
    fn from_driver(driver: &ReadSequenceTokenMbtDriver) -> Result<Self> {
        Ok(Self {
            last_case: driver.last_case.clone(),
            last_outcome: driver.last_outcome.clone(),
            decoded_completed_nodes: driver.decoded_completed_nodes.clone(),
            decoded_continuation_nodes: driver.decoded_continuation_nodes.clone(),
        })
    }
}

#[derive(Debug)]
struct ReadSequenceTokenMbtDriver {
    last_case: TokenCase,
    last_outcome: String,
    decoded_completed_nodes: BTreeSet<i64>,
    decoded_continuation_nodes: BTreeSet<i64>,
}

impl Default for ReadSequenceTokenMbtDriver {
    fn default() -> Self {
        Self {
            last_case: TokenCase {
                completed_nodes: BTreeSet::new(),
                continuation_nodes: BTreeSet::new(),
                tampered: false,
            },
            last_outcome: "not_checked".to_string(),
            decoded_completed_nodes: BTreeSet::new(),
            decoded_continuation_nodes: BTreeSet::new(),
        }
    }
}

impl Driver for ReadSequenceTokenMbtDriver {
    type State = ReadSequenceTokenMbtState;

    fn step(&mut self, step: &Step) -> Result {
        switch!(step {
            init => {
                *self = Self::default();
            },
            Check(completedNodes: BTreeSet<i64>, continuationNodes: BTreeSet<i64>, tampered: bool) => {
                self.check(completedNodes, continuationNodes, tampered)?;
            },
            step(
                completedNodes: BTreeSet<i64>?,
                continuationNodes: BTreeSet<i64>?,
                tampered: bool?,
            ) => {
                if let (Some(completed_nodes), Some(continuation_nodes), Some(tampered)) =
                    (completedNodes, continuationNodes, tampered)
                {
                    self.check(completed_nodes, continuation_nodes, tampered)?;
                }
            },
        })
    }
}

impl ReadSequenceTokenMbtDriver {
    fn check(
        &mut self,
        completed_nodes: BTreeSet<i64>,
        continuation_nodes: BTreeSet<i64>,
        tampered: bool,
    ) -> Result {
        self.last_case = TokenCase {
            completed_nodes: completed_nodes.clone(),
            continuation_nodes: continuation_nodes.clone(),
            tampered,
        };
        self.decoded_completed_nodes.clear();
        self.decoded_continuation_nodes.clear();

        let token = ReadSequenceToken {
            version: READ_SEQUENCE_TOKEN_VERSION,
            request_digest: "request".to_string(),
            metadata_digest: "metadata".to_string(),
            consistency: ReadSequenceConsistency::Eventual,
            next_node_ordinal: 0,
            invocation_ordinal: None,
            query_cursor: None,
            query_continuations: if continuation_nodes.is_empty() {
                None
            } else {
                Some(
                    continuation_nodes
                        .iter()
                        .map(|node_ordinal| {
                            Ok(ReadSequenceQueryContinuation {
                                node_ordinal: usize::try_from(*node_ordinal)?,
                                invocation_ordinal: 0,
                                query_cursor: ExclusiveStartKey::Token(format!(
                                    "cursor-{node_ordinal}"
                                )),
                            })
                        })
                        .collect::<std::result::Result<Vec<_>, std::num::TryFromIntError>>()?,
                )
            },
            provider_continuation: None,
            completed_nodes: completed_nodes
                .iter()
                .map(|node| usize::try_from(*node))
                .collect::<std::result::Result<Vec<_>, _>>()?,
            issued_at_epoch_seconds: 10,
            expires_at_epoch_seconds: i64::MAX,
            integrity: String::new(),
        };
        let mut encoded = encode_read_sequence_token(&token)
            .map_err(|error| anyhow::anyhow!("encode ReadSequence token: {error:?}"))?;
        if tampered {
            encoded.replace_range(0..2, "ff");
        }

        match decode_read_sequence_token(&encoded) {
            Ok(decoded) => {
                self.last_outcome = "accepted".to_string();
                self.decoded_completed_nodes = decoded
                    .completed_nodes
                    .into_iter()
                    .map(i64::try_from)
                    .collect::<std::result::Result<BTreeSet<_>, _>>()?;
                self.decoded_continuation_nodes = decoded
                    .query_continuations
                    .unwrap_or_default()
                    .into_iter()
                    .map(|continuation| i64::try_from(continuation.node_ordinal))
                    .collect::<std::result::Result<BTreeSet<_>, _>>()?;
            }
            Err(_) => {
                self.last_outcome = "rejected".to_string();
            }
        }
        Ok(())
    }
}

#[quint_run(
    spec = "../../quint/read_sequence_token_mbt.qnt",
    max_samples = 128,
    max_steps = 12,
    seed = "0x71eac0"
)]
fn read_sequence_token_mbt_matches_rust_codec() -> impl Driver {
    ReadSequenceTokenMbtDriver::default()
}
