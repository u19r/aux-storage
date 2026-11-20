//! Regional pair replication runtime of one connection: the pair wiring, the
//! pair job, the peer server handler.
mod attach;
mod jobs;
mod lag;
#[cfg(test)]
mod lag_tests;
mod pair;
mod pair_job;
#[cfg(test)]
mod pair_job_tests;
#[cfg(test)]
mod recorder_test_support;
mod service;
#[cfg(test)]
mod service_tests;
#[cfg(all(test, feature = "sqlite"))]
mod wiring_tests;

pub(crate) use attach::{
    RegionalAttachment, regional_attachment, regional_pair, start_regional_server,
};
pub(crate) use jobs::{
    PairJobCheckpoint, PairJobPayload, PairJobs, REGIONAL_PAIR_JOB_KIND, REGIONAL_QUEUE,
    encode_pair_job_payload,
};
pub(crate) use pair::RegionalPair;
pub(crate) use pair_job::RegionalPairHandler;
pub(crate) use service::RegionalService;
