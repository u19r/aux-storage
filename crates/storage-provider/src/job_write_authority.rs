use scheduled_bg_jobs::{JobEffectAuthority, JobTerminalAuthority, Timestamp, store::StoreKey};

#[derive(Clone)]
pub(crate) enum JobWriteAuthority {
    Effect(JobEffectAuthority),
    Terminal(JobTerminalAuthority),
}

impl JobWriteAuthority {
    /// The effect receipt key of this execution and caller token.
    /// An effect is identified by its token inside the execution's group. A
    /// terminal projection also binds its exact snapshot, so a later terminal
    /// state of a reused execution cannot replay an older projection.
    pub(crate) fn receipt_key(&self, token: &str) -> storage_types::StorageResult<StoreKey> {
        use sha2::{Digest, Sha256};
        let key = match self {
            Self::Effect(authority) => {
                let digest: [u8; 32] = Sha256::digest(token.as_bytes()).into();
                authority.receipt_key(&digest)
            }
            Self::Terminal(authority) => {
                let mut identity = Vec::new();
                authority.bind_request_identity(&mut identity);
                let mut hasher = Sha256::new();
                hasher.update((identity.len() as u64).to_be_bytes());
                hasher.update(&identity);
                hasher.update(token.as_bytes());
                let digest: [u8; 32] = hasher.finalize().into();
                authority.receipt_key(&digest)
            }
        };
        key.map_err(|_| storage_types::StorageError::internal("invalid scheduled-job receipt key"))
    }

    pub(crate) fn execution_identity(&self) -> [u8; 32] {
        use sha2::{Digest, Sha256};
        let mut identity = Vec::new();
        self.bind_request_identity(&mut identity);
        Sha256::digest(identity).into()
    }

    pub(crate) fn observation_keys(&self) -> &[StoreKey; 2] {
        match self {
            Self::Effect(authority) => authority.observation_keys(),
            Self::Terminal(authority) => authority.observation_keys(),
        }
    }

    pub(crate) fn matches_observations(
        &self,
        values: &[Option<bytes::Bytes>],
        now: Timestamp,
    ) -> Result<bool, scheduled_bg_jobs::store::StoreDataError> {
        match self {
            Self::Effect(authority) => authority.matches_observations(values, now),
            Self::Terminal(authority) => authority.matches_observations(values),
        }
    }

    pub(crate) fn bind_request_identity(&self, identity: &mut Vec<u8>) {
        match self {
            Self::Effect(authority) => authority.bind_request_identity(identity),
            Self::Terminal(authority) => authority.bind_request_identity(identity),
        }
    }
}
