use scheduled_bg_jobs::{JobEffectAuthority, JobTerminalAuthority, Timestamp, store::StoreKey};

#[derive(Clone)]
pub(crate) enum JobWriteAuthority {
    Effect(JobEffectAuthority),
    Terminal(JobTerminalAuthority),
}

impl JobWriteAuthority {
    pub(crate) fn receipt_key(&self, token: &str) -> String {
        use sha2::{Digest, Sha256};
        let mut identity = Vec::new();
        self.bind_request_identity(&mut identity);
        let mut digest = Sha256::new();
        digest.update(crate::constants::JOB_EFFECT_RECEIPT_PREFIX);
        digest.update(identity);
        digest.update(token.as_bytes());
        format!(
            "{}{}",
            crate::constants::JOB_EFFECT_RECEIPT_PREFIX,
            hex::encode(digest.finalize())
        )
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
