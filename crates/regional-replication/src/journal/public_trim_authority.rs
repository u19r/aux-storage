//! Required native trim authority; committed descriptions alone never authorize
//! visibility.
use infrastructure_provider::{
    physical_attempt::PhysicalRecordAttempt,
    record::{DurableRecord, DurableRecordKey},
};
use replication_types::{
    configuration::installed::InstalledTableState,
    identity::resource::{TableIdentity, TableLineage},
};
use serde::{Deserialize, Serialize};
use storage_types::{ReadProtection, Revision, StorageResult, TableHistoryId};

use super::JournalStore;
use crate::{
    constants::{PUBLIC_TRIM_HEAD_SUFFIX, PUBLIC_TRIM_INSTALLATION_SUFFIX},
    history::{
        publication_scope::SourcePublicationScope, source_floor::SourcePublicationFloors,
        trim_authority::TrimRevision,
    },
    native_record::corrupt_record,
    trim::commit::TrimCommitCertificate,
};

/// Exact native head identity, independent of names and configuration
/// revisions.
#[derive(Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(in crate::journal) struct PublicTrimIncarnation {
    local: TableIdentity,
    pub(in crate::journal) physical: TableHistoryId,
    pub(super) lineage: TableLineage,
}

impl From<(&InstalledTableState, TableLineage)> for PublicTrimIncarnation {
    fn from((state, lineage): (&InstalledTableState, TableLineage)) -> Self {
        Self {
            local: state.configuration.table,
            physical: state.physical,
            lineage,
        }
    }
}

impl PublicTrimIncarnation {
    pub(in crate::journal) fn require_cut(
        &self,
        cut: &crate::history::table_cut::CanonicalTableCut,
    ) -> StorageResult<()> {
        cut.require_inventory()?;
        if cut.lineage != self.lineage {
            return Err(corrupt_record());
        }
        Ok(())
    }
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) enum PublicTrimHead {
    Genesis {
        local: TableIdentity,
        physical: TableHistoryId,
        lineage: TableLineage,
    },
    Installed {
        installation: PublicTrimInstallationReference,
        introductions: Vec<PublicTrimSourceIntroduction>,
        stamp: Revision,
    },
    Transferred {
        incarnation: PublicTrimIncarnation,
        content: replication_types::protocol::CanonicalDigest,
        revision: TrimRevision,
        operation: replication_types::identity::resource::ControlOperationId,
        ordinal: u64,
        source_stamp: Revision,
        introductions: Vec<PublicTrimSourceIntroduction>,
        stamp: Revision,
    },
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct PublicTrimInstallationReference {
    pub(super) incarnation: PublicTrimIncarnation,
    pub(super) operation: replication_types::identity::resource::ControlOperationId,
    pub(super) content: replication_types::protocol::CanonicalDigest,
    pub(super) revision: TrimRevision,
    pub(super) stamp: Revision,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct PublicTrimInstallationState {
    pub(super) incarnation: PublicTrimIncarnation,
    pub(super) certificate: TrimCommitCertificate,
    pub(super) floors: SourcePublicationFloors,
    pub(super) stamp: Revision,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct PublicTrimSourceIntroduction {
    pub(super) region: replication_types::identity::replica::RegionId,
    pub(super) operation: replication_types::identity::resource::ControlOperationId,
    pub(super) content: replication_types::protocol::CanonicalDigest,
}

impl PublicTrimHead {
    fn incarnation(&self) -> PublicTrimIncarnation {
        match self {
            Self::Genesis {
                local,
                physical,
                lineage,
            } => PublicTrimIncarnation {
                local: *local,
                physical: *physical,
                lineage: *lineage,
            },
            Self::Installed { installation, .. } => installation.incarnation,
            Self::Transferred { incarnation, .. } => *incarnation,
        }
    }

    fn revision(&self) -> TrimRevision {
        match self {
            Self::Genesis { .. } => TrimRevision::GENESIS,
            Self::Installed { installation, .. } => installation.revision,
            Self::Transferred { revision, .. } => *revision,
        }
    }

    pub(super) fn require_stamp(&self) -> StorageResult<()> {
        let (initial, introductions, stamp) = match self {
            Self::Genesis { .. } => return Ok(()),
            Self::Installed {
                installation,
                introductions,
                stamp,
            } => {
                if installation.revision == TrimRevision::GENESIS
                    || installation.stamp == Revision::INITIAL
                {
                    return Err(corrupt_record());
                }
                (installation.stamp, introductions, stamp)
            }
            Self::Transferred {
                source_stamp,
                introductions,
                stamp,
                ..
            } => (*source_stamp, introductions, stamp),
        };
        let count = u64::try_from(introductions.len()).map_err(|_| corrupt_record())?;
        let expected = u64::from(initial)
            .checked_add(count)
            .ok_or_else(corrupt_record)?;
        if u64::from(*stamp) != expected {
            return Err(corrupt_record());
        }
        Ok(())
    }

    fn append_source_introduction(
        &mut self,
        introduction: PublicTrimSourceIntroduction,
    ) -> StorageResult<Revision> {
        self.require_stamp()?;
        let (introductions, stamp) = match self {
            Self::Installed {
                introductions,
                stamp,
                ..
            }
            | Self::Transferred {
                introductions,
                stamp,
                ..
            } => (introductions, stamp),
            Self::Genesis { .. } => return Err(corrupt_record()),
        };
        if introductions
            .iter()
            .any(|existing| existing.operation == introduction.operation)
        {
            return Err(corrupt_record());
        }
        let previous = *stamp;
        let next = previous.checked_next()?;
        introductions
            .try_reserve(1)
            .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
        introductions.push(introduction);
        *stamp = next;
        Ok(previous)
    }
}

/// A head is exposed only after its native address, revision and incarnation
/// verify.
pub(in crate::journal) struct PublicTrimAuthority {
    pub(super) head: PublicTrimHead,
    pub(super) visibility: PublicTrimVisibility,
}

pub(super) enum PublicTrimVisibility {
    Genesis,
    Installed {
        certificate: Box<TrimCommitCertificate>,
        floors: SourcePublicationFloors,
    },
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(in crate::journal) struct CapturedPublicTrimAuthority {
    pub(super) head: PublicTrimHead,
}

impl From<&CapturedPublicTrimAuthority> for PublicTrimIncarnation {
    fn from(captured: &CapturedPublicTrimAuthority) -> Self {
        captured.head.incarnation()
    }
}

impl CapturedPublicTrimAuthority {
    pub(in crate::journal) fn require_checkpoint_binding(
        &self,
        physical: TableHistoryId,
        lineage: TableLineage,
        trim: crate::history::trim_authority::TrimRevision,
    ) -> StorageResult<()> {
        self.head.require_stamp()?;
        let captured = self.head.incarnation();
        if physical != captured.physical
            || lineage != captured.lineage
            || trim != self.head.revision()
        {
            return Err(corrupt_record());
        }
        Ok(())
    }
}

impl PublicTrimAuthority {
    pub(super) fn require_current(&self, current: &Self) -> StorageResult<()> {
        current.require_incarnation(self.head.incarnation())?;
        current.require_captured_revision(self.revision())?;
        match (&self.visibility, &current.visibility) {
            (
                PublicTrimVisibility::Installed {
                    certificate,
                    floors,
                },
                PublicTrimVisibility::Installed {
                    certificate: actual,
                    floors: next,
                },
            ) => {
                next.require_predecessor(floors)?;
                if self.revision() == current.revision() && certificate != actual {
                    return Err(corrupt_record());
                }
            }
            (PublicTrimVisibility::Genesis, PublicTrimVisibility::Genesis) => {}
            (PublicTrimVisibility::Genesis, PublicTrimVisibility::Installed { .. })
                if self.revision() < current.revision() => {}
            _ => return Err(corrupt_record()),
        }
        Ok(())
    }

    pub(in crate::journal) fn capture_historical_visibility(
        &self,
    ) -> StorageResult<crate::history::trim_authority::HistoricalTrimSnapshot> {
        match &self.visibility {
            PublicTrimVisibility::Genesis => Ok(
                crate::history::trim_authority::HistoricalTrimSnapshot::from_genesis(
                    self.head.incarnation().lineage,
                ),
            ),
            PublicTrimVisibility::Installed {
                certificate,
                floors,
            } => Ok(
                crate::history::trim_authority::HistoricalTrimSnapshot::from_installed(
                    *certificate.clone(),
                    floors.clone(),
                ),
            ),
        }
    }

    pub(in crate::journal) fn capture(self) -> CapturedPublicTrimAuthority {
        CapturedPublicTrimAuthority { head: self.head }
    }

    pub(in crate::journal) fn require_incarnation(
        &self,
        expected: PublicTrimIncarnation,
    ) -> StorageResult<()> {
        if expected != self.head.incarnation() {
            return Err(corrupt_record());
        }
        Ok(())
    }

    pub(in crate::journal) fn require_lineage(&self, expected: TableLineage) -> StorageResult<()> {
        if self.head.incarnation().lineage != expected {
            return Err(corrupt_record());
        }
        Ok(())
    }

    pub(in crate::journal) fn revision(&self) -> crate::history::trim_authority::TrimRevision {
        self.head.revision()
    }

    pub(in crate::journal) fn require_revision(
        &self,
        expected: crate::history::trim_authority::TrimRevision,
    ) -> StorageResult<()> {
        if expected != self.revision() {
            return Err(storage_types::StorageEnum::HistoryExpired.into());
        }
        Ok(())
    }

    /// Native held receipts retain old authority, but cannot come from a future
    /// head.
    pub(in crate::journal) fn require_captured_revision(
        &self,
        captured: crate::history::trim_authority::TrimRevision,
    ) -> StorageResult<()> {
        if captured > self.revision() {
            return Err(corrupt_record());
        }
        Ok(())
    }

    pub(in crate::journal) fn source_bounds(
        &self,
        selection: &crate::history::source_selection::CanonicalSourceSelection,
        through: replication_types::identity::origin::OriginPosition,
    ) -> StorageResult<crate::history::source_selection::SourceHistoryBounds> {
        self.require_lineage(selection.lineage)?;
        let after = self.source_floor(selection)?;
        if after > through {
            return Err(storage_types::StorageEnum::HistoryUnavailable.into());
        }
        crate::history::source_selection::SourceHistoryBounds::try_new(
            self.revision(),
            after,
            through,
        )
    }

    fn source_floor(
        &self,
        selection: &crate::history::source_selection::CanonicalSourceSelection,
    ) -> StorageResult<replication_types::identity::origin::OriginPosition> {
        match &self.visibility {
            PublicTrimVisibility::Genesis => Ok(0.into()),
            PublicTrimVisibility::Installed { floors, .. } => {
                floors.floor_for(SourcePublicationScope::new(
                    selection.lineage,
                    &selection.origin,
                    selection.source.table,
                ))
            }
        }
    }

    fn require_preserved_configuration(&self, next: &InstalledTableState) -> StorageResult<()> {
        self.require_incarnation((next, next.configuration.lineage).into())?;
        if let PublicTrimVisibility::Installed { floors, .. } = &self.visibility {
            floors.require_sources(next)?;
        }
        Ok(())
    }

    pub(in crate::journal) fn retired_source_bounds(
        &self,
        selection: &crate::history::source_selection::CanonicalSourceSelection,
        through: replication_types::identity::origin::OriginPosition,
    ) -> StorageResult<crate::history::source_selection::SourceHistoryBounds> {
        self.require_lineage(selection.lineage)?;
        let after = self.source_floor(selection)?.min(through);
        crate::history::source_selection::SourceHistoryBounds::try_new(
            self.revision(),
            after,
            through,
        )
    }
}

impl JournalStore {
    pub(in crate::journal) async fn is_receiver_original_visible<
        A: stream_provider::retained_image::RetainedOwnershipAttempt,
    >(
        &self,
        attempt: &mut A,
        authority: &PublicTrimAuthority,
        event: &replication_types::event::content::CanonicalEvent,
        encoding: (
            &replication_provider::runtime_configuration::limits::WorkLimits,
            &replication_provider::canonical_event_codec::CanonicalJsonCodec,
        ),
    ) -> StorageResult<bool> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let PublicTrimVisibility::Installed {
                certificate,
                floors,
            } = &authority.visibility
            else {
                return Ok(true);
            };
            self.require_original_floor_visibility(
                attempt,
                (floors, certificate.proposal.candidate.membership),
                (&event.as_ref().id, event.as_ref().digest),
                encoding,
            )
            .await
        }
        .await;
        settlement.complete(result)
    }

    pub(super) fn public_trim_head_key(
        &self,
        physical: TableHistoryId,
        lineage: TableLineage,
    ) -> StorageResult<DurableRecordKey> {
        self.identity_key(PUBLIC_TRIM_HEAD_SUFFIX, &(physical, lineage))
    }

    pub(super) async fn qualify_public_trim_head<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        row: &DurableRecord,
        incarnation: PublicTrimIncarnation,
        protection: ReadProtection,
    ) -> StorageResult<PublicTrimAuthority> {
        let head = self.decode_public_trim_head(row, incarnation)?;
        self.qualify_public_trim_dependencies(attempt, head, protection)
            .await
    }

    fn decode_public_trim_head(
        &self,
        row: &DurableRecord,
        incarnation: PublicTrimIncarnation,
    ) -> StorageResult<PublicTrimHead> {
        let head: PublicTrimHead = self.records.decode(row)?;
        if row.key != self.public_trim_head_key(incarnation.physical, incarnation.lineage)?
            || head.incarnation() != incarnation
        {
            return Err(corrupt_record());
        }
        let stamp = match &head {
            PublicTrimHead::Genesis { .. } => Revision::INITIAL,
            PublicTrimHead::Installed { stamp, .. } | PublicTrimHead::Transferred { stamp, .. } => {
                *stamp
            }
        };
        if row.revision != stamp {
            return Err(corrupt_record());
        }
        head.require_stamp()?;
        Ok(head)
    }

    async fn load_public_trim_installation<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        certificate: &TrimCommitCertificate,
        protection: ReadProtection,
    ) -> StorageResult<PublicTrimInstallationState> {
        let key = self.public_trim_installation_key(&certificate.proposal)?;
        let row = self
            .records
            .load(attempt, std::slice::from_ref(&key), protection)
            .await?
            .pop()
            .flatten()
            .ok_or_else(corrupt_record)?;
        let actual = self.decode_public_trim_installation(&row)?;
        if actual.certificate != *certificate {
            return Err(corrupt_record());
        }
        Ok(actual)
    }

    pub(super) fn decode_public_trim_installation(
        &self,
        row: &DurableRecord,
    ) -> StorageResult<PublicTrimInstallationState> {
        let receipt: PublicTrimInstallationState = self.records.decode(row)?;
        let certificate = &receipt.certificate;
        certificate.require_integrity()?;
        let participant = certificate
            .proposal
            .participant(&self.region)
            .map_err(|_| corrupt_record())?;
        self.require_local_source(&participant.state)?;
        let incarnation = (&participant.state, certificate.proposal.candidate.lineage).into();
        if row.key != self.public_trim_installation_key(&certificate.proposal)?
            || row.revision != Revision::INITIAL
            || receipt.incarnation != incarnation
            || receipt.stamp == Revision::INITIAL
        {
            return Err(corrupt_record());
        }
        certificate.require_installed_floors(&receipt.floors)?;
        Ok(receipt)
    }

    pub(super) fn bind_public_trim_installation(
        &self,
        installation: &PublicTrimInstallationState,
    ) -> StorageResult<PublicTrimInstallationReference> {
        Ok(PublicTrimInstallationReference {
            incarnation: installation.incarnation,
            operation: installation.certificate.proposal.operation,
            content: self.records.content_digest(installation)?,
            revision: installation.certificate.proposal.next,
            stamp: installation.stamp,
        })
    }

    pub(super) fn public_trim_installation_key(
        &self,
        proposal: &crate::trim::proposal::PublicTrimProposal,
    ) -> StorageResult<DurableRecordKey> {
        let local = &proposal.participant(&self.region)?.state;
        let incarnation: PublicTrimIncarnation = (local, proposal.candidate.lineage).into();
        self.identity_key(
            PUBLIC_TRIM_INSTALLATION_SUFFIX,
            &(incarnation, proposal.operation),
        )
    }

    pub(super) async fn require_absent_trim_installation<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        proposal: &crate::trim::proposal::PublicTrimProposal,
        protection: ReadProtection,
    ) -> StorageResult<()> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let key = self.public_trim_installation_key(proposal)?;
            if self
                .records
                .load(attempt, &[key], protection)
                .await?
                .pop()
                .flatten()
                .is_some()
            {
                return Err(corrupt_record());
            }
            Ok(())
        }
        .await;
        settlement.complete(result)
    }

    pub(super) async fn load_trim_installation_acknowledgement<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        certificate: &TrimCommitCertificate,
        protection: ReadProtection,
    ) -> StorageResult<crate::trim::completion::TrimInstalledParticipant> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let actual = self
                .load_public_trim_installation(attempt, certificate, protection)
                .await?;
            Ok(crate::trim::completion::TrimInstalledParticipant {
                region: self.region.clone(),
                certificate: self.records.content_digest(certificate)?,
                grant: certificate.grant_for(&self.region)?,
                installation: self.records.content_digest(&actual)?,
            })
        }
        .await;
        settlement.complete(result)
    }

    pub(super) fn require_installed_trim_authority(
        &self,
        authority: &PublicTrimAuthority,
        proposal: &crate::trim::proposal::PublicTrimProposal,
        digest: replication_types::protocol::CanonicalDigest,
    ) -> StorageResult<()> {
        let PublicTrimHead::Installed { .. } = &authority.head else {
            return Err(corrupt_record());
        };
        let PublicTrimVisibility::Installed { certificate, .. } = &authority.visibility else {
            return Err(corrupt_record());
        };
        certificate.require_proposal(proposal)?;
        if self.records.content_digest(certificate)? != digest {
            return Err(corrupt_record());
        }
        authority.require_revision(proposal.next)
    }

    pub(super) async fn require_installed_trim_operation<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        proposal: &crate::trim::proposal::PublicTrimProposal,
        digest: replication_types::protocol::CanonicalDigest,
        protection: ReadProtection,
    ) -> StorageResult<()> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let local = &proposal.participant(&self.region)?.state;
            let authority = self
                .load_public_trim_head(attempt, (local, proposal.candidate.lineage), protection)
                .await?;
            self.require_installed_trim_authority(&authority, proposal, digest)
        }
        .await;
        settlement.complete(result)
    }

    pub(super) async fn prepare_public_trim_installation<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        certificate: &TrimCommitCertificate,
    ) -> StorageResult<Vec<infrastructure_provider::record::RecordChange>> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let proposal = &certificate.proposal;
            let local = &proposal.participant(&self.region)?.state;
            let incarnation: PublicTrimIncarnation = (local, proposal.candidate.lineage).into();
            let key = self.public_trim_head_key(incarnation.physical, incarnation.lineage)?;
            let receipt_key = self.public_trim_installation_key(proposal)?;
            let keys = [key, receipt_key];
            let mut rows = self
                .records
                .load(attempt, &keys, ReadProtection::Conflict)
                .await?;
            let [key, receipt_key] = keys;
            if rows.pop().ok_or_else(corrupt_record)?.is_some() {
                return Err(corrupt_record());
            }
            let row = rows.pop().flatten().ok_or_else(corrupt_record)?;
            let previous = self
                .qualify_public_trim_head(attempt, &row, incarnation, ReadProtection::Conflict)
                .await?;
            previous.require_revision(proposal.expected)?;
            let predecessor = match previous.visibility {
                PublicTrimVisibility::Genesis => None,
                PublicTrimVisibility::Installed { floors, .. } => Some(floors),
            };
            let floors =
                SourcePublicationFloors::advance_candidate(&proposal.candidate, predecessor)?;
            certificate.require_installed_floors(&floors)?;
            let installation = PublicTrimInstallationState {
                incarnation,
                certificate: certificate.clone(),
                floors,
                stamp: row.revision.checked_next()?,
            };
            let receipt = self.records.prepare(receipt_key, None, &installation)?;
            let head = PublicTrimHead::Installed {
                stamp: installation.stamp,
                installation: self.bind_public_trim_installation(&installation)?,
                introductions: Vec::new(),
            };
            Ok(vec![
                self.records.prepare(key, Some(row.revision), &head)?,
                receipt,
            ])
        }
        .await;
        settlement.complete(result)
    }

    pub(in crate::journal) async fn introduce_public_trim_sources<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        record: &crate::control::ControlRecord,
    ) -> StorageResult<()> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let participant = record
                .proposal
                .participant(&self.region)
                .map_err(|_| corrupt_record())?;
            let Some((before, incarnation)) = self.source_introduction_scope(&record.proposal)?
            else {
                return Ok(());
            };
            let mut authority = self
                .load_public_trim_head(attempt, incarnation, ReadProtection::Conflict)
                .await?;
            let PublicTrimVisibility::Installed { floors, .. } = &mut authority.visibility else {
                return Ok(());
            };
            if !floors.introduce_sources(before, &participant.proposed)? {
                return Ok(());
            }
            let row = self
                .load_control(attempt, &record.proposal, ReadProtection::Conflict)
                .await?
                .ok_or_else(corrupt_record)?;
            let native = self.decode_control(&row, &record.proposal)?;
            if native != *record || native.decision != crate::control::ControlDecision::Installed {
                return Err(corrupt_record());
            }
            let previous =
                authority
                    .head
                    .append_source_introduction(PublicTrimSourceIntroduction {
                        region: self.region.clone(),
                        operation: record.proposal.operation().id,
                        content: record.proposal.operation().content,
                    })?;
            let key = self.public_trim_head_key(incarnation.physical, incarnation.lineage)?;
            let capacity = self.records.observe_discretionary_capacity(attempt).await?;
            self.records
                .store(
                    attempt,
                    &[self.records.prepare(key, Some(previous), &authority.head)?],
                )
                .await?;
            self.records
                .require_discretionary_capacity(attempt, capacity)
                .await
        }
        .await;
        settlement.complete(result)
    }

    async fn is_cancelled_trim_transfer<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        head: &PublicTrimHead,
    ) -> StorageResult<bool> {
        let PublicTrimHead::Transferred { operation, .. } = head else {
            return Ok(false);
        };
        let key = self.identity_key(crate::constants::CONTROL_OPERATION_SUFFIX, operation)?;
        let row = self
            .records
            .load(attempt, &[key], ReadProtection::Conflict)
            .await?
            .pop()
            .flatten();
        match row {
            Some(row) => Ok(self.decode_control_record(&row)?.decision
                == crate::control::ControlDecision::Cancelled),
            None => self.is_cancelled_trim_capture(attempt, *operation).await,
        }
    }

    pub(in crate::journal) async fn stage_retained_trim_authority<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        local: &InstalledTableState,
        input: (
            replication_types::identity::resource::ControlOperationId,
            &crate::bootstrap::ReenableHistoryCut,
        ),
    ) -> StorageResult<()> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let (operation, history) = input;
            history.visibility.require_cut(&history.cut)?;
            let incarnation: PublicTrimIncarnation = (local, history.cut.lineage).into();
            let key = self.public_trim_head_key(incarnation.physical, incarnation.lineage)?;
            let row = self
                .records
                .load(
                    attempt,
                    std::slice::from_ref(&key),
                    ReadProtection::Conflict,
                )
                .await?
                .pop()
                .flatten();
            let previous = if let Some(row) = row {
                let head = self.decode_public_trim_head(&row, incarnation)?;
                if let PublicTrimHead::Transferred {
                    content,
                    revision,
                    operation: pending,
                    ..
                } = &head
                    && *pending == operation
                {
                    if *revision != history.visibility.revision()
                        || *content != self.records.content_digest(&history.visibility)?
                    {
                        return Err(corrupt_record());
                    }
                    return Ok(());
                }
                let is_hidden_genesis = matches!(head, PublicTrimHead::Genesis { .. })
                    && local.phase
                        == replication_types::configuration::installed::RegionalTablePhase::Joining;
                if !is_hidden_genesis && !self.is_cancelled_trim_transfer(attempt, &head).await? {
                    let authority = self
                        .qualify_public_trim_head(
                            attempt,
                            &row,
                            incarnation,
                            ReadProtection::Conflict,
                        )
                        .await?;
                    if authority.capture_historical_visibility()? != history.visibility {
                        return Err(corrupt_record());
                    }
                    return Ok(());
                }
                Some(row.revision)
            } else {
                None
            };
            let stamp = previous
                .map(Revision::checked_next)
                .transpose()?
                .unwrap_or(Revision::INITIAL);
            let head = PublicTrimHead::Transferred {
                incarnation,
                content: self.records.content_digest(&history.visibility)?,
                revision: history.visibility.revision(),
                operation,
                ordinal: history.ordinal,
                source_stamp: stamp,
                introductions: Vec::new(),
                stamp,
            };
            self.records
                .store(attempt, &[self.records.prepare(key, previous, &head)?])
                .await
        }
        .await;
        settlement.complete(result)
    }

    pub(in crate::journal) async fn initialize_public_trim_genesis<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        state: &InstalledTableState,
        lineage: TableLineage,
    ) -> StorageResult<()> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let key = self.public_trim_head_key(state.physical, lineage)?;
            let row = self
                .records
                .load(
                    attempt,
                    std::slice::from_ref(&key),
                    ReadProtection::Conflict,
                )
                .await?
                .pop()
                .flatten();
            if let Some(row) = row {
                return self
                    .qualify_public_trim_head(
                        attempt,
                        &row,
                        (state, lineage).into(),
                        ReadProtection::Conflict,
                    )
                    .await
                    .map(|_| ());
            }
            let head = PublicTrimHead::Genesis {
                local: state.configuration.table,
                physical: state.physical,
                lineage,
            };
            self.records
                .store(attempt, &[self.records.prepare(key, None, &head)?])
                .await
        }
        .await;
        settlement.complete(result)
    }

    pub(in crate::journal) async fn load_public_trim_head<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        incarnation: impl Into<PublicTrimIncarnation>,
        protection: ReadProtection,
    ) -> StorageResult<PublicTrimAuthority> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let incarnation = incarnation.into();
            let key = self.public_trim_head_key(incarnation.physical, incarnation.lineage)?;
            let row = self
                .records
                .load(attempt, &[key], protection)
                .await?
                .pop()
                .flatten()
                .ok_or_else(corrupt_record)?;
            self.qualify_public_trim_head(attempt, &row, incarnation, protection)
                .await
        }
        .await;
        settlement.complete(result)
    }

    pub(in crate::journal) async fn prepare_table_trim_authority<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        input: (Option<&InstalledTableState>, &InstalledTableState),
    ) -> StorageResult<()> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let (previous, next) = input;
            if let Some(previous) = previous {
                let authority = self
                    .load_public_trim_head(
                        attempt,
                        (previous, previous.configuration.lineage),
                        ReadProtection::Conflict,
                    )
                    .await?;
                if previous.physical == next.physical
                    && previous.configuration.lineage == next.configuration.lineage
                {
                    authority.require_preserved_configuration(next)?;
                    return Ok(());
                }
                if authority.revision() != TrimRevision::GENESIS {
                    self.require_fresh_trim_lineage(attempt, previous, next)
                        .await?;
                }
            }
            self.initialize_public_trim_genesis(attempt, next, next.configuration.lineage)
                .await
        }
        .await;
        settlement.complete(result)
    }
}
