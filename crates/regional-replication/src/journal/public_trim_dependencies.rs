//! Current and frozen heads recover source introductions from the same native
//! authority.
use infrastructure_provider::{
    physical_attempt::PhysicalRecordAttempt,
    record::{DurableRecord, DurableRecordKey},
};
use storage_types::{ReadProtection, StorageResult};

use super::{
    JournalStore,
    public_trim_authority::{
        CapturedPublicTrimAuthority, PublicTrimAuthority, PublicTrimHead, PublicTrimIncarnation,
        PublicTrimSourceIntroduction, PublicTrimVisibility,
    },
};
use crate::{history::source_floor::SourcePublicationFloors, native_record::corrupt_record};

impl JournalStore {
    pub(in crate::journal) async fn qualify_captured_public_trim_authority<
        A: PhysicalRecordAttempt,
    >(
        &self,
        attempt: &mut A,
        captured: &CapturedPublicTrimAuthority,
        current: PublicTrimAuthority,
        protection: ReadProtection,
    ) -> StorageResult<PublicTrimAuthority> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            if captured.head == current.head {
                return Ok(current);
            }
            let authority = self
                .qualify_public_trim_dependencies(attempt, captured.head.clone(), protection)
                .await?;
            authority.require_current(&current)?;
            Ok(authority)
        }
        .await;
        settlement.complete(result)
    }

    pub(super) async fn qualify_public_trim_dependencies<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        head: PublicTrimHead,
        protection: ReadProtection,
    ) -> StorageResult<PublicTrimAuthority> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            head.require_stamp()?;
            if matches!(head, PublicTrimHead::Genesis { .. }) {
                return Ok(PublicTrimAuthority {
                    head,
                    visibility: PublicTrimVisibility::Genesis,
                });
            }
            let keys = self.public_trim_dependency_keys(&head)?;
            let waves = self.records.read_waves(&keys, |key| key)?;
            let mut visibility = if matches!(head, PublicTrimHead::Installed { .. }) {
                None
            } else {
                Some(
                    Box::pin(self.load_public_trim_transfer_visibility(attempt, &head, protection))
                        .await?,
                )
            };
            for wave in waves {
                let children = self
                    .records
                    .load(attempt, &keys[wave.clone()], protection)
                    .await?;
                self.require_public_trim_dependency_wave(
                    (&head, &mut visibility),
                    wave,
                    &children,
                )?;
            }
            let visibility = visibility.ok_or_else(corrupt_record)?;
            Ok(PublicTrimAuthority { head, visibility })
        }
        .await;
        settlement.complete(result)
    }

    async fn load_public_trim_transfer_visibility<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        head: &PublicTrimHead,
        protection: ReadProtection,
    ) -> StorageResult<PublicTrimVisibility> {
        let PublicTrimHead::Transferred {
            incarnation,
            content,
            revision,
            operation,
            ordinal,
            ..
        } = head
        else {
            return Err(corrupt_record());
        };
        let actual = self
            .load_transferred_trim_snapshot(
                attempt,
                *incarnation,
                (*operation, *ordinal),
                protection,
            )
            .await?;
        if actual.revision() != *revision || self.records.content_digest(&actual)? != *content {
            return Err(corrupt_record());
        }
        match actual.into_qualified_installation(incarnation.lineage)? {
            None => Ok(PublicTrimVisibility::Genesis),
            Some((certificate, floors)) => Ok(PublicTrimVisibility::Installed {
                certificate: Box::new(certificate),
                floors,
            }),
        }
    }

    fn public_trim_dependency_keys(
        &self,
        head: &PublicTrimHead,
    ) -> StorageResult<Vec<DurableRecordKey>> {
        let (introductions, installation) = match head {
            PublicTrimHead::Genesis { .. } => return Ok(Vec::new()),
            PublicTrimHead::Installed {
                installation,
                introductions,
                ..
            } => (introductions, Some(installation)),
            PublicTrimHead::Transferred { introductions, .. } => (introductions, None),
        };
        let count = introductions
            .len()
            .checked_add(usize::from(installation.is_some()))
            .ok_or(storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
        let mut keys = Vec::new();
        let mut operations = std::collections::HashSet::new();
        keys.try_reserve_exact(count)
            .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
        operations
            .try_reserve(introductions.len())
            .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
        if let Some(installation) = installation {
            keys.push(self.identity_key(
                crate::constants::PUBLIC_TRIM_INSTALLATION_SUFFIX,
                &(installation.incarnation, installation.operation),
            )?);
        }
        for introduction in introductions {
            if introduction.region != self.region || !operations.insert(introduction.operation) {
                return Err(corrupt_record());
            }
            keys.push(self.identity_key(
                crate::constants::CONTROL_OPERATION_SUFFIX,
                &introduction.operation,
            )?);
        }
        Ok(keys)
    }

    fn require_public_trim_dependency_wave(
        &self,
        input: (&PublicTrimHead, &mut Option<PublicTrimVisibility>),
        wave: std::ops::Range<usize>,
        rows: &[Option<DurableRecord>],
    ) -> StorageResult<()> {
        let (incarnation, introductions, installation) = match input.0 {
            PublicTrimHead::Installed {
                installation,
                introductions,
                ..
            } => (installation.incarnation, introductions, Some(installation)),
            PublicTrimHead::Transferred {
                incarnation,
                introductions,
                ..
            } => (*incarnation, introductions, None),
            PublicTrimHead::Genesis { .. } => return Err(corrupt_record()),
        };
        if rows.len() != wave.len() {
            return Err(corrupt_record());
        }
        for (position, row) in wave.zip(rows) {
            let row = row.as_ref().ok_or_else(corrupt_record)?;
            if position == 0
                && let Some(installation) = installation
            {
                let actual = self.decode_public_trim_installation(row)?;
                if self.bind_public_trim_installation(&actual)? != *installation {
                    return Err(corrupt_record());
                }
                *input.1 = Some(PublicTrimVisibility::Installed {
                    certificate: Box::new(actual.certificate),
                    floors: actual.floors,
                });
                continue;
            }
            let introduction = introductions
                .get(
                    position
                        .checked_sub(usize::from(installation.is_some()))
                        .ok_or_else(corrupt_record)?,
                )
                .ok_or_else(corrupt_record)?;
            let Some(PublicTrimVisibility::Installed { floors, .. }) = input.1.as_mut() else {
                return Err(corrupt_record());
            };
            self.require_public_trim_source_introduction(row, (introduction, incarnation), floors)?;
        }
        Ok(())
    }

    pub(super) fn source_introduction_scope<'a>(
        &self,
        proposal: &'a replication_types::configuration::proposal::ControlProposal,
    ) -> StorageResult<
        Option<(
            &'a replication_types::configuration::installed::InstalledTableState,
            PublicTrimIncarnation,
        )>,
    > {
        let participant = proposal
            .participant(&self.region)
            .map_err(|_| corrupt_record())?;
        let incarnation = (
            &participant.proposed,
            participant.proposed.configuration.lineage,
        )
            .into();
        if let Some(before) = &participant.before {
            if PublicTrimIncarnation::from((before, before.configuration.lineage)) != incarnation {
                return Ok(None);
            }
            return Ok(Some((before, incarnation)));
        }
        let replication_types::configuration::control::ConfigurationChange::AddRegion { region } =
            &proposal.operation().change
        else {
            return Ok(None);
        };
        if region != &self.region {
            return Err(corrupt_record());
        }
        let before = proposal
            .participant(&proposal.operation().home_region)
            .map_err(|_| corrupt_record())?
            .before
            .as_ref()
            .ok_or_else(corrupt_record)?;
        if before.configuration.lineage != participant.proposed.configuration.lineage {
            return Err(corrupt_record());
        }
        Ok(Some((before, incarnation)))
    }

    fn require_public_trim_source_introduction(
        &self,
        row: &DurableRecord,
        input: (&PublicTrimSourceIntroduction, PublicTrimIncarnation),
        floors: &mut SourcePublicationFloors,
    ) -> StorageResult<()> {
        let (introduction, incarnation) = input;
        if introduction.region != self.region {
            return Err(corrupt_record());
        }
        let record = self.decode_control_record(row)?;
        let operation = record.proposal.operation();
        if operation.id != introduction.operation
            || operation.content != introduction.content
            || !matches!(
                record.decision,
                crate::control::ControlDecision::Installed
                    | crate::control::ControlDecision::Completed
            )
        {
            return Err(corrupt_record());
        }
        replication_provider::control::validate_preparation(&record.proposal)
            .map_err(|_| corrupt_record())?;
        let (before, actual) = self
            .source_introduction_scope(&record.proposal)?
            .ok_or_else(corrupt_record)?;
        let proposed = &record
            .proposal
            .participant(&self.region)
            .map_err(|_| corrupt_record())?
            .proposed;
        if actual != incarnation || !floors.introduce_sources(before, proposed)? {
            return Err(corrupt_record());
        }
        Ok(())
    }
}
