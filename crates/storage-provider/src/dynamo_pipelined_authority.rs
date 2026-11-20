//! PERF-FDB-1: a warm operation observes the catalog version in the same
//! native wave as its first data read. The data is used only after that
//! version, or the stamps when the version moved, confirmed every candidate
//! binding (stale-incarnation rule, `crates/AGENTS.md`).

use storage_types::{CatalogVersion, Exact, StorageResult, TableName};

use crate::{CatalogAttempt, CatalogStamp, TableReadCandidate};

/// How the candidates of one pipelined read were confirmed.
pub(crate) enum PipelinedAuthority {
    /// `stamps[i]` is the stamp observed for candidate `i`. The pipelined
    /// data is usable only when `confirmed`.
    Observed {
        version: Option<CatalogVersion>,
        stamps: Exact<Option<CatalogStamp>>,
        confirmed: bool,
    },
    /// The version moved and the stamps confirmed every candidate. The
    /// engine dropped the pipelined data undecoded; read it again.
    ReadAgain {
        version: Option<CatalogVersion>,
        stamps: Exact<Option<CatalogStamp>>,
    },
    /// No wave carried the version and the data read failed: its error is
    /// the answer.
    Failed,
}

/// Ask `attempt` to carry the catalog version in its next data wave. `false`
/// when a candidate is absent or has no cached version, when the candidates'
/// versions differ, or there are none, or when the engine does not pipeline
/// (every engine but FoundationDB): the caller keeps the version-first order.
pub(crate) fn begin_pipelined_authority<A: CatalogAttempt + ?Sized>(
    attempt: &mut A,
    candidate_versions: impl Iterator<Item = Option<CatalogVersion>>,
) -> StorageResult<bool> {
    let mut expected = None;
    for version in candidate_versions {
        let Some(version) = version else {
            return Ok(false);
        };
        if expected.is_some_and(|expected| expected != version) {
            return Ok(false);
        }
        expected = Some(version);
    }
    let Some(expected) = expected else {
        return Ok(false);
    };
    attempt.pipeline_catalog_version(expected)
}

/// Confirm the candidates after the data read; `data_ok` is that read's
/// success. Reads a stamp batch only when the observed version differs from
/// the candidates', or when no wave carried it.
pub(crate) async fn finish_pipelined_authority<'a, A, I>(
    attempt: &mut A,
    candidates: I,
    data_ok: bool,
) -> StorageResult<PipelinedAuthority>
where
    A: CatalogAttempt + ?Sized,
    I: ExactSizeIterator<Item = (&'a TableName, Option<TableReadCandidate<'a>>)> + Clone + Send,
{
    let observed = attempt.pipelined_catalog_version();
    match observed {
        Some(version)
            if version.is_some()
                && candidates.clone().all(|(_, candidate)| {
                    candidate.is_some_and(|candidate| candidate.catalog_version == version)
                }) =>
        {
            // The version is only a hint: protect the trusted stamps. A failed
            // data read already settled the attempt and answers with its error.
            if data_ok {
                let names = candidates.clone().map(|(name, _)| name).collect::<Vec<_>>();
                attempt.protect_catalog_stamps(&names)?;
            }
            Ok(PipelinedAuthority::Observed {
                version,
                stamps: {
                    let expected = candidates.len();
                    Exact::try_new(
                        expected,
                        candidates
                            .map(|(_, candidate)| candidate.map(|candidate| candidate.stamp))
                            .collect(),
                    )?
                },
                confirmed: true,
            })
        }
        Some(version) => {
            // Moved: the engine dropped the data undecoded and left the
            // attempt open. A failed raw read rejected it; this stamp read
            // then returns that failure.
            let stamps =
                crate::dynamo_catalog::observe_catalog_stamps(attempt, candidates.clone(), version)
                    .await?;
            let confirmed = candidates.zip(&stamps).all(|((_, candidate), stamp)| {
                candidate.map(|candidate| candidate.stamp) == *stamp
            });
            Ok(if confirmed {
                PipelinedAuthority::ReadAgain { version, stamps }
            } else {
                PipelinedAuthority::Observed {
                    version,
                    stamps,
                    confirmed: false,
                }
            })
        }
        None if !data_ok => Ok(PipelinedAuthority::Failed),
        None => {
            // No data wave carried it (for example an empty read): read it now.
            let version = attempt.read_catalog_version().await?;
            let stamps =
                crate::dynamo_catalog::observe_catalog_stamps(attempt, candidates.clone(), version)
                    .await?;
            let confirmed = candidates.zip(&stamps).all(|((_, candidate), stamp)| {
                candidate.map(|candidate| candidate.stamp) == *stamp
            });
            Ok(PipelinedAuthority::Observed {
                version,
                stamps,
                confirmed,
            })
        }
    }
}
