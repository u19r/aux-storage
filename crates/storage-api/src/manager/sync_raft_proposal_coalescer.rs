use std::{future::Future, sync::Arc, time::Duration};

use http_error::HttpApiError;
use storage_sync::{
    ResolvedSyncMutationBatch, SyncMutationResponse, SyncProposalBatch,
    SyncProposalCoalescingDecision, SyncProposalCoalescingGate, SyncProposalPipelineLimits,
    SyncProposalResponse, plan_proposal_coalescing,
};
use tokio::sync::{Mutex, Notify, oneshot};

pub(super) const DEFAULT_SYNC_PROPOSAL_COALESCING_WINDOW: Duration = Duration::from_micros(500);

pub(super) struct SyncRaftProposalCoalescer {
    window: Duration,
    max_operations: usize,
    state: Mutex<Option<PendingCoalescedProposal>>,
    flushed: Notify,
}

impl SyncRaftProposalCoalescer {
    pub(super) fn new(window: Duration) -> Self {
        Self::new_with_max_operations(
            window,
            SyncProposalPipelineLimits::default().max_batch_operations,
        )
    }

    pub(super) fn new_with_max_operations(window: Duration, max_operations: usize) -> Self {
        Self {
            window,
            max_operations,
            state: Mutex::new(None),
            flushed: Notify::new(),
        }
    }

    pub(super) async fn propose<F, Fut>(
        &self,
        proposal: SyncProposalBatch,
        propose_batch: F,
    ) -> Result<SyncProposalResponse, HttpApiError>
    where
        F: FnOnce(ResolvedSyncMutationBatch) -> Fut,
        Fut: Future<Output = Result<Vec<SyncMutationResponse>, HttpApiError>>,
    {
        if self.window.is_zero() {
            let proposal_id = proposal.proposal_id;
            let responses = propose_batch(proposal.batch).await?;
            return Ok(SyncProposalResponse::new(proposal_id, responses));
        }

        let mut proposal = Some(proposal);
        loop {
            let receiver = {
                let mut state = self.state.lock().await;
                if let Some(pending) = state.as_mut() {
                    let Some(next) = proposal.as_ref() else {
                        return Err(HttpApiError::internal_server_error(
                            "sync proposal coalescer lost pending proposal before append",
                        ));
                    };
                    match pending.try_append(next, self.max_operations) {
                        Some(receiver) => receiver,
                        None => {
                            let notified = self.flushed.notified();
                            drop(state);
                            notified.await;
                            continue;
                        }
                    }
                } else {
                    let (sender, receiver) = oneshot::channel();
                    let Some(pending_proposal) = proposal.take() else {
                        return Err(HttpApiError::internal_server_error(
                            "sync proposal coalescer lost pending proposal before queue",
                        ));
                    };
                    *state = Some(PendingCoalescedProposal::new(pending_proposal, sender));
                    receiver
                }
            };

            if proposal.is_none() {
                tokio::time::sleep(self.window).await;
                let pending = self.state.lock().await.take();
                if let Some(pending) = pending {
                    let (batch, completion) = pending.into_batch_and_completion();
                    let result = propose_batch(batch).await.map(Arc::new);
                    completion.complete(result);
                    self.flushed.notify_waiters();
                }
            }

            return receiver.await.map_err(|_| {
                HttpApiError::internal_server_error("sync proposal coalescer response was dropped")
            })?;
        }
    }
}

struct PendingCoalescedProposal {
    proposal: SyncProposalBatch,
    waiters: Vec<PendingProposalWaiter>,
}

impl PendingCoalescedProposal {
    fn new(proposal: SyncProposalBatch, sender: ProposalResultSender) -> Self {
        let response_len = proposal.batch.mutations.len();
        Self {
            proposal,
            waiters: vec![PendingProposalWaiter {
                offset: 0,
                len: response_len,
                proposal_id: None,
                sender,
            }],
        }
    }

    fn try_append(
        &mut self,
        next: &SyncProposalBatch,
        max_operations: usize,
    ) -> Option<ProposalResultReceiver> {
        let combined_operations = self
            .proposal
            .batch
            .mutations
            .len()
            .saturating_add(next.batch.mutations.len());
        if combined_operations > max_operations {
            return None;
        }
        if plan_proposal_coalescing(SyncProposalCoalescingGate {
            left: &self.proposal,
            right: next,
        }) != SyncProposalCoalescingDecision::Coalesce
        {
            return None;
        }

        let offset = self.proposal.batch.mutations.len();
        let len = next.batch.mutations.len();
        self.proposal
            .batch
            .mutations
            .extend(next.batch.mutations.iter().cloned());
        self.proposal
            .read_set
            .items
            .extend(next.read_set.items.iter().cloned());
        let (sender, receiver) = oneshot::channel();
        self.waiters.push(PendingProposalWaiter {
            offset,
            len,
            proposal_id: Some(next.proposal_id.clone()),
            sender,
        });
        Some(receiver)
    }

    fn into_batch_and_completion(self) -> (ResolvedSyncMutationBatch, CoalescedCompletion) {
        (
            self.proposal.batch,
            CoalescedCompletion {
                first_proposal_id: self.proposal.proposal_id,
                waiters: self.waiters,
            },
        )
    }
}

struct CoalescedCompletion {
    first_proposal_id: storage_sync::SyncProposalId,
    waiters: Vec<PendingProposalWaiter>,
}

impl CoalescedCompletion {
    fn complete(self, result: Result<Arc<Vec<SyncMutationResponse>>, HttpApiError>) {
        for waiter in self.waiters {
            let response = match &result {
                Ok(responses) => {
                    let end = waiter.offset.saturating_add(waiter.len);
                    if let Some(slice) = responses.get(waiter.offset..end) {
                        Ok(SyncProposalResponse::new(
                            waiter
                                .proposal_id
                                .unwrap_or_else(|| self.first_proposal_id.clone()),
                            slice.to_vec(),
                        ))
                    } else {
                        Err(HttpApiError::internal_server_error(
                            "coalesced sync proposal response count did not match request count",
                        ))
                    }
                }
                Err(error) => Err(error.clone()),
            };
            let _ = waiter.sender.send(response);
        }
    }
}

type ProposalResultSender = oneshot::Sender<Result<SyncProposalResponse, HttpApiError>>;
type ProposalResultReceiver = oneshot::Receiver<Result<SyncProposalResponse, HttpApiError>>;

struct PendingProposalWaiter {
    offset: usize,
    len: usize,
    proposal_id: Option<storage_sync::SyncProposalId>,
    sender: ProposalResultSender,
}
