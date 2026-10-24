//! A fixed native pipeline retains every submitted read through settlement.
use std::{
    future::{Future, poll_fn},
    task::Poll,
};

use crate::constants::FOUNDATIONDB_PIPELINE_REQUESTS;

pub(super) async fn settle_read_window<I, F: Future>(
    inputs: [Option<I>; FOUNDATIONDB_PIPELINE_REQUESTS],
    operation: impl Fn(I) -> F,
) -> [Option<F::Output>; FOUNDATIONDB_PIPELINE_REQUESTS] {
    // Pin each actual read on the heap: an inline 32-way join multiplies native
    // future size through every enclosing stream and maintenance future.
    let mut reads = inputs.map(|input| input.map(&operation).map(Box::pin));
    let mut results = std::array::from_fn(|_| None);
    poll_fn(|context| {
        let mut is_pending = false;
        for (read, result) in reads.iter_mut().zip(&mut results) {
            let Some(future) = read else {
                continue;
            };
            match future.as_mut().poll(context) {
                Poll::Ready(value) => {
                    *result = Some(value);
                    *read = None;
                }
                Poll::Pending => is_pending = true,
            }
        }
        if is_pending {
            Poll::Pending
        } else {
            Poll::Ready(())
        }
    })
    .await;
    results
}
