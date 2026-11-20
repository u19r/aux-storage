use std::collections::BTreeSet;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum PutOutcome {
    Done,
    Aborted,
    Unknown,
    Missing,
}

#[derive(Clone, Debug)]
pub(crate) struct PutSeen {
    pub(crate) key: String,
    pub(crate) called_at: i64,
    pub(crate) returned_at: i64,
    pub(crate) outcome: PutOutcome,
}

/// `last_create` is `(called_at, returned_at)` of the last acknowledged create.
pub(crate) fn audit_recreate(
    puts: &[PutSeen],
    last_create: (i64, i64),
    present: &BTreeSet<String>,
    fresh_present: &BTreeSet<String>,
) -> Result<(), String> {
    let (create_called, create_returned) = last_create;
    if present != fresh_present {
        let only_cached: Vec<_> = present.difference(fresh_present).collect();
        let only_fresh: Vec<_> = fresh_present.difference(present).collect();
        return Err(format!(
            "R4 cached and fresh views differ: cached-only {only_cached:?} fresh-only \
             {only_fresh:?}"
        ));
    }
    for put in puts {
        let is_present = present.contains(&put.key);
        match put.outcome {
            PutOutcome::Missing | PutOutcome::Aborted if is_present => {
                return Err(format!(
                    "R3 failed put is present: {} ({:?})",
                    put.key, put.outcome
                ));
            }
            PutOutcome::Done | PutOutcome::Unknown
                if is_present && put.returned_at < create_called =>
            {
                return Err(format!(
                    "R1 write from an older incarnation is present: {} returned {} before the \
                     last create {}",
                    put.key, put.returned_at, create_called
                ));
            }
            PutOutcome::Done if !is_present && put.called_at > create_returned => {
                return Err(format!(
                    "R2 acknowledged put after the last create is absent: {}",
                    put.key
                ));
            }
            PutOutcome::Missing if put.called_at > create_returned => {
                return Err("R2 put after the last create found no table".to_owned());
            }
            _ => {}
        }
    }
    let known: BTreeSet<&str> = puts.iter().map(|put| put.key.as_str()).collect();
    if let Some(stray) = present.iter().find(|key| !known.contains(key.as_str())) {
        return Err(format!("R1 key of no recorded put is present: {stray}"));
    }
    Ok(())
}

/// R5: the run exercised both sides of the last recreate.
pub(crate) fn is_vacuous(puts: &[PutSeen], last_create: (i64, i64)) -> bool {
    let after = puts
        .iter()
        .any(|put| put.outcome == PutOutcome::Done && put.called_at > last_create.1);
    let before = puts.iter().any(|put| put.returned_at < last_create.0);
    !(after && before)
}
