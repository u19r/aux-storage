//! The scheduling core with explicit `now` values (W3S plan 5.7, section 6):
//! goals (i) and (ii), the first burst to a new destination, the slot of a
//! panicked task, the deadline bypass, the sweep and the breaker.

use std::{
    num::{NonZeroU32, NonZeroUsize},
    time::Duration,
};

use pubsub_provider::{
    AskMode, ClaimedDelivery, DeliveryFence, DeliveryLease, DeliveryPayload, DeliveryRecordId,
    DeliveryState, DestinationClaim, DestinationHead, DestinationKey, SubscriptionArn,
    SubscriptionProtocol, SweepCursor, SweepPage,
};
use storage_types::TimestampMillis;

use crate::{
    delivery_config::BreakerConfig,
    delivery_scheduler::{BreakerSignal, Completed, DeliveryScheduler, SchedulerConfig},
};

fn at(millis: i64) -> TimestampMillis {
    TimestampMillis::from_timestamp(millis)
}

fn http(name: &str) -> DestinationKey {
    DestinationKey::of_endpoint(SubscriptionProtocol::Https, &format!("https://{name}/hook"))
}

fn queue(name: &str) -> DestinationKey {
    DestinationKey::of_endpoint(
        SubscriptionProtocol::Queue,
        &format!("arn:aws:sqs:us-east-1:000000000000:{name}"),
    )
}

fn scheduler() -> DeliveryScheduler {
    DeliveryScheduler::new(
        SchedulerConfig {
            max_in_flight: 64,
            per_destination: 8,
            local_batch: 32,
            claim_batch: 32,
            breaker: BreakerConfig {
                failures_to_open: NonZeroU32::new(5).unwrap(),
                open_initial: Duration::from_secs(5),
                open_max: Duration::from_secs(300),
                max_destinations: NonZeroUsize::new(3).unwrap(),
            },
            sweep_page: NonZeroU32::new(256).unwrap(),
            sweep_interval: Duration::from_secs(1),
        },
        None,
    )
}

/// A claim of `count` records of `destination`, with `next_due`.
fn claim(destination: &DestinationKey, count: usize, next_due: Option<i64>) -> DestinationClaim {
    let claimed = (0..count)
        .map(|index| {
            let state = DeliveryState {
                id: DeliveryRecordId(format!("{destination}-{index}")),
                destination: destination.clone(),
                due_at: at(30_000),
                subscription_arn: SubscriptionArn::new("arn:aws:sns:us-east-1:000000000000:t:s")
                    .unwrap(),
                retry_deadline: at(3_600_000),
                attempts: 0,
                generation: 1,
                lease: Some(DeliveryLease {
                    owner: "node".to_owned(),
                    nonce: uuid::Uuid::new_v4(),
                    until: at(30_000),
                }),
                last_failure: None,
            };
            ClaimedDelivery {
                fence: DeliveryFence::of(&state).unwrap(),
                payload: DeliveryPayload {
                    id: state.id.clone(),
                    kind: pubsub_provider::DeliveryRecordKind::Notification,
                    message_id: pubsub_provider::PubsubMessageId::new(),
                    destination: destination.clone(),
                    subscription: None,
                    message_body: None,
                    subject: None,
                    message_attributes: std::collections::HashMap::new(),
                    target: pubsub_provider::DeliveryTarget::BuiltIn,
                    created_at: at(0),
                },
                state,
            }
        })
        .collect();
    DestinationClaim {
        destination: destination.clone(),
        claimed,
        next_due: next_due.map(at),
    }
}

fn completed(destination: &DestinationKey, records: usize, signal: BreakerSignal) -> Completed {
    Completed {
        destination: destination.clone(),
        records,
        signal,
        retry_due: None,
        unsettled_until: None,
        jitter: 0,
    }
}

fn asks(scheduler: &mut DeliveryScheduler, now: i64) -> Vec<(DestinationKey, u32, AskMode)> {
    scheduler
        .next_claim(at(now), 0)
        .unwrap_or_default()
        .into_iter()
        .map(|ask| (ask.destination, ask.take.get(), ask.mode))
        .collect()
}

/// Five failures of one destination open its breaker.
fn open_breaker(scheduler: &mut DeliveryScheduler, destination: &DestinationKey, now: i64) {
    scheduler.on_materialized(&[(destination.clone(), at(now))], at(now));
    scheduler.on_claimed(&[claim(destination, 5, Some(now))], at(now));
    for _ in 0..5 {
        scheduler.on_completed(completed(destination, 1, BreakerSignal::Failed), at(now));
    }
}

/// Goal (ii): a slow destination with every slot busy does not delay a
/// healthy one: the next claim asks the healthy destination at once and
/// nothing is written for the slow one.
#[test]
fn given_slow_destination_saturated_when_healthy_destination_materialized_then_next_claim_asks_it()
{
    let mut core = scheduler();
    let slow = http("slow.example");
    core.on_materialized(&[(slow.clone(), at(0))], at(0));
    assert_eq!(asks(&mut core, 0), vec![(slow.clone(), 8, AskMode::Due)]);
    core.on_claimed(&[claim(&slow, 8, Some(0))], at(0));
    assert_eq!(
        asks(&mut core, 1),
        Vec::new(),
        "the slow destination is full"
    );
    let healthy = http("healthy.example");
    core.on_materialized(&[(healthy.clone(), at(2))], at(2));
    assert_eq!(asks(&mut core, 2), vec![(healthy, 8, AskMode::Due)]);
}

/// Goal (ii) with a dead destination: its open breaker is not asked; the
/// healthy destination is asked at the next claim.
#[test]
fn given_open_breaker_when_healthy_destination_materialized_then_next_claim_asks_only_it() {
    let mut core = scheduler();
    let dead = http("dead.example");
    open_breaker(&mut core, &dead, 0);
    core.on_sweep(
        &SweepPage {
            heads: vec![DestinationHead {
                destination: dead.clone(),
                head_due: at(0),
                head_retry_deadline: at(7_200_000),
            }],
            next: None,
        },
        at(1),
    );
    assert_eq!(asks(&mut core, 1), Vec::new());
    let healthy = http("healthy.example");
    core.on_materialized(&[(healthy.clone(), at(2))], at(2));
    assert_eq!(asks(&mut core, 2), vec![(healthy, 8, AskMode::Due)]);
    assert_eq!(core.open_breakers(), 1);
}

/// Goal (i) at the core: a dead destination is asked once per breaker
/// interval (one probe), so its writes follow the breaker cycle, not its
/// backlog.
#[test]
fn given_dead_destination_when_an_hour_passes_then_it_is_asked_once_per_breaker_interval() {
    let mut core = scheduler();
    let dead = http("dead.example");
    open_breaker(&mut core, &dead, 0);
    let mut probes = 0;
    let mut now = 0;
    while now < 3_600_000 {
        now += 100;
        let asked = asks(&mut core, now);
        if asked.is_empty() {
            continue;
        }
        assert_eq!(asked, vec![(dead.clone(), 1, AskMode::Probe)]);
        probes += 1;
        core.on_claimed(&[claim(&dead, 1, Some(now))], at(now));
        core.on_completed(completed(&dead, 1, BreakerSignal::Failed), at(now));
    }
    // 5 s doubling to 300 s with +-20% jitter (jitter 0: -20%): 4, 8, 16,
    // 32, 64, 128, 240 s, then 240 s; about 17 probes in an hour.
    assert!((15..=22).contains(&probes), "probes {probes}");
}

/// D-c: 32 records for a new destination: the first claim takes 8; the
/// other 24 start as slots return, with no deferral.
#[test]
fn given_burst_to_new_destination_when_claimed_then_eight_now_and_the_rest_as_slots_return() {
    let mut core = scheduler();
    let burst = http("burst.example");
    core.on_materialized(&[(burst.clone(), at(0))], at(0));
    let mut started = 0;
    for round in 0..4 {
        let asked = asks(&mut core, round);
        assert_eq!(
            asked,
            vec![(burst.clone(), 8, AskMode::Due)],
            "claim {round}"
        );
        let next_due = (started + 8 < 32).then_some(0);
        core.on_claimed(&[claim(&burst, 8, next_due)], at(round));
        started += 8;
        assert_eq!(
            asks(&mut core, round),
            Vec::new(),
            "full until a slot returns"
        );
        core.on_completed(completed(&burst, 8, BreakerSignal::Answered), at(round));
    }
    assert_eq!(started, 32);
    assert_eq!(asks(&mut core, 10), Vec::new(), "nothing is left");
}

/// Finding 8: a panicked task returns its slots (the runner maps the task
/// id to its destination and record count).
#[test]
fn given_eight_panicked_attempts_when_completed_then_the_next_claim_may_ask_eight() {
    let mut core = scheduler();
    let destination = http("panic.example");
    core.on_materialized(&[(destination.clone(), at(0))], at(0));
    core.on_claimed(&[claim(&destination, 8, Some(0))], at(0));
    assert_eq!(asks(&mut core, 1), Vec::new());
    for _ in 0..8 {
        let mut done = completed(&destination, 1, BreakerSignal::NotAttempted);
        done.unsettled_until = Some(at(30_000));
        core.on_completed(done, at(1));
    }
    assert_eq!(core.leased(), 0);
    assert_eq!(asks(&mut core, 2), vec![(destination, 8, AskMode::Due)]);
}

/// F6: an open breaker with a head past its retry deadline is asked for
/// those records only, bounded by its free slots.
#[test]
fn given_open_breaker_and_expired_head_when_claimed_then_deadline_only_with_free_slots() {
    let mut core = scheduler();
    let dead = http("dead.example");
    open_breaker(&mut core, &dead, 0);
    core.on_sweep(
        &SweepPage {
            heads: vec![DestinationHead {
                destination: dead.clone(),
                head_due: at(0),
                head_retry_deadline: at(500),
            }],
            next: None,
        },
        at(1_000),
    );
    assert_eq!(
        asks(&mut core, 1_000),
        vec![(dead, 8, AskMode::DeadlineOnly)]
    );
}

/// Finding 4: a local queue's slots count records; one queue can take a
/// claim of 32, and the node never leases more than `max_in_flight`.
#[test]
fn given_local_queue_backlog_when_claimed_then_slots_count_records_up_to_the_node_limit() {
    let mut core = scheduler();
    let local = queue("local");
    let other = queue("other");
    let third = queue("third");
    core.on_materialized(
        &[
            (local.clone(), at(0)),
            (other.clone(), at(0)),
            (third.clone(), at(0)),
        ],
        at(0),
    );
    let first = asks(&mut core, 0);
    assert_eq!(first.iter().map(|ask| ask.1).sum::<u32>(), 32);
    for (destination, take, _) in &first {
        core.on_claimed(&[claim(destination, *take as usize, Some(0))], at(0));
    }
    let second = asks(&mut core, 0);
    assert_eq!(second.iter().map(|ask| ask.1).sum::<u32>(), 32);
    for (destination, take, _) in &second {
        core.on_claimed(&[claim(destination, *take as usize, Some(0))], at(0));
    }
    assert_eq!(core.leased(), 64);
    assert_eq!(
        asks(&mut core, 0),
        Vec::new(),
        "the node holds max_in_flight"
    );
}

/// R6: an evicted destination is found again by the sweep and asked.
#[test]
fn given_evicted_destination_when_sweep_returns_it_then_it_is_asked() {
    let mut core = scheduler();
    let evicted = http("a.example");
    core.on_materialized(&[(evicted.clone(), at(5_000))], at(0));
    for name in ["b.example", "c.example", "d.example"] {
        core.on_materialized(&[(http(name), at(9_000))], at(0));
    }
    assert_eq!(
        asks(&mut core, 6_000),
        Vec::new(),
        "evicted: the candidate set holds 3"
    );
    let (cursor, limit) = core.sweep_request(at(6_000)).unwrap();
    assert_eq!((cursor, limit.get()), (SweepCursor::from_start(None), 256));
    core.on_sweep(
        &SweepPage {
            heads: vec![DestinationHead {
                destination: evicted.clone(),
                head_due: at(5_000),
                head_retry_deadline: at(3_600_000),
            }],
            next: None,
        },
        at(6_000),
    );
    assert!(
        core.sweep_request(at(6_500)).is_none(),
        "one page per interval"
    );
    assert_eq!(asks(&mut core, 6_000), vec![(evicted, 8, AskMode::Due)]);
}

/// Timers: a retry settle and a claim's future `next_due` wake the runner
/// then; an empty claim pauses the destination.
#[test]
fn given_retry_and_next_due_when_waiting_then_the_core_wakes_at_the_earliest() {
    let mut core = scheduler();
    let destination = http("timer.example");
    core.on_sweep(
        &SweepPage {
            heads: Vec::new(),
            next: None,
        },
        at(0),
    );
    core.on_materialized(&[(destination.clone(), at(0))], at(0));
    core.on_claimed(&[claim(&destination, 1, Some(4_000))], at(0));
    let mut done = completed(&destination, 1, BreakerSignal::Failed);
    done.retry_due = Some(at(700));
    core.on_completed(done, at(10));
    assert_eq!(core.next_wake(at(10)), Some(at(700)));
    assert_eq!(
        asks(&mut core, 700),
        vec![(destination.clone(), 8, AskMode::Due)]
    );
    core.on_claimed(&[claim(&destination, 0, Some(0))], at(700));
    assert_eq!(asks(&mut core, 701), Vec::new(), "an empty claim pauses");
    assert_eq!(core.next_wake(at(701)), Some(at(800)));
    assert_eq!(core.oldest_due_age(at(900)), Duration::from_millis(100));
}

/// W3S review finding 3: a failure in the current breaker window stops
/// refills while records are in flight; the destination is asked again
/// only when they have all ended.
#[test]
fn given_failure_in_window_when_a_slot_returns_then_the_destination_is_not_refilled() {
    let mut core = scheduler();
    let dead = http("dead.example");
    core.on_materialized(&[(dead.clone(), at(0))], at(0));
    assert_eq!(asks(&mut core, 0), vec![(dead.clone(), 8, AskMode::Due)]);
    core.on_claimed(&[claim(&dead, 8, Some(0))], at(0));
    core.on_completed(completed(&dead, 1, BreakerSignal::Failed), at(1));
    assert_eq!(asks(&mut core, 1), Vec::new(), "no refill after a failure");
    for _ in 0..3 {
        core.on_completed(completed(&dead, 1, BreakerSignal::Answered), at(2));
    }
    assert_eq!(
        asks(&mut core, 2),
        vec![(dead.clone(), 4, AskMode::Due)],
        "an answer closes the window"
    );
}

/// W3S review finding 4: a failed claim keeps each asked destination's
/// head time and pauses it for 100 ms.
#[test]
fn given_failed_claim_when_recorded_then_the_head_is_kept_and_paused() {
    let mut core = scheduler();
    let destination = http("busy.example");
    core.on_materialized(&[(destination.clone(), at(0))], at(0));
    let asked = core.next_claim(at(0), 0).unwrap();
    core.on_claim_failed(&asked, at(0));
    assert_eq!(asks(&mut core, 99), Vec::new(), "paused");
    assert_eq!(asks(&mut core, 100), vec![(destination, 8, AskMode::Due)]);
}

/// W3S review finding 7: eviction takes a closed entry before an open one,
/// so a dead destination keeps its breaker while it is open.
#[test]
fn given_full_candidate_set_when_a_new_destination_arrives_then_an_open_breaker_is_kept() {
    let mut core = scheduler();
    let dead = http("dead.example");
    open_breaker(&mut core, &dead, 0);
    for name in ["b.example", "c.example"] {
        core.on_materialized(&[(http(name), at(10))], at(10));
    }
    core.on_materialized(&[(http("d.example"), at(20))], at(20));
    assert_eq!(core.open_breakers(), 1, "the open entry stays");
}

/// W3S review finding 8: after the open interval a due head wakes the
/// runner at the head time, not only at the next sweep.
#[test]
fn given_open_interval_passed_when_head_is_later_then_next_wake_is_the_head() {
    let mut core = scheduler();
    let dead = http("dead.example");
    open_breaker(&mut core, &dead, 0);
    core.on_sweep(
        &SweepPage {
            heads: Vec::new(),
            next: None,
        },
        at(0),
    );
    // The breaker opened at 0 for 4 s (jitter 0); the head is due at 7 s.
    core.on_claimed(&[claim(&dead, 0, Some(7_000))], at(6_900));
    assert_eq!(core.next_wake(at(6_950)), Some(at(7_000)));
}
