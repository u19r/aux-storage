use replication_contract_tests::{HarnessResult, regional::fixture::RegionalFixture};

#[test]
fn given_partitioned_regions_when_reordered_duplicates_arrive_then_history_and_winner_converge()
-> HarnessResult<()> {
    for reverse in [false, true] {
        for copies in [1, 3] {
            let mut fixture = RegionalFixture::new()?;
            for position in 0..100 {
                fixture.us_east_1.write(1000 + position, Some(b"left"))?;
                fixture
                    .eu_central_1
                    .write(1000 + position, Some(b"right"))?;
            }
            fixture.heal(reverse, copies)?;
            fixture.assert_converged()?;
            assert_eq!(200, fixture.us_east_1.history().len());
            assert!(fixture.us_east_1.regional_stream().len() < fixture.us_east_1.history().len());
        }
    }
    Ok(())
}
#[test]
fn given_imported_future_clock_when_local_clock_moves_back_then_local_write_wins()
-> HarnessResult<()> {
    let mut fixture = RegionalFixture::new()?;
    fixture.us_east_1.write(100_000, Some(b"future"))?;
    fixture.heal(true, 2)?;
    let local = fixture.eu_central_1.write(1, Some(b"after observation"))?;
    fixture.heal(true, 2)?;
    fixture.assert_converged()?;
    assert_eq!(Some(&local), fixture.us_east_1.winner());
    Ok(())
}
#[test]
fn given_conflicting_delete_when_delivered_before_older_put_then_no_resurrection()
-> HarnessResult<()> {
    let mut fixture = RegionalFixture::new()?;
    fixture.us_east_1.write(10, Some(b"old"))?;
    let deleted = fixture.eu_central_1.write(20, None)?;
    fixture.heal(true, 3)?;
    fixture.assert_converged()?;
    assert_eq!(Some(&deleted), fixture.us_east_1.winner());
    assert_eq!(2, fixture.us_east_1.history().len());
    Ok(())
}
#[test]
fn given_same_ttl_victim_when_both_regions_expire_then_one_child_and_newer_put_survives()
-> HarnessResult<()> {
    let mut fixture = RegionalFixture::new()?;
    let victim = fixture.us_east_1.write(10, Some(b"expires"))?;
    fixture.heal(false, 1)?;
    let left = victim.expiration()?;
    let right = fixture
        .eu_central_1
        .winner()
        .ok_or("missing victim")?
        .expiration()?;
    assert_eq!(left, right);
    fixture.us_east_1.accept(left)?;
    fixture.eu_central_1.accept(right)?;
    fixture.heal(true, 3)?;
    fixture.assert_converged()?;
    assert_eq!(2, fixture.us_east_1.history().len());
    let newer = fixture.eu_central_1.write(11, Some(b"new"))?;
    fixture.us_east_1.accept(victim.expiration()?)?;
    fixture.heal(true, 2)?;
    fixture.assert_converged()?;
    assert_eq!(Some(&newer), fixture.us_east_1.winner());
    Ok(())
}
#[test]
fn given_existing_identity_when_changed_payload_arrives_then_history_is_unchanged()
-> HarnessResult<()> {
    let mut fixture = RegionalFixture::new()?;
    let event = fixture.us_east_1.write(1, Some(b"accepted"))?;
    fixture.eu_central_1.accept(event.clone())?;
    let mut corrupt = event;
    corrupt.value = None;
    assert!(fixture.eu_central_1.accept(corrupt).is_err());
    fixture.assert_converged()
}

#[test]
fn given_six_conflicting_events_when_all_delivery_permutations_run_then_max_and_union_are_invariant()
-> HarnessResult<()> {
    let mut fixture = RegionalFixture::new()?;
    let mut events = Vec::new();
    for clock in [10, 20, 30] {
        events.push(fixture.us_east_1.write(clock, Some(b"left"))?);
        events.push(fixture.eu_central_1.write(clock, Some(b"right"))?);
    }
    let expected = events
        .iter()
        .max_by_key(|event| &event.winner.order)
        .ok_or("empty events")?;
    let mut permutations = Vec::new();
    permute(&mut [0, 1, 2, 3, 4, 5], 0, &mut permutations);
    assert_eq!(720, permutations.len());
    for permutation in permutations {
        let mut replica =
            replication_contract_tests::regional::replica::RegionalReplica::new("us-east-2")?;
        for index in permutation {
            replica.accept(events[index].clone())?;
        }
        assert_eq!(Some(expected), replica.winner());
        assert_eq!(6, replica.history().len());
    }
    Ok(())
}
fn permute(indices: &mut [usize; 6], next: usize, output: &mut Vec<[usize; 6]>) {
    if next == indices.len() {
        output.push(*indices);
        return;
    }
    for index in next..indices.len() {
        indices.swap(next, index);
        permute(indices, next + 1, output);
        indices.swap(next, index);
    }
}

#[test]
fn given_three_partitioned_regions_when_seeded_multi_key_schedules_heal_then_exact_max_and_union_converge()
-> HarnessResult<()> {
    for seed in [0, 1, 17, 42, 65535, u64::MAX] {
        let report = replication_contract_tests::regional::stress::exercise(seed, 512)?;
        assert_eq!(32, report.keys);
        assert!(report.events > 512);
        assert!(report.deliveries > report.events * 3);
    }
    Ok(())
}
#[test]
fn given_observed_future_and_deleted_key_when_restart_and_clock_regression_occur_then_no_identity_reuse()
-> HarnessResult<()> {
    use replication_contract_tests::regional::{event::ItemMutation, replica::RegionalReplica};
    let mut source = RegionalReplica::new("us-east-1")?;
    let deleted = source.write_item(
        1_000_000,
        ItemMutation {
            key: b"key",
            value: None,
        },
    )?;
    let original_stream = source.regional_stream().to_vec();
    let mut recovered = source.restart()?;
    assert_eq!(original_stream, recovered.regional_stream());
    let later = recovered.write_item(
        0,
        ItemMutation {
            key: b"key",
            value: Some(b"after recovery"),
        },
    )?;
    assert_ne!(deleted.winner.event, later.winner.event);
    assert!(later.winner.order > deleted.winner.order);
    assert_eq!(2, recovered.history().len());
    Ok(())
}
