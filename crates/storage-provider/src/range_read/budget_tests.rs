use storage_types::{
    AttributeValue, ItemKey, LogicalItemSize, StorageEnum, TableName, context::WrappedError as _,
};

use super::{RangeAdmission, RangeReadBudget, RangeReadProgress};

fn key(number: i64) -> ItemKey {
    ItemKey::table_key(
        TableName::new("range-page"),
        AttributeValue::N(number.to_string()),
        None,
    )
}

fn size(bytes: u64) -> LogicalItemSize {
    LogicalItemSize::try_from(bytes).expect("positive logical item size")
}

fn progress_signature(progress: &RangeReadProgress) -> (u32, u64, Option<String>) {
    (
        progress.evaluated_count,
        progress.evaluated_bytes,
        progress
            .last_evaluated_key
            .as_ref()
            .map(|key| key.next_page_token().expect("valid item key")),
    )
}

#[test]
fn given_zero_item_budget_when_try_new_then_rejects_budget() {
    let error = RangeReadBudget::try_new(0, 1).expect_err("zero item budget");

    assert!(error.to_string().contains("positive"));
}

#[test]
fn given_zero_byte_budget_when_try_new_then_rejects_budget() {
    let error = RangeReadBudget::try_new(1, 0).expect_err("zero byte budget");

    assert!(error.to_string().contains("positive"));
}

#[test]
fn given_exact_item_and_byte_limits_when_admit_then_accepts_boundary_and_stops_next() {
    let budget = RangeReadBudget::try_new(3, 7).expect("byte budget");
    let mut progress = RangeReadProgress::default();

    assert_eq!(
        budget
            .admit(&mut progress, key(1), size(4))
            .expect("first item"),
        RangeAdmission::Admit
    );
    assert_eq!(
        budget
            .admit(&mut progress, key(2), size(3))
            .expect("exact byte boundary"),
        RangeAdmission::Admit
    );
    assert_eq!(progress.evaluated_count, 2);
    assert_eq!(progress.evaluated_bytes, 7);
    assert_eq!(
        progress_signature(&progress).2,
        Some(key(2).next_page_token().expect("key"))
    );

    let before_stop = progress_signature(&progress);
    assert_eq!(
        budget
            .admit(&mut progress, key(3), size(4))
            .expect("item limit stop"),
        RangeAdmission::Stop
    );
    assert_eq!(progress_signature(&progress), before_stop);

    let item_budget = RangeReadBudget::try_new(1, u64::MAX).expect("item budget");
    let mut item_limited_progress = RangeReadProgress::default();
    assert_eq!(
        item_budget
            .admit(&mut item_limited_progress, key(1), size(4))
            .expect("item boundary first item"),
        RangeAdmission::Admit
    );
    let before_item_stop = progress_signature(&item_limited_progress);
    assert_eq!(
        item_budget
            .admit(&mut item_limited_progress, key(2), size(3))
            .expect("item limit stop"),
        RangeAdmission::Stop
    );
    assert_eq!(progress_signature(&item_limited_progress), before_item_stop);
}

#[test]
fn given_reverse_page_when_first_budget_stops_then_next_page_resumes_after_last_key() {
    let first_page_budget = RangeReadBudget::try_new(3, 4).expect("first page budget");
    let mut first_page = RangeReadProgress::default();

    assert_eq!(
        first_page_budget
            .admit(&mut first_page, key(3), size(4))
            .expect("reverse first item"),
        RangeAdmission::Admit
    );
    let first_page_boundary = progress_signature(&first_page);
    assert_eq!(
        first_page_boundary.2,
        Some(key(3).next_page_token().expect("reverse boundary key"))
    );
    assert_eq!(
        first_page_budget
            .admit(&mut first_page, key(2), size(3))
            .expect("reverse byte stop"),
        RangeAdmission::Stop
    );
    assert_eq!(progress_signature(&first_page), first_page_boundary);

    let second_page_budget = RangeReadBudget::try_new(3, 4).expect("second page budget");
    let mut second_page = RangeReadProgress::default();
    assert_eq!(
        second_page_budget
            .admit(&mut second_page, key(2), size(3))
            .expect("resume after reverse boundary"),
        RangeAdmission::Admit
    );
    assert_eq!(second_page.evaluated_count, 1);
    assert_eq!(second_page.evaluated_bytes, 3);
    assert_eq!(
        progress_signature(&second_page).2,
        Some(key(2).next_page_token().expect("key"))
    );
}

#[test]
fn given_count_overflow_when_admit_then_leaves_progress_unchanged() {
    let budget = RangeReadBudget::try_new(u32::MAX, 1).expect("budget");
    let mut progress = RangeReadProgress {
        evaluated_count: u32::MAX,
        evaluated_bytes: 1,
        last_evaluated_key: Some(key(1)),
    };
    let before = progress_signature(&progress);

    let error = budget
        .admit(&mut progress, key(2), size(1))
        .expect_err("count overflow");

    assert!(matches!(
        error.to_enum(),
        StorageEnum::InternalServerError { message } if message.contains("count overflow")
    ));
    assert_eq!(progress_signature(&progress), before);
}

#[test]
fn given_byte_overflow_when_admit_then_leaves_progress_unchanged() {
    let budget = RangeReadBudget::try_new(1, u64::MAX).expect("budget");
    let mut progress = RangeReadProgress {
        evaluated_count: 0,
        evaluated_bytes: u64::MAX,
        last_evaluated_key: Some(key(1)),
    };
    let before = progress_signature(&progress);

    let error = budget
        .admit(&mut progress, key(2), size(1))
        .expect_err("byte overflow");

    assert!(matches!(
        error.to_enum(),
        StorageEnum::InternalServerError { message } if message.contains("byte count overflow")
    ));
    assert_eq!(progress_signature(&progress), before);
}
