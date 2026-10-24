use std::{hint::black_box, time::Instant};

use alloc_counter::AllocationGuard;
use storage_types::UserStreamName;
use stream_provider::{CursorName, CursorPosition};

use crate::{
    stream_test_support_tests::create_test_manager,
    validation::{CursorNameValidation, UserStreamNameValidation},
};

const ITERATIONS: usize = 64;

#[tokio::test]
#[ignore = "isolated allocation measurement; run alone with --test-threads=1"]
async fn given_stream_workloads_when_measuring_then_report_actual_provider_allocations() {
    for (label, name) in [
        ("single", "é"),
        ("invalid-middle", "a!b"),
        ("invalid-boundary", "_ab"),
    ] {
        let name = UserStreamName::new(name);
        report_sync(&format!("validation-stream-{label}"), || {
            assert_eq!(
                black_box(name.validate_stream_name()).is_ok(),
                label == "single"
            );
        });
    }
    for (label, length) in [("short", 8), ("long", 255), ("unicode", 63)] {
        let name = if label == "unicode" {
            format!("{}a", "é".repeat(31))
        } else {
            "a".repeat(length)
        };
        let stream_name = UserStreamName::new(&name);
        report_sync(&format!("validation-stream-{label}"), || {
            black_box(stream_name.validate_stream_name()).unwrap();
        });
        let cursor_name = CursorName::new(if label == "long" {
            name.get(..64).expect("ASCII cursor name boundary")
        } else {
            &name
        });
        report_sync(&format!("validation-cursor-{label}"), || {
            black_box(cursor_name.validate_cursor_name()).unwrap();
        });

        let manager = create_test_manager().await;
        manager.create_stream(&name, None).await.unwrap();
        let payload = vec![7; 1024];
        manager.append_item(&name, &payload).await.unwrap();
        manager.read_backward(&name, None, Some(1)).await.unwrap();
        for operation in ["append", "read", "missing", "create", "cursor"] {
            let name_limit = if operation == "cursor" { 64 } else { 255 };
            let mut suffix_end = name.len().min(name_limit) - 4;
            while !name.is_char_boundary(suffix_end) {
                suffix_end -= 1;
            }
            let names: Vec<_> = (0..ITERATIONS)
                .map(|index| {
                    format!(
                        "{index:04}{}",
                        name.get(..suffix_end).expect("validated name boundary")
                    )
                })
                .collect();
            let label = format!("sqlite-{operation}-{label}");
            let guard = start_measurement(&label);
            let start = Instant::now();
            for candidate_name in &names {
                match operation {
                    "append" => {
                        black_box(manager.append_item(&name, &payload).await.unwrap());
                    }
                    "read" => {
                        let page = manager.read_backward(&name, None, Some(1)).await.unwrap();
                        assert_eq!(page.items.len(), 1);
                        assert_eq!(page.items[0].data, payload);
                    }
                    "missing" => {
                        assert!(manager.append_item(candidate_name, &payload).await.is_err());
                    }
                    "create" => {
                        manager.create_stream(candidate_name, None).await.unwrap();
                    }
                    "cursor" => {
                        manager
                            .create_cursor(&name, candidate_name, CursorPosition::Head)
                            .await
                            .unwrap();
                    }
                    _ => unreachable!(),
                }
            }
            let elapsed = start.elapsed();
            let report = guard.finish();
            let _ = alloc_counter::emit_report(&report);
            println!(
                "WORKLOAD label={label} operations={ITERATIONS} elapsed_ns={}",
                elapsed.as_nanos()
            );
        }
    }
}

fn report_sync(label: &str, operation: impl Fn()) {
    let guard = start_measurement(label);
    for _ in 0..ITERATIONS {
        operation();
    }
    let _ = alloc_counter::emit_report(&guard.finish());
}

fn start_measurement(label: &str) -> AllocationGuard<'_> {
    AllocationGuard::start(
        module_path!(),
        "stream_provider_allocations",
        file!(),
        line!(),
        Some(label),
    )
}
