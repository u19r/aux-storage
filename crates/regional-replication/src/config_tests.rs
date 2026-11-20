use std::time::Duration;

use storage_types::RegionId;

use crate::config::RegionalConfig;

fn region(rank: u16) -> RegionId {
    RegionId(std::num::NonZeroU16::new(rank).unwrap())
}

fn file(regions: &str, extra: &str) -> String {
    format!(r#"{{"local_region":"a","listen":"127.0.0.1:7000","regions":{regions}{extra}}}"#)
}

const TWO: &str = r#"[{"name":"a","rank":1,"address":"a.example:7000"},{"name":"b","rank":2,"address":"b.example:7000"}]"#;

#[test]
fn given_two_regions_when_loaded_then_local_and_peer_split() {
    let config = RegionalConfig::from_json(&file(TWO, "")).unwrap();
    assert_eq!(
        (config.local.name.as_str(), config.local.id),
        ("a", region(1))
    );
    assert_eq!(
        (config.peer.name.as_str(), config.peer.id),
        ("b", region(2))
    );
    assert_eq!(config.peer.address, "b.example:7000");
    assert_eq!(config.page_entry_limit, 1000);
    assert_eq!(config.request_timeout, Duration::from_secs(30));
    assert_eq!(config.region_named("b").unwrap(), &config.peer);
    assert_eq!(config.region(region(1)).unwrap(), &config.local);
    assert!(config.region_named("c").is_none());
    assert!(config.region(region(3)).is_none());

    // The local region may be listed second.
    let swapped = r#"[{"name":"b","rank":2,"address":"b.example:7000"},{"name":"a","rank":1,"address":"a.example:7000"}]"#;
    let config = RegionalConfig::from_json(&file(swapped, "")).unwrap();
    assert_eq!(config.local.name, "a");
    assert_eq!(config.peer.name, "b");
}

#[test]
fn given_invalid_files_when_loaded_then_validation() {
    let one = r#"[{"name":"a","rank":1,"address":"a:1"}]"#;
    let three = r#"[{"name":"a","rank":1,"address":"a:1"},{"name":"b","rank":2,"address":"b:1"},{"name":"c","rank":3,"address":"c:1"}]"#;
    let equal_ranks =
        r#"[{"name":"a","rank":1,"address":"a:1"},{"name":"b","rank":1,"address":"b:1"}]"#;
    let rank_zero =
        r#"[{"name":"a","rank":0,"address":"a:1"},{"name":"b","rank":2,"address":"b:1"}]"#;
    let local_unlisted =
        r#"[{"name":"x","rank":1,"address":"a:1"},{"name":"b","rank":2,"address":"b:1"}]"#;
    let cases = [
        file(one, ""),
        file(three, ""),
        file(equal_ranks, ""),
        file(rank_zero, ""),
        file(local_unlisted, ""),
        file(TWO, r#","page_byte_limit":2048,"frame_limit_bytes":1024"#),
        file(TWO, r#","unknown_field":1"#),
    ];
    for json in cases {
        let error = RegionalConfig::from_json(&json).unwrap_err();
        assert!(
            matches!(
                error.base_error(),
                Some(
                    storage_types::StorageEnum::Validation { .. }
                        | storage_types::StorageEnum::RawValidation { .. }
                )
            ),
            "{json}: {error:?}"
        );
    }
}
