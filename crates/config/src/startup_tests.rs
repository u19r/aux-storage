use std::{path::PathBuf, str::FromStr};

use crate::startup::{
    ConfigStartupError, GeneratedProfileMetadata, StartupIntent, StartupProfileName,
    load_startup_config,
};

#[test]
fn startup_profile_names_round_trip_through_cli_strings() {
    for profile in [
        StartupProfileName::Test,
        StartupProfileName::SingleNode,
        StartupProfileName::Storage,
        StartupProfileName::RemoteStorage,
    ] {
        assert_eq!(
            StartupProfileName::from_str(profile.as_str()).expect("profile"),
            profile
        );
    }
}

#[test]
fn unknown_startup_profile_is_invalid_input() {
    let error = StartupProfileName::from_str("unknown").expect_err("unknown profile");

    assert!(matches!(error, ConfigStartupError::InvalidInput { .. }));
    assert!(error.to_string().contains("unknown startup profile"));
}

#[test]
fn config_path_and_profile_are_mutually_exclusive() {
    let intent = StartupIntent {
        config_path: Some(PathBuf::from("config.toml")),
        profile: Some("test".to_string()),
        storage_nodes: Vec::new(),
    };

    let error = load_startup_config(intent, |_, _| {
        Ok::<_, std::io::Error>(GeneratedProfileMetadata {
            profile: StartupProfileName::Test,
            config_path: PathBuf::from("generated.toml"),
            warnings: Vec::new(),
        })
    })
    .expect_err("mutually exclusive");

    assert!(matches!(error, ConfigStartupError::InvalidInput { .. }));
}

#[test]
fn profile_generation_errors_include_selected_profile() {
    let intent = StartupIntent {
        config_path: None,
        profile: Some("storage".to_string()),
        storage_nodes: vec!["node-a".to_string()],
    };

    let error = load_startup_config(intent, |profile, nodes| {
        assert_eq!(profile, StartupProfileName::Storage);
        assert_eq!(nodes, vec!["node-a"]);
        Err::<GeneratedProfileMetadata, _>("write failed")
    })
    .expect_err("generation error");

    assert!(matches!(
        error,
        ConfigStartupError::ProfileGeneration {
            profile: StartupProfileName::Storage,
            ..
        }
    ));
    assert!(error.to_string().contains("write failed"));
}
