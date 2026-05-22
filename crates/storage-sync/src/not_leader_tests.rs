use crate::SyncNotLeader;

#[test]
fn not_leader_message_includes_leader_hint_when_known() {
    let error = SyncNotLeader::new(Some("http://leader.test/storage".to_string()));

    assert!(
        error
            .message()
            .contains("retry against http://leader.test/storage")
    );
}
