//! Every integration test that builds a server first hides the host's
//! provider CLIs (`support::isolate_host`). CI runners have no `codex` or
//! `claude`, so only this check notices a test that would start a
//! developer's real ones: a turn, a status check or a model listing.

#[test]
fn every_server_test_isolates_the_host() {
    let tests = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests");
    let mut unguarded = Vec::new();
    for entry in std::fs::read_dir(&tests).unwrap() {
        let path = entry.unwrap().path();
        if path.extension().is_none_or(|extension| extension != "rs") {
            continue;
        }
        let text = std::fs::read_to_string(&path).unwrap();
        let builds_server = text.contains("AppState::") || text.contains("serve_bound(");
        if builds_server && !text.contains("support::isolate_host()") {
            unguarded.push(path.file_name().unwrap().to_string_lossy().into_owned());
        }
    }
    assert!(
        unguarded.is_empty(),
        "these tests build a server without support::isolate_host(), so it could run the \
         host's real provider CLIs: {unguarded:?}"
    );
}
