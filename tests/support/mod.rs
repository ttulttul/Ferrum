//! What every integration test that builds a server shares.

use std::path::{Path, PathBuf};

/// Agent CLIs a server may probe or start, under every name either server
/// looks one up by (as `PROVIDER_CLIS` in `scripts/parity_harness.py`).
pub const PROVIDER_CLIS: &[&str] = &[
    "codex",
    "claude",
    "cursor-agent",
    "cursor",
    "opencode",
    "gemini",
    "grok",
    "kiro-cli",
    "droid",
    "amp",
    "copilot",
    "qwen",
    "fx",
    "muse",
    "pi",
    "agy",
    "antigravity",
    "agy_acp_server.par",
];

/// Makes this test process a host without provider CLIs, as CI runners are,
/// so no test can start a developer's real `codex` or `claude` (a status
/// check, a turn, a model listing): `PATH` keeps every other program, `HOME`
/// is a temporary directory, and `SHELL` names no shell, so a login-shell
/// refresh cannot put `~/.local/bin` or nvm back on `PATH`.
///
/// Call it before starting a server. The first call in a process sets the
/// environment; later ones wait for it.
pub fn isolate_host() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        let root = std::env::temp_dir().join(format!("t3-test-host-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).unwrap();
        let path = std::env::var_os("PATH").unwrap_or_default();
        let directories: Vec<PathBuf> = std::env::split_paths(&path)
            .enumerate()
            .map(|(index, directory)| {
                without_provider_clis(&directory, &root.join(format!("path-{index}")))
            })
            .collect();
        let home = root.join("home");
        std::fs::create_dir_all(&home).unwrap();
        // Before any server thread exists: the only time this is sound.
        std::env::set_var("PATH", std::env::join_paths(directories).unwrap());
        std::env::set_var("HOME", &home);
        std::env::set_var("SHELL", root.join("no-login-shell"));
    });
}

/// `directory`, or a copy of it made of links that leaves the provider CLIs out.
fn without_provider_clis(directory: &Path, shim: &Path) -> PathBuf {
    if !PROVIDER_CLIS
        .iter()
        .any(|name| directory.join(name).exists())
    {
        return directory.to_owned();
    }
    std::fs::create_dir_all(shim).unwrap();
    for entry in std::fs::read_dir(directory).into_iter().flatten().flatten() {
        let name = entry.file_name();
        if !PROVIDER_CLIS.iter().any(|cli| name == *cli) {
            let _ = std::os::unix::fs::symlink(entry.path(), shim.join(&name));
        }
    }
    shim.to_owned()
}
