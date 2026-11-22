#[path = "../../../tests/support/mod.rs"]
mod host;
pub use host::isolate_host;

pub fn enable_test_terminal() {
    isolate_host();
    // The isolated host has no login shell. Supply an interactive-only shell
    // for this PTY; a PATH hydration request passes arguments and exits.
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let shell = std::env::var_os("SHELL").unwrap();
        std::fs::write(
            &shell,
            r#"#!/bin/sh
[ "$#" -eq 0 ] || exit 1
PS1= ENV= exec /bin/sh
"#,
        )
        .unwrap();
        std::fs::set_permissions(&shell, std::fs::Permissions::from_mode(0o700)).unwrap();
    }
}
