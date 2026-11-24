//! Why the preview browser would not start on this host, as the reference's
//! `PreviewBrowserHost` (`apps/server/src/preview/PreviewBrowserHost.ts`):
//! Chrome refusing its sandbox as root, a host that blocks the sandbox (the
//! AppArmor user-namespace restriction on Ubuntu 23.10+), or missing shared
//! libraries, each with the command that fixes it (`browser setup`).
use std::path::Path;
use std::time::Duration;

pub(crate) const NO_SANDBOX_SIGNATURE: &str = "No usable sandbox";
/// What chrome-headless-shell prints itself when started sandboxed as root.
const ROOT_SANDBOX_SIGNATURE: &str = "Running as root without --no-sandbox";
/// Set on Ubuntu 23.10+: unprivileged user namespaces need an AppArmor profile.
const USERNS_RESTRICTION: &str = "/proc/sys/kernel/apparmor_restrict_unprivileged_userns";
pub(crate) const APPARMOR_PROFILE_PATH: &str = "/etc/apparmor.d/t3-chrome-headless-shell";

/// Matches Ubuntu's Chrome profile; unconfined adds nothing beyond userns.
pub(crate) const APPARMOR_PROFILE: &str = "# Written by `t3 browser setup`: lets T3 Code's headless browser use Chrome's sandbox.
abi <abi/4.0>,
include <tunables/global>

profile t3-chrome-headless-shell /**/tools/chrome-headless-shell/*/*/chrome-headless-shell flags=(unconfined) {
  userns,

  include if exists <local/t3-chrome-headless-shell>
}
";

/// Ubuntu 24.04 and Debian 13 use some t64 library names; the first apt offers wins.
pub(crate) const DEBIAN_PACKAGES: &[&[&str]] = &[
    &["libnss3"],
    &["libglib2.0-0t64", "libglib2.0-0"],
    &["libatk1.0-0t64", "libatk1.0-0"],
    &["libatk-bridge2.0-0t64", "libatk-bridge2.0-0"],
    &["libatspi2.0-0t64", "libatspi2.0-0"],
    &["libdbus-1-3"],
    &["libx11-6"],
    &["libxcb1"],
    &["libxcomposite1"],
    &["libxdamage1"],
    &["libxext6"],
    &["libxfixes3"],
    &["libxrandr2"],
    &["libxkbcommon0"],
    &["libgbm1"],
    &["libasound2t64", "libasound2"],
    &["libexpat1"],
];

/// `PreviewBrowserRootError`'s message.
pub(crate) const ROOT_MESSAGE: &str = "T3 server is running as root; Chrome's sandbox cannot start. Run T3 as a nonroot user. Otherwise set `T3CODE_SERVER_BROWSER_SANDBOX=0` in the T3 server's environment and restart the server; this disables Chrome's sandbox. Manual: `T3CODE_SERVER_BROWSER_SANDBOX=0 t3 serve`. Systemd: add `Environment=T3CODE_SERVER_BROWSER_SANDBOX=0` under `[Service]`, reload systemd, then restart the service. `t3 browser setup` cannot fix root sandboxing.";

/// A diagnosed launch failure (`PreviewBrowserHostError`).
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum HostError {
    Sandbox {
        setup_command: String,
    },
    Libraries {
        setup_command: String,
        libraries: Vec<String>,
    },
    Root,
}

impl HostError {
    pub(crate) fn message(&self) -> String {
        match self {
            HostError::Sandbox { setup_command } => format!("This host blocks the sandbox T3's browser runs in (AppArmor on Ubuntu 23.10+). Run `{setup_command}` on the host once to allow it, then try again."),
            HostError::Libraries { setup_command, libraries } => format!("This host is missing libraries T3's browser needs ({}). Run `{setup_command}` on the host to install them, then try again.", libraries.join(", ")),
            HostError::Root => ROOT_MESSAGE.into(),
        }
    }
}

/// `sandboxDisabled`: Chrome keeps its sandbox unless `T3CODE_SERVER_BROWSER_SANDBOX=0`.
pub(crate) fn sandbox_disabled() -> bool {
    std::env::var("T3CODE_SERVER_BROWSER_SANDBOX").is_ok_and(|v| v == "0")
}

/// `HostProcess.UserId`.
pub(crate) fn user_id() -> u32 {
    #[cfg(unix)]
    {
        // SAFETY: getuid has no preconditions and cannot fail.
        unsafe { libc::getuid() }
    }
    #[cfg(not(unix))]
    {
        u32::MAX
    }
}

/// The command that sets this host up for the browser, run as root
/// (`resolveRootCliCommand`). sudo resets the environment, so it names this
/// binary by its full path, and the home when one was chosen.
pub(crate) fn setup_command(base_dir: Option<&Path>) -> String {
    let executable = std::env::current_exe()
        .map(|path| path.display().to_string())
        .unwrap_or_else(|_| "t3code-server".into());
    match base_dir {
        Some(base_dir) => format!(
            "sudo {executable} browser setup --base-dir {}",
            base_dir.display()
        ),
        None => format!("sudo {executable} browser setup"),
    }
}

/// The startup output the preview tabs' launcher reports: Playwright rewrites
/// Chrome's sandbox refusals into its "sandboxing failed" note, which is all
/// the reference's diagnosis sees of them.
pub(crate) fn launcher_output(stderr: &str) -> &'static str {
    let sandbox = ["crbug.com/357670", "No usable sandbox!", "crbug.com/638180"]
        .iter()
        .any(|marker| stderr.contains(marker));
    if sandbox {
        NO_SANDBOX_SIGNATURE
    } else {
        ""
    }
}

/// `diagnoseLaunchFailure`: what is wrong with this host, if it is the host.
pub(crate) async fn diagnose(
    executable: &Path,
    output: &str,
    setup_command: &str,
) -> Option<HostError> {
    if output.contains(ROOT_SANDBOX_SIGNATURE) {
        return Some(HostError::Root);
    }
    if output.contains(NO_SANDBOX_SIGNATURE) {
        // Chrome's refusal to sandbox as root reads as a sandbox failure too.
        return Some(if user_id() == 0 {
            HostError::Root
        } else {
            HostError::Sandbox {
                setup_command: setup_command.into(),
            }
        });
    }
    if !cfg!(target_os = "linux") {
        return None;
    }
    let libraries = missing_libraries(executable).await;
    (!libraries.is_empty()).then(|| HostError::Libraries {
        setup_command: setup_command.into(),
        libraries,
    })
}

/// A command's stdout and stderr, or nothing when it cannot run within 5 seconds.
async fn output_of(command: &str, args: &[&str]) -> String {
    let run = tokio::process::Command::new(command)
        .args(args)
        .stdin(std::process::Stdio::null())
        .kill_on_drop(true)
        .output();
    match tokio::time::timeout(Duration::from_secs(5), run).await {
        Ok(Ok(out)) => {
            let mut text = String::from_utf8_lossy(&out.stdout).into_owned();
            text.push_str(&String::from_utf8_lossy(&out.stderr));
            text
        }
        _ => String::new(),
    }
}

/// `missingLibraries`: the shared libraries the browser cannot load. Detected
/// with `--version`, since ldd can report misses that NixOS's nix-ld provides.
pub(crate) async fn missing_libraries(executable: &Path) -> Vec<String> {
    let executable = executable.display().to_string();
    let version = output_of(&executable, &["--version"]).await;
    let Some(loader_error) = loader_error(&version) else {
        return vec![];
    };
    let listed = not_found(&output_of("ldd", &[&executable]).await);
    if listed.is_empty() {
        vec![loader_error]
    } else {
        listed
    }
}

/// `LOADER_ERROR`: `error while loading shared libraries: <name>`.
fn loader_error(output: &str) -> Option<String> {
    let (_, rest) = output.split_once("error while loading shared libraries: ")?;
    let name: String = rest
        .chars()
        .take_while(|c| *c != ':' && !c.is_whitespace())
        .collect();
    (!name.is_empty()).then_some(name)
}

/// `MISSING_LIBRARY`: ldd's `<name> => not found` lines.
fn not_found(ldd: &str) -> Vec<String> {
    ldd.lines()
        .filter_map(|line| {
            let line = line.strip_suffix(" => not found")?;
            let name = line.trim_start();
            (!name.is_empty() && !name.contains(char::is_whitespace)).then(|| name.to_owned())
        })
        .collect()
}

/// `usernsRestricted`.
pub(crate) fn userns_restricted() -> bool {
    cfg!(target_os = "linux")
        && std::fs::read_to_string(USERNS_RESTRICTION).is_ok_and(|value| value.trim() == "1")
}

/// `sandboxBlocked`: restricted, and no profile on disk (only a failed launch
/// shows an unloaded one without root).
pub(crate) fn sandbox_blocked() -> bool {
    userns_restricted() && !Path::new(APPARMOR_PROFILE_PATH).exists()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reads_loader_and_ldd_output() {
        assert_eq!(
            loader_error("/x/chrome: error while loading shared libraries: libnss3.so: cannot open shared object file"),
            Some("libnss3.so".into())
        );
        assert_eq!(loader_error("Chrome 158"), None);
        assert_eq!(
            not_found("\tlinux-vdso.so.1 (0x7)\n\tlibnss3.so => not found\n\tlibc.so.6 => /lib/libc.so.6 (0x1)\n\tlibgbm.so.1 => not found"),
            ["libnss3.so", "libgbm.so.1"]
        );
        assert_eq!(
            launcher_output("x No usable sandbox! y"),
            NO_SANDBOX_SIGNATURE
        );
        assert_eq!(
            launcher_output("See https://crbug.com/638180."),
            NO_SANDBOX_SIGNATURE
        );
        assert_eq!(launcher_output("Missing X server"), "");
    }

    #[tokio::test]
    async fn diagnoses_root_before_the_sandbox_and_the_sandbox_before_libraries() {
        let missing = Path::new("/nonexistent/chrome-headless-shell");
        assert_eq!(
            diagnose(
                missing,
                "Running as root without --no-sandbox is not supported.",
                "fix"
            )
            .await,
            Some(HostError::Root)
        );
        let sandbox = diagnose(missing, "No usable sandbox! Update your kernel", "fix").await;
        assert_eq!(
            sandbox,
            Some(if user_id() == 0 {
                HostError::Root
            } else {
                HostError::Sandbox {
                    setup_command: "fix".into(),
                }
            })
        );
        assert_eq!(diagnose(missing, "", "fix").await, None);
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn names_the_libraries_the_loader_misses() {
        let dir = tempfile::tempdir().unwrap();
        let fake = dir.path().join("chrome-headless-shell");
        std::fs::write(&fake, "#!/bin/sh\necho \"$0: error while loading shared libraries: libgbm.so.1: cannot open shared object file\" >&2\nexit 127\n").unwrap();
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&fake, std::fs::Permissions::from_mode(0o755)).unwrap();
        let error = diagnose(&fake, "", "sudo fix").await.unwrap();
        assert_eq!(error.message(), "This host is missing libraries T3's browser needs (libgbm.so.1). Run `sudo fix` on the host to install them, then try again.");
    }
}
