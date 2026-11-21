//! The user's login-shell environment, read once at start-up.
//!
//! A server started by a service manager or a desktop launcher inherits a
//! bare `PATH` that misses directories the user's shell profile adds, such as
//! `~/.local/bin` where Claude Code installs itself. The reference server
//! (`fixPath` in `apps/server/src/os-jank.ts`) runs the user's login shell
//! before anything starts, reads its `PATH`, `__MISE_DIFF` and `JAVA_HOME`,
//! and rewrites its own environment with them. Every provider launch, status
//! probe and tool it starts later inherits that environment. This module
//! does the same, so it must run before any other thread starts.

use std::{
    io::Read,
    path::{Component, Path, PathBuf},
    process::{Command, Stdio},
    sync::mpsc,
    time::{Duration, Instant},
};

/// What the reference reads from the login shell. `__MISE_DIFF` must travel
/// with the shell's `PATH`, or `mise x` can rebuild `PATH` behind a same-named
/// wrapper that then runs itself forever.
pub const LOGIN_SHELL_ENV_NAMES: [&str; 3] = ["PATH", "__MISE_DIFF", "JAVA_HOME"];
const LOGIN_SHELL_TIMEOUT: Duration = Duration::from_secs(5);
const SLOW_LOGIN_SHELL_TIMEOUT: Duration = Duration::from_secs(20);

const DEBUG_TRAP_SHELLS: [&str; 5] = ["bash", "zsh", "sh", "ksh", "ksh93"];
const PRINTENV_LOOKUP_COMMAND: &str = "/bin/sh -c 'command -v printenv || { [ -n \"$1\" ] && PATH=\"$1\" command -v printenv; } || printf %s /usr/bin/printenv' t3";

/// Rewrites this process's `HOME` and `PATH` as the reference does at start-up.
/// `shell_environment_prepared` is the desktop bootstrap's flag: the launcher
/// already captured the shell, so only `HOME` is repaired.
///
/// Call this before any other thread exists: it sets environment variables.
pub fn fix_path(shell_environment_prepared: bool) {
    if !cfg!(any(target_os = "linux", target_os = "macos")) {
        return;
    }
    hydrate_home();
    if shell_environment_prepared {
        return;
    }
    hydrate_path();
}

fn hydrate_home() {
    if std::env::var("HOME").is_ok_and(|home| !home.trim().is_empty()) {
        return;
    }
    if let Some(home) = account().and_then(|account| account.home) {
        if !home.is_empty() {
            std::env::set_var("HOME", home);
        }
    }
}

fn hydrate_path() {
    let mut shell_environment = Vec::new();
    let mut waited_for_slow_shell = false;
    for shell in login_shell_candidates(
        std::env::var("SHELL").ok().as_deref(),
        account().and_then(|account| account.shell).as_deref(),
    ) {
        match read_environment_from_login_shell(&shell, LOGIN_SHELL_TIMEOUT) {
            Ok(environment) => shell_environment = environment,
            Err(error) => {
                warn(
                    &format!("Failed to read PATH from login shell {shell}."),
                    &error.message,
                );
                // A profile can be slow only while the machine boots. Wait for it once.
                if !waited_for_slow_shell && error.timed_out {
                    waited_for_slow_shell = true;
                    match read_environment_from_login_shell(&shell, SLOW_LOGIN_SHELL_TIMEOUT) {
                        Ok(environment) => shell_environment = environment,
                        Err(error) => warn(
                            &format!("Failed to read PATH from slow login shell {shell}."),
                            &error.message,
                        ),
                    }
                }
            }
        }
        if value(&shell_environment, "PATH").is_some() {
            break;
        }
    }
    let launchctl_path = if cfg!(target_os = "macos") && value(&shell_environment, "PATH").is_none()
    {
        read_path_from_launchctl()
    } else {
        None
    };
    apply_login_shell_environment(&shell_environment, launchctl_path.as_deref());
}

fn warn(message: &str, error: &str) {
    eprintln!("[server] {message} {error}");
}

fn value<'a>(environment: &'a [(String, String)], name: &str) -> Option<&'a str> {
    environment
        .iter()
        .find(|(key, _)| key == name)
        .map(|(_, value)| value.as_str())
}

fn apply_login_shell_environment(
    shell_environment: &[(String, String)],
    fallback_path: Option<&str>,
) {
    let shell_path = value(shell_environment, "PATH");
    let inherited = std::env::var("PATH").ok();
    if let Some(merged) = merge_path_entries(shell_path.or(fallback_path), inherited.as_deref()) {
        std::env::set_var("PATH", merged);
    }
    if shell_path.is_some() {
        if let Some(diff) = value(shell_environment, "__MISE_DIFF") {
            std::env::set_var("__MISE_DIFF", diff);
        }
    }
    // Android SDK tools are Java programs; a keg-only JDK is only reachable
    // through the JAVA_HOME the shell profile sets.
    if std::env::var_os("JAVA_HOME").is_none() {
        if let Some(java_home) = value(shell_environment, "JAVA_HOME") {
            std::env::set_var("JAVA_HOME", java_home);
        }
    }
}

/// The shells the reference tries, in order: `$SHELL`, the account's login
/// shell, then the platform default.
pub fn login_shell_candidates(shell: Option<&str>, user_shell: Option<&str>) -> Vec<String> {
    let fallback = if cfg!(target_os = "macos") {
        Some("/bin/zsh")
    } else if cfg!(target_os = "linux") {
        Some("/bin/bash")
    } else {
        None
    };
    let mut candidates: Vec<String> = Vec::new();
    for candidate in [
        shell.map(str::trim).filter(|s| !s.is_empty()),
        user_shell.map(str::trim).filter(|s| !s.is_empty()),
        fallback,
    ]
    .into_iter()
    .flatten()
    {
        if !candidates.iter().any(|seen| seen == candidate) {
            candidates.push(candidate.to_owned());
        }
    }
    candidates
}

/// `preferred` entries first, then `inherited` ones not already present.
/// POSIX entries are literal; only empty ones are dropped.
pub fn merge_path_entries(preferred: Option<&str>, inherited: Option<&str>) -> Option<String> {
    let mut merged: Vec<&str> = Vec::new();
    for value in [preferred, inherited].into_iter().flatten() {
        for entry in value.split(':') {
            if !entry.is_empty() && !merged.contains(&entry) {
                merged.push(entry);
            }
        }
    }
    (!merged.is_empty()).then(|| merged.join(":"))
}

fn env_capture_start(name: &str) -> String {
    format!("__T3CODE_ENV_{name}_START__")
}

fn env_capture_end(name: &str) -> String {
    format!("__T3CODE_ENV_{name}_END__")
}

fn posix_resolve(entry: &str, cwd: &Path) -> String {
    let joined = if entry.starts_with('/') {
        PathBuf::from(entry)
    } else {
        cwd.join(entry)
    };
    let mut parts: Vec<String> = Vec::new();
    for component in joined.components() {
        match component {
            Component::Normal(part) => parts.push(part.to_string_lossy().into_owned()),
            Component::ParentDir => {
                parts.pop();
            }
            _ => {}
        }
    }
    format!("/{}", parts.join("/"))
}

/// The command the reference passes to `<shell> -ilc` to print `names`
/// between markers, robust to profiles that echo or trap.
pub fn build_environment_capture_command(shell: &str, names: &[&str]) -> String {
    let executable = Path::new(shell)
        .file_name()
        .map(|name| name.to_string_lossy().to_lowercase())
        .unwrap_or_default();
    let is_nushell = executable.starts_with("nu");
    let syntax = match executable.as_str() {
        "bash" | "zsh" | "sh" | "ksh" | "ksh93" | "dash" | "ash" => {
            Some(("__T3CODE_PRINTENV=$(", " || "))
        }
        "fish" => Some(("set -l __T3CODE_PRINTENV (", "; or ")),
        _ => None,
    };
    let supports_fallback = syntax.is_some();
    let cwd = std::env::current_dir().unwrap_or_else(|_| PathBuf::from("/"));
    let inherited_path = std::env::var("PATH")
        .unwrap_or_default()
        .split(':')
        .filter(|entry| !entry.is_empty())
        .map(|entry| posix_resolve(entry, &cwd))
        .collect::<Vec<_>>()
        .join(":");
    let lookup_printenv = format!(
        "{PRINTENV_LOOKUP_COMMAND} '{}'",
        inherited_path.replace('\'', "'\\''")
    );
    let (assignment, or_else) = syntax.unwrap_or(("__T3CODE_PRINTENV=$(", " || "));
    let resolve_printenv = format!("{assignment}{lookup_printenv}); ");
    let capture = names
        .iter()
        .map(|name| {
            if is_nushell {
                let default_value = if *name == "PATH" {
                    "if ($env.PATH? | describe | str starts-with 'list') { \
                     $env.PATH | each {|p| if ($p | str starts-with '~') { \
                     $p | path expand --no-symlink } else { $p } } | str join (char esep) \
                     } else { $env.PATH? | default '' }"
                        .to_owned()
                } else {
                    format!("$env.{name}? | default ''")
                };
                let value = format!(
                    "if $env.{name}? == null {{ '' }} else if \
                     $env.ENV_CONVERSIONS?.{name}?.to_string? != null {{ \
                     $env.{name} | do $env.ENV_CONVERSIONS.{name}.to_string $env.{name} \
                     }} else {{ {default_value} }}"
                );
                return format!(
                    "print '{}'; print ({value}); print '{}'",
                    env_capture_start(name),
                    env_capture_end(name)
                );
            }
            let read = if supports_fallback {
                format!("printenv {name}{or_else}\"$__T3CODE_PRINTENV\" {name}{or_else}true")
            } else {
                format!("printenv {name} || true")
            };
            format!(
                "printf '%s\\n' '{}'; {read}; printf '%s\\n' '{}'",
                env_capture_start(name),
                env_capture_end(name)
            )
        })
        .collect::<Vec<_>>()
        .join("; ");
    let command = if supports_fallback {
        resolve_printenv + &capture
    } else {
        capture
    };
    if DEBUG_TRAP_SHELLS.contains(&executable.as_str()) {
        format!("(trap - DEBUG) 2>/dev/null && trap - DEBUG; {command}")
    } else {
        command
    }
}

/// The values `build_environment_capture_command` printed; unset or empty
/// names are omitted.
pub fn extract_environment_values(output: &str, names: &[&str]) -> Vec<(String, String)> {
    names
        .iter()
        .filter_map(|name| {
            let start_marker = env_capture_start(name);
            let start = output.find(&start_marker)? + start_marker.len();
            let end = start + output[start..].find(&env_capture_end(name))?;
            let mut value = &output[start..end];
            value = value
                .strip_prefix("\r\n")
                .or_else(|| value.strip_prefix('\n'))
                .unwrap_or(value);
            value = value
                .strip_suffix("\r\n")
                .or_else(|| value.strip_suffix('\n'))
                .unwrap_or(value);
            (!value.is_empty()).then(|| ((*name).to_owned(), value.to_owned()))
        })
        .collect()
}

struct ShellError {
    timed_out: bool,
    message: String,
}

fn read_environment_from_login_shell(
    shell: &str,
    timeout: Duration,
) -> Result<Vec<(String, String)>, ShellError> {
    let script = build_environment_capture_command(shell, &LOGIN_SHELL_ENV_NAMES);
    let output = run_detached(shell, &["-ilc", &script], timeout)?;
    Ok(extract_environment_values(&output, &LOGIN_SHELL_ENV_NAMES))
}

/// Runs `program` in its own session, as Node's `execFileSync` with
/// `detached: true` does, so job control on an inherited terminal cannot stop
/// it. Past `timeout` it is killed with SIGKILL.
fn run_detached(program: &str, args: &[&str], timeout: Duration) -> Result<String, ShellError> {
    let failed = |message: String| ShellError {
        timed_out: false,
        message,
    };
    let mut command = Command::new(program);
    command
        .args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null());
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;
        // SAFETY: setsid is async-signal-safe and touches no parent state.
        unsafe {
            command.pre_exec(|| {
                libc::setsid();
                Ok(())
            });
        }
    }
    let mut child = command
        .spawn()
        .map_err(|error| failed(format!("spawnSync {program} {error}")))?;
    let mut stdout = child.stdout.take().expect("piped stdout");
    let (sender, receiver) = mpsc::channel();
    std::thread::spawn(move || {
        let mut bytes = Vec::new();
        let _ = stdout.read_to_end(&mut bytes);
        let _ = sender.send(bytes);
    });
    let deadline = Instant::now() + timeout;
    let timed_out = |child: &mut std::process::Child| {
        let _ = child.kill();
        let _ = child.wait();
        ShellError {
            timed_out: true,
            message: format!("spawnSync {program} ETIMEDOUT"),
        }
    };
    let bytes = match receiver.recv_timeout(deadline.saturating_duration_since(Instant::now())) {
        Ok(bytes) => bytes,
        Err(_) => return Err(timed_out(&mut child)),
    };
    let status = loop {
        match child.try_wait() {
            Ok(Some(status)) => break status,
            Ok(None) if Instant::now() < deadline => std::thread::sleep(Duration::from_millis(5)),
            Ok(None) => return Err(timed_out(&mut child)),
            Err(error) => return Err(failed(error.to_string())),
        }
    };
    if !status.success() {
        return Err(failed(format!(
            "Command failed: {program} {}",
            args.join(" ")
        )));
    }
    Ok(String::from_utf8_lossy(&bytes).into_owned())
}

fn read_path_from_launchctl() -> Option<String> {
    let output = run_detached(
        "/bin/launchctl",
        &["getenv", "PATH"],
        Duration::from_secs(2),
    )
    .ok()?;
    let trimmed = output.trim();
    (!trimmed.is_empty()).then(|| trimmed.to_owned())
}

struct Account {
    home: Option<String>,
    shell: Option<String>,
}

/// The current user's passwd entry, as Node's `os.userInfo()` reads it.
#[cfg(unix)]
fn account() -> Option<Account> {
    use std::ffi::CStr;
    let mut buffer = vec![0 as libc::c_char; 16 * 1024];
    let mut entry: libc::passwd = unsafe { std::mem::zeroed() };
    let mut result: *mut libc::passwd = std::ptr::null_mut();
    // SAFETY: the buffer outlives every pointer getpwuid_r writes into `entry`.
    let code = unsafe {
        libc::getpwuid_r(
            libc::geteuid(),
            &mut entry,
            buffer.as_mut_ptr(),
            buffer.len(),
            &mut result,
        )
    };
    if code != 0 || result.is_null() {
        return None;
    }
    let read = |pointer: *const libc::c_char| {
        (!pointer.is_null()).then(|| {
            unsafe { CStr::from_ptr(pointer) }
                .to_string_lossy()
                .into_owned()
        })
    };
    Some(Account {
        home: read(entry.pw_dir),
        shell: read(entry.pw_shell),
    })
}

#[cfg(not(unix))]
fn account() -> Option<Account> {
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn candidates_follow_the_reference_order_without_duplicates() {
        let fallback = if cfg!(target_os = "macos") {
            "/bin/zsh"
        } else {
            "/bin/bash"
        };
        assert_eq!(
            login_shell_candidates(Some(" /usr/bin/fish "), Some("/usr/bin/fish")),
            vec!["/usr/bin/fish".to_owned(), fallback.to_owned()]
        );
        assert_eq!(
            login_shell_candidates(Some(""), None),
            vec![fallback.to_owned()]
        );
    }

    #[test]
    fn shell_path_comes_first_and_duplicates_are_dropped() {
        assert_eq!(
            merge_path_entries(Some("/home/u/.local/bin:/usr/bin"), Some("/usr/bin::/bin")),
            Some("/home/u/.local/bin:/usr/bin:/bin".to_owned())
        );
        assert_eq!(merge_path_entries(None, Some("")), None);
    }

    #[test]
    fn markers_delimit_each_value() {
        let output = "noise\n__T3CODE_ENV_PATH_START__\n/a:/b\n__T3CODE_ENV_PATH_END__\n\
                      __T3CODE_ENV_JAVA_HOME_START__\n__T3CODE_ENV_JAVA_HOME_END__\n";
        assert_eq!(
            extract_environment_values(output, &LOGIN_SHELL_ENV_NAMES),
            vec![("PATH".to_owned(), "/a:/b".to_owned())]
        );
    }

    #[test]
    fn posix_shells_trap_and_fall_back_to_a_located_printenv() {
        let command = build_environment_capture_command("/bin/bash", &["PATH"]);
        assert!(command.starts_with(
            "(trap - DEBUG) 2>/dev/null && trap - DEBUG; __T3CODE_PRINTENV=$(/bin/sh -c "
        ));
        assert!(command.ends_with(
            "printf '%s\\n' '__T3CODE_ENV_PATH_START__'; printenv PATH || \"$__T3CODE_PRINTENV\" PATH || true; printf '%s\\n' '__T3CODE_ENV_PATH_END__'"
        ));
        let fish = build_environment_capture_command("/usr/bin/fish", &["PATH"]);
        assert!(fish.starts_with("set -l __T3CODE_PRINTENV ("));
        assert!(fish.contains("printenv PATH; or \"$__T3CODE_PRINTENV\" PATH; or true"));
        let other = build_environment_capture_command("/usr/bin/xonsh", &["PATH"]);
        assert!(
            other.starts_with("printf '%s\\n' '__T3CODE_ENV_PATH_START__'; printenv PATH || true")
        );
    }

    #[test]
    fn a_real_login_shell_reports_its_path() {
        if !Path::new("/bin/sh").exists() {
            return;
        }
        match read_environment_from_login_shell("/bin/sh", LOGIN_SHELL_TIMEOUT) {
            Ok(environment) => assert!(value(&environment, "PATH").is_some()),
            Err(error) => panic!("{}", error.message),
        }
    }
}
