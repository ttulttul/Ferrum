//! The user's login-shell environment, read at start-up and again at runtime.
//!
//! A server started by a service manager or a desktop launcher inherits a
//! bare `PATH` that misses directories the user's shell profile adds, such as
//! `~/.local/bin` where Claude Code installs itself. The reference server
//! (`fixPath` in `apps/server/src/os-jank.ts`) runs the user's login shell
//! before anything starts, reads its `PATH`, `__MISE_DIFF` and `JAVA_HOME`,
//! and rewrites its own environment with them. Every provider launch, status
//! probe and tool it starts later inherits that environment. This module
//! does the same, so it must run before any other thread starts.
//!
//! The reference also re-reads the login shell while it runs
//! (`refreshPathFromLoginShell`): source-control discovery does it, at most
//! once per ten seconds, so a tool installed since launch resolves there and
//! in every later spawn. Node can rewrite `process.env` in place; Rust must
//! not set environment variables once other threads run. So the login-shell
//! variables live in a process-wide snapshot that a refresh swaps whole, and
//! every child process takes them from it: build commands with [`command`] or
//! [`tokio_command`], and search `PATH` with [`path`].

use std::{
    ffi::{OsStr, OsString},
    io::Read,
    path::{Component, Path, PathBuf},
    process::{Command, ExitStatus, Stdio},
    sync::{mpsc, Arc, RwLock},
    time::{Duration, Instant},
};

/// What the reference reads from the login shell. `__MISE_DIFF` must travel
/// with the shell's `PATH`, or `mise x` can rebuild `PATH` behind a same-named
/// wrapper that then runs itself forever.
pub const LOGIN_SHELL_ENV_NAMES: [&str; 4] =
    ["PATH", "__MISE_DIFF", "JAVA_HOME", "T3CODE_FEATURE_FLAGS"];
const LOGIN_SHELL_TIMEOUT: Duration = Duration::from_secs(5);
const SLOW_LOGIN_SHELL_TIMEOUT: Duration = Duration::from_secs(20);

/// How long one runtime refresh serves later callers, as the reference's
/// `Effect.cachedWithTTL(Duration.seconds(10))` in source-control discovery.
pub const REFRESH_TTL: Duration = Duration::from_secs(10);

/// The variables a login shell contributes to every child process.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct LoginEnvironment {
    pub path: Option<OsString>,
    pub mise_diff: Option<OsString>,
    pub java_home: Option<OsString>,
    /// `T3CODE_FEATURE_FLAGS`, which a profile may set for the server.
    pub feature_flags: Option<OsString>,
}

impl LoginEnvironment {
    /// What this process's own environment holds now.
    pub fn from_process() -> Self {
        Self {
            path: std::env::var_os("PATH"),
            mise_diff: std::env::var_os("__MISE_DIFF"),
            java_home: std::env::var_os("JAVA_HOME"),
            feature_flags: std::env::var_os("T3CODE_FEATURE_FLAGS"),
        }
    }

    /// The environment after a login shell printed `shell_environment`, as the
    /// reference's `applyLoginShellEnvironment`: the shell's `PATH` entries
    /// first, then the current ones; `__MISE_DIFF` with that `PATH`;
    /// `JAVA_HOME` only where none is set.
    pub fn merged_with(
        &self,
        shell_environment: &[(String, String)],
        fallback_path: Option<&str>,
    ) -> Self {
        let shell_path = value(shell_environment, "PATH");
        let current_path = self.path.as_deref().map(OsStr::to_string_lossy);
        let mut next = self.clone();
        if let Some(merged) =
            merge_path_entries(shell_path.or(fallback_path), current_path.as_deref())
        {
            next.path = Some(merged.into());
        }
        if shell_path.is_some() {
            if let Some(diff) = value(shell_environment, "__MISE_DIFF") {
                next.mise_diff = Some(diff.into());
            }
        }
        // Android SDK tools are Java programs; a keg-only JDK is only reachable
        // through the JAVA_HOME the shell profile sets.
        if next.java_home.is_none() {
            if let Some(java_home) = value(shell_environment, "JAVA_HOME") {
                next.java_home = Some(java_home.into());
            }
        }
        if next.feature_flags.is_none() {
            if let Some(flags) = value(shell_environment, "T3CODE_FEATURE_FLAGS") {
                next.feature_flags = Some(flags.into());
            }
        }
        next
    }

    /// The variables that are set, by name.
    pub fn variables(&self) -> impl Iterator<Item = (&'static str, &OsStr)> {
        [
            ("PATH", self.path.as_deref()),
            ("__MISE_DIFF", self.mise_diff.as_deref()),
            ("JAVA_HOME", self.java_home.as_deref()),
            ("T3CODE_FEATURE_FLAGS", self.feature_flags.as_deref()),
        ]
        .into_iter()
        .filter_map(|(name, value)| value.map(|value| (name, value)))
    }
}

/// A login-shell environment that can be swapped while other threads read
/// it. Readers get an `Arc` of one whole snapshot, never a half-written one.
pub struct SharedEnvironment {
    slot: RwLock<Option<Arc<LoginEnvironment>>>,
}

impl SharedEnvironment {
    pub const fn new() -> Self {
        Self {
            slot: RwLock::new(None),
        }
    }

    /// The snapshot now; until one is stored, this process's own environment.
    pub fn current(&self) -> Arc<LoginEnvironment> {
        if let Some(environment) = self
            .slot
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .as_ref()
        {
            return environment.clone();
        }
        self.update(|current| current.clone())
    }

    /// Replaces the snapshot. Snapshots already handed out are unchanged.
    pub fn store(&self, environment: LoginEnvironment) {
        *self
            .slot
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(Arc::new(environment));
    }

    /// Replaces the snapshot with `change` of the current one, atomically, so
    /// concurrent updates are never lost.
    pub fn update(
        &self,
        change: impl FnOnce(&LoginEnvironment) -> LoginEnvironment,
    ) -> Arc<LoginEnvironment> {
        let mut slot = self
            .slot
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let base = slot
            .take()
            .unwrap_or_else(|| Arc::new(LoginEnvironment::from_process()));
        let next = Arc::new(change(&base));
        *slot = Some(next.clone());
        next
    }
}

impl Default for SharedEnvironment {
    fn default() -> Self {
        Self::new()
    }
}

static CURRENT: SharedEnvironment = SharedEnvironment::new();

/// The login-shell environment every child process starts with now. Until
/// start-up or a refresh stores one, it is this process's own environment.
pub fn current() -> Arc<LoginEnvironment> {
    CURRENT.current()
}

/// Replaces the environment later child processes start with. Commands
/// already built keep the one they were given.
pub fn store(environment: LoginEnvironment) {
    CURRENT.store(environment);
}

/// The `PATH` child processes get, for code that searches it itself.
pub fn path() -> Option<OsString> {
    current().path.clone()
}

/// The login-shell variables a child must be given explicitly: those whose
/// value differs from this process's own. A child inherits the rest, so its
/// environment is the same either way.
///
/// Leaving an unchanged `PATH` off the command matters: the standard library
/// starts a command that sets `PATH` and names a bare program with `fork` and
/// `execvp`, which copies this whole process's page tables, and that costs
/// more the more memory the server holds. Without it, a command is started
/// with `posix_spawn`, which does not copy them. `fix_path` gives this process
/// the login `PATH` at start-up, so usually only a later refresh sets it here.
fn differing_variables() -> Vec<(&'static str, OsString)> {
    differing(&current(), |name| std::env::var_os(name))
}

fn differing(
    environment: &LoginEnvironment,
    own: impl Fn(&str) -> Option<OsString>,
) -> Vec<(&'static str, OsString)> {
    environment
        .variables()
        .filter(|(name, value)| own(name).as_deref() != Some(*value))
        .map(|(name, value)| (name, value.to_os_string()))
        .collect()
}

/// A command for `program` with the current login-shell environment. A bare
/// `program` is looked up on that environment's `PATH`. Later `env` and
/// `env_remove` calls on it still win.
pub fn command(program: impl AsRef<OsStr>) -> Command {
    let mut command = Command::new(program);
    for (name, value) in differing_variables() {
        command.env(name, value);
    }
    command
}

/// [`command`] for tokio.
pub fn tokio_command(program: impl AsRef<OsStr>) -> tokio::process::Command {
    let mut command = tokio::process::Command::new(program);
    for (name, value) in differing_variables() {
        command.env(name, value);
    }
    command
}

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
    if !shell_environment_prepared {
        hydrate_path();
    }
    store(LoginEnvironment::from_process());
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
    let current = LoginEnvironment::from_process();
    let next = current.merged_with(shell_environment, fallback_path);
    for (name, before, after) in [
        ("PATH", &current.path, &next.path),
        ("__MISE_DIFF", &current.mise_diff, &next.mise_diff),
        ("JAVA_HOME", &current.java_home, &next.java_home),
        (
            "T3CODE_FEATURE_FLAGS",
            &current.feature_flags,
            &next.feature_flags,
        ),
    ] {
        if before != after {
            if let Some(after) = after {
                std::env::set_var(name, after);
            }
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
    let inherited_path = path()
        .unwrap_or_default()
        .to_string_lossy()
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
    /// Set when the program ran and exited unsuccessfully.
    exit_status: Option<ExitStatus>,
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
        exit_status: None,
        message,
    };
    // The shell starts with what every other child process gets now.
    let mut command = self::command(program);
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
            exit_status: None,
            message: format!("spawnSync {program} ETIMEDOUT"),
        }
    };
    let bytes = match receiver.recv_timeout(deadline.saturating_duration_since(Instant::now())) {
        Ok(bytes) => bytes,
        Err(_) => {
            // The reader is scheduled independently from the child. Under
            // load, a shell can have exited while its reader thread has not
            // run yet. Do not turn that scheduling delay into a shell timeout.
            if child.try_wait().ok().flatten().is_some() {
                match receiver.recv_timeout(Duration::from_secs(1)) {
                    Ok(bytes) => bytes,
                    Err(_) => return Err(timed_out(&mut child)),
                }
            } else {
                return Err(timed_out(&mut child));
            }
        }
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
        return Err(ShellError {
            exit_status: Some(status),
            ..failed(format!("Command failed: {program} {}", args.join(" ")))
        });
    }
    Ok(String::from_utf8_lossy(&bytes).into_owned())
}

/// What one runtime refresh did, for tests and logs.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RefreshOutcome {
    /// Not Linux or macOS, or no shell to ask.
    Skipped,
    /// The shell printed a `PATH`; it was merged in.
    Applied,
    /// The shell failed, printed no `PATH` or timed out; nothing changed.
    Unchanged,
}

/// Re-reads the user's login shell and merges what it prints into the
/// environment later child processes start with, as the reference's
/// `refreshPathFromLoginShell`. It runs even when the desktop prepared the
/// environment, since that capture was taken once at launch. Only the user's
/// own shell counts; a fallback shell's `PATH` is not their environment.
/// Failure leaves the environment alone. This blocks for up to five seconds.
pub fn refresh_path_from_login_shell() -> RefreshOutcome {
    if !cfg!(any(target_os = "linux", target_os = "macos")) {
        return RefreshOutcome::Skipped;
    }
    let Some(shell) = login_shell_candidates(
        std::env::var("SHELL").ok().as_deref(),
        account().and_then(|account| account.shell).as_deref(),
    )
    .into_iter()
    .next() else {
        return RefreshOutcome::Skipped;
    };
    refresh_with_shell(&CURRENT, &shell, LOGIN_SHELL_TIMEOUT)
}

fn refresh_with_shell(
    target: &SharedEnvironment,
    shell: &str,
    timeout: Duration,
) -> RefreshOutcome {
    let script = build_environment_capture_command(shell, &LOGIN_SHELL_ENV_NAMES);
    let output = match run_detached(shell, &["-ilc", &script], timeout) {
        Ok(output) => output,
        Err(error) if error.timed_out => {
            tracing::warn!(
                "Timed out reading PATH from login shell {shell} after {}; PATH is unchanged.",
                format_timeout(timeout)
            );
            return RefreshOutcome::Unchanged;
        }
        Err(ShellError {
            exit_status: Some(status),
            ..
        }) => {
            let code = status
                .code()
                .map_or_else(|| "null".to_owned(), |code| code.to_string());
            tracing::warn!("Login shell {shell} exited with code {code} while reading PATH.");
            String::new()
        }
        Err(error) => {
            tracing::warn!(
                "Failed to read PATH from login shell {shell}. {}",
                error.message
            );
            String::new()
        }
    };
    let shell_environment = extract_environment_values(&output, &LOGIN_SHELL_ENV_NAMES);
    if value(&shell_environment, "PATH").is_none() {
        tracing::warn!("Login shell {shell} printed no PATH; PATH is unchanged.");
        return RefreshOutcome::Unchanged;
    }
    // Merge into whatever is current when the shell answers, not what it was
    // when it started, so a concurrent store is never lost.
    target.update(|current| current.merged_with(&shell_environment, None));
    RefreshOutcome::Applied
}

fn format_timeout(timeout: Duration) -> String {
    let seconds = timeout.as_secs();
    if seconds == 1 {
        "1 second".to_owned()
    } else {
        format!("{seconds} seconds")
    }
}

/// Shares one refresh among callers within `ttl` of each other, as the
/// reference's `Effect.cachedWithTTL`: a caller arriving while a refresh runs
/// waits for it, and one arriving soon after it finished reuses it.
pub struct RefreshThrottle {
    ttl: Duration,
    last: tokio::sync::Mutex<Option<Instant>>,
}

impl RefreshThrottle {
    pub const fn new(ttl: Duration) -> Self {
        Self {
            ttl,
            last: tokio::sync::Mutex::const_new(None),
        }
    }

    /// Runs `refresh` on a blocking thread unless a recent run still serves.
    /// Returns whether it ran.
    pub async fn run<F>(&self, refresh: F) -> bool
    where
        F: FnOnce() + Send + 'static,
    {
        let mut last = self.last.lock().await;
        if last.is_some_and(|finished| finished.elapsed() < self.ttl) {
            return false;
        }
        let _ = tokio::task::spawn_blocking(refresh).await;
        *last = Some(Instant::now());
        true
    }
}

static SOURCE_CONTROL_REFRESH: RefreshThrottle = RefreshThrottle::new(REFRESH_TTL);

/// The refresh source-control discovery runs before it probes for tools.
pub async fn refresh_for_source_control_discovery() {
    SOURCE_CONTROL_REFRESH
        .run(|| {
            refresh_path_from_login_shell();
        })
        .await;
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

    fn environment(path: &str) -> LoginEnvironment {
        LoginEnvironment {
            path: Some(path.into()),
            mise_diff: None,
            java_home: None,
            feature_flags: None,
        }
    }

    fn pairs(entries: &[(&str, &str)]) -> Vec<(String, String)> {
        entries
            .iter()
            .map(|(name, value)| ((*name).to_owned(), (*value).to_owned()))
            .collect()
    }

    #[test]
    fn a_refresh_puts_the_shell_path_first_and_keeps_an_existing_java_home() {
        let before = LoginEnvironment {
            path: Some("/usr/bin:/bin".into()),
            mise_diff: Some("old".into()),
            java_home: Some("/jdk/kept".into()),
            feature_flags: None,
        };
        let after = before.merged_with(
            &pairs(&[
                ("PATH", "/home/u/.local/bin:/usr/bin"),
                ("__MISE_DIFF", "new"),
                ("JAVA_HOME", "/jdk/shell"),
            ]),
            None,
        );
        assert_eq!(
            after,
            LoginEnvironment {
                path: Some("/home/u/.local/bin:/usr/bin:/bin".into()),
                mise_diff: Some("new".into()),
                java_home: Some("/jdk/kept".into()),
                feature_flags: None,
            }
        );
        // JAVA_HOME fills only a gap; __MISE_DIFF travels only with a PATH.
        let bare = environment("/usr/bin").merged_with(
            &pairs(&[("__MISE_DIFF", "orphan"), ("JAVA_HOME", "/jdk/shell")]),
            None,
        );
        assert_eq!(bare.path.as_deref(), Some(OsStr::new("/usr/bin")));
        assert_eq!(bare.mise_diff, None);
        assert_eq!(bare.java_home.as_deref(), Some(OsStr::new("/jdk/shell")));
    }

    #[test]
    fn a_stored_snapshot_reaches_later_readers_and_spares_earlier_ones() {
        let shared = SharedEnvironment::new();
        shared.store(environment("/one"));
        let earlier = shared.current();
        shared.store(environment("/two"));
        assert_eq!(earlier.path.as_deref(), Some(OsStr::new("/one")));
        assert_eq!(shared.current().path.as_deref(), Some(OsStr::new("/two")));
    }

    #[test]
    fn an_unset_snapshot_is_the_process_environment() {
        assert_eq!(
            *SharedEnvironment::new().current(),
            LoginEnvironment::from_process()
        );
    }

    #[test]
    fn concurrent_updates_are_never_lost() {
        let shared = Arc::new(SharedEnvironment::new());
        shared.store(environment("/base"));
        let threads: Vec<_> = (0..16)
            .map(|index| {
                let shared = shared.clone();
                std::thread::spawn(move || {
                    shared.update(|current| {
                        current.merged_with(&pairs(&[("PATH", &format!("/t{index}"))]), None)
                    });
                })
            })
            .collect();
        for thread in threads {
            thread.join().unwrap();
        }
        let path = shared.current().path.clone().unwrap();
        let entries: Vec<_> = path.to_str().unwrap().split(':').collect();
        assert_eq!(entries.len(), 17);
        assert!(entries.contains(&"/base"));
        for index in 0..16 {
            assert!(entries.contains(&format!("/t{index}").as_str()));
        }
    }

    #[test]
    fn commands_carry_the_snapshot_and_later_env_calls_win() {
        // A command's PATH is the snapshot's: set on it, or inherited when this
        // process already has the same one.
        let command = command("true");
        let path = command
            .get_envs()
            .find(|(name, _)| *name == "PATH")
            .and_then(|(_, value)| value.map(OsStr::to_os_string))
            .or_else(|| std::env::var_os("PATH"));
        assert_eq!(path, current().path);
        let mut overridden = self::command("true");
        overridden.env("PATH", "/explicit");
        assert_eq!(
            overridden
                .get_envs()
                .find(|(name, _)| *name == "PATH")
                .and_then(|(_, value)| value),
            Some(OsStr::new("/explicit"))
        );
    }

    #[test]
    fn only_variables_this_process_lacks_are_set_on_a_command() {
        let environment = LoginEnvironment {
            path: Some("/login/bin:/usr/bin".into()),
            java_home: Some("/jdk".into()),
            ..LoginEnvironment::default()
        };
        let own = |path: &'static str| {
            move |name: &str| match name {
                "PATH" => Some(OsString::from(path)),
                "JAVA_HOME" => Some(OsString::from("/jdk")),
                _ => None,
            }
        };
        // The same PATH is inherited, so the command can start with posix_spawn.
        assert!(differing(&environment, own("/login/bin:/usr/bin")).is_empty());
        // A different one is set, so the child still gets the login PATH.
        assert_eq!(
            differing(&environment, own("/usr/bin")),
            vec![("PATH", OsString::from("/login/bin:/usr/bin"))]
        );
    }

    #[cfg(unix)]
    fn fake_shell(dir: &Path, body: &str) -> String {
        use std::os::unix::fs::PermissionsExt;
        let shell = dir.join("fake-shell");
        std::fs::write(&shell, format!("#!/bin/sh\n{body}\n")).unwrap();
        std::fs::set_permissions(&shell, std::fs::Permissions::from_mode(0o755)).unwrap();
        shell.to_string_lossy().into_owned()
    }

    #[cfg(unix)]
    #[test]
    fn a_runtime_refresh_merges_only_a_printed_path() {
        let dir = tempfile::tempdir().unwrap();
        let shared = SharedEnvironment::new();
        shared.store(environment("/usr/bin"));

        let printing = fake_shell(
            dir.path(),
            "printf '%s\\n' __T3CODE_ENV_PATH_START__ /installed/later:/usr/bin __T3CODE_ENV_PATH_END__",
        );
        assert_eq!(
            refresh_with_shell(&shared, &printing, LOGIN_SHELL_TIMEOUT),
            RefreshOutcome::Applied
        );
        assert_eq!(
            shared.current().path.as_deref(),
            Some(OsStr::new("/installed/later:/usr/bin"))
        );

        for body in [
            "exit 3",
            "echo no markers",
            "printf '%s\\n' __T3CODE_ENV_PATH_START__ /ignored __T3CODE_ENV_PATH_END__; exit 1",
        ] {
            let failing = fake_shell(dir.path(), body);
            assert_eq!(
                refresh_with_shell(&shared, &failing, LOGIN_SHELL_TIMEOUT),
                RefreshOutcome::Unchanged,
                "{body}"
            );
        }
        let slow = fake_shell(dir.path(), "sleep 5");
        let started = Instant::now();
        assert_eq!(
            refresh_with_shell(&shared, &slow, Duration::from_millis(200)),
            RefreshOutcome::Unchanged
        );
        assert!(started.elapsed() < Duration::from_secs(3));
        assert_eq!(
            refresh_with_shell(&shared, "/nonexistent/shell", LOGIN_SHELL_TIMEOUT),
            RefreshOutcome::Unchanged
        );
        assert_eq!(
            shared.current().path.as_deref(),
            Some(OsStr::new("/installed/later:/usr/bin"))
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn the_throttle_shares_one_refresh_within_its_window() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let throttle = Arc::new(RefreshThrottle::new(Duration::from_secs(60)));
        let runs = Arc::new(AtomicUsize::new(0));
        let callers: Vec<_> = (0..8)
            .map(|_| {
                let throttle = throttle.clone();
                let runs = runs.clone();
                tokio::spawn(async move {
                    throttle
                        .run(move || {
                            std::thread::sleep(Duration::from_millis(50));
                            runs.fetch_add(1, Ordering::SeqCst);
                        })
                        .await
                })
            })
            .collect();
        let mut ran = 0;
        for caller in callers {
            ran += usize::from(caller.await.unwrap());
        }
        assert_eq!((ran, runs.load(Ordering::SeqCst)), (1, 1));

        let expiring = RefreshThrottle::new(Duration::ZERO);
        for _ in 0..2 {
            let runs = runs.clone();
            assert!(
                expiring
                    .run(move || {
                        runs.fetch_add(1, Ordering::SeqCst);
                    })
                    .await
            );
        }
        assert_eq!(runs.load(Ordering::SeqCst), 3);
    }
}
