//! `t3code-server browser setup [--base-dir <dir>]`, as the reference's
//! `t3 browser setup` (`apps/server/src/cli/browser.ts`): sets this Linux host
//! up for the preview browser. As root it installs and loads the AppArmor
//! profile that allows Chrome's sandbox, and installs the browser's missing
//! libraries and fonts with apt; otherwise it says what is needed and the
//! command to run as root.
use crate::preview_browser_host as host;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

const FONT_PACKAGES: [&str; 2] = ["fontconfig", "fonts-liberation"];

/// `BrowserSetupStepError`.
struct StepError {
    step: String,
    detail: String,
}

fn run_step(step: &str, command: &str, args: &[&str]) -> Result<(), StepError> {
    let status = Command::new(command)
        .args(args)
        .stdin(Stdio::inherit())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .status()
        .map_err(|error| StepError {
            step: step.into(),
            detail: error.to_string(),
        })?;
    match status.code() {
        Some(0) => Ok(()),
        code => Err(StepError {
            step: step.into(),
            detail: format!(
                "{command} exited with {}",
                code.map_or_else(|| "a signal".to_owned(), |c| c.to_string())
            ),
        }),
    }
}

fn quiet(command: &str, args: &[&str]) -> String {
    Command::new(command)
        .args(args)
        .stdin(Stdio::null())
        .stderr(Stdio::null())
        .output()
        .map(|out| String::from_utf8_lossy(&out.stdout).into_owned())
        .unwrap_or_default()
}

/// `resolveBaseDir`: `~` expanded and made absolute; T3's default home otherwise.
fn resolve_base_dir(raw: Option<&str>) -> PathBuf {
    let Some(raw) = raw.map(str::trim).filter(|raw| !raw.is_empty()) else {
        return crate::config::default_base_dir();
    };
    let home = || {
        std::env::var_os("HOME")
            .map(PathBuf::from)
            .unwrap_or_default()
    };
    let path = if raw == "~" {
        home()
    } else if let Some(rest) = raw.strip_prefix("~/") {
        home().join(rest)
    } else {
        PathBuf::from(raw)
    };
    std::path::absolute(&path).unwrap_or(path)
}

/// `setupBaseDir`: under sudo, an unspecified home is the invoking user's `~/.t3`.
fn setup_base_dir(explicit: Option<&str>) -> PathBuf {
    let raw = explicit
        .map(str::to_owned)
        .or_else(|| std::env::var("T3CODE_HOME").ok());
    let sudo_user = std::env::var("SUDO_USER").ok();
    if raw.is_some() || sudo_user.is_none() {
        return resolve_base_dir(raw.as_deref());
    }
    let entry = quiet(
        "getent",
        &["passwd", sudo_user.as_deref().unwrap_or_default()],
    );
    match entry
        .trim()
        .split(':')
        .nth(5)
        .filter(|home| !home.is_empty())
    {
        Some(home) => Path::new(home).join(".t3"),
        None => resolve_base_dir(None),
    }
}

/// `aptOffers`: whether apt has a candidate for the package.
fn apt_offers(name: &str) -> bool {
    let policy = quiet("apt-cache", &["policy", name]);
    policy
        .split_once("Candidate:")
        .and_then(|(_, rest)| rest.split_whitespace().next())
        .is_some_and(|candidate| candidate != "(none)")
}

/// `installedBrowser`: any installed headless shell under the home.
fn installed_browser(base_dir: &Path) -> Option<PathBuf> {
    let root = base_dir.join("tools").join("chrome-headless-shell");
    let entries = |dir: &Path| -> Vec<PathBuf> {
        std::fs::read_dir(dir)
            .into_iter()
            .flatten()
            .flatten()
            .map(|e| e.path())
            .collect()
    };
    entries(&root).into_iter().find_map(|platform| {
        entries(&platform)
            .into_iter()
            .map(|version| version.join("chrome-headless-shell"))
            .find(|executable| executable.exists())
    })
}

/// `fontAvailable`: Chrome starts without fonts, but pages then render no
/// text; without fontconfig there is no `fc-match`, which counts as no font.
async fn font_available() -> bool {
    let run = tokio::process::Command::new("fc-match")
        .args(["--format=%{file}", "sans-serif"])
        .stdin(Stdio::null())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .output();
    match tokio::time::timeout(std::time::Duration::from_secs(5), run).await {
        Ok(Ok(out)) => !String::from_utf8_lossy(&out.stdout).trim().is_empty(),
        _ => false,
    }
}

/// Runs `browser setup` with the arguments after it; the process exit code.
pub fn run(args: &[String]) -> i32 {
    let mut base_dir = None;
    let mut rest = args.iter();
    while let Some(arg) = rest.next() {
        if arg == "--base-dir" {
            base_dir = rest.next().cloned();
        } else if let Some(value) = arg.strip_prefix("--base-dir=") {
            base_dir = Some(value.to_owned());
        } else if arg == "--help" || arg == "-h" {
            println!("Set up this Linux host for T3's browser: allow Chrome's sandbox and install its libraries and fonts.\n\nUsage: t3code-server browser setup [--base-dir <dir>]");
            return 0;
        } else {
            eprintln!("Unknown argument: {arg}");
            return 2;
        }
    }
    let runtime = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(error) => {
            eprintln!("{error}");
            return 1;
        }
    };
    match runtime.block_on(setup(base_dir.as_deref())) {
        Ok(()) => 0,
        Err(StepError { step, detail }) => {
            eprintln!("Could not {step}: {detail}");
            1
        }
    }
}

async fn setup(explicit: Option<&str>) -> Result<(), StepError> {
    if !cfg!(target_os = "linux") {
        println!("Nothing to set up: T3's browser runs as is on this system.");
        return Ok(());
    }
    let is_root = host::user_id() == 0;
    let base_dir = setup_base_dir(explicit);
    // sudo resets the environment, so a chosen home has to be on the command line.
    let chosen = explicit.is_some() || std::env::var_os("T3CODE_HOME").is_some();
    let setup_command = host::setup_command(chosen.then_some(base_dir.as_path()));
    // Root, unless sudo came from another user, is likely the server's own account.
    let sudo_user = std::env::var("SUDO_USER").unwrap_or_else(|_| "root".into());
    let root_warning =
        (is_root && sudo_user == "root" && !host::sandbox_disabled()).then_some(host::ROOT_MESSAGE);

    let restricted = host::userns_restricted();
    // The kernel can lack a profile that is on disk, as on a host restored from
    // an image, and only root can read the loaded list, so root reloads it on every run.
    let needs_profile = if is_root {
        restricted
    } else {
        host::sandbox_blocked()
    };
    let reload_hint = if restricted && !is_root {
        format!("\nIf the browser still reports a blocked sandbox, the AppArmor profile at {} is not loaded. To load it, run:\n\n  {setup_command}\n", host::APPARMOR_PROFILE_PATH)
    } else {
        String::new()
    };
    let browser = installed_browser(&base_dir);
    let missing = match &browser {
        Some(executable) => host::missing_libraries(executable).await,
        None => vec![],
    };
    let needs_fonts = !font_available().await;
    let has_apt = Path::new("/usr/bin/apt-get").exists();

    if !needs_profile && missing.is_empty() && !needs_fonts {
        let ready = match root_warning {
            Some(warning) => warning.to_owned(),
            None if browser.is_some() => "This host is ready for T3's browser.".to_owned(),
            None => "Chrome's sandbox is allowed here. T3's browser installs on first use; if it then reports missing libraries, run this again.".to_owned(),
        };
        println!("{ready}{reload_hint}");
        return Ok(());
    }

    if !is_root {
        if needs_profile {
            println!("This host blocks the sandbox T3's browser runs in. Setup installs an AppArmor profile at {} that allows it.", host::APPARMOR_PROFILE_PATH);
        }
        if !missing.is_empty() {
            println!(
                "T3's browser is missing {}; setup installs them.",
                missing.join(", ")
            );
        }
        if needs_fonts {
            println!("T3's browser needs fontconfig and system fonts; setup installs them.");
        }
        println!("\nThis needs root. Run:\n\n  {setup_command}\n");
        return Ok(());
    }

    if needs_profile {
        // An existing profile may carry an administrator's edits, so it is reloaded as is.
        let on_disk = Path::new(host::APPARMOR_PROFILE_PATH).exists();
        if !on_disk {
            std::fs::write(host::APPARMOR_PROFILE_PATH, host::APPARMOR_PROFILE).map_err(
                |error| StepError {
                    step: format!("write {}", host::APPARMOR_PROFILE_PATH),
                    detail: error.to_string(),
                },
            )?;
        }
        run_step(
            "load the AppArmor profile",
            "apparmor_parser",
            &["-r", host::APPARMOR_PROFILE_PATH],
        )?;
        println!(
            "{} the AppArmor profile at {} that allows Chrome's sandbox for T3's browser.",
            if on_disk {
                "Reloaded"
            } else {
                "Wrote and loaded"
            },
            host::APPARMOR_PROFILE_PATH
        );
    }

    if (!missing.is_empty() || needs_fonts) && !has_apt {
        if !missing.is_empty() {
            println!("T3's browser is missing {}. Install them with your package manager, then run this again.", missing.join(", "));
        }
        if needs_fonts {
            println!("T3's browser needs fontconfig and system fonts. Install them with your package manager, then run this again.");
        }
        return Ok(());
    }
    if !missing.is_empty() || needs_fonts {
        run_step("refresh the package lists", "apt-get", &["update"])?;
    }
    if !missing.is_empty() {
        let packages: Vec<&str> = host::DEBIAN_PACKAGES
            .iter()
            .map(|names| {
                names
                    .iter()
                    .copied()
                    .find(|name| apt_offers(name))
                    .unwrap_or(names[0])
            })
            .collect();
        let mut args = vec!["install", "-y", "--no-install-recommends"];
        args.extend(packages);
        run_step("install the browser's libraries", "apt-get", &args)?;
        println!("Installed the browser's libraries.");
    }
    if needs_fonts {
        let mut args = vec!["install", "-y", "--no-install-recommends"];
        args.extend(FONT_PACKAGES);
        run_step("install the browser's fonts", "apt-get", &args)?;
        if !font_available().await {
            println!("Font packages are installed, but Fontconfig still finds no system font. Check its configuration and run setup again.");
            return Ok(());
        }
        println!("Installed the browser's fonts. Restart the T3 server if browser tabs were already open.");
    }
    println!(
        "{}",
        root_warning.unwrap_or("This host is ready for T3's browser.")
    );
    Ok(())
}
