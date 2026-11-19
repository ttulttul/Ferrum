use std::{
    env, fs,
    net::IpAddr,
    path::{Path, PathBuf},
};

/// Paths shared by the TypeScript server and the Rust replacement.
/// `base_dir` is the user-facing T3 home. Mutable runtime state lives below
/// `userdata`, matching ServerConfig.deriveServerPaths in the reference.
#[derive(Clone, Debug)]
pub struct ServerPaths {
    pub base_dir: PathBuf,
    pub state_dir: PathBuf,
    pub settings: PathBuf,
    pub keybindings: PathBuf,
    pub themes: PathBuf,
    pub caches: PathBuf,
    pub worktrees: PathBuf,
    pub attachments: PathBuf,
    pub browser_artifacts: PathBuf,
    pub logs: PathBuf,
    pub provider_logs: PathBuf,
    pub terminal_logs: PathBuf,
    pub secrets: PathBuf,
    pub environment_id: PathBuf,
    pub anonymous_id: PathBuf,
    pub runtime_state: PathBuf,
}

impl ServerPaths {
    pub fn from_base(base_dir: impl Into<PathBuf>) -> Self {
        let base_dir = base_dir.into();
        let state_dir = base_dir.join("userdata");
        let logs = state_dir.join("logs");
        Self {
            base_dir: base_dir.clone(),
            state_dir: state_dir.clone(),
            settings: state_dir.join("settings.json"),
            keybindings: state_dir.join("keybindings.json"),
            themes: state_dir.join("themes"),
            caches: base_dir.join("caches"),
            worktrees: base_dir.join("worktrees"),
            attachments: state_dir.join("attachments"),
            browser_artifacts: state_dir.join("browser-artifacts"),
            provider_logs: logs.join("provider"),
            terminal_logs: logs.join("terminals"),
            logs,
            secrets: state_dir.join("secrets"),
            environment_id: state_dir.join("environment-id"),
            anonymous_id: state_dir.join("anonymous-id"),
            runtime_state: state_dir.join("server-runtime.json"),
        }
    }

    /// Construct paths when an embedding caller already resolved the
    /// TypeScript `userdata` directory. This preserves the existing library
    /// API while keeping the public CLI base-directory semantics.
    pub fn from_state_dir(state_dir: impl Into<PathBuf>) -> Self {
        let state_dir = state_dir.into();
        let base_dir = state_dir.parent().unwrap_or(&state_dir).to_path_buf();
        let mut paths = Self::from_base(base_dir);
        paths.state_dir = state_dir.clone();
        paths.settings = state_dir.join("settings.json");
        paths.keybindings = state_dir.join("keybindings.json");
        paths.themes = state_dir.join("themes");
        paths.attachments = state_dir.join("attachments");
        paths.browser_artifacts = state_dir.join("browser-artifacts");
        paths.logs = state_dir.join("logs");
        paths.provider_logs = paths.logs.join("provider");
        paths.terminal_logs = paths.logs.join("terminals");
        paths.secrets = state_dir.join("secrets");
        paths.environment_id = state_dir.join("environment-id");
        paths.anonymous_id = state_dir.join("anonymous-id");
        paths.runtime_state = state_dir.join("server-runtime.json");
        paths
    }

    pub fn ensure(&self) -> anyhow::Result<()> {
        for path in [
            &self.state_dir,
            &self.themes,
            &self.caches,
            &self.worktrees,
            &self.attachments,
            &self.browser_artifacts,
            &self.logs,
            &self.provider_logs,
            &self.terminal_logs,
            &self.secrets,
        ] {
            fs::create_dir_all(path)?;
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Default)]
pub struct RuntimeConfig {
    pub mode: String,
    pub host: Option<IpAddr>,
    pub port: Option<u16>,
    pub no_browser: Option<bool>,
    pub auto_bootstrap_project_from_cwd: Option<bool>,
    pub log_websocket_events: Option<bool>,
    pub tailscale_serve: Option<bool>,
    pub tailscale_serve_port: Option<u16>,
    pub dev_allowed_origins: Vec<String>,
}

fn bool_env(name: &str) -> Option<bool> {
    env::var(name)
        .ok()
        .and_then(|v| match v.trim().to_ascii_lowercase().as_str() {
            "1" | "true" | "yes" | "on" => Some(true),
            "0" | "false" | "no" | "off" => Some(false),
            _ => None,
        })
}

impl RuntimeConfig {
    pub fn load() -> anyhow::Result<Self> {
        let dev_allowed_origins = env::var("T3CODE_DEV_ALLOWED_ORIGINS")
            .unwrap_or_default()
            .split(',')
            .map(str::trim)
            .filter(|v| !v.is_empty())
            .map(str::to_owned)
            .collect();
        Ok(Self {
            mode: env::var("T3CODE_MODE").unwrap_or_else(|_| "web".into()),
            host: env::var("T3CODE_HOST").ok().and_then(|v| v.parse().ok()),
            port: env::var("T3CODE_PORT").ok().and_then(|v| v.parse().ok()),
            no_browser: bool_env("T3CODE_NO_BROWSER"),
            auto_bootstrap_project_from_cwd: bool_env("T3CODE_AUTO_BOOTSTRAP_PROJECT_FROM_CWD"),
            log_websocket_events: bool_env("T3CODE_LOG_WS_EVENTS"),
            tailscale_serve: bool_env("T3CODE_TAILSCALE_SERVE"),
            tailscale_serve_port: env::var("T3CODE_TAILSCALE_SERVE_PORT")
                .ok()
                .and_then(|v| v.parse().ok()),
            dev_allowed_origins,
        })
    }
}

pub fn default_base_dir() -> PathBuf {
    env::var_os("T3CODE_HOME")
        .map(PathBuf::from)
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or_else(|| dirs_home().join(".t3"))
}

fn dirs_home() -> PathBuf {
    env::var_os("HOME")
        .map(PathBuf::from)
        .or_else(|| env::var_os("USERPROFILE").map(PathBuf::from))
        .unwrap_or_else(|| Path::new(".").to_path_buf())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn derives_reference_layout_from_base_and_state_dirs() {
        let base = ServerPaths::from_base("/tmp/t3-home");
        assert_eq!(base.state_dir, PathBuf::from("/tmp/t3-home/userdata"));
        assert_eq!(
            base.settings,
            PathBuf::from("/tmp/t3-home/userdata/settings.json")
        );
        assert_eq!(base.caches, PathBuf::from("/tmp/t3-home/caches"));
        let state = ServerPaths::from_state_dir("/tmp/t3-home/userdata");
        assert_eq!(state.base_dir, PathBuf::from("/tmp/t3-home"));
        assert_eq!(
            state.terminal_logs,
            PathBuf::from("/tmp/t3-home/userdata/logs/terminals")
        );
    }
}
