//! Local implementations for the provider-backed VCS, terminal, preview, and
//! device capabilities. These services deliberately keep host integration
//! behind small async APIs so the WebSocket dispatcher can expose the same
//! operations without embedding process or filesystem logic in RPC handlers.

use anyhow::{anyhow, Context, Result};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, path::Path, sync::Arc};
use tokio::{process::Command, sync::Mutex};
use uuid::Uuid;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct VcsStatus {
    pub branch: Option<String>,
    pub staged: Vec<String>,
    pub unstaged: Vec<String>,
    pub untracked: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct VcsRef {
    pub name: String,
    pub object: String,
    pub kind: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct VcsResult {
    pub success: bool,
    pub stdout: String,
    pub stderr: String,
    pub code: Option<i32>,
}

#[derive(Clone, Debug, Default)]
pub struct VcsService;

impl VcsService {
    pub async fn status(&self, cwd: impl AsRef<Path>) -> Result<VcsStatus> {
        let cwd = cwd.as_ref();
        let branch = git(cwd, &["branch", "--show-current"])
            .await?
            .stdout
            .trim()
            .to_owned();
        let porcelain = git(cwd, &["status", "--porcelain=v1"]).await?.stdout;
        let mut status = VcsStatus {
            branch: (!branch.is_empty()).then_some(branch),
            staged: vec![],
            unstaged: vec![],
            untracked: vec![],
        };
        for line in porcelain.lines().filter(|line| line.len() >= 3) {
            let code = &line[..2];
            let path = line[3..].to_owned();
            if code == "??" {
                status.untracked.push(path);
                continue;
            }
            if code.as_bytes().first().is_some_and(|c| *c != b' ') {
                status.staged.push(path.clone());
            }
            if code.as_bytes().get(1).is_some_and(|c| *c != b' ') {
                status.unstaged.push(path);
            }
        }
        Ok(status)
    }

    pub async fn refs(&self, cwd: impl AsRef<Path>) -> Result<Vec<VcsRef>> {
        let result = git(
            cwd.as_ref(),
            &[
                "for-each-ref",
                "--format=%(refname)\t%(objectname)\t%(objecttype)",
            ],
        )
        .await?;
        Ok(result
            .stdout
            .lines()
            .filter_map(|line| {
                let mut fields = line.splitn(3, '\t');
                Some(VcsRef {
                    name: fields.next()?.to_owned(),
                    object: fields.next()?.to_owned(),
                    kind: fields.next()?.to_owned(),
                })
            })
            .collect())
    }

    pub async fn diff(&self, cwd: impl AsRef<Path>, staged: bool) -> Result<String> {
        let args = if staged {
            vec!["diff", "--cached"]
        } else {
            vec!["diff"]
        };
        Ok(git(cwd.as_ref(), &args).await?.stdout)
    }

    pub async fn init(&self, cwd: impl AsRef<Path>) -> Result<VcsResult> {
        git(cwd.as_ref(), &["init"]).await
    }
    pub async fn pull(&self, cwd: impl AsRef<Path>) -> Result<VcsResult> {
        git(cwd.as_ref(), &["pull"]).await
    }

    pub async fn create_worktree(
        &self,
        cwd: impl AsRef<Path>,
        path: impl AsRef<Path>,
        branch: &str,
    ) -> Result<VcsResult> {
        let path = path.as_ref().to_string_lossy().into_owned();
        git(cwd.as_ref(), &["worktree", "add", &path, branch]).await
    }

    pub async fn remove_worktree(
        &self,
        cwd: impl AsRef<Path>,
        path: impl AsRef<Path>,
    ) -> Result<VcsResult> {
        let path = path.as_ref().to_string_lossy().into_owned();
        git(cwd.as_ref(), &["worktree", "remove", &path]).await
    }
}

async fn git(cwd: &Path, args: &[&str]) -> Result<VcsResult> {
    if !cwd.is_dir() {
        return Err(anyhow!(
            "working directory does not exist: {}",
            cwd.display()
        ));
    }
    let output = Command::new("git")
        .args(args)
        .current_dir(cwd)
        .output()
        .await
        .context("run git")?;
    Ok(VcsResult {
        success: output.status.success(),
        stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
        stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
        code: output.status.code(),
    })
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct TerminalSession {
    pub id: Uuid,
    pub cwd: String,
    pub command: String,
    pub output: String,
    pub running: bool,
    pub exit_code: Option<i32>,
}

#[derive(Clone, Default)]
pub struct TerminalService {
    sessions: Arc<Mutex<HashMap<Uuid, TerminalSession>>>,
}

impl TerminalService {
    pub async fn open(&self, cwd: impl AsRef<Path>, command: &str) -> Result<TerminalSession> {
        let cwd = cwd.as_ref();
        if !cwd.is_dir() {
            return Err(anyhow!(
                "working directory does not exist: {}",
                cwd.display()
            ));
        }
        let id = Uuid::new_v4();
        let result = Command::new("sh")
            .arg("-lc")
            .arg(command)
            .current_dir(cwd)
            .output()
            .await
            .context("run terminal command")?;
        let session = TerminalSession {
            id,
            cwd: cwd.to_string_lossy().into_owned(),
            command: command.to_owned(),
            output: format!(
                "{}{}",
                String::from_utf8_lossy(&result.stdout),
                String::from_utf8_lossy(&result.stderr)
            ),
            running: false,
            exit_code: result.status.code(),
        };
        self.sessions.lock().await.insert(id, session.clone());
        Ok(session)
    }

    pub async fn observe(&self, id: Uuid) -> Option<TerminalSession> {
        self.sessions.lock().await.get(&id).cloned()
    }
    pub async fn clear(&self, id: Uuid) -> bool {
        self.sessions
            .lock()
            .await
            .get_mut(&id)
            .map(|session| {
                session.output.clear();
                true
            })
            .unwrap_or(false)
    }
    pub async fn close(&self, id: Uuid) -> bool {
        self.sessions.lock().await.remove(&id).is_some()
    }
    pub async fn list(&self) -> Vec<TerminalSession> {
        self.sessions.lock().await.values().cloned().collect()
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct PreviewTab {
    pub id: Uuid,
    pub url: String,
    pub title: Option<String>,
    pub status: String,
}

#[derive(Clone, Default)]
pub struct PreviewService {
    tabs: Arc<Mutex<HashMap<Uuid, PreviewTab>>>,
}

impl PreviewService {
    pub async fn open(&self, url: &str) -> Result<PreviewTab> {
        if !(url.starts_with("http://") || url.starts_with("https://")) {
            return Err(anyhow!("preview URL must use http or https"));
        }
        let tab = PreviewTab {
            id: Uuid::new_v4(),
            url: url.to_owned(),
            title: None,
            status: "open".into(),
        };
        self.tabs.lock().await.insert(tab.id, tab.clone());
        Ok(tab)
    }
    pub async fn list(&self) -> Vec<PreviewTab> {
        self.tabs.lock().await.values().cloned().collect()
    }
    pub async fn close(&self, id: Uuid) -> bool {
        self.tabs.lock().await.remove(&id).is_some()
    }
    pub async fn resize(&self, id: Uuid, _width: u32, _height: u32) -> Result<PreviewTab> {
        self.tabs
            .lock()
            .await
            .get(&id)
            .cloned()
            .ok_or_else(|| anyhow!("preview tab not found"))
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Device {
    pub id: String,
    pub name: String,
    pub platform: String,
    pub connected: bool,
}

#[derive(Clone, Default)]
pub struct DeviceService {
    devices: Arc<Mutex<HashMap<String, Device>>>,
}

impl DeviceService {
    pub async fn configure(&self, device: Device) -> Device {
        self.devices
            .lock()
            .await
            .insert(device.id.clone(), device.clone());
        device
    }
    pub async fn list(&self) -> Vec<Device> {
        self.devices.lock().await.values().cloned().collect()
    }
    pub async fn get(&self, id: &str) -> Option<Device> {
        self.devices.lock().await.get(id).cloned()
    }
    pub async fn close(&self, id: &str) -> bool {
        self.devices.lock().await.remove(id).is_some()
    }
    pub async fn test_host(&self, id: &str) -> Result<Device> {
        self.get(id)
            .await
            .ok_or_else(|| anyhow!("device not found"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    #[tokio::test]
    async fn vcs_status_parses_porcelain() {
        let dir = tempfile_dir();
        Command::new("git")
            .args(["init", "-q"])
            .current_dir(&dir)
            .output()
            .await
            .unwrap();
        tokio::fs::write(dir.join("untracked.txt"), "x")
            .await
            .unwrap();
        let status = VcsService.status(&dir).await.unwrap();
        assert_eq!(status.untracked, vec!["untracked.txt"]);
    }
    #[tokio::test]
    async fn preview_rejects_non_http_urls() {
        assert!(PreviewService::default()
            .open("file:///tmp/a")
            .await
            .is_err());
    }
    #[tokio::test]
    async fn device_lifecycle_is_deterministic() {
        let service = DeviceService::default();
        service
            .configure(Device {
                id: "one".into(),
                name: "Phone".into(),
                platform: "ios".into(),
                connected: true,
            })
            .await;
        assert!(service.test_host("one").await.is_ok());
        assert!(service.close("one").await);
    }
    fn tempfile_dir() -> PathBuf {
        let path = std::env::temp_dir().join(format!("t3code-vcs-{}", Uuid::new_v4()));
        std::fs::create_dir_all(&path).unwrap();
        path
    }
}
