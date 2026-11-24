//! The secrets an agent asks the user for (`request_secret`), as the
//! reference's `SecretRequests` keeps them: each saved answer under a one-use
//! ref (`secret-ref:<hex>`, an HMAC of its thread and card keyed by the
//! store's `secret-request-salt`), in an owner-only `<name>.bin` file of the
//! server's secrets directory beside the reference's own, so either server
//! on a shared home reads the other's; a `file` request's value, and a value
//! pasted in chat, in an owner-only file under `files/`.
use hmac::{Hmac, Mac};
use serde_json::{json, Value};
use sha2::Sha256;
use std::io::{self, Write};
use std::path::{Path, PathBuf};

pub(crate) const REF_PREFIX: &str = "secret-ref:";
/// How long a saved value waits for its ref to be used (`SECRET_REF_TTL_MS`).
const REF_TTL_MS: i64 = 24 * 60 * 60 * 1000;
const SALT: &str = "secret-request-salt";

/// Why a secret request could not go on (`SecretRequestFailureReason`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Failure {
    LoadFailed,
    NotFound,
    AlreadyAnswered,
    AgentStopped,
    StoreFailed,
    ReferenceUnreadable,
    RecordFailed,
    ReadFailed,
    RefUnavailable,
    RefExpired,
    ConsumeFailed,
    NotAFileRequest,
    InvalidRef,
}

impl Failure {
    pub(crate) fn reason(self) -> &'static str {
        match self {
            Failure::LoadFailed => "load_failed",
            Failure::NotFound => "not_found",
            Failure::AlreadyAnswered => "already_answered",
            Failure::AgentStopped => "agent_stopped",
            Failure::StoreFailed => "store_failed",
            Failure::ReferenceUnreadable => "reference_unreadable",
            Failure::RecordFailed => "record_failed",
            Failure::ReadFailed => "read_failed",
            Failure::RefUnavailable => "ref_unavailable",
            Failure::RefExpired => "ref_expired",
            Failure::ConsumeFailed => "consume_failed",
            Failure::NotAFileRequest => "not_a_file_request",
            Failure::InvalidRef => "invalid_ref",
        }
    }

    /// The reference's `SecretRequestError.message`.
    pub(crate) fn message(self) -> &'static str {
        match self {
            Failure::LoadFailed => "Could not load the secret request.",
            Failure::NotFound => "This secret request no longer exists.",
            Failure::AlreadyAnswered => "This secret request was already answered.",
            Failure::AgentStopped => {
                "The agent that asked has stopped, so this secret can't be used."
            }
            Failure::StoreFailed => "Could not store the secret.",
            Failure::ReferenceUnreadable => "Could not read the secret from the secret provider.",
            Failure::RecordFailed => "Saved the secret, but could not update the request.",
            Failure::ReadFailed => "Could not read the secret.",
            Failure::RefUnavailable => {
                "That secretRef was already used or does not exist. Ask the user again with request_secret."
            }
            Failure::RefExpired => "That secretRef expired. Ask the user again with request_secret.",
            Failure::ConsumeFailed => "Could not use that secretRef. Try again.",
            Failure::InvalidRef => "That secretRef is not valid.",
            Failure::NotAFileRequest => {
                "The user did not save this secret for shell use. Ask again with request_secret and delivery: \"file\"."
            }
        }
    }
}

/// The store, in the server's secrets directory.
pub(crate) struct Store {
    dir: PathBuf,
}

fn now_ms() -> i64 {
    crate::platform::now_ms()
}

#[cfg(unix)]
fn owner_only(options: &mut std::fs::OpenOptions) {
    use std::os::unix::fs::OpenOptionsExt;
    options.mode(0o600);
}
#[cfg(not(unix))]
fn owner_only(_: &mut std::fs::OpenOptions) {}

#[cfg(unix)]
fn owner_only_dir(dir: &Path) {
    use std::os::unix::fs::PermissionsExt;
    let _ = std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o700));
}
#[cfg(not(unix))]
fn owner_only_dir(_: &Path) {}

/// Writes `bytes` to a new file at `path`, never replacing one that exists.
fn create_new(path: &Path, bytes: &[u8]) -> io::Result<()> {
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    owner_only(&mut options);
    let mut file = options.open(path)?;
    file.write_all(bytes)?;
    file.sync_all()
}

impl Store {
    pub(crate) fn new(dir: impl Into<PathBuf>) -> Self {
        Store { dir: dir.into() }
    }

    fn path(&self, name: &str) -> PathBuf {
        self.dir.join(format!("{name}.bin"))
    }

    fn files(&self) -> PathBuf {
        self.dir.join("files")
    }

    fn get(&self, name: &str) -> io::Result<Option<Vec<u8>>> {
        match std::fs::read(self.path(name)) {
            Ok(bytes) => Ok(Some(bytes)),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(error),
        }
    }

    /// `ServerSecretStore.create`: a value no one wrote before; an existing
    /// one stays.
    fn create(&self, name: &str, bytes: &[u8]) -> io::Result<()> {
        std::fs::create_dir_all(&self.dir)?;
        owner_only_dir(&self.dir);
        match create_new(&self.path(name), bytes) {
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => Ok(()),
            other => other,
        }
    }

    fn remove(&self, name: &str) -> bool {
        match std::fs::remove_file(self.path(name)) {
            Ok(()) => true,
            Err(error) => error.kind() == io::ErrorKind::NotFound,
        }
    }

    /// `getOrCreateRandom`: the salt, made once (a 0-byte file is never a key).
    fn salt(&self) -> io::Result<Vec<u8>> {
        if let Some(bytes) = self.get(SALT)?.filter(|b| !b.is_empty()) {
            return Ok(bytes);
        }
        let _ = std::fs::remove_file(self.path(SALT));
        let fresh: [u8; 32] = crate::platform::random_bytes();
        self.create(SALT, &fresh)?;
        Ok(self.get(SALT)?.unwrap_or_else(|| fresh.to_vec()))
    }

    /// The ref of `thread_id`'s card `item`: `secret-ref:` and the first 32
    /// hex digits of HMAC-SHA256(hex(salt), "<thread>\0<item>").
    pub(crate) fn ref_for(&self, thread_id: &str, item: &str) -> io::Result<String> {
        let key = hex::encode(self.salt()?);
        let mut mac = Hmac::<Sha256>::new_from_slice(key.as_bytes()).expect("any key length");
        mac.update(format!("{thread_id}\u{0}{item}").as_bytes());
        let tag = hex::encode(mac.finalize().into_bytes());
        Ok(format!("{REF_PREFIX}{}", &tag[..32]))
    }

    fn store_name(reference: &str) -> String {
        format!(
            "secret-request-{}",
            reference.trim_start_matches(REF_PREFIX)
        )
    }

    /// Keeps the user's answer to card `item` under its ref.
    pub(crate) fn save(
        &self,
        thread_id: &str,
        item: &str,
        project_id: &str,
        secret: &str,
    ) -> Result<(), Failure> {
        let reference = self
            .ref_for(thread_id, item)
            .map_err(|_| Failure::StoreFailed)?;
        let stored = json!({"projectId": project_id, "value": secret, "savedAt": now_ms()});
        self.create(&Self::store_name(&reference), stored.to_string().as_bytes())
            .map_err(|_| Failure::StoreFailed)
    }

    /// Drops the answer to card `item`, as when no one can receive its ref.
    pub(crate) fn discard(&self, thread_id: &str, item: &str) {
        if let Ok(reference) = self.ref_for(thread_id, item) {
            self.remove(&Self::store_name(&reference));
        }
    }

    /// `savedRef`: card `item`'s ref, while its answer is kept.
    pub(crate) fn saved_ref(&self, thread_id: &str, item: &str) -> Option<String> {
        let reference = self.ref_for(thread_id, item).ok()?;
        self.get(&Self::store_name(&reference))
            .ok()
            .flatten()
            .map(|_| reference)
    }

    /// `readRef`: a live ref's value, for its project.
    fn read(&self, reference: &str, project_id: &str) -> Result<String, Failure> {
        let name = Self::store_name(reference);
        let stored = self.get(&name).map_err(|_| Failure::ReadFailed)?;
        let decoded = stored
            .and_then(|bytes| serde_json::from_slice::<Value>(&bytes).ok())
            .filter(|v| v["projectId"] == project_id && v["value"].is_string());
        let Some(decoded) = decoded else {
            return Err(Failure::RefUnavailable);
        };
        if now_ms() - decoded["savedAt"].as_i64().unwrap_or(0) > REF_TTL_MS {
            self.remove(&name);
            return Err(Failure::RefExpired);
        }
        Ok(decoded["value"].as_str().unwrap_or_default().to_owned())
    }

    /// `consume`: a live ref's value for its project, used once. The stored
    /// copy goes first; a value whose copy cannot be removed is not handed out,
    /// so a ref is never used twice. One consume at a time.
    pub(crate) fn consume(&self, reference: &str, project_id: &str) -> Result<String, Failure> {
        static CONSUMING: std::sync::Mutex<()> = std::sync::Mutex::new(());
        let _held = CONSUMING.lock().unwrap_or_else(|e| e.into_inner());
        let hex = reference.strip_prefix(REF_PREFIX).unwrap_or_default();
        let valid = hex.len() == 32 && hex.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'));
        if !valid {
            return Err(Failure::InvalidRef);
        }
        let value = self.read(reference, project_id)?;
        if !self.remove(&Self::store_name(reference)) {
            return Err(Failure::ConsumeFailed);
        }
        Ok(value)
    }

    /// Writes `value` to an owner-only file `file` in `files/`, through a
    /// partial file that never follows or replaces a path.
    fn write_file(&self, file: &Path, value: &str) -> Result<(), Failure> {
        let dir = self.files();
        std::fs::create_dir_all(&dir).map_err(|_| Failure::StoreFailed)?;
        owner_only_dir(&dir);
        let partial = file.with_extension("partial");
        let _ = std::fs::remove_file(&partial);
        create_new(&partial, value.as_bytes()).map_err(|_| Failure::StoreFailed)?;
        std::fs::rename(&partial, file).map_err(|_| Failure::StoreFailed)
    }

    /// `saveToFile`: a saved `file` request's value, moved to an owner-only
    /// file whose path the agent's commands read. A retry gets the same file.
    pub(crate) fn save_to_file(
        &self,
        thread_id: &str,
        item: &str,
        project_id: &str,
    ) -> Result<String, Failure> {
        let reference = self
            .ref_for(thread_id, item)
            .map_err(|_| Failure::StoreFailed)?;
        let file = self.files().join(reference.trim_start_matches(REF_PREFIX));
        if !file.exists() {
            let value = self.read(&reference, project_id)?;
            self.write_file(&file, &value)?;
        }
        // One use: the file counts as delivered once the stored copy is gone.
        if !self.remove(&Self::store_name(&reference)) {
            return Err(Failure::ConsumeFailed);
        }
        Ok(file.to_string_lossy().into_owned())
    }

    /// `savePasted`: a secret the user pasted in chat, in an owner-only file
    /// keyed by the provider session and request; a retry gets that file.
    pub(crate) fn save_pasted(
        &self,
        thread_id: &str,
        namespace: &str,
        key: &str,
        value: &str,
    ) -> Result<String, Failure> {
        let reference = self
            .ref_for(thread_id, &format!("pasted\u{0}{namespace}\u{0}{key}"))
            .map_err(|_| Failure::StoreFailed)?;
        let file = self.files().join(reference.trim_start_matches(REF_PREFIX));
        if !file.exists() {
            self.write_file(&file, value)?;
        }
        Ok(file.to_string_lossy().into_owned())
    }
}

impl Store {
    /// `sweepExpired` and `sweepExpiredFiles`: drops saved values whose ref
    /// has expired or cannot be read, and files given out longer ago than a
    /// ref lives. Returns how many of each it removed.
    pub(crate) fn sweep(&self) -> (usize, usize) {
        let now = now_ms();
        let is_hex = |name: &str| {
            name.len() == 32 && name.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'))
        };
        let mut values = 0;
        for entry in std::fs::read_dir(&self.dir).into_iter().flatten().flatten() {
            let file = entry.file_name().to_string_lossy().into_owned();
            let Some(name) = file.strip_suffix(".bin") else {
                continue;
            };
            if !name.strip_prefix("secret-request-").is_some_and(is_hex) {
                continue;
            }
            let live = self
                .get(name)
                .ok()
                .flatten()
                .and_then(|bytes| serde_json::from_slice::<Value>(&bytes).ok())
                .and_then(|v| {
                    v["savedAt"]
                        .as_i64()
                        .filter(|_| v["value"].is_string() && v["projectId"].is_string())
                })
                .is_some_and(|saved| now - saved <= REF_TTL_MS);
            if !live && self.remove(name) {
                values += 1;
            }
        }
        let mut files = 0;
        for entry in std::fs::read_dir(self.files())
            .into_iter()
            .flatten()
            .flatten()
        {
            let file = entry.file_name().to_string_lossy().into_owned();
            if !is_hex(file.strip_suffix(".partial").unwrap_or(&file)) {
                continue;
            }
            let modified = entry
                .metadata()
                .and_then(|m| m.modified())
                .ok()
                .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                .map(|d| d.as_millis() as i64);
            if modified.is_some_and(|at| now - at <= REF_TTL_MS) {
                continue;
            }
            if std::fs::remove_file(entry.path()).is_ok() {
                files += 1;
            }
        }
        (values, files)
    }
}

/// Sweeps `dir` now and every hour, as the reference's store does while
/// the server runs.
#[cfg(feature = "native")]
pub(crate) async fn sweep_hourly(dir: PathBuf) {
    let store = Store::new(dir);
    loop {
        let (values, files) = store.sweep();
        if values + files > 0 {
            tracing::debug!(values, files, "swept expired secret requests");
        }
        tokio::time::sleep(std::time::Duration::from_secs(60 * 60)).await;
    }
}

/// A failure as the RPC sends it (`SecretRequestError`).
pub(crate) fn error_json(failure: Failure, provider: Option<&str>, cause: Option<Value>) -> Value {
    let mut error = json!({"_tag": "SecretRequestError", "reason": failure.reason()});
    if let Some(provider) = provider {
        error["provider"] = json!(provider);
    }
    if let Some(cause) = cause {
        error["cause"] = cause;
    }
    error
}

/// The message an agent hears for `failure`.
pub(crate) fn failure_message(failure: Failure, provider: Option<&str>) -> String {
    match provider {
        Some(provider) if failure == Failure::ReferenceUnreadable => format!(
            "Could not read the secret from the secret provider \"{provider}\". Unlock it and try again, or enter the value."
        ),
        _ => failure.message().to_owned(),
    }
}

/// Whether a run with `status` has ended (`isTerminalRunStatus`).
pub(crate) fn terminal_run(status: &str) -> bool {
    matches!(
        status,
        "completed" | "failed" | "cancelled" | "interrupted" | "rolled_back"
    )
}

/// A `secret_request.record` command that repeats `card` with `status`.
pub(crate) fn card_command(card: &Value, command_id: &str, status: &str) -> Value {
    let mut command = json!({
        "type": "secret_request.record",
        "commandId": command_id,
        "threadId": card["threadId"],
        "runId": card["runId"],
        "nodeId": card["nodeId"],
        "turnItemId": card["id"],
        "label": card["label"],
        "reason": card["reason"],
    });
    for key in ["url", "placeholder", "delivery"] {
        if !card[key].is_null() {
            command[key] = card[key].clone();
        }
    }
    command["secretStatus"] = json!(status);
    command
}

/// What an `answer` failed with, as the RPC sends it.
pub(crate) struct AnswerError {
    pub failure: Failure,
    pub provider: Option<String>,
    pub cause: Option<Value>,
}

impl AnswerError {
    fn new(failure: Failure) -> Self {
        AnswerError {
            failure,
            provider: None,
            cause: None,
        }
    }

    pub(crate) fn to_json(&self) -> Value {
        error_json(self.failure, self.provider.as_deref(), self.cause.clone())
    }
}

/// `secrets.answerRequest` (`SecretRequests.answer`): the user saves a
/// secret for card `turnItemId`, picks one from a secret provider
/// (`read_reference`, which gives the value or the provider's label), or
/// declines. The value is stored before the card says so, and a value the
/// card no longer waits for is dropped, as no one can receive its ref.
pub(crate) async fn answer<R, RFut, D, DFut>(
    control: &crate::control_state::ControlState,
    store: &Store,
    params: &Value,
    read_reference: R,
    dispatch: D,
) -> Result<(), AnswerError>
where
    R: FnOnce(Value) -> RFut,
    RFut: std::future::Future<Output = Result<String, String>>,
    D: Fn(Value) -> DFut,
    DFut: std::future::Future<Output = Result<u64, Value>>,
{
    let thread_id = params["threadId"].as_str().unwrap_or_default().to_owned();
    let card_id = params["turnItemId"].as_str().unwrap_or_default().to_owned();
    let answer = &params["answer"];
    let (card, project_id) = {
        let threads = control.store.read().await;
        let Some(thread) = threads.threads.get(&thread_id) else {
            return Err(AnswerError {
                failure: Failure::LoadFailed,
                provider: None,
                cause: Some(
                    json!({"name":"OrchestratorProjectionError","message":format!("Failed to load orchestration projection for thread {thread_id}."),"cause":{"name":"ProjectionStoreThreadNotFoundError","message":format!("No orchestration projection exists for thread {thread_id}.")}}),
                ),
            });
        };
        let card = thread
            .link_requests
            .iter()
            .find(|c| c["id"] == card_id.as_str() && c["type"] == "secret_request")
            .filter(|c| !c["runId"].is_null() && !c["nodeId"].is_null())
            .cloned()
            .ok_or_else(|| AnswerError::new(Failure::NotFound))?;
        if card["secretStatus"] != "pending" {
            return Err(AnswerError::new(Failure::AlreadyAnswered));
        }
        let run = thread.runs.iter().find(|run| {
            card["runId"] == run.id.to_string().as_str() || card["runId"] == run.wire_id().as_str()
        });
        if run.is_none_or(|run| terminal_run(thread.projected_run_status(run).0)) {
            return Err(AnswerError::new(Failure::AgentStopped));
        }
        (card, thread.project_id.clone())
    };
    let secret = match answer["type"].as_str() {
        // `TrimmedNonEmptyString`: the value is kept trimmed.
        Some("save") => Some(
            answer["secret"]
                .as_str()
                .unwrap_or_default()
                .trim()
                .to_owned(),
        ),
        Some("reference") => Some(read_reference(answer["reference"].clone()).await.map_err(
            |provider| AnswerError {
                failure: Failure::ReferenceUnreadable,
                provider: Some(provider),
                cause: None,
            },
        )?),
        _ => None,
    };
    if let Some(secret) = &secret {
        store
            .save(&thread_id, &card_id, &project_id, secret)
            .map_err(AnswerError::new)?;
    }
    let status = if secret.is_some() {
        "saved"
    } else {
        "declined"
    };
    let command = card_command(&card, &format!("secret-request:{card_id}:{status}"), status);
    if let Err(cause) = dispatch(command).await {
        // A failed save is cleared so the pending card can be answered again.
        if secret.is_some() {
            store.discard(&thread_id, &card_id);
        }
        return Err(AnswerError {
            failure: Failure::RecordFailed,
            provider: None,
            cause: Some(cause),
        });
    }
    if secret.is_none() {
        return Ok(());
    }
    // The wait may have closed the card first.
    let saved = control
        .store
        .read()
        .await
        .threads
        .get(&thread_id)
        .and_then(|t| {
            t.link_requests
                .iter()
                .find(|c| c["id"] == card_id.as_str())
                .map(|c| c["secretStatus"] == "saved")
        });
    if saved == Some(false) {
        store.discard(&thread_id, &card_id);
        return Err(AnswerError::new(Failure::AgentStopped));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_ref_is_consumed_once_for_its_project() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::new(dir.path());
        store.save("t", "card", "project:a", "s3cret").unwrap();
        let reference = store.ref_for("t", "card").unwrap();
        assert_eq!(
            store.consume("secret-ref:XYZ", "project:a"),
            Err(Failure::InvalidRef)
        );
        assert_eq!(
            store.consume(&reference, "project:b"),
            Err(Failure::RefUnavailable)
        );
        assert_eq!(
            store.consume(&reference, "project:a").as_deref(),
            Ok("s3cret")
        );
        assert_eq!(
            store.consume(&reference, "project:a"),
            Err(Failure::RefUnavailable)
        );
    }

    #[test]
    fn a_saved_answer_is_kept_under_its_ref_once_and_moved_to_a_file() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::new(dir.path());
        let reference = store.ref_for("t", "card").unwrap();
        assert!(reference.starts_with(REF_PREFIX));
        assert_eq!(reference.len(), REF_PREFIX.len() + 32);
        // The same salt gives the same ref; another card another.
        assert_eq!(store.ref_for("t", "card").unwrap(), reference);
        assert_ne!(store.ref_for("t", "other").unwrap(), reference);
        assert_eq!(store.saved_ref("t", "card"), None);
        store.save("t", "card", "p", "s3cret").unwrap();
        // A second answer never replaces the first.
        store.save("t", "card", "p", "other").unwrap();
        assert_eq!(store.saved_ref("t", "card"), Some(reference.clone()));
        assert_eq!(store.read(&reference, "q"), Err(Failure::RefUnavailable));
        let file = store.save_to_file("t", "card", "p").unwrap();
        assert_eq!(std::fs::read_to_string(&file).unwrap(), "s3cret");
        assert_eq!(store.saved_ref("t", "card"), None);
        // A retry gets the same file.
        assert_eq!(store.save_to_file("t", "card", "p").unwrap(), file);
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(&file).unwrap().permissions().mode() & 0o777;
            assert_eq!(mode, 0o600);
        }
        let pasted = store.save_pasted("t", "ns", "k", "pasted").unwrap();
        assert_eq!(std::fs::read_to_string(&pasted).unwrap(), "pasted");
        assert_eq!(
            store.save_pasted("t", "ns", "k", "changed").unwrap(),
            pasted
        );
        assert_eq!(std::fs::read_to_string(&pasted).unwrap(), "pasted");
    }

    #[test]
    fn a_sweep_drops_expired_values_and_old_files_only() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::new(dir.path());
        store.save("t", "fresh", "p", "v").unwrap();
        let old = store.ref_for("t", "old").unwrap();
        let stale = json!({"projectId": "p", "value": "v", "savedAt": now_ms() - REF_TTL_MS - 1});
        store
            .create(&Store::store_name(&old), stale.to_string().as_bytes())
            .unwrap();
        std::fs::write(dir.path().join("other.bin"), b"kept").unwrap();
        store.save_pasted("t", "ns", "k", "v").unwrap();
        assert_eq!(store.sweep(), (1, 0));
        assert!(store.saved_ref("t", "fresh").is_some());
        assert!(store.saved_ref("t", "old").is_none());
        assert!(dir.path().join("other.bin").exists());
        assert!(dir.path().join("secret-request-salt.bin").exists());
    }
}
