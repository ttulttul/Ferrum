use serde_json::{json, Value};
use std::{
    fmt, io,
    path::{Path, PathBuf},
    process::Command,
    sync::OnceLock,
};

#[derive(Debug)]
pub(super) struct Failure {
    operation: &'static str,
    cause: Value,
    env: Option<(String, String)>,
}

impl fmt::Display for Failure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.cause["message"].as_str().unwrap_or(self.operation))
    }
}

impl std::error::Error for Failure {}

// FileSystem errors retain the exact path and syscall where storage failed, including a
// temporary path when a publish fails before renaming into place.
#[derive(Debug)]
struct Step {
    method: &'static str,
    syscall: &'static str,
    path: PathBuf,
    target: Option<PathBuf>,
    source: io::Error,
}

impl fmt::Display for Step {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.source.fmt(f)
    }
}
impl std::error::Error for Step {}

pub(super) fn step(
    error: io::Error,
    method: &'static str,
    syscall: &'static str,
    path: &Path,
    target: Option<&Path>,
) -> io::Error {
    io::Error::new(
        error.kind(),
        Step {
            method,
            syscall,
            path: path.to_owned(),
            target: target.map(Path::to_owned),
            source: error,
        },
    )
}

pub(super) fn store(
    dir: &Path,
    name: &str,
    operation: &'static str,
    env: Option<(&str, &str)>,
    error: io::Error,
) -> io::Error {
    let kind = error.kind();
    let path = dir.join(format!("{name}.bin"));
    let (class, verb, method, syscall) = match operation {
        "read-secret" => (
            "SecretStoreReadError",
            "read",
            "readFile",
            if kind == io::ErrorKind::IsADirectory {
                "read"
            } else {
                "open"
            },
        ),
        "write-secret" => ("SecretStorePersistError", "persist", "writeFile", "open"),
        _ => ("SecretStoreRemoveError", "remove", "remove", "unlink"),
    };
    let cause = platform(&error, method, syscall, &path);
    io::Error::new(
        kind,
        Failure {
            operation,
            env: env.map(|(id, var)| (id.to_owned(), var.to_owned())),
            cause: json!({"name":class,"message":format!("Failed to {verb} secret {name}."),
                    "cause":cause}),
        },
    )
}

fn platform(error: &io::Error, method: &str, syscall: &str, path: &Path) -> Value {
    let kind = error.kind();
    let step = error.get_ref().and_then(|e| e.downcast_ref::<Step>());
    let (method, syscall, path) = step.map_or((method, syscall, path), |s| {
        (s.method, s.syscall, s.path.as_path())
    });
    let target = step
        .and_then(|s| s.target.as_ref())
        .map(|p| format!(" -> '{}'", p.display()))
        .unwrap_or_default();
    let (reason, errno, description) = match kind {
        io::ErrorKind::NotFound => ("NotFound", "ENOENT", "no such file or directory"),
        io::ErrorKind::PermissionDenied => ("PermissionDenied", "EACCES", "permission denied"),
        io::ErrorKind::AlreadyExists => ("AlreadyExists", "EEXIST", "file already exists"),
        io::ErrorKind::IsADirectory => {
            ("BadResource", "EISDIR", "illegal operation on a directory")
        }
        io::ErrorKind::NotADirectory => ("BadResource", "ENOTDIR", "not a directory"),
        io::ErrorKind::ResourceBusy => ("Busy", "EBUSY", "resource busy or locked"),
        io::ErrorKind::ReadOnlyFilesystem => ("Unknown", "EROFS", "read-only file system"),
        io::ErrorKind::StorageFull => ("Unknown", "ENOSPC", "no space left on device"),
        _ if error
            .to_string()
            .starts_with("Too many levels of symbolic links") =>
        {
            (
                "BadResource",
                "ELOOP",
                "too many symbolic links encountered",
            )
        }
        _ => ("Unknown", "", ""),
    };
    let node_message = if errno.is_empty() {
        error.to_string()
    } else if kind == io::ErrorKind::IsADirectory && !node_directory_error_includes_path() {
        format!("{errno}: {description}, {syscall}")
    } else {
        format!(
            "{errno}: {description}, {syscall} '{}'{target}",
            path.display()
        )
    };
    json!({"name":"PlatformError","message":format!("{reason}: FileSystem.{method} ({})",path.display()),
           "cause":{"name":"Error","message":node_message}})
}

fn node_directory_error_includes_path() -> bool {
    static INCLUDES_PATH: OnceLock<bool> = OnceLock::new();
    *INCLUDES_PATH.get_or_init(|| {
        let output = Command::new("node").arg("--version").output();
        let Ok(output) = output else {
            return false;
        };
        let version = String::from_utf8_lossy(&output.stdout);
        let major = version
            .strip_prefix('v')
            .and_then(|value| value.split('.').next())
            .and_then(|value| value.parse::<u32>().ok());
        major.is_some_and(|major| major >= 24)
    })
}

pub(super) fn file(
    error: io::Error,
    operation: &'static str,
    method: &str,
    syscall: &str,
    path: &Path,
) -> io::Error {
    io::Error::new(
        error.kind(),
        Failure {
            operation,
            cause: platform(&error, method, syscall, path),
            env: None,
        },
    )
}
pub(crate) fn rpc(path: &Path, error: &io::Error) -> Option<Value> {
    let error = error.get_ref()?.downcast_ref::<Failure>()?;
    let (instance, variable) = error
        .env
        .as_ref()
        .map_or((None, None), |(id, var)| (Some(id), Some(var)));
    let mut wire =
        json!({"_tag":"ServerSettingsError","settingsPath":path,"operation":error.operation});
    if error.operation.ends_with("-secret") {
        wire["providerInstanceId"] = json!(instance);
        wire["environmentVariable"] = json!(variable);
    }
    wire["cause"] = error.cause.clone();
    Some(wire)
}
