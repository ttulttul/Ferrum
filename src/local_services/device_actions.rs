use super::{invalid, string, timestamp, Reply};
use serde_json::{json, Value};
use std::{path::Path, process::Stdio, time::Duration};
use tokio::io::AsyncWriteExt;

struct Target<'a> {
    platform: &'a str,
    id: &'a str,
    tools: &'a Path,
}
impl Target<'_> {
    fn unavailable(&self, name: &str, reason: &str) -> Value {
        json!({"_tag":"DeviceActionUnavailableError","operation":name,"platform":self.platform,"reason":reason})
    }
    async fn command(
        &self,
        args: &[&str],
        operation: &str,
        stdin: Option<String>,
    ) -> Result<String, Value> {
        let mut c = if self.platform == "ios" {
            let mut c = crate::login_shell::tokio_command("xcrun");
            c.arg("simctl").arg(args[0]).arg(self.id).args(&args[1..]);
            c
        } else {
            let binary = super::devices::sdk_root()
                .map(|p| {
                    p.join("platform-tools")
                        .join(if cfg!(windows) { "adb.exe" } else { "adb" })
                })
                .unwrap_or_else(|| "adb".into());
            let mut c = crate::login_shell::tokio_command(binary);
            c.arg("-s").arg(self.id);
            if args.first() == Some(&"shell") {
                c.arg("shell");
                for arg in &args[1..] {
                    c.arg(format!("'{}'", arg.replace('\'', "'\\''")));
                }
            } else {
                c.args(args);
            }
            c
        };
        c.kill_on_drop(true)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        c.stdin(if stdin.is_some() {
            Stdio::piped()
        } else {
            Stdio::null()
        });
        let fail = |msg: &str| json!({"_tag":"DeviceOperationError","operation":operation,"reason":"command_failed","cause":{"name":"Error","message":msg}});
        let mut child = c.spawn().map_err(|e| fail(&e.to_string()))?;
        if let Some(body) = stdin {
            child
                .stdin
                .take()
                .unwrap()
                .write_all(body.as_bytes())
                .await
                .map_err(|e| fail(&e.to_string()))?;
        }
        let res = tokio::time::timeout(Duration::from_secs(30), child.wait_with_output())
            .await
            .map_err(|_| fail("Device command timed out."))?
            .map_err(|e| fail(&e.to_string()))?;
        if !res.status.success() {
            return Err(
                json!({"_tag":"DeviceOperationError","operation":operation,"reason":"command_failed","exitCode":res.status.code(),"cause":{"code":res.status.code(),"stderr":String::from_utf8_lossy(&res.stderr),"stdout":String::from_utf8_lossy(&res.stdout)}}),
            );
        }
        Ok(String::from_utf8_lossy(&res.stdout).trim().to_owned())
    }
    async fn read(&self) -> (Value, Value) {
        let mut settings = json!({});
        let mut app = Value::Null;
        if self.platform == "ios" {
            if let Ok(n) = self
                .command(&["ui", "appearance"], "appearance", None)
                .await
            {
                if matches!(n.as_str(), "light" | "dark") {
                    settings["appearance"] = json!(n)
                }
            }
            if let Ok(n) = self
                .command(&["ui", "content_size"], "text size", None)
                .await
            {
                settings["textSize"] = json!(if n.starts_with("accessibility") {
                    "extra-large"
                } else if matches!(n.as_str(), "extra-small" | "small" | "medium") {
                    "small"
                } else if n.contains("extra") {
                    "large"
                } else {
                    "default"
                });
            }
            if let Ok(n) = self
                .command(&["ui", "increase_contrast"], "increase contrast", None)
                .await
            {
                settings["increaseContrast"] = json!(n == "enabled");
            }
        } else {
            if let Ok(n) = self
                .command(&["shell", "cmd", "uimode", "night"], "appearance", None)
                .await
            {
                if n.contains("yes") {
                    settings["appearance"] = json!("dark");
                } else if n.contains("no") {
                    settings["appearance"] = json!("light");
                }
            }
            if let Some(n) = self
                .command(
                    &["shell", "settings", "get", "system", "font_scale"],
                    "text size",
                    None,
                )
                .await
                .ok()
                .and_then(|s| s.parse::<f64>().ok())
            {
                settings["textSize"] = json!(if n <= 0.9 {
                    "small"
                } else if n >= 1.25 {
                    "extra-large"
                } else if n >= 1.1 {
                    "large"
                } else {
                    "default"
                });
            }
            if let Some(n) = self
                .command(
                    &[
                        "shell",
                        "settings",
                        "get",
                        "global",
                        "animator_duration_scale",
                    ],
                    "reduce motion",
                    None,
                )
                .await
                .ok()
                .and_then(|s| s.parse::<f64>().ok())
            {
                settings["reduceMotion"] = json!(n == 0.0);
            }
            if let Ok(n) = self
                .command(
                    &["shell", "settings", "get", "global", "wifi_on"],
                    "network",
                    None,
                )
                .await
            {
                if n == "0" || n == "1" {
                    settings["networkEnabled"] = json!(n == "1");
                }
            }
            if let Ok(s) = self
                .command(&["shell", "dumpsys", "window"], "foreground app", None)
                .await
            {
                if let Some(id) = s
                    .lines()
                    .filter(|l| l.contains("mCurrentFocus=") || l.contains("mFocusedApp="))
                    .flat_map(str::split_whitespace)
                    .find_map(|s| s.split_once('/').map(|(id, _)| id))
                {
                    app = json!({"id":id});
                }
            }
        }
        (settings, app)
    }
    async fn action(&self, input: &Value) -> Result<(), Value> {
        let ty = string(input, "type")?;
        let value = input["value"].as_str().unwrap_or("");
        let enabled = input["value"] == true;
        let lon = input["longitude"].to_string();
        let lat = input["latitude"].to_string();
        let coords = format!("{lat},{lon}");
        let text = match value {
            "small" => ("small", "0.85"),
            "default" => ("large", "1.0"),
            "large" => ("extra-extra-large", "1.15"),
            "extra-large" => ("accessibility-large", "1.3"),
            _ => ("", ""),
        };
        let mut stdin = None;
        let args = match (self.platform, ty) {
            ("ios", "setAppearance") => vec!["ui", "appearance", value],
            ("ios", "setTextSize") => vec!["ui", "content_size", text.0],
            ("ios", "setToggle") if input["setting"] == "increaseContrast" => vec![
                "ui",
                "increase_contrast",
                if enabled { "enabled" } else { "disabled" },
            ],
            ("ios", "setLocation") => vec!["location", "set", &coords],
            ("ios", "clearLocation") => vec!["location", "clear"],
            ("ios", "setPermission") if input["permission"] != "notifications" => vec![
                "privacy",
                string(input, "decision")?,
                string(input, "permission")?,
                string(input, "appId")?,
            ],
            ("ios", "openUrl") => vec!["openurl", string(input, "url")?],
            ("ios", "launchApp") => vec!["launch", string(input, "appId")?],
            ("ios", "terminateApp") => vec!["terminate", string(input, "appId")?],
            ("ios", "sendPush") => {
                let payload = input
                    .get("payload")
                    .ok_or_else(|| invalid("payload", "Missing key"))?;
                stdin = Some(if let Some(s) = payload.as_str() {
                    json!({"aps":{"alert":s}}).to_string()
                } else {
                    payload.to_string()
                });
                vec!["push", string(input, "appId")?, "-"]
            }
            ("ios", "setLiquidGlass" | "setColorFilter" | "setToggle") => {
                let helper = self
                    .tools
                    .join("node_modules/serve-sim/bin/serve-sim-ax-settings");
                let Some(path) = helper.to_str().filter(|_| helper.exists()) else {
                    return Err(self.unavailable("accessibility", "helper_missing"));
                };
                let setting = match ty {
                    "setLiquidGlass" => "liquid-glass",
                    "setColorFilter" => "color-filter",
                    _ => match input["setting"].as_str() {
                        Some("reduceMotion") => "reduce-motion",
                        Some("reduceTransparency") => "reduce-transparency",
                        Some("showBorders") => "show-borders",
                        Some("voiceOver") => "voiceover",
                        _ => return Err(self.unavailable(ty, "unsupported")),
                    },
                };
                self.command(
                    &[
                        "spawn",
                        path,
                        "set",
                        setting,
                        if ty == "setToggle" {
                            if enabled {
                                "on"
                            } else {
                                "off"
                            }
                        } else {
                            value
                        },
                    ],
                    ty,
                    None,
                )
                .await?;
                return Ok(());
            }
            ("android", "setAppearance") => vec![
                "shell",
                "cmd",
                "uimode",
                "night",
                if value == "dark" { "yes" } else { "no" },
            ],
            ("android", "setTextSize") => {
                vec!["shell", "settings", "put", "system", "font_scale", text.1]
            }
            ("android", "setToggle") if input["setting"] == "networkEnabled" => {
                let s = if enabled { "enable" } else { "disable" };
                self.command(&["shell", "svc", "wifi", s], "network", None)
                    .await?;
                vec!["shell", "svc", "data", s]
            }
            ("android", "setToggle") if input["setting"] == "reduceMotion" => {
                for key in [
                    "animator_duration_scale",
                    "transition_animation_scale",
                    "window_animation_scale",
                ] {
                    self.command(
                        &[
                            "shell",
                            "settings",
                            "put",
                            "global",
                            key,
                            if enabled { "0" } else { "1" },
                        ],
                        "reduce motion",
                        None,
                    )
                    .await?;
                }
                return Ok(());
            }
            ("android", "setLocation") => vec!["emu", "geo", "fix", &lon, &lat],
            ("android", "clearLocation") => return Ok(()),
            ("android", "openUrl") => vec![
                "shell",
                "am",
                "start",
                "-a",
                "android.intent.action.VIEW",
                "-d",
                string(input, "url")?,
            ],
            ("android", "launchApp") => vec![
                "shell",
                "monkey",
                "-p",
                string(input, "appId")?,
                "-c",
                "android.intent.category.LAUNCHER",
                "1",
            ],
            ("android", "terminateApp") => {
                vec!["shell", "am", "force-stop", string(input, "appId")?]
            }
            ("android", "setOrientation") => {
                let mut rotation = match value {
                    "portrait" => 0,
                    "landscape_left" => 1,
                    "portrait_upside_down" => 2,
                    "landscape_right" => 3,
                    _ => return Err(invalid("value", "Unknown device orientation")),
                };
                if let Ok(s) = self
                    .command(&["shell", "wm", "size"], "orientation", None)
                    .await
                {
                    if let Some((w, h)) = s
                        .lines()
                        .last()
                        .and_then(|l| l.split_whitespace().last())
                        .and_then(|s| s.split_once('x'))
                        .and_then(|(w, h)| Some((w.parse::<u32>().ok()?, h.parse::<u32>().ok()?)))
                    {
                        if w > h {
                            rotation = (rotation + 3) % 4;
                        }
                    }
                }
                if self.id.starts_with("emulator-") {
                    self.command(
                        &[
                            "shell",
                            "settings",
                            "put",
                            "system",
                            "accelerometer_rotation",
                            "1",
                        ],
                        "orientation",
                        None,
                    )
                    .await?;
                    self.command(
                        &["shell", "cmd", "window", "user-rotation", "free"],
                        "orientation",
                        None,
                    )
                    .await?;
                    vec![
                        "emu",
                        "sensor",
                        "set",
                        "acceleration",
                        ["0:9.81:0", "9.81:0:0", "0:-9.81:0", "-9.81:0:0"][rotation],
                    ]
                } else {
                    vec![
                        "shell",
                        "cmd",
                        "window",
                        "user-rotation",
                        "lock",
                        ["0", "1", "2", "3"][rotation],
                    ]
                }
            }
            ("android", "setPermission") => {
                let app = string(input, "appId")?;
                let verb = if input["decision"] == "grant" {
                    "grant"
                } else {
                    "revoke"
                };
                let permissions: &[&str] = match string(input, "permission")? {
                    "camera" => &["CAMERA"],
                    "microphone" => &["RECORD_AUDIO"],
                    "photos" => &["READ_MEDIA_IMAGES", "READ_EXTERNAL_STORAGE"],
                    "contacts" => &["READ_CONTACTS", "WRITE_CONTACTS"],
                    "calendar" => &["READ_CALENDAR", "WRITE_CALENDAR"],
                    "location" => &["ACCESS_FINE_LOCATION", "ACCESS_COARSE_LOCATION"],
                    "notifications" => &["POST_NOTIFICATIONS"],
                    "motion" => &["ACTIVITY_RECOGNITION"],
                    _ => return Err(self.unavailable("permission", "unsupported")),
                };
                let mut ok = false;
                let mut failure = None;
                for p in permissions {
                    match self
                        .command(
                            &["shell", "pm", verb, app, &format!("android.permission.{p}")],
                            "permission",
                            None,
                        )
                        .await
                    {
                        Ok(_) => ok = true,
                        Err(e) => failure = Some(e),
                    }
                }
                return if ok { Ok(()) } else { Err(failure.unwrap()) };
            }
            _ => return Err(self.unavailable(ty, "unsupported")),
        };
        self.command(&args, ty, stdin).await?;
        Ok(())
    }
}
pub(super) async fn handle(name: &str, device: &Value, input: &Value, tools: &Path) -> Reply {
    let target = Target {
        platform: device["platform"].as_str().unwrap_or(""),
        id: string(input, "deviceId")?,
        tools,
    };
    if name == "action" {
        target.action(input).await?;
    }
    let (settings, app) = target.read().await;
    Ok(
        json!({"hostId":"local","deviceId":target.id,"settings":settings,"foregroundApp":app,"readAt":timestamp()}),
    )
}
