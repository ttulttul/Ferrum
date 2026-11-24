//! Device MCP tools (`toolkits/device/handlers.ts` in the reference).
//!
//! `device_list`, `device_open`, `device_screenshot` and `device_close` act as the calling thread against the host's
//! device service. They require the `"device"` thread capability (enabled when
//! both Device support and Agent device access were on when the session started).
use super::*;

const ACCESS_OFF_REASON: &str = "Agent device access is turned off for this environment. Ask the user to turn on the Device hub and Agent device access in Settings > Integrations > Devices; it applies when the agent session next starts, such as in a new thread.";

fn require_device_access<'a, S: McpHost>(host: &S, caller: &'a Caller) -> Result<&'a str, Refusal> {
    let Some(scope) = caller.granted.thread.as_ref() else {
        return Err(failure(
            "thread_credential_required",
            "This tool acts as the calling T3 thread, so it needs an agent running inside T3 Code. This MCP client signed in from outside a thread.",
        ));
    };
    if !host.oauth().threads.device_access(scope) {
        return Err(Refusal::Text(ACCESS_OFF_REASON.to_owned()));
    }
    Ok(scope.thread_id.as_str())
}

/// Formats a `DeviceToolError` payload as its `@t3tools/contracts` `.message` getter.
pub(super) fn format_device_error(error: &Value) -> String {
    match error["_tag"].as_str() {
        Some("DeviceToolUnavailableError") => error["reason"]
            .as_str()
            .unwrap_or("Device tool is unavailable.")
            .to_owned(),
        Some("DeviceHostUnavailableError") => {
            let host_id = error["hostId"].as_str().unwrap_or("local");
            let reason = error["reason"].as_str().unwrap_or("");
            match error["hostLabel"].as_str() {
                Some(label) => format!("{label} is unavailable: {reason}"),
                None => format!("Device host {host_id} is unavailable: {reason}"),
            }
        }
        Some("DevicePlatformUnavailableError") => {
            let platform = error["platform"].as_str().unwrap_or("unknown");
            let host_id = error["hostId"].as_str().unwrap_or("local");
            let reason = error["reason"].as_str().unwrap_or("");
            format!("{platform} devices are unavailable on host {host_id}: {reason}")
        }
        Some("DeviceNotFoundError") => {
            let device_id = error["deviceId"].as_str().unwrap_or("");
            let host_id = error["hostId"].as_str().unwrap_or("local");
            format!("Device {device_id} was not found on host {host_id}.")
        }
        Some("DeviceBootError") => {
            let device_id = error["deviceId"].as_str().unwrap_or("");
            let explanation = match error["reason"].as_str() {
                Some("disk_space") => "There is not enough free disk space on its device host.",
                Some("timeout") => "The device did not become ready in time.",
                _ => "The simulator or emulator could not start.",
            };
            let summary = format!("Device {device_id} failed to boot: {explanation}");
            match error["diagnostics"].as_str().filter(|s| !s.is_empty()) {
                Some(diagnostics) => format!("{summary}\n\n{diagnostics}"),
                None => format!(
                    "{summary} The device host sent no details. Try again or open another device; if it keeps failing, check the device on that host."
                ),
            }
        }
        Some("DeviceOperationError") => {
            let operation = error["operation"].as_str().unwrap_or("operation");
            let explanation = match error["reason"].as_str() {
                Some("command_failed") => match error["exitCode"].as_i64() {
                    Some(code) => format!("The device command failed (exit code {code})."),
                    None => "The device command failed.".to_owned(),
                },
                Some("request_failed") => {
                    "Could not communicate with device support. Try refreshing devices.".to_owned()
                }
                Some("invalid_payload") => "The device request could not be encoded.".to_owned(),
                Some("settings_failed") => "Could not read or save device settings.".to_owned(),
                Some("hub_rejected") => {
                    "The device hub could not complete the request.".to_owned()
                }
                Some("package_manager_missing") => {
                    "Device support needs npm, which is not on the T3 server's PATH. Install npm (it comes with Node.js from nodejs.org), restart T3 Code, then retry."
                        .to_owned()
                }
                _ => "Could not communicate with device support. Try refreshing devices.".to_owned(),
            };
            format!("Device {operation} failed: {explanation}")
        }
        Some("DeviceActionUnavailableError") => {
            let operation = error["operation"].as_str().unwrap_or("action");
            let platform = error["platform"].as_str().unwrap_or("unknown");
            if error["reason"] == "helper_missing" {
                format!("Device {operation} requires a helper missing from this install. Set up device support again.")
            } else {
                format!("Device {operation} is not supported on {platform}.")
            }
        }
        _ => error["message"]
            .as_str()
            .or_else(|| error["reason"].as_str())
            .unwrap_or("Device operation failed.")
            .to_owned(),
    }
}

/// `device_list` (`readsAsCaller`): lists hosts, host statuses, devices, and
/// open sessions for the calling thread.
pub(super) async fn list<S: McpHost>(host: &Arc<S>, caller: &Caller, args: &Value) -> Outcome {
    let thread_id = require_device_access(&**host, caller)?;
    let mut input = json!({ "threadId": thread_id });
    if let Some(host_id) = args.get("hostId").and_then(Value::as_str) {
        input["hostId"] = json!(host_id);
    }
    host.device_tool("list", input)
        .await
        .map_err(|error| Refusal::Text(format_device_error(&error)))
}

/// `device_close` (`actsAsCaller`): closes one or all open device sessions for
/// the calling thread, optionally shutting down the underlying device(s).
pub(super) async fn close<S: McpHost>(host: &Arc<S>, caller: &Caller, args: &Value) -> Outcome {
    threads::writing_caller(&**host, caller).await?;
    let thread_id = require_device_access(&**host, caller)?;
    let mut input = json!({ "threadId": thread_id });
    if let Some(host_id) = args
        .get("hostId")
        .and_then(Value::as_str)
        .map(crate::settings::schema::trim)
        .filter(|s| !s.is_empty())
    {
        input["hostId"] = json!(host_id);
    }
    if let Some(device_id) = args
        .get("deviceId")
        .and_then(Value::as_str)
        .map(crate::settings::schema::trim)
        .filter(|s| !s.is_empty())
    {
        input["deviceId"] = json!(device_id);
    }
    if let Some(shutdown) = args.get("shutdown").and_then(Value::as_bool) {
        input["shutdown"] = json!(shutdown);
    }
    host.device_tool("close", input)
        .await
        .map_err(|error| Refusal::Text(format_device_error(&error)))
}

fn trimmed_arg<'a>(args: &'a Value, key: &str) -> Option<&'a str> {
    args.get(key)
        .and_then(Value::as_str)
        .map(crate::settings::schema::trim)
        .filter(|s| !s.is_empty())
}

/// `device_open` (`actsAsCaller`): opens a device for the calling thread and
/// returns the `agent-device` launcher bound to it.
pub(super) async fn open<S: McpHost>(host: &Arc<S>, caller: &Caller, args: &Value) -> Outcome {
    threads::writing_caller(&**host, caller).await?;
    let thread_id = require_device_access(&**host, caller)?;
    let mut input = json!({ "threadId": thread_id });
    for key in ["deviceId", "hostId", "platform"] {
        if let Some(value) = trimmed_arg(args, key) {
            input[key] = json!(value);
        }
    }
    host.device_tool("open", input)
        .await
        .map_err(|error| Refusal::Text(format_device_error(&error)))
}

/// `device_screenshot` (`readsAsCaller`, an image tool): the screen of a
/// device open in the calling thread, as image content.
pub(super) async fn screenshot<S: McpHost>(
    host: &Arc<S>,
    caller: &Caller,
    args: &Value,
) -> Outcome {
    let failed = |error: &Value| {
        let tag = error["_tag"].as_str().unwrap_or("device_screenshotError");
        let mut message = format_device_error(error);
        if error["_tag"] == "DeviceOperationError" && error["reason"] == "hub_rejected" {
            message.push_str(" Call device_list to check the device state, and device_open if it needs to be reopened.");
        }
        html::image_failure_in(tag, "screenshot", message)
    };
    let thread_id = match require_device_access(&**host, caller) {
        Ok(thread_id) => thread_id,
        Err(_) => {
            return Ok(failed(&json!({
                "_tag": "DeviceToolUnavailableError",
                "reason": ACCESS_OFF_REASON,
            })))
        }
    };
    let mut input = json!({ "threadId": thread_id });
    for key in ["deviceId", "hostId"] {
        if let Some(value) = trimmed_arg(args, key) {
            input[key] = json!(value);
        }
    }
    match host.device_tool("screenshot", input).await {
        Err(error) => Ok(failed(&error)),
        Ok(mut result) => {
            let shot = result["screenshot"].take();
            result["screenshot"] = json!({
                "mimeType": shot["mimeType"],
                "width": shot["width"],
                "height": shot["height"],
            });
            Ok(json!({html::RAW_RESULT: {
                "isError": false,
                "structuredContent": result,
                "content": [
                    {"type": "text", "text": result.to_string()},
                    {"type": "image", "data": shot["data"], "mimeType": shot["mimeType"]},
                ],
            }}))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn formats_contract_device_errors() {
        assert_eq!(
            format_device_error(&json!({
                "_tag": "DeviceToolUnavailableError",
                "reason": "No device host foo. Known hosts: This machine (local)."
            })),
            "No device host foo. Known hosts: This machine (local)."
        );
        assert_eq!(
            format_device_error(&json!({
                "_tag": "DeviceHostUnavailableError",
                "hostId": "local",
                "hostLabel": "This machine",
                "reason": "Device support is off."
            })),
            "This machine is unavailable: Device support is off."
        );
        assert_eq!(
            format_device_error(&json!({
                "_tag": "DeviceHostUnavailableError",
                "hostId": "local",
                "reason": "Device host is not running. Reconnect before retrying close."
            })),
            "Device host local is unavailable: Device host is not running. Reconnect before retrying close."
        );
        assert_eq!(
            format_device_error(&json!({
                "_tag": "DeviceOperationError",
                "operation": "shutdown",
                "reason": "hub_rejected"
            })),
            "Device shutdown failed: The device hub could not complete the request."
        );
    }
}
