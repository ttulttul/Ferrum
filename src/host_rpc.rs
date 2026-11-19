pub use crate::local_services::media::routes;
use crate::AppState;
use serde_json::Value;

/// Dispatch a host-backed unary RPC or obtain the first value of its stream.
pub async fn dispatch(
    state: &AppState,
    method: &str,
    input: &Value,
) -> Option<Result<Value, Value>> {
    let (service, name) = method.split_once('.')?;
    match service {
        "vcs" => state.vcs.call(name, input, &state.data_dir).await,
        "terminal" => state.terminals.call(name, input).await,
        "preview" if matches!(name, "reportProfiles" | "clearProfile") => {
            Some(state.previews.profile(name, input, &state.data_dir))
        }
        "preview" => state.previews.call(name, input),
        "device" => state.devices.call(name, input, &state.data_dir).await,
        _ => None,
    }
}
