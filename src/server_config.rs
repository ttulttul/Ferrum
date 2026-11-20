//! Pieces of the server config snapshot that need no host: the bundled
//! provider catalogs and keybinding defaults, keybinding compilation and the
//! provider list. Shared by the native config RPCs and the workers control plane.
pub(crate) mod keybindings;
pub(crate) mod providers;

use serde_json::Value;
use std::sync::LazyLock;

pub(crate) static DEFAULTS: LazyLock<Value> = LazyLock::new(|| {
    serde_json::from_str(include_str!("../contracts/config-defaults.json"))
        .expect("provider catalogs and keybinding defaults")
});
