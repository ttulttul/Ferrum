use serde::Deserialize;

#[derive(Clone, Copy, Debug, Deserialize)]
pub struct HttpContract {
    pub method: &'static str,
    pub path: &'static str,
}

#[derive(Clone, Copy, Debug, Deserialize)]
pub struct RpcContract {
    #[allow(dead_code)]
    pub group: &'static str,
    pub tag: &'static str,
}

// These lists are generated from the pinned upstream checkout with
// scripts/update_contract_inventory.py. Keeping them in the binary lets the
// server reject unknown protocol names consistently instead of silently
// treating a typo as an application command.
pub const HTTP_CONTRACTS: &[HttpContract] =
    include!(concat!(env!("OUT_DIR"), "/http_contracts.rs"));

pub const RPC_CONTRACTS: &[RpcContract] = include!(concat!(env!("OUT_DIR"), "/rpc_contracts.rs"));

pub fn rpc_known(tag: &str) -> bool {
    RPC_CONTRACTS.iter().any(|contract| contract.tag == tag)
}

pub fn http_known(method: &str, path: &str) -> bool {
    HTTP_CONTRACTS
        .iter()
        .any(|contract| contract.method == method && path_matches(contract.path, path))
}

fn path_matches(pattern: &str, path: &str) -> bool {
    let pattern_parts = pattern.trim_matches('/').split('/');
    let path_parts = path.trim_matches('/').split('/');
    let pattern_parts = pattern_parts.peekable();
    let mut path_parts = path_parts.peekable();
    for pattern_part in pattern_parts {
        if pattern_part == "*" {
            return true;
        }
        let Some(path_part) = path_parts.next() else {
            return false;
        };
        if !pattern_part.starts_with(':') && pattern_part != path_part {
            return false;
        }
    }
    path_parts.next().is_none()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn inventory_contains_upstream_surface() {
        assert!(HTTP_CONTRACTS.len() >= 78);
        assert!(RPC_CONTRACTS.len() >= 235);
        assert!(rpc_known("orchestration.dispatchCommand"));
        assert!(rpc_known("orchestration.subscribePromptSuggestion"));
        assert!(http_known("GET", "/api/orchestration/shell"));
        assert!(http_known("POST", "/v1/environments/demo/connect"));
    }

    #[test]
    fn route_patterns_support_parameters_and_wildcards() {
        assert!(path_matches(
            "/v1/environments/:environmentId/status",
            "/v1/environments/demo/status"
        ));
        assert!(path_matches(
            "/api/assets/*",
            "/api/assets/signed/path/file.png"
        ));
        assert!(!path_matches("/api/projects", "/api/projects/extra"));
    }
}
