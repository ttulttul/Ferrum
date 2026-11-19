use std::{env, fs, path::PathBuf};

fn main() {
    println!("cargo:rerun-if-changed=contracts/http.json");
    println!("cargo:rerun-if-changed=contracts/rpc.json");
    let out = PathBuf::from(env::var_os("OUT_DIR").expect("OUT_DIR"));
    let http: Vec<serde_json::Value> = serde_json::from_str(
        &fs::read_to_string("contracts/http.json").expect("contracts/http.json"),
    )
    .expect("valid HTTP contract inventory");
    let rpc: Vec<serde_json::Value> = serde_json::from_str(
        &fs::read_to_string("contracts/rpc.json").expect("contracts/rpc.json"),
    )
    .expect("valid RPC contract inventory");
    #[allow(clippy::format_collect)]
    let http_source = format!(
        "&[\n{}]",
        http.iter()
            .map(|entry| {
                format!(
                    "HttpContract {{ method: {:?}, path: {:?} }},\n",
                    entry["method"].as_str().unwrap(),
                    entry["path"].as_str().unwrap()
                )
            })
            .collect::<String>()
    );
    #[allow(clippy::format_collect)]
    let rpc_source = format!(
        "&[\n{}]",
        rpc.iter()
            .map(|entry| {
                format!(
                    "RpcContract {{ group: {:?}, tag: {:?} }},\n",
                    entry["group"].as_str().unwrap(),
                    entry["tag"].as_str().unwrap()
                )
            })
            .collect::<String>()
    );
    fs::write(out.join("http_contracts.rs"), http_source).expect("write HTTP inventory");
    fs::write(out.join("rpc_contracts.rs"), rpc_source).expect("write RPC inventory");
}
