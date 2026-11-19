#!/usr/bin/env python3
"""Regenerate the HTTP, RPC and command inventories from upstream T3 Code."""
import json
import os
import subprocess
import re
import sys
from pathlib import Path

root = Path(sys.argv[1] if len(sys.argv) > 1 else "/tmp/t3code-reference")
rpc = []
source = (root / "packages/contracts/src/rpc.ts").read_text()
block = source[source.index("export const WS_METHODS"):source.index("} as const;", source.index("export const WS_METHODS"))]
for name, tag in re.findall(r'^  (\w+): "([^"]+)"', block, re.M):
    rpc.append({"group": "general", "name": name, "tag": tag})
source = (root / "packages/contracts/src/orchestrationV2.ts").read_text()
block = source[source.index("export const ORCHESTRATION_V2_WS_METHODS"):source.index("} as const;", source.index("export const ORCHESTRATION_V2_WS_METHODS"))]
for name, tag in re.findall(r'^  (\w+): "([^"]+)"', block, re.M):
    rpc.append({"group": "orchestration_v2", "name": name, "tag": tag})
registered = json.loads(subprocess.check_output([
    os.environ.get("NODE", "node"), str(Path(__file__).with_name("read_registered_contracts.mjs")), str(root)
]))
http = registered["http"]
fields = registered["fields"]
auth = (root / "packages/contracts/src/auth.ts").read_text()
scope_names = dict(re.findall(r'const (Auth\w+Scope) = "([^"]+)"', auth))
scope_by_method = {}
for scope_source in ("packages/contracts/src/clientRpcPermissions.ts", "apps/server/src/auth/RpcAuthorization.ts"):
    for method, scope in re.findall(r'\[(?:WS_METHODS|ORCHESTRATION_V2_WS_METHODS)\.(\w+)\]: (Auth\w+Scope)', (root / scope_source).read_text()):
        scope_by_method[method] = scope_names[scope]
for entry in rpc:
    if entry["tag"] in fields:
        entry["required"] = fields[entry["tag"]]
    entry["scope"] = scope_by_method[entry["name"]]
Path("contracts/http.json").write_text(json.dumps(http, indent=2) + "\n")
Path("contracts/rpc.json").write_text(json.dumps(rpc, indent=2) + "\n")
Path("contracts/commands.json").write_text(json.dumps(registered["commands"], indent=2) + "\n")
Path("contracts/projections.json").write_text(json.dumps(registered["projections"], indent=2) + "\n")
Path("contracts/settings.json").write_bytes(subprocess.check_output([
    os.environ.get("NODE", "node"), str(Path(__file__).with_name("read_settings_schema.mjs")), str(root)
]))
subprocess.run([
    os.environ.get("NODE", "node"), str(Path(__file__).with_name("read_config_defaults.mjs")),
    str(root), "contracts/config-defaults.json",
], check=True)
