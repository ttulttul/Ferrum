#!/usr/bin/env python3
"""Regenerate contracts/http.json and contracts/rpc.json from upstream T3 Code."""
import json
import re
import sys
from pathlib import Path

root = Path(sys.argv[1] if len(sys.argv) > 1 else "/tmp/t3code-reference")
http = []
for source in (root / "packages/contracts/src/environmentHttp.ts", root / "packages/contracts/src/relay.ts"):
    text = source.read_text()
    for match in re.finditer(r"HttpApiEndpoint\.(get|post|put|patch|delete)\(\s*['\"]([^'\"]+)['\"]\s*,\s*['\"]([^'\"]+)['\"]", text):
        http.append({"source": str(source.relative_to(root)), "name": match.group(2), "method": match.group(1).upper(), "path": match.group(3)})
rpc = []
source = (root / "packages/contracts/src/rpc.ts").read_text()
block = source[source.index("export const WS_METHODS"):source.index("} as const;", source.index("export const WS_METHODS"))]
for name, tag in re.findall(r'^  (\w+): "([^"]+)"', block, re.M):
    rpc.append({"group": "general", "name": name, "tag": tag})
source = (root / "packages/contracts/src/orchestrationV2.ts").read_text()
block = source[source.index("export const ORCHESTRATION_V2_WS_METHODS"):source.index("} as const;", source.index("export const ORCHESTRATION_V2_WS_METHODS"))]
for name, tag in re.findall(r'^  (\w+): "([^"]+)"', block, re.M):
    rpc.append({"group": "orchestration_v2", "name": name, "tag": tag})
Path("contracts/http.json").write_text(json.dumps(http, indent=2) + "\n")
Path("contracts/rpc.json").write_text(json.dumps(rpc, indent=2) + "\n")
