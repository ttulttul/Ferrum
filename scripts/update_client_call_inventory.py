#!/usr/bin/env python3
"""Regenerate the inventory of server calls the pinned T3 Code client makes.

    python3 scripts/update_client_call_inventory.py <reference-checkout>

Reads the pinned upstream source (no dependencies needed) and writes
contracts/client-calls.json and docs/client-call-inventory.md:

- every Effect RPC registered in packages/contracts/src/rpc.ts, with its
  payload/success/error schemas, whether it streams, its auth scope, and every
  place apps/web, apps/desktop, packages/client-runtime or packages/shared
  invokes it (apps/mobile callers are listed separately);
- every environment HTTP endpoint and the client code that calls it, plus the
  raw HTTP and websocket paths the client builds by hand;
- which existing parity script exercises each call, and whether the
  client-driven harness (client_rpc_parity.py) does.

When the client is triggered (connect, opening a project or thread, a user
action, background) comes from the CALL_PHASE table below, keyed by the
caller's module, with per-call overrides.
"""
import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
CLIENT_DIRS = ["apps/web/src", "apps/desktop/src", "packages/client-runtime/src", "packages/shared/src"]
MOBILE_DIRS = ["apps/mobile/src"]

# Modules whose subscriptions or reads run as soon as an environment connects.
CONNECT_MODULES = ("rpc/session.ts", "state/shell.ts", "connection/", "state/auth.ts", "state/session.ts",
                   "environment/descriptor.ts", "shellSnapshotHttp.ts", "environments/primary/")
OPEN_MODULES = ("state/threads.ts", "state/threadDetail", "state/vcs.ts", "state/terminal.ts",
                "state/threadHistoryHttp.ts", "state/threadTranscriptHttp.ts", "boundedThreadSnapshotHttp.ts",
                "threadSnapshotHttp.ts", "state/review.ts", "state/fileMetadata.ts", "state/filesystem.ts",
                "state/olderSettledThreads.ts", "state/threadFamilies.ts", "state/composerPathSearch.ts")
BACKGROUND_MODULES = ("backgroundActivityReporter", "outdatedHostUpdate", "state/usage", "requestLatencyState",
                      "observability/", "relay/")
# Calls whose trigger differs from their module's default.
CALL_PHASE = {
    "server.probe": "connect", "subscribeServerConfig": "connect", "subscribeServerLifecycle": "connect",
    "subscribeAuthAccess": "connect", "subscribeDeviceState": "connect", "subscribeProjectClones": "connect",
    "server.reportClientActivity": "background", "preview.reportProfiles": "connect",
    "server.refreshProviders": "connect", "orchestration.subscribeShell": "connect",
    "pullRequests.subscribeRefreshes": "open project/thread", "subscribeTerminalMetadata": "open project/thread",
    "orchestration.subscribeThread": "open project/thread", "subscribeWorktreeSetup": "open project/thread",
    "subscribePreviewEvents": "open project/thread", "subscribeDiscoveredLocalServers": "open project/thread",
    "orchestration.getThreadTitles": "open project/thread", "orchestration.getThreadLinkMetadata": "open project/thread",
    "orchestration.getWorktreeThreadIds": "open project/thread", "server.getSettings": "user action",
    "projects.listEntries": "open project/thread", "projects.listIgnoredFiles": "open project/thread",
    "pullRequests.reportState": "background", "subscribeResourceTelemetry": "user action",
    "scheduledTasks.subscribe": "user action", "server.getUsageSummary": "background",
    "GET /.well-known/t3/environment": "connect", "POST /oauth/token": "connect",
    "GET /api/auth/session": "connect", "POST /api/auth/browser-session": "connect",
    "POST /api/auth/websocket-ticket": "connect", "POST /api/auth/websocket-ticket/:use": "connect",
    "GET /api/orchestration/shell": "connect", "GET /ws": "connect",
}
# Hand-built requests (fetch, URL strings) the typed API client does not cover.
RAW_HTTP = [
    ("GET", "/ws", "packages/client-runtime/src/rpc/session.ts", "socketUrl",
     "websocket upgrade: orchestrationProtocol=2, frameParts=1, clientCapabilities; cookie, or wsTicket from a bearer/DPoP ticket"),
    ("GET", "/api/attachments/:id", "packages/client-runtime/src/pullRequestMarkdown.ts", "/api/attachments/",
     "attachment bytes shown in the transcript"),
    ("POST", "/api/attachments/upload/:token", "apps/web/src/lib/attachmentUploadQueue.ts", 'xhr.open("POST"',
     "unauthenticated XHR upload to the signed URL attachments.createUploadUrl returns"),
    ("GET", "/api/assets/:token", "packages/shared/src/preview.ts", "/api/assets/", "asset URLs from assets.createUrl"),
    ("GET", "/api/preview-stream", "packages/client-runtime/src/preview/serverBrowserStream.ts", "/api/preview-stream",
     "server-side browser preview frames (websocket)"),
    ("GET", "/api/device-hub", "apps/web/src/state/device.ts", "/api/device-hub", "device hub proxy (websocket)"),
    ("POST", "/api/observability/v1/traces", "apps/web/src/observability/clientTracing.ts",
     "/api/observability/v1/traces", "browser trace export"),
]


def lines_of(path):
    try:
        return path.read_text(errors="replace").splitlines()
    except OSError:
        return []


def source_files(reference, dirs):
    for directory in dirs:
        for path in sorted((reference / directory).rglob("*")):
            if path.suffix in (".ts", ".tsx", ".mts") and ".test." not in path.name and ".bench." not in path.name \
                    and "__tests__" not in path.parts and path.is_file():
                yield path


def balanced(text, start):
    """The text of the bracketed expression opening at text[start]."""
    depth = 0
    for index in range(start, len(text)):
        if text[index] in "({[":
            depth += 1
        elif text[index] in ")}]":
            depth -= 1
            if depth == 0:
                return text[start:index + 1]
    raise ValueError("unbalanced")


def field(body, key):
    match = re.search(rf"\n\s*{key}:\s*", body)
    if not match:
        return None
    rest = body[match.end():]
    value, depth = "", 0
    for char in rest:
        if char in "({[":
            depth += 1
        elif char in ")}]":
            if depth == 0:
                break
            depth -= 1
        elif char == "," and depth == 0:
            break
        value += char
    value = " ".join(value.split())
    if value.startswith("Schema.Struct({"):
        return "Struct{" + ",".join(re.findall(r"(\w+)\??:", value[len("Schema.Struct({"):])[:6]) + "}"
    return value if len(value) <= 90 else value[:87] + "..."


def rpc_definitions(reference):
    source = (reference / "packages/contracts/src/rpc.ts").read_text()
    names = {}
    for constant, path in (("WS_METHODS", "packages/contracts/src/rpc.ts"),
                           ("ORCHESTRATION_V2_WS_METHODS", "packages/contracts/src/orchestrationV2.ts")):
        text = (reference / path).read_text()
        start = text.index(f"export const {constant} = {{")
        block = text[start:text.index("} as const;", start)]
        for name, tag in re.findall(r'^  (\w+): "([^"]+)"', block, re.M):
            names[(constant, name)] = tag
    definitions = {}
    for match in re.finditer(r"Rpc\.make\(\s*(WS_METHODS|ORCHESTRATION_V2_WS_METHODS)\.(\w+),", source):
        body = balanced(source, source.index("{", match.end()))
        tag = names[(match.group(1), match.group(2))]
        definitions[tag] = {
            "constant": f"{match.group(1)}.{match.group(2)}",
            "payload": field(body, "payload") or "Struct{}",
            "success": field(body, "success") or "void",
            "error": field(body, "error") or "never",
            "stream": bool(re.search(r"\n\s*stream:\s*true", body)),
            "definedAt": f"packages/contracts/src/rpc.ts:{source[:match.start()].count(chr(10)) + 1}",
        }
    return names, definitions


def find_rpc_callers(reference, names, dirs):
    constants = {f"{c}.{n}": tag for (c, n), tag in names.items()}
    pattern = re.compile(r"\b(?:ORCHESTRATION_V2_WS_METHODS|WS_METHODS)\.(\w+)")
    literal = re.compile(r"""(?:client|session\.client)\s*(?:\[\s*"([a-zA-Z.]+)"\s*\]|\.(\w+)\s*\()""")
    callers = {}
    for path in source_files(reference, dirs):
        relative = path.relative_to(reference).as_posix()
        for number, line in enumerate(lines_of(path), 1):
            stripped = line.strip()
            if stripped.startswith(("//", "*", "import")):
                continue
            for match in pattern.finditer(line):
                before = line[:match.start()]
                # Type positions and observations of another call are not calls.
                if before.rstrip().endswith("typeof") or re.search(r"[!=]==\s*$", before) \
                        or re.search(r"^\s*(?:===|!==)", line[match.end():]):
                    continue
                if before.strip() == "|":
                    continue
                prefix = "ORCHESTRATION_V2_WS_METHODS" if "ORCHESTRATION_V2" in match.group(0) else "WS_METHODS"
                tag = constants.get(f"{prefix}.{match.group(1)}")
                if tag:
                    callers.setdefault(tag, []).append(f"{relative}:{number}")
            for match in literal.finditer(line):
                tag = match.group(1) or match.group(2)
                if tag in names.values():
                    callers.setdefault(tag, []).append(f"{relative}:{number}")
    return {tag: sorted(set(found), key=found.index) for tag, found in callers.items()}


def http_endpoints(reference):
    """Endpoints of the environment API, read from HttpApiGroup.make chains."""
    text = (reference / "packages/contracts/src/environmentHttp.ts").read_text()
    endpoints = []
    for group in re.finditer(r'HttpApiGroup\.make\("(\w+)"\)', text):
        end = text.find("HttpApiGroup.make(", group.end())
        chunk = text[group.end(): end if end != -1 else len(text)]
        for endpoint in re.finditer(
                r'HttpApiEndpoint\.(get|post|put|patch|delete|del)\(\s*"(\w+)",\s*"([^"]+)"', chunk):
            method = endpoint.group(1).upper().replace("DEL", "DELETE").replace("DELETEETE", "DELETE")
            # The middleware list of this endpoint, up to the next endpoint.
            following = chunk[endpoint.end(): chunk.find("HttpApiEndpoint.", endpoint.end())]
            endpoints.append({"group": group.group(1), "name": endpoint.group(2), "method": method,
                              "path": endpoint.group(3),
                              "authenticated": "EnvironmentAuthenticated" in following
                              or "Authenticated" in chunk[:200] and "middleware" in chunk[:400]})
    return endpoints


def find_http_callers(reference, endpoints, dirs):
    files = [(p.relative_to(reference).as_posix(), lines_of(p)) for p in source_files(reference, dirs)]
    callers = {}
    for endpoint in endpoints:
        key = f"{endpoint['method']} {endpoint['path']}"
        group = endpoint["group"]
        call = re.compile(rf"(?:^\s*|\b(?:client|api|http)(?:\.{group})?)\.{endpoint['name']}\(")
        literal_path = re.escape(endpoint["path"].split("/:")[0])
        for relative, content in files:
            whole = "\n".join(content)
            uses_group = (re.search(rf'group: "{group}"|HttpApiGroupClient\([^)]*"{group}"\)', whole)
                          or f"client.{group}." in whole)
            for number, line in enumerate(content, 1):
                if (uses_group and call.search(line)) or re.search(rf'["`]{literal_path}["`/$]', line):
                    callers.setdefault(key, []).append(f"{relative}:{number}")
    return callers


def phase_of(tag, callers):
    if tag in CALL_PHASE:
        return CALL_PHASE[tag]
    if not callers:
        return "-"
    first = callers[0]
    if any(module in first for module in CONNECT_MODULES):
        return "connect"
    if any(module in first for module in BACKGROUND_MODULES):
        return "background"
    if any(module in first for module in OPEN_MODULES):
        return "open project/thread"
    return "user action"


def existing_coverage(tags_or_paths):
    """Which committed parity scripts or fixtures exercise each call, besides missing-field validation."""
    sources = {}
    for path in sorted((ROOT / "scripts").glob("*_parity.py")) + sorted((ROOT / "tests").glob("*parity*.json")):
        if path.name == "client_rpc_parity.py":
            continue
        sources[path.name] = path.read_text()
    # Node drivers that call the client runtime, by the suite that runs them.
    drivers = {suite: (ROOT / "scripts" / driver).read_text() for driver, suite in (
        ("client_initial_sync.mjs", "client_initial_sync_parity.py"),
        ("client_thread.mjs", "client_thread_parity.py"))}
    covered = {}
    for key in tags_or_paths:
        needle = key.split(" ", 1)[-1]
        found = []
        for name, text in sources.items():
            if re.search(rf'["\']{re.escape(needle)}["\'?]', text) or (
                    "/" in needle and needle.split("/:")[0] in text and len(needle.split("/:")[0]) > 5):
                found.append(name)
        for suite, text in drivers.items():
            if re.search(rf'["\[.]{re.escape(needle)}["\]( ]', text):
                found.append(suite)
        covered[key] = sorted(set(found))
    return covered


# Calls client_rpc_driver.mjs makes itself on every run, before the plan.
DRIVER_CALLS = {
    "POST /oauth/token": "auth.bootstrapBearerSession", "GET /api/auth/session": "auth.sessionState",
    "POST /api/auth/websocket-ticket/:use": "auth.webSocketTicket", "GET /ws": "environment.initialSync",
    "subscribeServerConfig": "environment.initialSync", "GET /.well-known/t3/environment": "environment.initialSync",
}


def harness_cases():
    """Case names per call in client_rpc_cases.json, the driver's own calls and the raw replays."""
    plan = ROOT / "scripts/client_rpc_cases.json"
    if not plan.exists():
        return {}, {}
    cases = {call: [name] for call, name in DRIVER_CALLS.items()}
    background = {}

    def visit(step):
        for child in step.get("parallel", []):
            visit(child)
        if "name" not in step:
            return
        call = step.get("rpc") or step.get("call") or (f"{step['method']} {step['path']}" if step.get("http") else None)
        if step.get("await"):
            call = background.get(step["await"])
        if step.get("stream", {}).get("background"):
            background[step["name"]] = call
        if call:
            cases.setdefault(call, []).append(step["name"])
    for step in json.loads(plan.read_text())["steps"]:
        visit(step)
    sys.path.insert(0, str(ROOT / "scripts"))
    from client_rpc_parity import RAW_REPLAYS
    for index, (tag, _) in enumerate(RAW_REPLAYS):
        cases.setdefault(tag, []).append(f"raw.{tag}#{index + 1}")
    xfail = json.loads((ROOT / "scripts/client_rpc_parity_xfail.json").read_text())["cases"]
    return cases, xfail


def main():
    reference = Path(sys.argv[1] if len(sys.argv) > 1 else "/tmp/t3code-reference").resolve()
    names, definitions = rpc_definitions(reference)
    callers = find_rpc_callers(reference, names, CLIENT_DIRS)
    mobile = find_rpc_callers(reference, names, MOBILE_DIRS)
    rpc_meta = {entry["tag"]: entry for entry in json.loads((ROOT / "contracts/rpc.json").read_text())}
    endpoints = http_endpoints(reference)
    http_callers = find_http_callers(reference, endpoints, CLIENT_DIRS)
    http_mobile = find_http_callers(reference, endpoints, MOBILE_DIRS)
    http_meta = {f"{e['method']} {e['path']}": e for e in json.loads((ROOT / "contracts/http.json").read_text())
                 if e["source"].endswith("environmentHttp.ts")}
    harness, xfail = harness_cases()

    calls = []
    for tag in sorted(definitions):
        definition = definitions[tag]
        found = callers.get(tag, [])
        calls.append({
            "kind": "rpc-stream" if definition["stream"] else "rpc",
            "call": tag,
            "callers": found[:4],
            "callerCount": len(found),
            "mobileCallers": mobile.get(tag, [])[:2],
            "trigger": phase_of(tag, found),
            "request": definition["payload"],
            "response": definition["success"],
            "error": definition["error"],
            "stream": definition["stream"],
            "auth": f"session ({rpc_meta.get(tag, {}).get('scope', '?')})",
            "usage": "client" if found else ("mobile-only" if mobile.get(tag) else "server-only/unused"),
        })
    for endpoint in endpoints:
        key = f"{endpoint['method']} {endpoint['path']}"
        found = http_callers.get(key, [])
        meta = http_meta.get(key, {})
        auth = "session (cookie/bearer/DPoP)" if meta.get("auth") else "none"
        calls.append({
            "kind": "http", "call": key, "callers": found[:4], "callerCount": len(found),
            "mobileCallers": http_mobile.get(key, [])[:2], "trigger": phase_of(key, found),
            "request": f"{endpoint['group']}.{endpoint['name']}", "response": f"{endpoint['group']}.{endpoint['name']}",
            "error": "", "stream": False, "auth": auth,
            "usage": "client" if found else ("mobile-only" if http_mobile.get(key) else "server-only/unused"),
        })
    for method, path, caller, needle, note in RAW_HTTP:
        key = f"{method} {path}"
        located = [f"{caller}:{n}" for n, line in enumerate(lines_of(reference / caller), 1) if needle in line][:2]
        calls.append({"kind": "http-raw", "call": key, "callers": located, "callerCount": len(located),
                      "mobileCallers": [], "trigger": CALL_PHASE.get(key, "user action"),
                      "request": note, "response": "", "error": "", "stream": path in (
                          "/ws", "/api/preview-stream", "/api/device-hub"),
                      "auth": "signed URL" if "/:token" in path else "session", "usage": "client"})
    coverage = existing_coverage([c["call"] for c in calls])
    for call in calls:
        call["existingCoverage"] = coverage[call["call"]]
        call["validationOnly"] = call["kind"].startswith("rpc") and not coverage[call["call"]]
        call["clientHarness"] = harness.get(call["call"], [])
        call["knownGaps"] = [name for name in call["clientHarness"] if name in xfail]

    (ROOT / "contracts/client-calls.json").write_text(json.dumps(
        {"reference": "pingdotgg/t3code@fd0331ca9b1596ed7018c80f52b7e6730014f593",
         "generatedBy": "scripts/update_client_call_inventory.py", "calls": calls}, indent=2) + "\n")
    write_markdown(calls)


def write_markdown(calls):
    def count(predicate):
        return sum(1 for call in calls if predicate(call))

    kinds = ["rpc", "rpc-stream", "http", "http-raw"]
    out = ["# Client call inventory", "",
           "Every place the pinned T3 Code client (`pingdotgg/t3code@fd0331ca`) talks to its server, generated by",
           "`scripts/update_client_call_inventory.py` from the upstream source. The machine-readable form is",
           "[`contracts/client-calls.json`](../contracts/client-calls.json). Regenerate both after changing the pin",
           "or the harness cases:", "", "```sh", "python3 scripts/update_client_call_inventory.py /path/to/reference",
           "```", "",
           "Callers are searched in `apps/web`, `apps/desktop` (main and preloads), `packages/client-runtime` and",
           "`packages/shared`; `apps/mobile` callers are listed separately. *Trigger* is when a client issues the",
           "call: on connect, on opening a project or thread, on a user action, or in the background.",
           "*Existing* lists committed parity scripts that already exercise the call (beyond the generic",
           "missing-field check every RPC gets from `parity_harness.py --check-required-fields`). *Client harness*",
           "lists the `client_rpc_parity.py` cases that call it through the reference client runtime.", "",
           "## Summary", "", "| Kind | Calls | Used by the client | Mobile only | Server-only / unused | "
           "Existing parity | Client harness | Either |", "|---|---|---|---|---|---|---|---|"]
    for kind in kinds:
        subset = [c for c in calls if c["kind"] == kind]
        out.append(f"| {kind} | {len(subset)} | {sum(c['usage'] == 'client' for c in subset)} | "
                   f"{sum(c['usage'] == 'mobile-only' for c in subset)} | "
                   f"{sum(c['usage'] == 'server-only/unused' for c in subset)} | "
                   f"{sum(bool(c['existingCoverage']) for c in subset)} | "
                   f"{sum(bool(c['clientHarness']) for c in subset)} | "
                   f"{sum(bool(c['existingCoverage'] or c['clientHarness']) for c in subset)} |")
    out.append(f"| **all** | {len(calls)} | {count(lambda c: c['usage'] == 'client')} | "
               f"{count(lambda c: c['usage'] == 'mobile-only')} | "
               f"{count(lambda c: c['usage'] == 'server-only/unused')} | "
               f"{count(lambda c: bool(c['existingCoverage']))} | {count(lambda c: bool(c['clientHarness']))} | "
               f"{count(lambda c: bool(c['existingCoverage'] or c['clientHarness']))} |")
    out += ["", "Newly covered by the client harness (no earlier parity script): "
            f"{count(lambda c: bool(c['clientHarness']) and not c['existingCoverage'])}. Calls with a known Rust "
            f"gap (an expected failure in `scripts/client_rpc_parity_xfail.json`): "
            f"{count(lambda c: bool(c['knownGaps']))}.", "",
            "Calls the harness does not make need credentials or hardware it cannot have in CI (GitHub, Linear,",
            "model providers, T3 Connect relay, voice models, simulators, a browser), are destructive to the host",
            "(server self-update, process signals, opening an editor), or are covered by the scripts in *Existing*.",
            ""]
    titles = {"rpc": "Unary RPCs", "rpc-stream": "Streaming RPCs", "http": "Environment HTTP API",
              "http-raw": "Hand-built HTTP and websocket requests"}
    for kind in kinds:
        out += [f"## {titles[kind]}", "",
                "| Call | Trigger | Callers | Request | Response | Auth | Usage | Existing | Client harness |",
                "|---|---|---|---|---|---|---|---|---|"]
        for call in [c for c in calls if c["kind"] == kind]:
            callers = "<br>".join(f"`{c}`" for c in call["callers"][:2]) or "—"
            if call["callerCount"] > 2:
                callers += f"<br>(+{call['callerCount'] - 2})"
            if call["mobileCallers"] and not call["callers"]:
                callers = "mobile: " + "<br>".join(f"`{c}`" for c in call["mobileCallers"])
            cell = lambda text: (text or "").replace("|", "\\|")
            harness = f"{len(call['clientHarness'])} case(s)" if call["clientHarness"] else "—"
            if call["knownGaps"]:
                harness += f", {len(call['knownGaps'])} xfail"
            out.append(f"| `{call['call']}` | {call['trigger']} | {callers} | {cell(call['request'])} | "
                       f"{cell(call['response'])} | {cell(call['auth'])} | {call['usage']} | "
                       f"{', '.join(call['existingCoverage']) or ('validation only' if call['validationOnly'] else '—')}"
                       f" | {harness} |")
        out.append("")
    (ROOT / "docs").mkdir(exist_ok=True)
    (ROOT / "docs/client-call-inventory.md").write_text("\n".join(out))


if __name__ == "__main__":
    main()
