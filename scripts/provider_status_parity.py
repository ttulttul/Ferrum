#!/usr/bin/env python3
"""Compare the provider status checks of both servers with fake provider CLIs.

Each scenario starts each server on a fresh home with fake `claude` and
`codex` executables (or none), settings seeded before the start, and waits
until the server has checked the scenario's provider instance. It then
compares that instance's row in `server.getConfig`: installed, version,
status, auth, message, models, slash commands, skills, usage limits and the
version and compatibility advisories, with check times left out and the
temporary home masked. A scenario with "refresh" then changes what the fake
CLI answers, calls `server.refreshProviders` with a config subscription open,
and compares the refresh's row and the `providerStatuses` event the
subscription receives.

Every scenario turns `enableProviderUpdateChecks` off, so neither server
looks up the latest CLI version or the model manifest on the network, and
the fakes never call a model. The fakes replay tests/fixtures/providers/
status_*.json through fake_cli.py; `--version` is answered by the wrapper.

Scenarios run side by side (--jobs at a time), and so do both servers within
a scenario: each has its own home, port and fakes, and most of a scenario is
waiting on the servers' own probe timeouts.

Known differences are listed in provider_status_parity_xfail.json; an entry
that starts matching fails the run until it is removed.
"""
import argparse
import contextlib
import hashlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import threading
import json
import os
from pathlib import Path
import secrets
import shlex
import shutil
import sys
import tempfile
import time

from websockets.sync.client import connect

from client_initial_sync_parity import rpc
from fake_https_proxy import FakeHttpsProxy
from parity_harness import PROVIDER_CLIS, RELAY_TOOLS, encode, managed_server, path_without, run_concurrently

ROOT = Path(__file__).resolve().parents[1]
FIXTURES = ROOT / "tests/fixtures/providers"
XFAIL = Path(__file__).with_name("provider_status_parity_xfail.json")
TIMES = {"checkedAt", "updatedAt", "lastCheckedAt"}
# Package managers and installers a server may ask for a provider's latest version. They are hidden
# from both servers, so only a scenario's fakes answer and no lookup reaches a real registry.
INSTALLERS = {"npm", "pnpm", "bun", "mise", "brew", "vp", "yarn", "volta", "port"}

# How each fake answers `--version`.
VERSION = {
    "claude": {"ok": 'echo "2.1.300 (Claude Code)"; exit 0', "old": 'echo "2.1.200 (Claude Code)"; exit 0',
               "fails": 'echo "claude: cannot load config" >&2; exit 2', "hangs": "exec sleep 60"},
    "codex": {"ok": 'echo "codex-cli 0.170.0"; exit 0', "fails": 'echo "codex: broken" >&2; exit 2',
              "hangs": "exec sleep 60"},
}
# How the fake Codex answers `app-server`.
APP_SERVER = {
    "ok": 'exec python3 "$FAKE_PROVIDER_CLI" "$@"',
    "exits": 'echo "codex: unsupported config key" >&2; exit 3',
}


def claude_script(version):
    return f"""#!/bin/sh
case "$*" in
  --version*) {VERSION["claude"][version]};;
  *--output-format*stream-json*) exec python3 "$FAKE_PROVIDER_CLI" "$@";;
esac
exit 0
"""


def codex_script(version, app_server):
    return f"""#!/bin/sh
case "$1" in
  --version) {VERSION["codex"][version]};;
  app-server) {APP_SERVER[app_server]};;
esac
exit 0
"""


# name, instance to compare, fakes to install ({cli: {...}}), settings, and how long the
# reference may take (a timeout scenario waits out its deadline).
SCENARIOS = [
    {"name": "claude signed in", "instance": "claudeAgent",
     "fakes": {"claude": {"version": "ok", "fixture": "status_claude_signed_in.json"}}},
    {"name": "claude signed out", "instance": "claudeAgent",
     "fakes": {"claude": {"version": "ok", "fixture": "status_claude_signed_out.json"}}},
    {"name": "claude signed out with a config directory", "instance": "claudeAgent",
     "fakes": {"claude": {"version": "ok", "fixture": "status_claude_signed_out.json"}},
     "settings": {"providerInstances": {"claudeAgent": {"driver": "claudeAgent",
                                                        "config": {"homePath": "~/claude-work"}}}}},
    {"name": "claude too old for the newest models", "instance": "claudeAgent",
     "fakes": {"claude": {"version": "old", "fixture": "status_claude_signed_in.json"}}},
    {"name": "claude missing", "instance": "claudeAgent", "fakes": {}},
    {"name": "claude version fails", "instance": "claudeAgent",
     "fakes": {"claude": {"version": "fails", "fixture": "status_claude_signed_in.json"}}},
    {"name": "claude version times out", "instance": "claudeAgent", "wait": 45,
     "fakes": {"claude": {"version": "hangs", "fixture": "status_claude_signed_in.json"}}},
    {"name": "claude account check times out", "instance": "claudeAgent", "wait": 45,
     "fakes": {"claude": {"version": "ok", "fixture": "status_silent.json"}}},
    {"name": "claude disabled", "instance": "claudeAgent", "settled": True,
     "fakes": {"claude": {"version": "ok", "fixture": "status_claude_signed_in.json"}},
     "settings": {"providerInstances": {"claudeAgent": {"driver": "claudeAgent", "enabled": False}}}},
    {"name": "claude custom binaryPath", "instance": "claudeAgent",
     "fakes": {"claude": {"version": "ok", "fixture": "status_claude_signed_in.json", "at": "tools/claude-custom"}},
     "settings": {"providerInstances": {"claudeAgent": {"driver": "claudeAgent",
                                                        "config": {"binaryPath": "~/tools/claude-custom"}}}}},
    {"name": "claude custom binaryPath missing", "instance": "claudeAgent",
     "fakes": {"claude": {"version": "ok", "fixture": "status_claude_signed_in.json"}},
     "settings": {"providerInstances": {"claudeAgent": {"driver": "claudeAgent",
                                                        "config": {"binaryPath": "/nonexistent/claude"}}}}},
    {"name": "claude sign-in picked up by a refresh", "instance": "claudeAgent",
     "fakes": {"claude": {"version": "ok", "fixture": "status_claude_signed_out.json"}},
     "refresh": "status_claude_signed_in.json"},
    {"name": "codex signed in", "instance": "codex",
     "fakes": {"codex": {"version": "ok", "app": "ok", "fixture": "status_codex_signed_in.json"}}},
    {"name": "codex signed out", "instance": "codex",
     "fakes": {"codex": {"version": "ok", "app": "ok", "fixture": "status_codex_signed_out.json"}}},
    {"name": "codex signed out with a home", "instance": "codex",
     "fakes": {"codex": {"version": "ok", "app": "ok", "fixture": "status_codex_signed_out.json"}},
     "settings": {"providerInstances": {"codex": {"driver": "codex", "config": {"homePath": "~/codex-work"}}}}},
    {"name": "codex missing", "instance": "codex", "fakes": {}},
    {"name": "codex app-server exits", "instance": "codex",
     "fakes": {"codex": {"version": "ok", "app": "exits", "fixture": "status_codex_signed_in.json"}}},
    {"name": "codex app-server exits and version fails", "instance": "codex",
     "fakes": {"codex": {"version": "fails", "app": "exits", "fixture": "status_codex_signed_in.json"}}},
    {"name": "codex times out", "instance": "codex", "wait": 45,
     "fakes": {"codex": {"version": "ok", "app": "ok", "fixture": "status_silent.json"}}},
    {"name": "codex disabled", "instance": "codex", "settled": True,
     "fakes": {"codex": {"version": "ok", "app": "ok", "fixture": "status_codex_signed_in.json"}},
     "settings": {"providerInstances": {"codex": {"driver": "codex", "enabled": False}}}},
    {"name": "codex custom binaryPath", "instance": "codex",
     "fakes": {"codex": {"version": "ok", "app": "ok", "fixture": "status_codex_signed_in.json",
                         "at": "tools/codex-custom"}},
     "settings": {"providerInstances": {"codex": {"driver": "codex",
                                                  "config": {"binaryPath": "~/tools/codex-custom"}}}}},
    {"name": "codex custom binaryPath missing", "instance": "codex",
     "fakes": {"codex": {"version": "ok", "app": "ok", "fixture": "status_codex_signed_in.json"}},
     "settings": {"providerInstances": {"codex": {"driver": "codex",
                                                  "config": {"binaryPath": "/nonexistent/codex"}}}}},
    {"name": "codex sign-in picked up by a refresh", "instance": "codex",
     "fakes": {"codex": {"version": "ok", "app": "ok", "fixture": "status_codex_signed_out.json"}},
     "refresh": "status_codex_signed_in.json"},
]


def enabled(instance, driver=None, **config):
    """Settings that enable `instance` (off by default for every driver but Codex and Claude)."""
    entry = {"driver": driver or instance, "enabled": True}
    if config:
        entry["config"] = config
    return {"providerInstances": {instance: entry}}


def ok(stdout="", exit=0, stderr=""):
    return {"stdout": stdout, "stderr": stderr, "exit": exit}


def driver_scenarios(instance, cli, version, signed_in, variants, wait=30):
    """The scenarios every driver gets, from its commands when found and signed in (`signed_in`,
    a list of spec entries after `--version`) and its own `variants` ({name: (commands, extra)}):
    missing, `--version` failing and hanging, disabled, and a custom binaryPath."""
    version_entry = {"args": ["--version"], **ok(version)}
    def spec(commands):
        return {"commands": commands}
    scenarios = [
        {"name": f"{instance} signed in", "instance": instance, "settings": enabled(instance),
         "fakes": {cli: {"spec": spec([version_entry, *signed_in])}}},
        {"name": f"{instance} missing", "instance": instance, "settings": enabled(instance), "fakes": {}},
        {"name": f"{instance} version fails", "instance": instance, "settings": enabled(instance),
         "fakes": {cli: {"spec": spec([{"args": ["--version"], **ok("", 2, f"{cli}: cannot start")}])}}},
        {"name": f"{instance} version times out", "instance": instance, "settings": enabled(instance), "wait": wait,
         "fakes": {cli: {"spec": spec([{"args": ["--version"], "sleep": 60}])}}},
        {"name": f"{instance} disabled", "instance": instance, "settled": True,
         "settings": {"providerInstances": {instance: {"driver": instance, "enabled": False}}},
         "fakes": {cli: {"spec": spec([version_entry, *signed_in])}}},
        {"name": f"{instance} custom binaryPath", "instance": instance,
         "settings": enabled(instance, binaryPath=f"~/tools/{cli}-custom"),
         "fakes": {cli: {"spec": spec([version_entry, *signed_in]), "at": f"tools/{cli}-custom"}}},
    ]
    for name, (commands, extra) in variants.items():
        scenarios.append({"name": f"{instance} {name}", "instance": instance, "settings": enabled(instance),
                          "fakes": {cli: {"spec": spec([version_entry, *commands])}}, **extra})
    return scenarios


FX_STATUS = {"args": ["status", "--json"], **ok('{"auth": "oauth", "model": "fx-large"}')}
FX_MODELS = {"args": ["models", "--json"], **ok('{"ids": ["fx-large", "fx-small", "default", "fx-small"]}')}
SCENARIOS += driver_scenarios("fx", "fx", "fx 0.0.13", [FX_STATUS, FX_MODELS], {
    "signed out": ([{"args": ["status", "--json"], **ok('{"auth": "missing"}')}, FX_MODELS], {}),
    "login unknown": ([{"args": ["status", "--json"], **ok("not json")}, FX_MODELS], {}),
    "model listing fails": ([FX_STATUS, {"args": ["models", "--json"], **ok("", 1, "offline")}], {}),
    "status times out": ([{"args": ["status", "--json"], "sleep": 60}, FX_MODELS], {"wait": 40}),
    "sign-in picked up by a refresh": ([{"args": ["status", "--json"], **ok('{"auth": "missing"}')}, FX_MODELS],
                                       {"refresh": {"fx": {"commands": [{"args": ["--version"], **ok("fx 0.0.13")},
                                                                        FX_STATUS, FX_MODELS]}}}),
})

KIRO_TEMPLATE = {"configOptions": [{"id": "mode", "type": "select", "currentValue": "kiro_default", "options": [
    {"value": "kiro_default", "name": "Default"},
    {"value": "planner", "name": "Planner", "description": "Plans before it edits"},
    {"value": " ", "name": "blank"}]}]}
KIRO_ACP = {"prefix": ["acp"], "rpc": {
    "initialize": {"protocolVersion": 1, "agentCapabilities": {"loadSession": True}, "authMethods": []},
    "_kiro/config/template": KIRO_TEMPLATE}}
# Agents that refuse over JSON-RPC rather than exit: a process that exits while the reference writes
# its first request races that write against the exit, and the reference can then never finish
# the check (seen on CI). A refusal fails the same request deterministically.
KIRO_ACP_NO_AGENTS = {"prefix": ["acp"], "rpc": {
    "initialize": {"protocolVersion": 1, "agentCapabilities": {"loadSession": True}, "authMethods": []}},
    "rpcErrors": {"_kiro/config/template": {"code": -32603, "message": "agents unavailable"}}}
KIRO_WHOAMI = {"args": ["whoami", "--format", "json"],
               **ok('{"accountType": "BuilderId", "email": "dev@example.com"}\nProfile: default')}
KIRO_MODELS = {"args": ["chat", "--list-models", "--format", "json"], **ok(json.dumps({
    "models": [{"model_id": "auto", "model_name": "Auto"}, {"model_id": "claude-opus-5.5", "model_name": "Opus 5.5"},
               {"model_id": "mystery-1"}], "default_model": "auto"}))}
SCENARIOS += driver_scenarios("kiro", "kiro-cli", "kiro-cli 2.32.0", [KIRO_WHOAMI, KIRO_MODELS, KIRO_ACP], {
    "signed out": ([{"args": ["whoami", "--format", "json"], **ok('{"account":null}', 1)}, KIRO_MODELS, KIRO_ACP], {}),
    "sign-in unknown": ([{"args": ["whoami", "--format", "json"], **ok("boom", 3)}, KIRO_MODELS, KIRO_ACP], {}),
    "api key": ([{"args": ["whoami", "--format", "json"], **ok('{"accountType": "ApiKey"}')}, KIRO_MODELS,
                 KIRO_ACP], {}),
    "without a model list or agents": ([KIRO_WHOAMI, {"args": ["chat", "--list-models", "--format", "json"],
                                                      **ok("", 1)}, KIRO_ACP_NO_AGENTS], {}),
    "default model not listed": ([KIRO_WHOAMI, {"args": ["chat", "--list-models", "--format", "json"], **ok(
        json.dumps({"models": [{"model_id": "claude-sonnet-5", "model_name": "Sonnet 5"}]}))}, KIRO_ACP], {}),
})

GROK_MODELS = {"args": ["models"], **ok("You are logged in as dev@example.com.\n\nModels:\n  * grok-build (default)\n  * grok-4-fast\n  - grok_code\n")}
GROK_SKILLS = {"args": ["inspect", "--json"], **ok(json.dumps({"skills": [
    {"name": "zeta", "description": "Last", "source": {"type": "user", "path": "/skills/zeta/SKILL.md"}},
    {"name": "Deploy", "description": " Ship it ", "userInvocable": False,
     "source": {"type": "project", "path": "/repo/.grok/skills/deploy/SKILL.md"}},
    {"name": "no-path", "source": {}}]}))}
GROK_INITIALIZE = {"protocolVersion": 1, "agentCapabilities": {"loadSession": True}, "authMethods": [], "_meta": {
    "modelState": {"currentModelId": "grok-build", "availableModels": [
        {"modelId": "grok-build", "name": "Grok Build", "_meta": {"reasoningEffort": "high", "reasoningEfforts": [
            {"value": "max", "label": "Max"}, {"value": "high", "label": "High", "default": True},
            {"id": "low", "label": "Low", "description": "Quick"}]}},
        {"modelId": "grok-4-fast", "name": " ", "_meta": {"supportsReasoningEffort": False}}]},
    "availableCommands": [{"name": "review", "description": "Review changes", "input": {"hint": " paths "}},
                          {"name": "compact", "description": "Compact now"},
                          {"name": "context", "description": "Show context"},
                          {"name": "always-approve", "description": "Approve all"}, {"name": "bad"}]}}
GROK_ACP = {"prefix": ["agent", "stdio"], "rpc": {"initialize": GROK_INITIALIZE}}
GROK_ACP_REFUSES = {"prefix": ["agent", "stdio"], "rpc": {},
                    "rpcErrors": {"initialize": {"code": -32603, "message": "agent failed to start"}}}
SCENARIOS += driver_scenarios("grok", "grok", "grok 1.0.52", [GROK_MODELS, GROK_SKILLS, GROK_ACP], {
    "signed out": ([{"args": ["models"], **ok("Not authenticated. Run `grok login`.")}, GROK_SKILLS, GROK_ACP], {}),
    "sign-in unknown": ([{"args": ["models"], **ok("", 1, "boom")}, GROK_SKILLS, GROK_ACP], {}),
    "acp initialize fails": ([GROK_MODELS, GROK_SKILLS, GROK_ACP_REFUSES], {}),
    "no models listed": ([{"args": ["models"], **ok("You are logged in.")}, GROK_SKILLS,
                          {"prefix": ["agent", "stdio"], "rpc": {"initialize": {
                              "protocolVersion": 1, "agentCapabilities": {}, "authMethods": []}}}], {}),
    "api key": ([{"args": ["models"], **ok("Not logged in.")}, GROK_SKILLS, GROK_ACP],
                {"settings": {"providerInstances": {"grok": {"driver": "grok", "enabled": True, "environment": [
                    {"name": "XAI_API_KEY", "value": "xai-test-key"}]}}}}),
    "skills fail": ([GROK_MODELS, {"args": ["inspect", "--json"], **ok("not json")}, GROK_ACP], {}),
})

PI_STATE = {"model": {"provider": "anthropic", "id": "claude-sonnet-5"}, "thinkingLevel": "minimal"}
PI_MODELS = {"models": [
    {"provider": "anthropic", "id": "claude-sonnet-5", "name": "Sonnet 5", "reasoning": True,
     "thinkingLevelMap": {"minimal": None, "max": "max"}},
    {"provider": "openai", "id": "gpt-5.6", "reasoning": False,
     "serviceTiers": [{"id": "flex", "name": "Flex", "description": "Cheaper"}, {"id": "default"}, {"id": " "}]},
    {"provider": "anthropic", "id": "claude-sonnet-5"}]}
PI_COMMANDS = {"commands": [
    {"name": "review", "description": " Review a diff "}, {"name": "compact", "description": "theirs"},
    {"name": "skill:deploy", "source": "skill", "description": "Ship it",
     "sourceInfo": {"path": "/skills/deploy/SKILL.md", "scope": "Global", "displayName": "Deploy"}},
    {"name": "lint", "source": "skill", "location": "workspace", "interface": {"shortDescription": "Lints"}},
    {"name": " "}]}
PI_TEXT_GENERATION = {"prefix": ["--mode", "rpc", "--no-session", "--offline", "--no-extensions"],
                      "pi": {"get_available_models": {"models": [{"provider": "openai", "id": "gpt-5.6"}]}}}
def pi_rpc(**data):
    return {"prefix": ["--mode", "rpc"], "pi": {"get_state": PI_STATE, "get_available_models": PI_MODELS,
                                                  "get_commands": PI_COMMANDS, **data}}
SCENARIOS += driver_scenarios("pi", "pi", "0.85.0", [PI_TEXT_GENERATION, pi_rpc()], {
    "too old": ([], {"fakes": {"pi": {"spec": {"commands": [{"args": ["--version"], **ok("pi 0.84.2")}]}}}}),
    "version unreadable": ([], {"fakes": {"pi": {"spec": {"commands": [{"args": ["--version"], **ok("pi dev")}]}}}}),
    "no usable models": ([pi_rpc(get_available_models={"models": []})], {}),
    "default model unavailable": ([PI_TEXT_GENERATION, pi_rpc(get_state={"model": {"provider": "google", "id": "g-1"}}),
                                   {"args": ["auth", "check", "--provider", "google", "--json", "--no-refresh"],
                                    **ok('{"reason": "credentials_not_configured"}')}], {}),
    "discovery fails": ([pi_rpc(get_available_models=None) | {"piErrors": ["get_available_models"]}], {}),
    "discovery exits then declines project trust": ([PI_TEXT_GENERATION,
        {"prefix": ["--mode", "rpc", "--no-session", "--no-approve"], "pi": pi_rpc()["pi"]},
        # Pi's output closes after its state: both servers see a closed output (not a failed write).
        {"prefix": ["--mode", "rpc"], "pi": {"get_state": PI_STATE}, "closeOutputAfter": 1}], {}),
    # Pi stops reading after its state while its output stays open: the next request fails to
    # write, which neither server retries.
    "discovery stops reading": ([PI_TEXT_GENERATION,
        {"prefix": ["--mode", "rpc"], "pi": {"get_state": PI_STATE}, "closeInputAfter": 1}], {}),
    "text generation has no models": ([{**PI_TEXT_GENERATION, "pi": {"get_available_models": {"models": []}}},
                                       pi_rpc()], {}),
    "bad launch arguments": ([pi_rpc()], {"settings": enabled("pi", launchArgs="--mode json")}),
    "absolute binaryPath": ([PI_TEXT_GENERATION, pi_rpc()], {}),
})
def cursor_credential(instance, **fields):
    """A browser sign-in as the reference's secret store keeps it."""
    digest = hashlib.sha256(f"6:cursor{instance}".encode()).hexdigest()
    return {f"userdata/secrets/provider-auth-{digest}.bin": json.dumps({
        "version": 1, "backendUrl": "https://api.cursor.com", "apiKey": "browser-key",
        "createdAtMs": 1760000000000, **fields})}


CURSOR_USER = {"status": 200, "body": {"apiKeyName": "laptop", "createdAt": "2026-01-01T00:00:00Z",
                                       "userId": 7, "userEmail": " dev@example.com "}}
CURSOR_MODELS = {"status": 200, "body": {"items": [
    {"id": "composer-2", "displayName": "Composer 2", "aliases": ["composer", " "],
     "parameters": [
         {"id": "fast", "values": [{"value": "true"}, {"value": "false"}]},
         {"id": "thinking", "displayName": "Thinking", "values": [{"value": "on"}]},
         {"id": "effort", "values": [{"value": "low", "displayName": "Low"}, {"value": "high"}, {"value": " "}]},
         {"id": "context", "displayName": "Context", "values": [{"value": "200k"}, {"value": "1m", "displayName": "1M"}]},
         {"id": "empty", "values": []}],
     "variants": [{"displayName": "Default", "isDefault": True,
                   "params": [{"id": "effort", "value": "high"}, {"id": "fast", "value": "false"}]}]},
    {"id": "claude-opus-5-5", "displayName": "Opus 5.5"},
    {"id": "composer-2", "displayName": "Duplicate"}, {"id": "nameless", "displayName": " "}]}}
CURSOR_USAGE = {"status": 200, "body": {"billingCycleStart": "1764547200000", "billingCycleEnd": 1767225600000,
                                        "planUsage": {"totalPercentUsed": 42.5, "autoPercentUsed": 10,
                                                      "apiPercentUsed": 120}}}
CURSOR_API = {"GET /v1/me": CURSOR_USER, "GET /v1/models": CURSOR_MODELS,
              "POST /aiserver.v1.DashboardService/GetCurrentPeriodUsage": CURSOR_USAGE}


def cursor_key(**environment):
    """Cursor with an API key, its usage read from the fake API (never the real endpoint)."""
    env = {"CURSOR_API_KEY": "key-123", "CURSOR_AUTH_TOKEN": "token-456", **environment}
    return {"providerInstances": {"cursor": {
        "driver": "cursor", "enabled": True, "config": {"apiEndpoint": "<api>"},
        "environment": [{"name": k, "value": v} for k, v in env.items() if v is not None]}}}


def cursor(name, settings=None, api=None, **extra):
    return {"name": f"cursor {name}", "instance": "cursor", "settings": settings or cursor_key(),
            "api": {**CURSOR_API, **(api or {})}, "fakes": {}, **extra}


SCENARIOS += [
    cursor("signed out", enabled("cursor")),
    cursor("signed in with an API key"),
    cursor("disabled", {"providerInstances": {"cursor": {"driver": "cursor", "enabled": False}}}, settled=True),
    cursor("API key rejected", api={"GET /v1/models": {"status": 401, "body": {"error": {"message": "bad key"}}}}),
    cursor("plan required", api={"GET /v1/models": {"status": 403, "body": {"error": {"code": "plan_required"}}}}),
    cursor("account details blocked", api={"GET /v1/me": {"status": 403, "body": {"code": "team_blocked"}}}),
    cursor("catalog fails", api={"GET /v1/models": {"status": 500, "body": {}}}),
    cursor("account response invalid", api={"GET /v1/me": {"status": 200, "body": {"apiKeyName": "x"}}}),
    cursor("catalog times out", api={"GET /v1/models": {**CURSOR_MODELS, "sleep": 20}}, wait=60),
    cursor("no models", api={"GET /v1/models": {"status": 200, "body": {"items": []}}}),
    cursor("usage needs the default endpoint", cursor_key(CURSOR_AUTH_TOKEN=None)),
    cursor("usage fails", api={"POST /aiserver.v1.DashboardService/GetCurrentPeriodUsage": {"status": 500}}),
    cursor("usage without plan figures",
           api={"POST /aiserver.v1.DashboardService/GetCurrentPeriodUsage": {"status": 200, "body": {}}}),
    cursor("signed in with a browser", {"providerInstances": {"cursor": {
        "driver": "cursor", "enabled": True, "config": {"apiEndpoint": "<api>"},
        "environment": [{"name": "CURSOR_AUTH_TOKEN", "value": "token-456"}]}}},
        files=cursor_credential("cursor", email="dev@example.com")),
    cursor("browser sign-in expired", enabled("cursor"), files=cursor_credential("cursor", apiKeyExpiresAtMs=1000)),
    cursor("browser sign-in rejected", enabled("cursor", apiEndpoint="<api>"),
           api={"GET /v1/me": {"status": 401, "body": {}}}, files=cursor_credential("cursor")),
    cursor("legacy browser sign-in", enabled("cursor", apiEndpoint="<api>"), files={
        "userdata/provider-auth/cursor/cursor.json": json.dumps({
            "version": 1, "backendUrl": "https://api.cursor.com", "apiKey": "legacy-key", "createdAtMs": 1})}),
]


OC_V1_PROVIDERS = {"all": [
    {"id": "anthropic", "name": "Anthropic", "models": {
        "claude-opus-5-5": {"id": "claude-opus-5-5", "name": "Claude Opus 5.5", "family": "claude-opus",
                            "release_date": "2026-09-01", "status": "active",
                            "variants": {"low": {}, "high": {}, "max": {}}},
        "claude-opus-5": {"id": "claude-opus-5", "name": "Claude Opus 5", "family": "claude-opus",
                          "release_date": "2026-03-01", "variants": {"high": {}}},
        "claude-old": {"id": "claude-old", "name": "Claude Old", "release_date": "2024-01-01", "status": "deprecated"},
        "nameless": {"id": "nameless", "name": " ", "release_date": "2026-01-01"}}},
    {"id": "openai", "name": "OpenAI", "models": {
        "gpt-5.6": {"id": "gpt-5.6", "name": "GPT-5.6", "release_date": "not a date",
                    "variants": {"minimal": {}, "medium": {}, "high": {}}}}},
    {"id": "unconnected", "name": "Unconnected", "models": {
        "x": {"id": "x", "name": "X", "release_date": "2026-01-01"}}}],
    "connected": ["anthropic", "openai"], "default": {}}
OC_V1_ROUTES = {
    "GET /global/health": {"body": {"healthy": True, "version": "1.15.0"}},
    "GET /provider": {"body": OC_V1_PROVIDERS},
    "GET /agent": {"body": [{"name": "plan", "mode": "primary"}, {"name": "build", "mode": "all"},
                            {"name": "general", "mode": "subagent"}, {"name": "secret", "mode": "primary", "hidden": True}]},
    "GET /skill": {"body": [{"name": "zeta", "description": " Last ", "location": "/skills/zeta/SKILL.md"},
                            {"name": "alpha", "location": "/skills/alpha/SKILL.md"}, {"name": "no-location"}]},
    "GET /command": {"body": [{"name": "review", "description": "Review", "source": "command", "hints": ["$1", "files"]},
                              {"name": "compact", "hints": []}, {"name": "alpha", "source": "skill", "hints": []},
                              {"name": " ", "hints": []}]},
}
def oc_model(provider, id, name, variants=(), released=1767225600000, family=None, status="active"):
    model = {"id": id, "modelID": id, "providerID": provider, "name": name,
             "capabilities": {"tools": True, "input": ["text"], "output": ["text"]},
             "variants": [{"id": v} for v in variants], "time": {"released": released}, "cost": [],
             "status": status, "enabled": True, "limit": {"context": 200000, "output": 32000}}
    if family:
        model["family"] = family
    return model
OC_V2_MODELS = [oc_model("anthropic", "claude-opus-5-5", "Claude Opus 5.5", ["low", "high"], family="claude-opus"),
                oc_model("anthropic", "claude-opus-5", "Claude Opus 5", ["high"], 1750000000000, "claude-opus"),
                oc_model("opencode", "big-pickle", "Big Pickle", ["low", "medium"]),
                oc_model("openai", "gpt-5.6", "GPT-5.6", ["medium"], status="deprecated")]
OC_V2_ROUTES = {
    "GET /api/info": {"body": {"version": "2.2.5", "pid": 4242, "urls": [], "paths": {"tmp": "/tmp"}}},
    "GET /api/model": {"body": {"location": {"directory": "/"}, "data": OC_V2_MODELS}},
    "GET /api/provider": {"body": {"location": {"directory": "/"}, "data": [
        {"id": "anthropic", "name": "Anthropic", "activation": "auto", "package": ""},
        {"id": "opencode", "name": "OpenCode", "activation": "auto", "package": ""}]}},
}
def opencode(name, version="1.15.0", routes=None, settings=None, serve=None, **extra):
    spec = {"version": {"stdout": version}, "serve": serve or {"routes": routes or OC_V1_ROUTES}}
    return {"name": f"opencode {name}", "instance": "opencode", "settings": settings or enabled("opencode"),
            "fakes": {"opencode": {"opencode": spec}}, **extra}
def oc_routes(base, **changes):
    """`base` with routes replaced (None removes one)."""
    routes = {**base, **changes}
    return {k: v for k, v in routes.items() if v is not None}


def oc_external(name, routes, binary=None, password="secret-pw", **extra):
    """A configured OpenCode server (the harness's fake API), with a local CLI or none."""
    settings = enabled("opencode", serverUrl="<api>", serverPassword=password)
    fakes = {"opencode": {"opencode": binary}} if binary else {}
    return {"name": f"opencode {name}", "instance": "opencode", "settings": settings, "api": routes,
            "fakes": fakes, **extra}


OC_ACTIVATING = {"_tag": "ServiceUnavailableError", "message": "Plugins are activating", "service": "plugin"}
OC_USAGE = {"location": {"directory": "/"}, "data": {
    "workspace": {"id": "ws_1", "name": "Team", "usageURL": "https://opencode.ai/workspace/ws_1/usage"},
    "go": {"rolling": {"percent": 12.5, "limited": False, "resetsAt": 1767225600000},
           "weekly": {"percent": 40, "limited": True, "resetsAt": 1767830400000},
           "monthly": {"percent": 150, "limited": False, "resetsAt": 1769904000000}},
    "budgets": []}}
SCENARIOS += [
    opencode("1.x signed in"),
    opencode("2.x signed in", "2.2.5", OC_V2_ROUTES),
    {"name": "opencode missing", "instance": "opencode", "settings": enabled("opencode"), "fakes": {}},
    opencode("version unreadable", "", serve={"routes": OC_V1_ROUTES}),
    {**opencode("version times out"),
     "fakes": {"opencode": {"opencode": {"version": {"sleep": 60}}}}},
    opencode("disabled", settings={"providerInstances": {"opencode": {"driver": "opencode", "enabled": False}}},
             settled=True),
    opencode("disabled with a server URL", settled=True, settings={"providerInstances": {"opencode": {
        "driver": "opencode", "enabled": False, "config": {"serverUrl": "http://example.invalid:4096"}}}}),
    {**opencode("custom binaryPath", settings=enabled("opencode", binaryPath="~/tools/opencode-custom")),
     "fakes": {"opencode": {"opencode": {"version": {"stdout": "1.15.0"}, "serve": {"routes": OC_V1_ROUTES}},
                            "at": "tools/opencode-custom"}}},
    opencode("custom models", settings=enabled("opencode", customModels=["mine/one", {"slug": "mine/two", "name": "Two"}])),
    opencode("1.x too old", "1.10.0"),
    opencode("1.x server exits on start", serve={"exit": {"code": 3, "stderr": "Error: invalid config"}}),
    {**opencode("1.x server never starts", serve={"silent": True, "routes": OC_V1_ROUTES}), "wait": 50},
    opencode("1.x server unhealthy", routes=oc_routes(OC_V1_ROUTES, **{
        "GET /global/health": {"body": {"healthy": False, "version": "1.15.0"}}})),
    opencode("1.x provider list fails", routes=oc_routes(OC_V1_ROUTES, **{
        "GET /provider": {"status": 500, "body": {"message": "provider boom"}}})),
    opencode("1.x optional lists fail", routes=oc_routes(OC_V1_ROUTES, **{
        "GET /agent": {"status": 500, "body": {}}, "GET /skill": None, "GET /command": {"status": 500}})),
    opencode("1.x no connected providers", routes=oc_routes(OC_V1_ROUTES, **{
        "GET /provider": {"body": {**OC_V1_PROVIDERS, "connected": []}}})),
    opencode("1.x connected without models", routes=oc_routes(OC_V1_ROUTES, **{
        "GET /provider": {"body": {"all": [{"id": "anthropic", "name": "Anthropic", "models": {}}],
                                   "connected": ["anthropic"], "default": {}}}})),
    opencode("2.x no models", "2.2.5", oc_routes(OC_V2_ROUTES, **{
        "GET /api/model": {"body": {"location": {"directory": "/"}, "data": []}}})),
    opencode("2.x model list fails", "2.2.5", oc_routes(OC_V2_ROUTES, **{
        "GET /api/model": {"status": 500, "body": {}}})),
    opencode("2.x plugins activating", "2.2.5", oc_routes(OC_V2_ROUTES, **{
        "GET /api/model": {"sequence": [{"status": 503, "body": OC_ACTIVATING}, {"status": 503, "body": OC_ACTIVATING},
                                        OC_V2_ROUTES["GET /api/model"]]}})),
    # Without a message the 503 is not OpenCode's ServiceUnavailableError, so it is not waited out.
    opencode("2.x unreadable 503", "2.2.5", oc_routes(OC_V2_ROUTES, **{
        "GET /api/model": {"status": 503, "body": {"_tag": "ServiceUnavailableError", "service": "plugin"}}})),
    opencode("2.x partial catalog settles", "2.2.5", oc_routes(OC_V2_ROUTES, **{
        "GET /api/model": {"sequence": [{"body": {"location": {"directory": "/"}, "data": OC_V2_MODELS[:1]}},
                                        OC_V2_ROUTES["GET /api/model"]]}})),
    opencode("2.x without provider names", "2.2.5", oc_routes(OC_V2_ROUTES, **{"GET /api/provider": None})),
    opencode("2.x workspace usage", "2.2.5", oc_routes(OC_V2_ROUTES, **{
        "GET /api/experimental/usage/limits": {"body": OC_USAGE}})),
    opencode("2.x no console workspace", "2.2.5", oc_routes(OC_V2_ROUTES, **{
        "GET /api/experimental/usage/limits": {"body": {"location": {"directory": "/"}, "data": None}}})),
    opencode("2.x usage unreadable", "2.2.5", oc_routes(OC_V2_ROUTES, **{
        "GET /api/experimental/usage/limits": {"status": 500, "body": {}}})),
    {**opencode("1.x installed as an npm global"), "fakes": {"opencode": {
        "opencode": {"version": {"stdout": "1.15.0"}, "serve": {"routes": OC_V1_ROUTES}},
        "at": "npm-prefix/lib/node_modules/opencode-ai/bin/opencode", "link": "fake-bin/opencode"}}},
    {**opencode("installed by the curl installer", settings=enabled("opencode", binaryPath="<root>/.opencode/bin/opencode")),
     "fakes": {"opencode": {"opencode": {"version": {"stdout": "1.15.0"}, "serve": {"routes": OC_V1_ROUTES}},
                            "at": ".opencode/bin/opencode"}}},
    oc_external("1.x configured server", OC_V1_ROUTES),
    oc_external("1.x configured server with a local CLI", OC_V1_ROUTES, {"version": {"stdout": "1.16.0"}}),
    oc_external("2.x configured server", oc_routes(OC_V2_ROUTES, **{
        "GET /api/experimental/usage/limits": {"body": OC_USAGE}})),
    oc_external("configured server rejects the password", {"GET /api/info": {"status": 401, "body": {}}}),
    oc_external("configured server is not OpenCode", {"GET /api/info": {"body": "<html>", "contentType": "text/html"}}),
    {"name": "opencode configured server unreachable", "instance": "opencode", "fakes": {},
     "settings": enabled("opencode", serverUrl="http://127.0.0.1:9")},
]


AGY_RELEASE = "a" * 64
AGY_MANAGED = "tools/antigravity-acp/linux-x64"
AGY_RUNTIME = "#!/bin/sh\nexit 0\n"


def agy_managed(version="1.6.2", **record):
    """A managed Antigravity runtime as the reference's installer leaves it."""
    directory = f"{AGY_MANAGED}/versions/{AGY_RELEASE}"
    files = {f"{directory}/agy_acp_server.par": AGY_RUNTIME, f"{directory}/localharness_external": AGY_RUNTIME,
             f"{AGY_MANAGED}/active.json": json.dumps({"releaseId": AGY_RELEASE})}
    files[f"{directory}/.install-complete.json"] = json.dumps({
        "releaseId": AGY_RELEASE, "version": version,
        "executable": {"name": "agy_acp_server.par", "bytes": len(AGY_RUNTIME)},
        "harness": {"name": "localharness_external", "bytes": len(AGY_RUNTIME)}, **record})
    return {"files": files, "executables": [f"{directory}/agy_acp_server.par", f"{directory}/localharness_external"]}


def agy(name, settings=None, **extra):
    return {"name": f"antigravity {name}", "instance": "antigravity", "settings": settings or enabled("antigravity"),
            "fakes": {}, **extra}


SCENARIOS += [
    agy("not installed"),
    agy("disabled", {"providerInstances": {"antigravity": {"driver": "antigravity", "enabled": False}}}, settled=True),
    agy("agy CLI on PATH", files={"fake-bin/agy": AGY_RUNTIME}, executables=["fake-bin/agy"]),
    agy("ACP runtime on PATH", files={"fake-bin/agy_acp_server.par": AGY_RUNTIME, "fake-bin/localharness_external": AGY_RUNTIME},
        executables=["fake-bin/agy_acp_server.par", "fake-bin/localharness_external"]),
    agy("ACP runtime without its harness", files={"fake-bin/agy_acp_server.par": AGY_RUNTIME},
        executables=["fake-bin/agy_acp_server.par"]),
    agy("managed runtime", **agy_managed()),
    agy("managed runtime incomplete", **{**agy_managed(), "executables": []}),
    agy("managed runtime withdrawn", **agy_managed("0.0.1")),
    agy("custom executable", enabled("antigravity", binaryPath="~/tools/agy"),
        files={"tools/agy": AGY_RUNTIME}, executables=["tools/agy"]),
    agy("custom executable missing", enabled("antigravity", binaryPath="/nonexistent/agy")),
    agy("Gemini API key missing", enabled("antigravity", authMethod="gemini-api-key"),
        files={"fake-bin/agy": AGY_RUNTIME}, executables=["fake-bin/agy"]),
    agy("Gemini API key", enabled("antigravity", authMethod="gemini-api-key", apiKey="gk-1"),
        files={"fake-bin/agy": AGY_RUNTIME}, executables=["fake-bin/agy"]),
    # Global MCP servers in the profile could act before a helper denies a tool: no text generation.
    agy("profile with MCP servers", files={
        "fake-bin/agy": AGY_RUNTIME,
        f"userdata/providers/antigravity/{hashlib.sha256(b'antigravity').hexdigest()}/config/mcp_config.json":
            json.dumps({"mcpServers": {"docs": {"command": "docs-server"}}})}, executables=["fake-bin/agy"]),
    agy("profile with empty hooks", files={
        "fake-bin/agy": AGY_RUNTIME,
        f"userdata/providers/antigravity/{hashlib.sha256(b'antigravity').hexdigest()}/config/hooks.json":
            json.dumps({"hooks": {}})}, executables=["fake-bin/agy"]),
    agy("Gemini Enterprise without a project", enabled("antigravity", authMethod="oauth-business"),
        files={"fake-bin/agy": AGY_RUNTIME}, executables=["fake-bin/agy"]),
]


MUSE_INIT = {"schema": {"version": 1, "fingerprint": "sha256:fixture"}, "grantedCapabilities": []}
MUSE_MODELS = {"providerId": "meta", "models": [
    {"modelId": "muse-spark-pro", "displayLabel": "muse-spark-pro", "providerId": "meta", "isDefault": True,
     "variants": ["low", "high", "ultra", "turbo"], "defaultReasoningEffort": "ultra",
     "reasoningEffortVariants": [{"tier": "ultra", "description": "Deepest"}]},
    {"modelId": "muse-lite", "displayLabel": " Muse Lite ", "providerId": "meta", "isDefault": False, "variants": "unknown"},
    {"modelId": "muse-none", "displayLabel": "", "providerId": "meta", "isDefault": False, "variants": ["turbo"]},
    {"modelId": "other-model", "displayLabel": "Other", "providerId": "openai", "isDefault": False},
    {"modelId": "muse-lite", "displayLabel": "Duplicate", "providerId": "meta", "isDefault": False}]}


MUSE_SERVE_REFUSES = {"prefix": ["serve"], "rpc": {}, "rpcErrors": {"initialize": {
    "code": -32603, "message": "host failed to start", "data": {"kind": "internal", "retryable": False}}}}


def muse_serve(models=MUSE_MODELS, init=MUSE_INIT):
    return {"prefix": ["serve"], "rpc": {"initialize": init, "model/list": models}}


SCENARIOS += driver_scenarios("muse", "muse", "muse 2.1.0-R4", [muse_serve()], {
    "not finished installing": ([], {"fakes": {"muse": {"spec": {"commands": [{"args": ["--version"], **ok(
        "", 1, "error: installed binary is missing; run muse --version to finish setup")}]}}}}),
    "no models": ([muse_serve({"providerId": "meta", "models": []})], {}),
    "catalog fails": ([MUSE_SERVE_REFUSES], {}),
    "catalog for another provider": ([muse_serve({"providerId": "openai", "models": []})], {}),
    "unsupported protocol": ([muse_serve(init={"schema": {"version": 2, "fingerprint": "x"}})], {}),
    "custom models": ([muse_serve()], {"settings": enabled("muse", customModels=["mine/one"])}),
    "installed by its launcher": ([muse_serve()], {}),
}, wait=30)
for scenario in SCENARIOS:
    if scenario["name"] == "muse installed by its launcher":
        scenario["fakes"]["muse"]["comment"] = "muse-code/launcher-v3 (refreshes with MUSE_SYNC_UPDATE=1)"


def npm_view(package, version):
    """A fake `npm` whose configured registry has `version` as `package`'s latest."""
    return {"spec": {"commands": [{"args": ["view", "--global", "--json", f"{package}@latest", "version"],
                                   **ok(json.dumps(version))}]}}


def checks_on(settings):
    return {**settings, "enableProviderUpdateChecks": True}


GROK_FAKE = {"spec": {"commands": [{"args": ["--version"], **ok("grok 1.0.52")}, GROK_MODELS, GROK_SKILLS, GROK_ACP]}}
SCENARIOS += [
    {"name": "advisory grok update available", "instance": "grok", "settings": checks_on(enabled("grok")),
     "fakes": {"grok": GROK_FAKE, "npm": npm_view("@xai-official/grok", "1.2.0")}},
    {"name": "advisory grok up to date", "instance": "grok", "settings": checks_on(enabled("grok")),
     "fakes": {"grok": GROK_FAKE, "npm": npm_view("@xai-official/grok", "1.0.52")}},
    {"name": "advisory grok from the public registry", "instance": "grok", "settings": checks_on(enabled("grok")),
     "fakes": {"grok": GROK_FAKE},
     "proxy": {"GET registry.npmjs.org/%40xai-official%2Fgrok/latest": {"body": {"version": "1.3.0"}}}},
    {"name": "advisory grok registry unreachable", "instance": "grok", "settings": checks_on(enabled("grok")),
     "fakes": {"grok": GROK_FAKE}, "proxy": {"GET registry.npmjs.org/%40xai-official%2Fgrok/latest": {"status": 503}}},
    {"name": "advisory codex installed elsewhere", "instance": "codex", "settings": checks_on({}),
     "fakes": {"codex": {"version": "ok", "app": "ok", "fixture": "status_codex_signed_in.json"},
               "npm": npm_view("@openai/codex", "0.200.0")}},
    {"name": "advisory codex standalone release", "instance": "codex",
     "settings": checks_on({"providerInstances": {"codex": {"driver": "codex", "config": {
         "binaryPath": "<root>/codex-home/packages/standalone/bin/codex"}}}}),
     "fakes": {"codex": {"version": "ok", "app": "ok", "fixture": "status_codex_signed_in.json",
                         "at": "codex-home/packages/standalone/bin/codex"}},
     "proxy": {"GET releases.openai.com/codex/channels/latest": {"body": {"tag_name": "rust-v0.180.0"}}}},
    {"name": "advisory claude native release", "instance": "claudeAgent",
     "settings": checks_on({"providerInstances": {"claudeAgent": {"driver": "claudeAgent", "config": {
         "binaryPath": "<root>/.local/bin/claude"}}}}),
     "fakes": {"claude": {"version": "ok", "fixture": "status_claude_signed_in.json",
                          "at": ".local/share/claude/versions/2.1.300", "link": ".local/bin/claude"}},
     "proxy": {"GET downloads.claude.ai/claude-code-releases/latest": {"body": "2.1.310\n"}}},
    {"name": "advisory pi npm global update available", "instance": "pi", "settings": checks_on(enabled("pi")),
     "fakes": {"pi": {"spec": {"commands": [{"args": ["--version"], **ok("0.85.0")}, PI_TEXT_GENERATION, pi_rpc()]},
                      "at": "npm-prefix/lib/node_modules/@earendil-works/pi-coding-agent/dist/cli.js", "link": "fake-bin/pi"},
               "npm": {"spec": {"commands": [{"prefix": ["outdated"], **ok(json.dumps(
                   {"@earendil-works/pi-coding-agent": {"current": "0.85.0", "latest": "0.90.0"}}), 1)}]}}}},
    {"name": "advisory pi npm global up to date", "instance": "pi", "settings": checks_on(enabled("pi")),
     "fakes": {"pi": {"spec": {"commands": [{"args": ["--version"], **ok("0.85.0")}, PI_TEXT_GENERATION, pi_rpc()]},
                      "at": "npm-prefix/lib/node_modules/@earendil-works/pi-coding-agent/dist/cli.js", "link": "fake-bin/pi"},
               "npm": {"spec": {"commands": [{"prefix": ["outdated"], **ok("{}")}, {"prefix": ["ls"], **ok(json.dumps(
                   {"dependencies": {"@earendil-works/pi-coding-agent": {"version": "0.85.0"}}}))}]}}}},
    # mise owns codex (an active `latest` install, launched through mise's `latest` directory), so
    # only `mise latest` says what an upgrade installs.
    {"name": "advisory codex installed by mise", "instance": "codex",
     "settings": checks_on({"providerInstances": {"codex": {"driver": "codex", "config": {
         "binaryPath": "<root>/mise/installs/codex/latest/bin/codex"}}}}),
     "fakes": {"codex": {"version": "ok", "app": "ok", "fixture": "status_codex_signed_in.json",
                         "at": "mise/installs/codex/0.170.0/bin/codex", "link": "mise/installs/codex/latest/bin/codex"},
               "mise": {"spec": {"commands": [
                   {"args": ["which", "codex"], **ok("<root>/mise/installs/codex/0.170.0/bin/codex")},
                   {"args": ["ls", "--json", "--installed", "codex"], **ok(json.dumps([
                       {"install_path": "<root>/mise/installs/codex/0.170.0", "requested_version": "latest",
                        "active": True}]))},
                   {"args": ["ls", "--json", "--installed"], **ok(json.dumps({"codex": [
                       {"install_path": "<root>/mise/installs/codex/0.170.0", "requested_version": "latest",
                        "active": True}]}))},
                   {"args": ["latest", "codex"], **ok("0.190.0")}]}}}},
    {"name": "advisory muse channel", "instance": "muse", "settings": checks_on(enabled("muse")),
     "fakes": {"muse": {"spec": {"commands": [{"args": ["--version"], **ok("muse 2.1.0-R4")}, muse_serve()]}}},
     "proxy": {"GET api.meta.ai/muse-code/channels/muse-stable": {"body": {"version": "2.1.0-R9"}}}},
    {"name": "advisory opencode npm global", "instance": "opencode", "settings": checks_on(enabled("opencode")),
     "fakes": {"opencode": {"opencode": {"version": {"stdout": "1.15.0"}, "serve": {"routes": OC_V1_ROUTES}},
                            "at": "npm-prefix/lib/node_modules/opencode-ai/bin/opencode", "link": "fake-bin/opencode"},
               "npm": {"spec": {"commands": [{"prefix": ["outdated"], **ok(json.dumps(
                   {"opencode-ai": {"latest": "1.16.2"}}), 1)}]}}}},
]


PI_NPM = "npm-prefix/lib/node_modules/@earendil-works/pi-coding-agent/dist/cli.js"
SCENARIOS += [
    {"name": "pi installed as an npm global", "instance": "pi", "settings": enabled("pi"),
     "fakes": {"pi": {"spec": {"commands": [{"args": ["--version"], **ok("0.85.0")}, PI_TEXT_GENERATION, pi_rpc()]},
                      "at": PI_NPM, "link": "fake-bin/pi"}}},
    {"name": "pi installed as a bun global", "instance": "pi", "settings": enabled("pi", binaryPath="<root>/.bun/bin/pi"),
     "fakes": {"pi": {"spec": {"commands": [{"args": ["--version"], **ok("0.85.0")}, PI_TEXT_GENERATION, pi_rpc()]},
                      "at": ".bun/install/global/node_modules/@earendil-works/pi-coding-agent/cli.js",
                      "link": ".bun/bin/pi"}}},
    {"name": "grok absolute binaryPath", "instance": "grok", "settings": enabled("grok", binaryPath="<root>/tools/grok"),
     "fakes": {"grok": {"spec": {"commands": [{"args": ["--version"], **ok("grok 1.0.52")}, GROK_MODELS, GROK_SKILLS,
                                              GROK_ACP]}, "at": "tools/grok"}}},
]
for scenario in SCENARIOS:
    if scenario["name"] == "pi absolute binaryPath":
        scenario["settings"] = enabled("pi", binaryPath="<root>/tools/pi-abs")
        scenario["fakes"]["pi"]["at"] = "tools/pi-abs"
    if scenario["name"] == "pi discovery fails":
        scenario["fakes"]["pi"]["spec"]["commands"][1]["pi"].pop("get_available_models")


def skill(description=None, **fields):
    """A SKILL.md with YAML frontmatter (`None` for none at all)."""
    if description is None and not fields:
        return "Instructions only.\n"
    lines = ([f"description: {description}"] if description is not None else []) + [
        f"{key.replace('_', '-')}: {value}" for key, value in fields.items()]
    return "---\n" + "\n".join(lines) + "\n---\nInstructions.\n"


CLAUDE_SIGNED_IN = {"claude": {"version": "ok", "fixture": "status_claude_signed_in.json"}}
ORG, ACCOUNT = "0f8e2a6c-1b3d-4e5f-8a9b-0c1d2e3f4a5b", "9a8b7c6d-5e4f-4a3b-9c2d-1e0f9a8b7c6d"
CEDAR_EMBER = {"eligible": True, "next_grant_id": "weekly_2", "grants": [
    {"id": "session_1", "resets_left": 1, "usable_now": True, "clears": ["five_hour"]},
    {"id": "weekly_2", "resets_left": 2, "usable_now": True, "type": "weekly", "ends_at": "2099-01-01T05:00:00.5+05:00"},
    {"id": "paused_3", "resets_left": 3, "usable_now": True, "paused": True},
    {"id": "ended_4", "resets_left": 1, "usable_now": True, "ends_at": "2020-01-01T00:00:00Z"},
    {"id": "fable_5", "resets_left": 1, "usable_now": True, "gives_back_weekly": True, "ends_at": None},
    {"id": "Bad-Id", "resets_left": 1, "usable_now": True}]}
USAGE_ROUTE = "GET api.anthropic.com/api/oauth/usage"
SCENARIOS += [
    # User skills in the config directory, project skills in the server's state directory (the
    # status check's cwd), `--add-dir` launch directories, and `skillOverrides` from settings files.
    {"name": "claude skills", "instance": "claudeAgent", "fakes": CLAUDE_SIGNED_IN,
     "settings": {"providerInstances": {"claudeAgent": {"driver": "claudeAgent", "config": {
         "launchArgs": "--add-dir extra --model x --add-dir=/nonexistent"}}}},
     "files": {".claude/skills/zeta/SKILL.md": skill("Last one"),
               ".claude/skills/alpha/SKILL.md": skill("First", disable_model_invocation="yes"),
               ".claude/skills/quiet/SKILL.md": skill("Quiet", user_invocable="off"),
               ".claude/skills/colon/SKILL.md": skill("Deploys: to prod  # a comment"),
               ".claude/skills/plain/SKILL.md": skill(),
               ".claude/skills/hidden/SKILL.md": skill("Hidden"),
               ".claude/skills/broken/SKILL.md": "---\ndescription: [unclosed\n---\n",
               ".claude/skills/empty/SKILL.md": "---\n\n---\n",
               ".claude/skills/no-skill-file/README.md": "nothing",
               ".claude/settings.json": '{\n  // Claude allows comments\n  "skillOverrides": {"hidden": "off", "plain": '
                                        '"user-invocable-only", "Zeta": "off",},\n}\n',
               "userdata/.claude/skills/zeta/SKILL.md": skill("Shadowed by the user skill"),
               "userdata/.claude/skills/project-only/SKILL.md": skill("Project"),
               "userdata/.claude/settings.local.json": '{"skillOverrides": {"project-only": "name-only", "alpha": "on"}}',
               "userdata/extra/.claude/skills/added/SKILL.md": skill("From --add-dir")}},
    # One unknown override value drops every override in that file, as Claude Code does.
    {"name": "claude skill overrides rejected", "instance": "claudeAgent", "fakes": CLAUDE_SIGNED_IN,
     "files": {".claude/skills/hidden/SKILL.md": skill("Hidden"),
               ".claude/skills/shown/SKILL.md": skill("Shown"),
               ".claude/settings.json": '{"skillOverrides": {"hidden": "off", "shown": "sometimes"}}',
               "userdata/.claude/settings.json": '{"skillOverrides": {"shown": "off"}}'}},
    # Installed plugins enabled in settings (user and project scope), a plugin's declared skill
    # roots, a single-SKILL.md plugin, and inline plugin folders from the settings `env`.
    {"name": "claude plugin skills", "instance": "claudeAgent", "fakes": CLAUDE_SIGNED_IN,
     "files": {
         ".claude/settings.json": json.dumps({
             "enabledPlugins": {"tools@market": True, "dup@market": True, "bad@market": True, "solo@market": True, "here@market": True,
                                "elsewhere@market": True, "off@market": False, "gone@market": True,
                                "quiet@inline": False},
             "env": {"CLAUDE_CODE_PLUGIN_DIRS": "<root>/inline:relative/ignored"}}),
         ".claude/plugins/installed_plugins.json": json.dumps({"plugins": {
             "tools@market": [{"scope": "user", "installPath": "<root>/plugins/tools"}],
             "solo@market": [{"scope": "user", "installPath": "<root>/plugins/solo"}],
             "here@market": [{"scope": "project", "installPath": "<root>/plugins/here", "projectPath": "<root>/userdata"}],
             "elsewhere@market": [{"scope": "local", "installPath": "<root>/plugins/elsewhere", "projectPath": "<root>/other"}],
             "off@market": [{"scope": "user", "installPath": "<root>/plugins/off"}],
             "gone@market": [{"scope": "user", "installPath": "<root>/plugins/gone"}],
             "dup@market": [{"scope": "user", "installPath": "<root>/plugins/dup"}],
             "bad@market": [{"scope": "nowhere", "installPath": "x"}]}}),
         "plugins/dup/.claude-plugin/plugin.json": json.dumps({"name": "dup"}),
         "plugins/dup/skills/installed-copy/SKILL.md": skill("Replaced by the inline copy"),
         "plugins/tools/.claude-plugin/plugin.json": json.dumps({"name": "toolkit", "skills": ["./more", "../escape"]}),
         "plugins/tools/skills/lint it/SKILL.md": skill("Lints", name="ignored"),
         "plugins/tools/skills/format/SKILL.md": skill("Formats"),
         "plugins/tools/more/review/SKILL.md": skill("Reviews"),
         "plugins/escape/SKILL.md": skill("Outside the plugin"),
         "plugins/solo/SKILL.md": skill("A single skill", name="solo:Deploy Now"),
         "plugins/here/skills/local/SKILL.md": skill("Project plugin"),
         "plugins/elsewhere/skills/other/SKILL.md": skill("Another project's plugin"),
         "plugins/off/skills/never/SKILL.md": skill("Disabled plugin"),
         "inline/alpha/.claude-plugin/plugin.json": json.dumps({"name": "alpha"}),
         "inline/alpha/skills/run/SKILL.md": skill("Inline"),
         "inline/quiet/.claude-plugin/plugin.json": json.dumps({"name": "quiet"}),
         "inline/quiet/skills/hush/SKILL.md": skill("Disabled inline"),
         "inline/shy/.claude-plugin/plugin.json": json.dumps({"name": "shy", "defaultEnabled": False}),
         "inline/shy/skills/hide/SKILL.md": skill("Off by default"),
         "inline/dup-copy/.claude-plugin/plugin.json": json.dumps({"name": "DUP"}),
         "inline/dup-copy/skills/inline-tool/SKILL.md": skill("The inline copy wins")}},
    # The signed-in account's claude.ai plugins, synced into `plugins/synced/<org>_<account>`.
    {"name": "claude synced plugin skills", "instance": "claudeAgent", "fakes": CLAUDE_SIGNED_IN,
     "files": {
         ".claude.json": json.dumps({"oauthAccount": {"organizationUuid": ORG.upper(), "accountUuid": ACCOUNT}}),
         ".claude/settings.json": json.dumps({"enabledPlugins": {"muted@synced": False}}),
         f".claude/plugins/synced/.bucket-{ORG}_{ACCOUNT}": "",
         f".claude/plugins/synced/{ORG}_{ACCOUNT}/manifest.json": json.dumps({"plugins": [
             {"pluginId": "p1", "name": "Docs: Writer.", "generation": 2},
             {"pluginId": "p2", "name": "muted"},
             {"pluginId": "p3", "name": "Docs"},
             {"pluginId": "p4", "name": ".hidden"}]}),
         f".claude/plugins/synced/{ORG}_{ACCOUNT}/Docs_ Writer~g2/.claude-plugin/plugin.json": json.dumps({"name": "docs"}),
         f".claude/plugins/synced/{ORG}_{ACCOUNT}/Docs_ Writer~g2/skills/draft/SKILL.md": skill("Drafts"),
         f".claude/plugins/synced/{ORG}_{ACCOUNT}/muted/skills/mute/SKILL.md": skill("Muted"),
         f".claude/plugins/synced/{ORG}_{ACCOUNT}/Docs/skills/dupe/SKILL.md": skill("Same name, so not loaded")}},
    # A subscription's reset credits, read with the stored claude.ai login through the test proxy.
    {"name": "claude reset credits", "instance": "claudeAgent", "fakes": CLAUDE_SIGNED_IN,
     "files": {".claude/.credentials.json": json.dumps({"claudeAiOauth": {"accessToken": "test-token"}})},
     "proxy": {USAGE_ROUTE: {"body": {"cedar_ember": CEDAR_EMBER}}}},
    {"name": "claude reset credits ineligible", "instance": "claudeAgent", "fakes": CLAUDE_SIGNED_IN,
     "files": {".claude/.credentials.json": json.dumps({"claudeAiOauth": {"accessToken": "test-token"}})},
     "proxy": {USAGE_ROUTE: {"body": {"cedar_ember": {"eligible": False}}}}},
    {"name": "claude reset credits unavailable", "instance": "claudeAgent", "fakes": CLAUDE_SIGNED_IN,
     "files": {".claude/.credentials.json": json.dumps({"claudeAiOauth": {"accessToken": "test-token"}})},
     "proxy": {USAGE_ROUTE: {"status": 500, "body": {"error": "down"}}}},
    {"name": "claude reset credits without a login", "instance": "claudeAgent", "fakes": CLAUDE_SIGNED_IN,
     "proxy": {USAGE_ROUTE: {"body": {"cedar_ember": CEDAR_EMBER}}}},
]


class FakeApi:
    """A provider's HTTP API for one server: answers each `METHOD /path` from `routes`
    ({"GET /v1/me": {"status": 200, "body": {...}, "sleep": seconds}}) and records the
    requests to its own API, with their bearer token, for comparison.

    Its own API is every path whose first segment one of `routes` starts with (`/v1/...` for
    `GET /v1/me`), so a server calling an unexpected route is still recorded. Anything else is
    answered but not recorded: servers pick free ports by binding port 0, so a T3 client of
    another scenario can briefly reach this port (`GET /ws`)."""

    def __init__(self, routes):
        self.routes, self.requests = routes, []
        self.prefixes = {"/" + route.split(" ", 1)[1].lstrip("/").split("/", 1)[0] for route in routes}
        api = self

        class Handler(BaseHTTPRequestHandler):
            def answer(self):
                length = int(self.headers.get("Content-Length") or 0)
                if length:
                    self.rfile.read(length)
                path = self.path.split("?", 1)[0]
                token = (self.headers.get("Authorization") or "").removeprefix("Bearer ")
                if api.owns(path):
                    api.requests.append(f"{self.command} {path} {token}".strip())
                route = api.routes.get(f"{self.command} {path}", {"status": 404, "body": {"error": "not found"}})
                if route.get("sleep"):
                    time.sleep(route["sleep"])
                body = json.dumps(route.get("body", {})).encode()
                try:
                    self.send_response(route.get("status", 200))
                    self.send_header("Content-Type", route.get("contentType", "application/json"))
                    self.send_header("Content-Length", str(len(body)))
                    self.end_headers()
                    self.wfile.write(body)
                except (BrokenPipeError, ConnectionResetError):
                    pass

            do_GET = do_POST = answer

            def log_message(self, *args):
                pass

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.server.daemon_threads = True
        self.url = f"http://127.0.0.1:{self.server.server_address[1]}"
        threading.Thread(target=self.server.serve_forever, daemon=True).start()

    def owns(self, path):
        """Whether `path` belongs to this API (see the class)."""
        segment = "/" + path.lstrip("/").split("/", 1)[0]
        return segment in self.prefixes

    def close(self):
        self.server.shutdown()
        self.server.server_close()


def strip(value, root):
    """`value` without check times and with the temporary home masked."""
    if isinstance(value, dict):
        return {key: strip(item, root) for key, item in value.items() if key not in TIMES}
    if isinstance(value, list):
        return [strip(item, root) for item in value]
    if isinstance(value, str):
        for server in ("original", "rust"):
            value = value.replace(f"{root}/{server}", "<home>")
        return value.replace(str(root), "<tmp>")
    return value


def row_of(providers, instance):
    return next((p for p in providers if p.get("instanceId") == instance), None)


def pending(row):
    """Whether `row` is the placeholder an instance shows until its first check reports."""
    message = str(row.get("message"))
    return "has not been checked" in message or (
        message.startswith("Checking ") and (message.endswith("...") or message.endswith("availability.")))


# Drivers whose version advisory is added by an enrichment pass after the probe's row is
# published: their row is final only once it carries the advisory.
ENRICHED_DRIVERS = {"codex", "claudeAgent", "grok", "pi", "opencode", "muse"}
# How long a server may take to finish an instance's first check on a loaded machine.
CHECK_DEADLINE = 180


def final(row):
    """Whether `row` is a completed check in its final shape: past the placeholder, and with the
    version advisory an enriched driver adds once its probe is published."""
    if row is None or pending(row):
        return False
    return row.get("driver") not in ENRICHED_DRIVERS or row.get("enabled") is not True \
        or "versionAdvisory" in row


def wait_checked(url, cookie, instance, seconds, settled, root, stable=1.5):
    """The instance's row once its first check has completed (`final`) and the row has stopped
    changing for `stable` seconds, or after a few seconds for a scenario whose row is final from
    the start. A server that has not finished the check within CHECK_DEADLINE (or the scenario's
    longer `wait`) gives a timeout marker, so a pending row is never compared."""
    if settled:
        time.sleep(4)
        return row_of(rpc(url, cookie, "server.getConfig", {})["providers"], instance)
    deadline = time.monotonic() + max(seconds, CHECK_DEADLINE)
    last, stable_since = None, None
    while True:
        row = row_of(rpc(url, cookie, "server.getConfig", {})["providers"], instance)
        if final(row):
            if strip(row, root) == last:
                if time.monotonic() - stable_since >= stable:
                    return row
            else:
                last, stable_since = strip(row, root), time.monotonic()
        else:
            last, stable_since = None, None
        if time.monotonic() > deadline:
            return {"timedOut": f"no completed check within {max(seconds, CHECK_DEADLINE)}s", "row": row}
        time.sleep(0.25)


def config_socket(url, cookie):
    ws = connect(url.replace("http", "ws", 1) + "/ws?orchestrationProtocol=2",
                 additional_headers={"Cookie": cookie}, proxy=None, compression=None, max_size=None)
    ws.send(encode({"_tag": "Request", "id": "config", "tag": "subscribeServerConfig", "payload": {},
                    "headers": []}).decode())
    return ws


def next_provider_event(ws, instance, seconds=15):
    """The instance's row in the next `providerStatuses` event whose row is checked."""
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        try:
            frame = json.loads(ws.recv(timeout=max(0.1, deadline - time.monotonic())))
        except TimeoutError:
            break
        if frame.get("_tag") == "Ping":
            ws.send(encode({"_tag": "Pong"}).decode())
            continue
        if frame.get("_tag") != "Chunk":
            continue
        ws.send(encode({"_tag": "Ack", "requestId": "config"}).decode())
        for event in frame.get("values", []):
            if event.get("type") == "providerStatuses":
                row = row_of(event["payload"]["providers"], instance)
                if row is not None and row.get("auth", {}).get("status") == "authenticated":
                    return row
    return None


def run_server(args, command, root, scenario, temporary):
    """One server's outcome for `scenario`."""
    root.mkdir(parents=True)
    bin_dir = root / "fake-bin"
    bin_dir.mkdir()
    fixture = root / "fixture.json"
    log = root / "provider.log"
    for cli, fake in scenario["fakes"].items():
        target = root / fake["at"] if "at" in fake else bin_dir / cli
        target.parent.mkdir(parents=True, exist_ok=True)
        if "opencode" in fake:
            spec = root / f"{cli}.spec.json"
            spec.write_text(json.dumps(fake["opencode"]))
            target.write_text(f'#!/bin/sh\nexec python3 "$FAKE_OPENCODE" {shlex.quote(str(spec))} "$@"\n')
        elif "spec" in fake:
            spec = root / f"{cli}.spec.json"
            spec.write_text(json.dumps(fake["spec"]).replace("<root>", str(root)))
            # A "comment" is written into the wrapper, as an installer's marker would be.
            comment = "".join(f"# {line}\n" for line in fake.get("comment", "").splitlines())
            target.write_text(f'#!/bin/sh\n{comment}exec python3 "$FAKE_STATUS_CLI" {shlex.quote(str(spec))} "$@"\n')
        else:
            target.write_text(claude_script(fake["version"]) if cli == "claude"
                              else codex_script(fake["version"], fake["app"]))
            shutil.copyfile(FIXTURES / fake["fixture"], fixture)
        target.chmod(0o755)
        if "link" in fake:
            # The CLI is found through a link elsewhere, as a package manager installs it.
            link = root / fake["link"]
            link.parent.mkdir(parents=True, exist_ok=True)
            link.symlink_to(target)
    api = FakeApi(scenario.get("api", {}))
    proxy = FakeHttpsProxy(scenario["proxy"], root / "proxy") if "proxy" in scenario else None
    settings = json.loads(json.dumps({"enableProviderUpdateChecks": False, **scenario.get("settings", {})})
                          .replace("<root>", str(root)).replace("<api>", api.url))
    for name, text in scenario.get("files", {}).items():
        # Files seeded in the server's home before it starts, such as stored sign-ins.
        target = root / name
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(text.replace("<root>", str(root)))
        if name in scenario.get("executables", ()):
            target.chmod(0o755)
    (root / "userdata").mkdir(parents=True, exist_ok=True)
    (root / "userdata/settings.json").write_text(json.dumps(settings))
    env = {"PATH": os.pathsep.join([str(bin_dir), path_without(PROVIDER_CLIS | RELAY_TOOLS | INSTALLERS,
                                                               temporary / "path")]),
           "FAKE_PROVIDER_CLI": str(FIXTURES / "fake_cli.py"),
           "FAKE_STATUS_CLI": str(FIXTURES / "status_cli.py"),
           "FAKE_OPENCODE": str(FIXTURES / "fake_opencode.py"),
           "CURSOR_BACKEND_URL": api.url, "NO_PROXY": "127.0.0.1", "no_proxy": "127.0.0.1", "FAKE_PROVIDER_LOG": str(log),
           "FAKE_PROVIDER_SCENARIO": str(fixture)}
    if proxy:
        env.update(proxy.env())
    outcome = {}
    with contextlib.ExitStack() as stack:
        stack.callback(api.close)
        if proxy:
            stack.callback(proxy.close)
        url, cookie = managed_server(stack, command, root, secrets.token_hex(16), secrets.token_bytes(32), env=env)
        outcome["row"] = strip(wait_checked(url, cookie, scenario["instance"], scenario.get("wait", 20),
                                            scenario.get("settled", False), temporary,
                                            scenario.get("stable", 1.5)), temporary)
        if "refresh" in scenario:
            if isinstance(scenario["refresh"], dict):
                for cli, spec in scenario["refresh"].items():
                    (root / f"{cli}.spec.json").write_text(json.dumps(spec))
            else:
                shutil.copyfile(FIXTURES / scenario["refresh"], fixture)
            ws = stack.enter_context(contextlib.closing(config_socket(url, cookie)))
            refreshed = rpc(url, cookie, "server.refreshProviders", {})
            outcome["refresh"] = strip(row_of(refreshed["providers"], scenario["instance"]), temporary)
            outcome["event"] = strip(next_provider_event(ws, scenario["instance"]), temporary)
    launches = Path(str(log) + ".args")
    # `codex app-server --help` is the reference's managed Codex runtime deciding whether a
    # local codex can stand in for the one it downloads, not a status check; this server does
    # not manage a Codex runtime.
    # The provider API requests each server made (distinct, as reads can repeat).
    served = Path(str(log) + ".http")
    outcome["requests"] = sorted(set(api.requests) | (set(served.read_text().splitlines()) if served.exists() else set()))
    if proxy:
        # Only requests to the scenario's own hosts: other startup fetches (the model manifest)
        # also pass through the proxy, and a client without its CA never sends them.
        hosts = {route.split()[1].split("/", 1)[0] for route in scenario["proxy"]}
        outcome["requests"] += sorted({r for r in proxy.requests if r.split("/")[2] in hosts})
    outcome["launches"] = sorted({strip(line, temporary) for line in launches.read_text().splitlines()
                                  if line != "app-server --help"}) if launches.exists() else []
    return outcome


def compare(outcomes):
    problems = {}
    original, rust = outcomes["original"], outcomes["rust"]
    for check in ("row", "refresh", "event"):
        if original.get(check) != rust.get(check):
            problems[check] = [f"original {json.dumps(original.get(check), sort_keys=True)}",
                               f"rust     {json.dumps(rust.get(check), sort_keys=True)}"]
    # The CLI sessions each server started for its checks (arguments only).
    if original["requests"] != rust["requests"]:
        problems["api requests"] = [f"original {original['requests']}", f"rust     {rust['requests']}"]
    if original["launches"] != rust["launches"]:
        problems["cli launches"] = [f"original {original['launches']}", f"rust     {rust['launches']}"]
    return problems


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--original-command", required=True)
    parser.add_argument("--rust-command", required=True)
    parser.add_argument("--scenario", action="append", help="run only this scenario (repeatable)")
    parser.add_argument("--dump", type=Path, help="write each server's outcome here")
    parser.add_argument("--jobs", type=int, default=4, help="scenarios to run at once (1 runs them one by one)")
    args = parser.parse_args()
    xfail = json.loads(XFAIL.read_text()) if XFAIL.exists() else {}
    failures, unexpected_passes = [], []
    selected = [s for s in SCENARIOS if not args.scenario or s["name"] in args.scenario]
    def outcomes_of(scenario):
        with tempfile.TemporaryDirectory(prefix="t3-status-") as directory:
            temporary = Path(directory)
            commands = (("original", shlex.split(args.original_command)), ("rust", shlex.split(args.rust_command)))
            results = run_concurrently(
                [lambda name=name, command=command: run_server(args, command, temporary / name, scenario, temporary)
                 for name, command in commands], 2 if args.jobs > 1 else 1)
            return dict(zip((name for name, _ in commands), results))

    # The scenarios that wait out a probe timeout start first, so none of them is left for last.
    order = sorted(range(len(selected)), key=lambda index: -selected[index].get("wait", 20))
    done = run_concurrently([lambda scenario=selected[index]: outcomes_of(scenario) for index in order], args.jobs)
    every = [outcome for _, outcome in sorted(zip(order, done))]
    for scenario, outcomes in zip(selected, every):
        if args.dump:
            args.dump.mkdir(parents=True, exist_ok=True)
            (args.dump / (scenario["name"].replace(" ", "-") + ".json")).write_text(
                json.dumps(outcomes, indent=1, sort_keys=True))
        problems = compare(outcomes)
        expected = xfail.get(scenario["name"], {})
        for check, lines in problems.items():
            if check in expected:
                print(f"XFAIL {scenario['name']}: {check}: {expected[check]}")
                continue
            failures.append(scenario["name"])
            print(f"FAIL {scenario['name']}: {check}")
            for line in lines:
                print(f"  {line[:4000]}")
        for check in expected:
            if check not in problems:
                unexpected_passes.append(f"{scenario['name']}: {check}")
        if not problems:
            print(f"ok   {scenario['name']}")
    for entry in unexpected_passes:
        print(f"XPASS {entry} (remove it from {XFAIL.name})")
    if failures or unexpected_passes:
        sys.exit(1)
    print(f"provider status parity: {len(selected)} scenarios match")


if __name__ == "__main__":
    main()