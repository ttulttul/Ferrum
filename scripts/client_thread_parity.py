#!/usr/bin/env python3
"""Open a seeded thread with the pinned reference client against both servers.

Each server runs in its own temporary home with two projects on git checkouts
whose remotes are on Bitbucket and GitHub, and one thread that has had a user
message dispatched. The Node driver (client_thread.mjs) uses the reference
client-runtime, so every reply is decoded with the client's own schemas, and
records:

- `orchestration.subscribeThread` up to `synchronized`, with the options the
  0.1.10 client sends and without them, and for a missing thread;
- `subscribeWorktreeSetup` and `worktreeSetupCancel` for the thread;
- each project's repository identity on the shell subscription;
- `pullRequests.list` for each project, without host credentials.

Each step must end the same way on both servers. Decoded values are compared
by shape (every key path and value type) and, where the value does not depend
on time or generated ids, exactly.

Then each provider turn scenario (PROVIDER_SCENARIOS) runs on a fresh pair of
servers with fake `codex` and `claude` executables on PATH. They replay the
recorded protocol sessions in tests/fixtures/providers, so no model is called.
The driver subscribes to the thread, dispatches a message, and keeps what
streams in until the run settles; the turn's projection is then compared the
same way, along with the turn as the client renders it: items in order with
their types, statuses and text, the messages, and the run, attempt, provider
turn, provider thread and provider session states. Scenarios with runtime
requests set the thread's runtime mode and answer (`respond`) or interrupt the
first request once its card shows; the `cli approvals` check compares what each
server told the fake CLI: the runtime policy of its thread and turn, and its
answer to each request.

Restart scenarios (those with "restart") stop the server the way a service
manager does (SIGTERM) once the turn has started, is running or has settled
(after queueing more messages, if the scenario says), change what the scenario
says (settings, or the CLI removed), start the server again on the same home,
wait for whatever it continues to settle (putting the CLI back for a retry, if
the scenario says), and then compare the thread: how the cut-off run ended, the
notice and the continuation run, the provider session, each turn launch of the
CLI (`cli launches`: a new session or a resumed one), and how soon after the
server listened it sent the continuation (`continuation timing`). Known differences are listed
in client_thread_parity_xfail.json; an entry that starts matching fails the
suite until it is removed, unless its reason starts with "Racy:" (a race
either server can win, so the check matches on some runs only).

A restarted server has settled once it says its recovery has finished: its
`server.startup` span in userdata/logs/server.trace.ndjson, which both servers
end once their recovery has decided what to continue, and then nothing left
to do now: no run active and, for the reference, no effect due or running in
its outbox. With --faketime-lib, the reference's continuation retry a minute
after a failed start (`cli restored`) is reached by moving both servers'
clocks on instead of waiting. --real-time waits the minute instead, and also
lets every restarted server run for at least 8 seconds before the thread is
read, so a continuation sent late is still seen; CI runs it nightly and on
pushes to main.
"""
import argparse
import contextlib
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import secrets
import shlex
import urllib.parse
import sqlite3
import subprocess
import tempfile
import threading
import time

from client_initial_sync_parity import rpc
from remote_task_peer import RemoteTaskPeer
from parity_harness import PROVIDER_CLIS, RELAY_TOOLS, encode, http, isolated_git_env, managed_server, path_without

ROOT = Path(__file__).resolve().parents[1]
# The start of an expected failure's reason that marks it a race: the check
# matches on some runs and not on others, so matching does not fail the suite.
RACY = "Racy:"
FIXTURES = ROOT / "tests/fixtures/providers"
# Each scenario: the thread's provider instance and model, and the recorded session its CLI replays.
# A signed-in Grok (as provider_status_parity.py has it) whose ACP sessions answer a prompt with a title.
_GROK_INITIALIZE = {"protocolVersion": 1, "agentCapabilities": {"loadSession": True}, "authMethods": [], "_meta": {
    "modelState": {"currentModelId": "grok-build", "availableModels": [
        {"modelId": "grok-build", "name": "Grok Build"}, {"modelId": "grok-4-fast", "name": "Grok 4 Fast"}]}}}
_GROK_ACP = {"rpc": {
    "initialize": _GROK_INITIALIZE,
    "session/new": {"sessionId": "grok-session-1", "models": {
        "currentModelId": "grok-build",
        "availableModels": [{"modelId": "grok-build", "name": "Grok Build"},
                            {"modelId": "grok-4-fast", "name": "Grok 4 Fast"}]}},
    "session/set_model": {}, "session/set_config_option": {}, "session/prompt": {"stopReason": "end_turn"},
    "session/cancel": {}},
    "notifyBefore": {"session/prompt": [{"method": "session/update", "params": {
        "sessionId": "grok-session-1", "update": {"sessionUpdate": "agent_message_chunk", "content": {
            "type": "text", "text": '{"title": "Echo Greeting via Grok", "needsRefinement": false}'}}}}]}}
GROK_TITLE_SPEC = {"commands": [
    {"args": ["--version"], "stdout": "grok 1.0.52", "stderr": "", "exit": 0},
    # A text generation run asks for approvals (approval-required).
    {"prefix": ["--permission-mode", "default", "agent", "stdio"], **_GROK_ACP},
    {"args": ["models"], "stdout": "You are logged in as dev@example.com.\n\nModels:\n  * grok-build (default)\n  * grok-4-fast\n",
     "stderr": "", "exit": 0},
    {"args": ["inspect", "--json"], "stdout": '{"skills": []}', "stderr": "", "exit": 0},
    {"prefix": ["agent", "stdio"], **_GROK_ACP}]}

# A signed-in OpenCode 1.x (as provider_status_parity.py has it) whose server answers a title session.
_OC_PROVIDERS = {"all": [{"id": "anthropic", "name": "Anthropic", "models": {
    "claude-opus-5-5": {"id": "claude-opus-5-5", "name": "Claude Opus 5.5", "release_date": "2026-09-01",
                        "status": "active", "variants": {"low": {}, "high": {}}}}}],
    "default": {"anthropic": "claude-opus-5-5"}, "connected": ["anthropic"]}
OPENCODE_TITLE_SPEC = {"version": {"stdout": "1.15.0"}, "serve": {"routes": {
    "GET /global/health": {"body": {"healthy": True, "version": "1.15.0"}},
    "GET /provider": {"body": _OC_PROVIDERS},
    "GET /agent": {"body": [{"name": "build", "mode": "primary"}]},
    "GET /skill": {"body": []}, "GET /command": {"body": []},
    "POST /session": {"body": {"id": "ses_title", "title": "T3 Code generateThreadTitle", "version": "1.15.0",
                               "time": {"created": 1767225600000, "updated": 1767225600000}}},
    "POST /session/ses_title/message": {"body": {"info": {"id": "msg_title", "role": "assistant", "sessionID": "ses_title"},
        "parts": [{"type": "text", "text": '{"title": "Echo Greeting via OpenCode", "needsRefinement": false}'}]}},
    "DELETE /session/ses_title": {"body": True}, "POST /session/ses_title/abort": {"body": True}}}}
# OpenCode 2: its event stream carries the reply. After the prompt it asks for a tool (which both
# servers must reject), ends the title's text, and ends the execution. Every body is one the
# reference's OpenCode 2 schemas decode.
_OC2_EVENT = {"created": 1767225600000, "durable": {"aggregateID": "ses_title", "seq": 1, "version": 1}}
OPENCODE2_TITLE_SPEC = {"version": {"stdout": "2.2.5"}, "serve": {"routes": {
    "GET /api/info": {"body": {"version": "2.2.5", "pid": 4242, "urls": [], "paths": {"tmp": "/tmp"}}},
    "GET /api/model": {"body": {"location": {"directory": "/"}, "data": [
        {"id": "claude-opus-5-5", "modelID": "claude-opus-5-5", "providerID": "anthropic", "name": "Claude Opus 5.5",
         "capabilities": {"tools": True, "input": ["text"], "output": ["text"]}, "variants": [{"id": "high"}],
         "time": {"released": 1767225600000}, "cost": [], "status": "active", "enabled": True,
         "limit": {"context": 200000, "output": 32000}}]}},
    "GET /api/provider": {"body": {"location": {"directory": "/"}, "data": [
        {"id": "anthropic", "name": "Anthropic", "activation": "auto", "package": ""}]}},
    "GET /api/event": {"sse": True},
    "POST /api/session": {"body": {"data": {
        "id": "ses_title", "projectID": "prj_fixture", "title": "T3 Code generateThreadTitle", "cost": 0,
        "tokens": {"input": 0, "output": 0, "reasoning": 0, "cache": {"read": 0, "write": 0}},
        "time": {"created": 1767225600000, "updated": 1767225600000}, "location": {"directory": "/tmp"}}}},
    "POST /api/session/ses_title/prompt": {"body": {"data": {
        "id": "msg_title", "sessionID": "ses_title", "type": "user", "time": {"created": 1767225600000},
        "payload": {"text": "title"}, "delivery": "queue"}}},
    "POST /api/session/ses_title/permission/per_title/reply": {"status": 204, "body": None},
    "POST /api/session/ses_title/interrupt": {"body": {"data": {"interrupted": True}}},
    "DELETE /api/session/ses_title": {"status": 204, "body": None}},
    "events": {"POST /api/session/ses_title/prompt": [
        {**_OC2_EVENT, "id": "evt_ask", "type": "permission.asked", "durable": None, "data": {
            "id": "per_title", "sessionID": "ses_title", "action": "read", "resources": ["/etc/hosts"],
            "source": {"type": "tool", "messageID": "msg_reply", "id": "call_1"}}},
        {**_OC2_EVENT, "id": "evt_text", "type": "session.text.ended", "data": {
            "sessionID": "ses_title", "assistantMessageID": "msg_reply", "ordinal": 0, "state": {},
            "text": '{"title": "Echo Greeting via OpenCode 2", "needsRefinement": false}'}},
        {**_OC2_EVENT, "id": "evt_done", "type": "session.execution.succeeded", "data": {"sessionID": "ses_title"}}]}}}
for _event in OPENCODE2_TITLE_SPEC["serve"]["events"]["POST /api/session/ses_title/prompt"]:
    if _event.get("durable") is None:
        _event.pop("durable", None)
# An Antigravity agy CLI on PATH (as provider_status_parity.py has it) serving ACP, whose session
# answers a prompt with a title.
_AGY_SESSION = "0b6a3c1e-5d2f-4a8b-9c7d-1e2f3a4b5c6d"
_AGY_ACP = {"rpc": {
    "initialize": {"protocolVersion": 1, "agentCapabilities": {"loadSession": True},
                   "authMethods": [{"id": "oauth-personal", "name": "Google"}]},
    "authenticate": {},
    "session/new": {"sessionId": _AGY_SESSION,
                    "modes": {"currentModeId": "default", "availableModes": [{"id": "default", "name": "Default"}]},
                    "models": {"currentModelId": "antigravity-default",
                               "availableModels": [{"modelId": "antigravity-default", "name": "Default"}]}},
    "session/set_mode": {}, "session/set_model": {}, "session/set_config_option": {"configOptions": []},
    "session/prompt": {"stopReason": "end_turn"}, "session/cancel": {}},
    "notifyBefore": {"session/prompt": [{"method": "session/update", "params": {
        "sessionId": _AGY_SESSION, "update": {"sessionUpdate": "agent_message_chunk", "content": {
            "type": "text", "text": '{"title": "Echo Greeting via Antigravity", "needsRefinement": false}'}}}}]}}
AGY_TITLE_SPEC = {"logEnv": ["GEMINI_HOME", "AGY_ACP_FORCE_FILE_STORAGE", "BROWSER", "PYTHONUNBUFFERED",
                              "ELECTRON_RUN_AS_NODE", "TMPDIR", "SSL_CERT_FILE", "GEMINI_API_KEY", "GOOGLE_API_KEY",
                              "ANTIGRAVITY_HARNESS_PATH", "GOOGLE_CLOUD_PROJECT"], "commands": [
    {"args": ["--version"], "stdout": "agy 1.6.2", "stderr": "", "exit": 0},
    {"prefix": ["acp"], **_AGY_ACP}]}

# The same CLI naming a release the manifest rates broken (anything before 1.5): the runtime is
# refused once it answers `initialize` (#38970).
AGY_BROKEN_TITLE_SPEC = {**AGY_TITLE_SPEC, "commands": [
    {"args": ["--version"], "stdout": "agy 1.4.5", "stderr": "", "exit": 0},
    {"prefix": ["acp"], **{**_AGY_ACP, "rpc": {**_AGY_ACP["rpc"], "initialize": {
        **_AGY_ACP["rpc"]["initialize"], "agentInfo": {"name": "agy", "version": "1.4.5"}}}}}]}

# A signed-in Pi (as provider_status_parity.py has it) whose text generation runs answer with a title.
_PI_STATE = {"model": {"provider": "openai", "id": "gpt-5.6"}, "thinkingLevel": "minimal"}
_PI_MODELS = {"models": [{"provider": "openai", "id": "gpt-5.6", "reasoning": False}]}
PI_TITLE_SPEC = {"commands": [
    {"args": ["--version"], "stdout": "pi 0.85.0", "stderr": "", "exit": 0},
    {"prefix": ["--mode", "rpc", "--no-session", "--offline", "--no-extensions"],
     "pi": {"get_available_models": _PI_MODELS, "set_model": {}, "set_thinking_level": {}, "prompt": {},
            "get_last_assistant_text": {"text": '{"title": "Echo Greeting via Pi", "needsRefinement": false}'}},
     "piEvents": {"prompt": [
         {"type": "message_end", "message": {"role": "assistant", "stopReason": "stop"}},
         {"type": "agent_settled"}]}},
    {"prefix": ["--mode", "rpc"], "pi": {"get_state": _PI_STATE, "get_available_models": _PI_MODELS,
                                         "get_commands": {"commands": []}}}]}

# A 1x1 PNG and a text file a client uploads and sends with the turn's message.
_ATTACHMENT_PNG = bytes.fromhex(
    "89504e470d0a1a0a0000000d4948445200000001000000010806000000"
    "1f15c4890000000d49444154789c63f8cfc0f01f0005000201a5f5cc2a0000000049454e44ae426082")
_UPLOADS = [
    {"type": "image", "name": "pixel.png", "mimeType": "image/png", "content": _ATTACHMENT_PNG},
    {"type": "file", "name": "notes.md", "mimeType": "text/markdown", "content": "# Notes\nFixture file.\n"},
]

# A local ACP Registry agent instance, run as `fixture-acp --acp` from PATH.
_ACP_LOCAL = {"driver": "acpRegistry", "displayName": "Fixture ACP",
              "config": {"source": "local", "commandPath": "fixture-acp", "commandArgs": ["--acp"]}}
PROVIDER_SCENARIOS = [
    {"name": "codex turn", "instance": "codex", "model": "fixture", "scenario": "codex_turn.json"},
    # The message carries uploads: claimed for the thread, shown on it, and sent to the CLI.
    {"name": "codex attachments", "instance": "codex", "model": "fixture", "scenario": "codex_turn.json",
     "uploads": _UPLOADS},
    {"name": "claude attachments", "instance": "claudeAgent", "model": "claude-opus-5-5",
     "scenario": "claude_turn.json", "uploads": _UPLOADS},
    # The turn calls T3's tools on the MCP server the server gave its CLI.
    {"name": "codex agent mcp", "instance": "codex", "model": "fixture", "scenario": "codex_mcp.json"},
    {"name": "claude agent mcp", "instance": "claudeAgent", "model": "claude-opus-5-5", "scenario": "claude_mcp.json"},
    # The thread toolkit, called by the thread on itself and on a thread it forks.
    {"name": "codex thread tools", "instance": "codex", "model": "fixture", "scenario": "codex_thread_tools.json"},
    # A delegated task the parent waits on: its child runs in a process of its own (Claude runs one
    # per thread on both servers), and its result answers the wait.
    {"name": "claude delegate", "instance": "claudeAgent", "model": "claude-opus-5-5",
     "scenario": "claude_delegate.json", "awaitStatus": True},
    # A delegated task in its own worktree (`isolation: "worktree"`): the child waits, preparing,
    # while the server checks out the parent's commit on a temporary branch and renames it to a
    # generated name, then runs in that checkout.
    {"name": "claude delegate worktree", "instance": "claudeAgent", "model": "claude-opus-5-5",
     "scenario": "claude_delegate_worktree.json", "awaitStatus": True},
    # A delegated task in a linked environment (`target.environmentId`): the task's thread is
    # launched there through the link, followed until its run ends, and its reply becomes the
    # task's result here.
    {"name": "claude delegate remote", "instance": "claudeAgent", "model": "claude-opus-5-5",
     "scenario": "claude_delegate_remote.json", "awaitStatus": True, "peer": {"finish_after": 1}},
    # A task in a linked environment cancelled while its thread there runs: the thread there is
    # stopped through the link, and cancelling it again stops it there again. The cancelled result
    # is steered into the parent's turn, which ends before reading it; the CLI then answers it as
    # its next turn, as Claude Code does.
    {"name": "claude delegate remote cancel", "instance": "claudeAgent", "model": "claude-opus-5-5",
     "scenario": "claude_delegate_remote_cancel.json", "awaitStatus": True, "peer": {"finish_after": None},
     "settleRuns": 2},
    # A delegated task that runs on after the parent's turn: its result wakes the idle parent
    # with a delivery turn, which reads it with task_status.
    {"name": "claude delegate async", "instance": "claudeAgent", "model": "claude-opus-5-5",
     "scenario": "claude_delegate_async.json", "awaitStatus": True, "settleRuns": 2},
    # A delegated task the parent cancels while its child runs: the child is interrupted, the
    # task ends interrupted without waking the parent, and cancelling it again keeps its result.
    {"name": "claude delegate cancel", "instance": "claudeAgent", "model": "claude-opus-5-5",
     "scenario": "claude_delegate_cancel.json", "awaitStatus": True},
    # The user stops a parent whose delegated task still runs: the stop reaches the child too.
    {"name": "claude delegate stop", "instance": "claudeAgent", "model": "claude-opus-5-5",
     "scenario": "claude_delegate_stop.json", "awaitStatus": True,
     "change": {"type": "thread.stop"}, "changeAfter": "subagent"},
    # Threads the agent creates: one started with its prompt, one left idle, each a card on the
    # parent's timeline.
    {"name": "claude create threads", "instance": "claudeAgent", "model": "claude-opus-5-5",
     "scenario": "claude_create_threads.json", "awaitStatus": True},
    # A follow-up to a finished child: the parent sends it more work, which becomes a follow-up
    # task whose result wakes the parent.
    {"name": "claude delegate followup", "instance": "claudeAgent", "model": "claude-opus-5-5",
     "scenario": "claude_delegate_followup.json", "awaitStatus": True, "settleRuns": 2},
    # A restart while a delegated task's child runs: the child's work continues after the
    # restart, and its result reaches the parent.
    # A delegated task that finishes while its parent's Claude turn still runs: its result waits
    # behind that turn (steering a Claude turn would interrupt its tools) and runs next.
    {"name": "claude delegate live", "instance": "claudeAgent", "model": "claude-opus-5-5",
     "scenario": "claude_delegate_live.json", "awaitStatus": True, "settleRuns": 2},
    # A delegated task that finishes while its parent's Codex turn still runs: its result steers
    # into that turn (turn/steer), which reads it.
    {"name": "codex delegate live", "instance": "codex", "model": "fixture",
     "scenario": "codex_delegate_live.json", "awaitStatus": True},
    # Codex delegation counterparts: parent and child share the instance's app-server.
    {"name": "codex delegate", "instance": "codex", "model": "fixture",
     "scenario": "codex_delegate.json", "awaitStatus": True},
    {"name": "codex delegate async", "instance": "codex", "model": "fixture",
     "scenario": "codex_delegate_async.json", "awaitStatus": True, "settleRuns": 2},
    {"name": "codex delegate followup", "instance": "codex", "model": "fixture",
     "scenario": "codex_delegate_followup.json", "awaitStatus": True, "settleRuns": 2},
    {"name": "codex delegate cancel", "instance": "codex", "model": "fixture",
     "scenario": "codex_delegate_cancel.json", "awaitStatus": True},
    {"name": "codex delegate stop", "instance": "codex", "model": "fixture",
     "scenario": "codex_delegate_stop.json", "awaitStatus": True,
     "change": {"type": "thread.stop"}, "changeAfter": "command"},
    {"name": "codex delegate restart", "instance": "codex", "model": "fixture",
     "scenario": "codex_delegate_restart.json", "awaitStatus": True, "settleRuns": 2,
     "restart": {"after": "settled"}},
    {"name": "claude delegate restart", "instance": "claudeAgent", "model": "claude-opus-5-5",
     "scenario": "claude_delegate_restart.json", "awaitStatus": True, "settleRuns": 2,
     "restart": {"after": "settled"}},
    {"name": "codex failed turn", "instance": "codex", "model": "fixture", "scenario": "codex_failed.json"},
    # Runtime requests: what each server asks the user, and what it answers the CLI.
    {"name": "codex approval", "instance": "codex", "model": "fixture", "scenario": "codex_approval.json",
     "respond": {"decision": "accept"}},
    {"name": "codex approval for session", "instance": "codex", "model": "fixture",
     "scenario": "codex_approval.json", "respond": {"decision": "acceptForSession"}},
    {"name": "codex approval declined", "instance": "codex", "model": "fixture", "scenario": "codex_approval.json",
     "respond": {"decision": "decline"}},
    {"name": "codex approval interrupted", "instance": "codex", "model": "fixture",
     "scenario": "codex_approval_interrupt.json", "interrupt": "request"},
    {"name": "codex approval-required", "instance": "codex", "model": "fixture", "scenario": "codex_approval.json",
     "runtimeMode": "approval-required", "respond": {"decision": "accept"}},
    {"name": "codex auto-accept-edits", "instance": "codex", "model": "fixture", "scenario": "codex_approval.json",
     "runtimeMode": "auto-accept-edits", "respond": {"decision": "decline"}},
    {"name": "codex user input", "instance": "codex", "model": "fixture", "scenario": "codex_user_input.json",
     "respond": {"answers": {"color": "Blue"}}},
    {"name": "codex crash", "instance": "codex", "model": "fixture", "scenario": "codex_crash.json"},
    # A turn that completes while its command still runs: the command is background work.
    {"name": "codex background command", "instance": "codex", "model": "fixture",
     "scenario": "codex_background.json"},
    {"name": "codex missing", "instance": "codex", "model": "fixture", "scenario": None},
    {"name": "codex interrupt", "instance": "codex", "model": "fixture", "scenario": "codex_interrupt.json",
     "interrupt": "command"},
    # An ACP Registry agent run from a local command (`source: local`), in the generic ACP flavor.
    {"name": "acp local turn", "instance": "acp-local", "model": "default", "scenario": "acp_turn.json",
     "enable": _ACP_LOCAL},
    # A model the session lists and is not on is set before the prompt (`session/set_model`).
    {"name": "acp model", "instance": "acp-local", "model": "fixture-large", "scenario": "acp_turn.json",
     "enable": _ACP_LOCAL},
    # A second turn prompts the session the thread's agent keeps.
    {"name": "acp second turn", "instance": "acp-local", "model": "default", "scenario": "acp_two_turns.json",
     "enable": _ACP_LOCAL, "followUp": "Now append a line"},
    # Supervised: the agent's permission requests go to the user, with the default decisions.
    {"name": "acp approval", "instance": "acp-local", "model": "default", "scenario": "acp_approval.json",
     "enable": _ACP_LOCAL, "runtimeMode": "approval-required", "respond": {"decision": "accept"}},
    {"name": "acp approval for session", "instance": "acp-local", "model": "default",
     "scenario": "acp_approval.json", "enable": _ACP_LOCAL, "runtimeMode": "approval-required",
     "respond": {"decision": "acceptForSession"}},
    {"name": "acp approval declined", "instance": "acp-local", "model": "default", "scenario": "acp_approval.json",
     "enable": _ACP_LOCAL, "runtimeMode": "approval-required", "respond": {"decision": "decline"}},
    # Auto: the generic flavor answers the agent's requests itself (Grok's asks).
    {"name": "acp auto", "instance": "acp-local", "model": "default", "scenario": "acp_approval.json",
     "enable": _ACP_LOCAL, "runtimeMode": "auto"},
    # Stop cancels the prompt (`session/cancel`), and the agent keeps running for the next turn.
    {"name": "acp interrupt", "instance": "acp-local", "model": "default", "scenario": "acp_interrupt.json",
     "enable": _ACP_LOCAL, "interrupt": "command"},
    {"name": "acp approval interrupted", "instance": "acp-local", "model": "default",
     "scenario": "acp_approval_interrupt.json", "enable": _ACP_LOCAL, "runtimeMode": "approval-required",
     "interrupt": "request"},
    {"name": "acp stop then turn", "instance": "acp-local", "model": "default",
     "scenario": "acp_stop_then_turn.json", "enable": _ACP_LOCAL, "interrupt": "command",
     "followUp": "Now append a line"},
    # A prompt the agent fails fails the turn with its message.
    {"name": "acp failed turn", "instance": "acp-local", "model": "default", "scenario": "acp_failed.json",
     "enable": _ACP_LOCAL},
    # A local executable that is not on PATH.
    {"name": "acp local missing", "instance": "acp-local", "model": "default", "scenario": None,
     "enable": _ACP_LOCAL},
    # An agent from the ACP Registry: its downloaded command, with its distribution's arguments
    # and environment.
    {"name": "acp registry turn", "instance": "acp-registry", "model": "default", "scenario": "acp_turn.json",
     "enable": {"driver": "acpRegistry", "displayName": "Fixture Agent",
                "config": {"source": "registry", "agentId": "fixture-agent"}}, "registryAgent": True},
    # Grok over ACP: its turns run in the `grok agent stdio` session the thread keeps, and stream
    # reasoning, a command and the reply.
    {"name": "grok turn", "instance": "grok", "enable": "grok", "model": "grok-build", "scenario": "grok_turn.json"},
    # The turn calls T3's tools through the stdio MCP bridge (`acp-mcp-bridge`) each server gave it.
    {"name": "grok agent mcp", "instance": "grok", "enable": "grok", "model": "grok-build",
     "scenario": "grok_mcp.json"},
    # A second turn goes to the agent the thread keeps, on its session, without the instructions.
    {"name": "grok second turn", "instance": "grok", "enable": "grok", "model": "grok-build",
     "scenario": "grok_two_turns.json", "followUp": "Now append a line"},
    # A model the session is not on is set before the prompt.
    {"name": "grok model", "instance": "grok", "enable": "grok", "model": "grok-4-fast", "scenario": "grok_turn.json"},
    # Supervised: Grok asks before it runs a command or edits, and each answer picks one of its options.
    {"name": "grok approval", "instance": "grok", "enable": "grok", "model": "grok-build",
     "scenario": "grok_approval.json", "runtimeMode": "approval-required", "respond": {"decision": "accept"}},
    {"name": "grok approval declined", "instance": "grok", "enable": "grok", "model": "grok-build",
     "scenario": "grok_approval.json", "runtimeMode": "approval-required", "respond": {"decision": "decline"}},
    {"name": "grok approval cancelled", "instance": "grok", "enable": "grok", "model": "grok-build",
     "scenario": "grok_approval.json", "runtimeMode": "approval-required", "respond": {"decision": "cancel"}},
    # Allowing all edits for the session is offered for Grok's edit approvals only.
    {"name": "grok edit approval for session", "instance": "grok", "enable": "grok", "model": "grok-build",
     "scenario": "grok_edit_approval.json", "runtimeMode": "approval-required",
     "respond": {"decision": "acceptForSession"}},
    # Grok has no auto-accept-edits mode: its edits go to the user as in approval-required.
    {"name": "grok auto-accept-edits", "instance": "grok", "enable": "grok", "model": "grok-build",
     "scenario": "grok_edit_approval.json", "runtimeMode": "auto-accept-edits", "respond": {"decision": "accept"}},
    # Auto: every request Grok makes goes to the user.
    {"name": "grok auto", "instance": "grok", "enable": "grok", "model": "grok-build",
     "scenario": "grok_edit_approval.json", "runtimeMode": "auto", "respond": {"decision": "accept"}},
    # Stop kills Grok's process (no session/cancel): the running command and a waiting request end.
    {"name": "grok interrupt", "instance": "grok", "enable": "grok", "model": "grok-build",
     "scenario": "grok_interrupt.json", "interrupt": "command"},
    {"name": "grok approval interrupted", "instance": "grok", "enable": "grok", "model": "grok-build",
     "scenario": "grok_approval_interrupt.json", "runtimeMode": "approval-required", "interrupt": "request"},
    # The turn after a Stop starts a new process, which loads the thread's session.
    {"name": "grok stop then resume", "instance": "grok", "enable": "grok", "model": "grok-build",
     "scenario": "grok_stop_resume.json", "interrupt": "command", "followUp": "Now append a line"},
    # After a server restart a new turn's process loads the thread's session (no session/new), with
    # the plain item ids of a session that server did not create.
    {"name": "grok restart idle", "instance": "grok", "enable": "grok", "model": "grok-build",
     "scenario": "grok_restart.json", "restart": {"after": "settled", "followUp": "Now append a line"}},
    # A message steers the running turn (`_x.ai/interject`), which goes on to take it. This turn
    # ends with Grok's completion notice, which settles a prompt before its reply does.
    {"name": "grok steer", "instance": "grok", "enable": "grok", "model": "grok-build",
     "scenario": "grok_steer.json", "steer": {"text": "Also print the date", "ack": "Steered: printing the date too"}},
    # A prompt Grok ends with an error fails the turn with Grok's own message.
    {"name": "grok failed turn", "instance": "grok", "enable": "grok", "model": "grok-build",
     "scenario": "grok_failed.json"},
    {"name": "grok missing", "instance": "grok", "enable": "grok", "model": "grok-build", "scenario": None},
    {"name": "claude turn", "instance": "claudeAgent", "model": "claude-opus-5-5", "scenario": "claude_turn.json"},
    # A second turn on the same server runs in the instance's shared app-server, on the thread it
    # still has loaded.
    # A second Claude turn goes to the CLI the thread keeps running, as on the reference.
    {"name": "claude second turn", "instance": "claudeAgent", "model": "claude-opus-5-5",
     "scenario": "claude_two_turns.json", "followUp": "Now append a line"},
    {"name": "codex second turn", "instance": "codex", "model": "fixture", "scenario": "codex_two_turns.json",
     "followUp": "Now append a line"},
    # A message steers the running turn (Codex `turn/steer`), which then answers it, and Stop
    # interrupts it.
    # A message steers the running Claude turn: it goes to the CLI the turn runs (priority
    # "now"), pending until Claude echoes its uuid, and the turn goes on to complete.
    {"name": "claude steer", "instance": "claudeAgent", "model": "claude-opus-5-5", "scenario": "claude_steer.json",
     "steer": {"text": "Also print the date", "ack": "Steered: printing the date too"}},
    # A steer Claude has not taken when the user stops the turn: it ends cancelled.
    {"name": "claude steer then stop", "instance": "claudeAgent", "model": "claude-opus-5-5",
     "scenario": "claude_steer_stop.json", "interrupt": "steer",
     "steer": {"text": "Also print the date", "ack": "<no answer: Claude never takes it>"}},
    {"name": "codex steer then stop", "instance": "codex", "model": "fixture", "scenario": "codex_steer.json",
     "steer": {"text": "Also print the date", "ack": "Steered: printing the date too"}, "interrupt": "steer"},
    # A seeded first message titles the thread with the text generation model (Codex), and a
    # regenerate request titles it again from the whole conversation.
    {"name": "codex title", "instance": "codex", "model": "fixture", "scenario": "codex_title.json",
     "dispatch": {"titleSeed": "Run echo hi and write notes.txt"}, "title": "regenerate", "titleAfterTurn": True},
    # Without Codex installed, the title falls back to Claude's text generation model.
    {"name": "claude title fallback", "instance": "claudeAgent", "model": "claude-opus-5-5",
     "scenario": "claude_title.json", "dispatch": {"titleSeed": "Run echo hi and write notes.txt"},
     "title": "initial", "titleAfterTurn": True},
    # Titles from another provider's text generation model, which the thread's turns never run on:
    # Pi over its RPC mode.
    {"name": "pi title", "instance": "codex", "model": "fixture", "scenario": "codex_turn.json",
     "dispatch": {"titleSeed": "Run echo hi and write notes.txt"}, "title": "regenerate", "titleAfterTurn": True,
     "textProvider": {"instance": "pi", "driver": "pi", "cli": "pi", "model": "openai/gpt-5.6",
                      # get_available_models is the status probe's too.
                      "launch": "--no-tools", "records": ["set_model", "set_thinking_level", "prompt",
                                                          "get_last_assistant_text"],
                      "spec": PI_TITLE_SPEC}},
    # Grok over ACP.
    {"name": "grok title", "instance": "codex", "model": "fixture", "scenario": "codex_turn.json",
     "dispatch": {"titleSeed": "Run echo hi and write notes.txt"}, "title": "regenerate", "titleAfterTurn": True,
     "textProvider": {"instance": "grok", "driver": "grok", "cli": "grok", "model": "grok-4-fast",
                      "launch": "agent", "spec": GROK_TITLE_SPEC, "skip": "t3-code-provider-probe",
                      "records": ["initialize", "authenticate", "session/new", "session/set_model",
                                  "session/set_config_option", "session/prompt", "session/cancel"]}},
    # OpenCode 1.x through the server T3 starts for it.
    {"name": "opencode title", "instance": "codex", "model": "fixture", "scenario": "codex_turn.json",
     "dispatch": {"titleSeed": "Run echo hi and write notes.txt"}, "title": "regenerate", "titleAfterTurn": True,
     "textProvider": {"instance": "opencode", "driver": "opencode", "cli": "opencode", "opencode": True,
                      "model": "anthropic/claude-opus-5-5", "options": [{"id": "variant", "value": "high"}],
                      "launch": "serve", "spec": OPENCODE_TITLE_SPEC}},
    # OpenCode 2 through the server T3 starts for it: a session, its event stream, a refused tool.
    {"name": "opencode2 title", "instance": "codex", "model": "fixture", "scenario": "codex_turn.json",
     "dispatch": {"titleSeed": "Run echo hi and write notes.txt"}, "title": "regenerate", "titleAfterTurn": True,
     "textProvider": {"instance": "opencode", "driver": "opencode", "cli": "opencode", "opencode": True,
                      "model": "anthropic/claude-opus-5-5", "options": [{"id": "variant", "value": "high"}],
                      "launch": "serve", "spec": OPENCODE2_TITLE_SPEC}},
    # Antigravity's agy CLI over ACP.
    {"name": "antigravity title", "instance": "codex", "model": "fixture", "scenario": "codex_turn.json",
     "dispatch": {"titleSeed": "Run echo hi and write notes.txt"}, "title": "regenerate", "titleAfterTurn": True,
     "textProvider": {"instance": "antigravity", "driver": "antigravity", "cli": "agy", "model": "antigravity-default",
                      "launch": "acp", "spec": AGY_TITLE_SPEC,
                      "records": ["initialize", "authenticate", "session/new", "session/set_mode", "session/set_model",
                                  "session/set_config_option", "session/prompt", "session/cancel"]}},
    {"name": "antigravity title refused for a broken runtime", "instance": "codex", "model": "fixture",
     "scenario": "codex_turn.json", "dispatch": {"titleSeed": "Run echo hi and write notes.txt"},
     "title": "regenerate", "titleAfterTurn": True,
     "textProvider": {"instance": "antigravity", "driver": "antigravity", "cli": "agy", "model": "antigravity-default",
                      "launch": "acp", "spec": AGY_BROKEN_TITLE_SPEC,
                      "records": ["initialize", "authenticate", "session/new", "session/set_mode", "session/set_model",
                                  "session/set_config_option", "session/prompt", "session/cancel"]}},
    # A title the provider cannot generate leaves the seed, and a regenerate reports why.
    {"name": "codex title failed", "instance": "codex", "model": "fixture", "scenario": "codex_turn.json",
     "dispatch": {"titleSeed": "Run echo hi and write notes.txt"}, "title": "regenerate"},
    {"name": "claude crash", "instance": "claudeAgent", "model": "claude-opus-5-5", "scenario": "claude_crash.json"},
    {"name": "claude missing", "instance": "claudeAgent", "model": "claude-opus-5-5", "scenario": None},
    # Installed where the user's login shell, not the server's own PATH, finds it.
    {"name": "claude in the login shell PATH", "instance": "claudeAgent", "model": "claude-opus-5-5",
     "scenario": "claude_turn.json", "install": "login-shell", "status": True},
    {"name": "codex in the login shell PATH", "instance": "codex", "model": "fixture", "scenario": "codex_turn.json",
     "install": "login-shell"},
    {"name": "claude missing from the login shell", "instance": "claudeAgent", "model": "claude-opus-5-5",
     "scenario": None, "install": "login-shell", "status": True},
    {"name": "claude interrupt", "instance": "claudeAgent", "model": "claude-opus-5-5",
     "scenario": "claude_interrupt.json", "interrupt": "command"},
    # A thread change sent over the WebSocket while the turn runs: what each server does to the
    # turn and its provider session (detach, interrupt, or refuse the change).
    {"name": "codex archive mid-turn", "instance": "codex", "model": "fixture",
     "scenario": "codex_interrupt.json", "change": {"type": "thread.archive"}, "changeAfter": "quiet"},
    {"name": "codex model change mid-turn", "instance": "codex", "model": "fixture",
     "scenario": "codex_interrupt.json", "change": {"type": "thread.model-selection.set",
                                                    "modelSelection": {"instanceId": "codex", "model": "other"}}, "changeAfter": "quiet"},
    {"name": "codex provider change mid-turn", "instance": "codex", "model": "fixture",
     "scenario": "codex_interrupt.json", "change": {"type": "thread.model-selection.set",
                                                    "modelSelection": {"instanceId": "claudeAgent",
                                                                       "model": "claude-opus-5-5"}}, "changeAfter": "quiet"},
    {"name": "codex settle mid-turn", "instance": "codex", "model": "fixture",
     "scenario": "codex_interrupt.json", "change": {"type": "thread.settle"}, "changeAfter": "quiet"},
    {"name": "codex runtime mode change mid-turn", "instance": "codex", "model": "fixture",
     "scenario": "codex_interrupt.json", "change": {"type": "thread.runtime-mode.set",
                                                    "runtimeMode": "approval-required"}, "changeAfter": "quiet"},
    {"name": "claude archive mid-turn", "instance": "claudeAgent", "model": "claude-opus-5-5",
     "scenario": "claude_interrupt.json", "change": {"type": "thread.archive"}, "changeAfter": "quiet"},
    {"name": "claude model change mid-turn", "instance": "claudeAgent", "model": "claude-opus-5-5",
     "scenario": "claude_interrupt.json", "change": {"type": "thread.model-selection.set",
                                                     "modelSelection": {"instanceId": "claudeAgent",
                                                                        "model": "claude-sonnet-5-5"}}, "changeAfter": "quiet"},
    {"name": "claude runtime mode change mid-turn", "instance": "claudeAgent", "model": "claude-opus-5-5",
     "scenario": "claude_interrupt.json", "change": {"type": "thread.runtime-mode.set",
                                                     "runtimeMode": "approval-required"}, "changeAfter": "quiet"},
    {"name": "claude approval", "instance": "claudeAgent", "model": "claude-opus-5-5",
     "scenario": "claude_approval.json", "respond": {"decision": "accept"}},
    {"name": "claude approval declined", "instance": "claudeAgent", "model": "claude-opus-5-5",
     "scenario": "claude_approval.json", "respond": {"decision": "decline"}},
    {"name": "claude approval interrupted", "instance": "claudeAgent", "model": "claude-opus-5-5",
     "scenario": "claude_approval_interrupt.json", "interrupt": "request"},
    {"name": "claude question", "instance": "claudeAgent", "model": "claude-opus-5-5",
     "scenario": "claude_question.json", "respond": {"answers": {"Which color?": "Blue"}}},
    {"name": "claude approval-required", "instance": "claudeAgent", "model": "claude-opus-5-5",
     "scenario": "claude_approval.json", "runtimeMode": "approval-required", "respond": {"decision": "acceptForSession"}},
    {"name": "claude auto-accept-edits", "instance": "claudeAgent", "model": "claude-opus-5-5",
     "scenario": "claude_approval.json", "runtimeMode": "auto-accept-edits", "respond": {"decision": "cancel"}},
    # Server restarts: the reference's ProviderRuntimeRecoveryService and restart continuation.
    {"name": "claude restart mid-turn", "instance": "claudeAgent", "model": "claude-opus-5-5",
     "scenario": "claude_restart.json", "restart": {"after": "running"}},
    {"name": "claude restart idle", "instance": "claudeAgent", "model": "claude-opus-5-5",
     "scenario": "claude_restart_idle.json", "restart": {"after": "settled"}},
    {"name": "claude restart continuation off", "instance": "claudeAgent", "model": "claude-opus-5-5",
     "scenario": "claude_restart.json",
     "restart": {"after": "running", "settings": {"continueThreadsAfterServerUpdate": False}}},
    {"name": "claude restart instance disabled", "instance": "claudeAgent", "model": "claude-opus-5-5",
     "scenario": "claude_restart.json", "restart": {"after": "running", "settings": {
         "providerInstances": {"claudeAgent": {"driver": "claudeAgent", "enabled": False}}}}},
    {"name": "claude restart cli missing", "instance": "claudeAgent", "model": "claude-opus-5-5",
     "scenario": "claude_restart.json", "restart": {"after": "running", "removeCli": True}},
    # A continuation that fails to start (no CLI) is tried again a minute later.
    {"name": "claude restart cli restored", "instance": "claudeAgent", "model": "claude-opus-5-5",
     "scenario": "claude_restart.json", "restart": {"after": "running", "removeCli": True, "restoreCli": True}},
    # What a client watching the thread sees when the server stops mid-turn.
    {"name": "claude restart live", "instance": "claudeAgent", "model": "claude-opus-5-5",
     "scenario": "claude_restart.json", "restart": {"after": "running", "live": True}},
    {"name": "codex restart live", "instance": "codex", "model": "fixture",
     "scenario": "codex_restart.json", "restart": {"after": "running", "live": True}},
    {"name": "codex restart mid-turn", "instance": "codex", "model": "fixture",
     "scenario": "codex_restart.json", "restart": {"after": "running"}},
    # Every app-server answers initialize late, so the restarted server's status probe starts
    # while the continuation's session is still initializing: the session's
    # initialize (with the MCP app extension and turn/diff/updated opt-out) and theirs (without)
    # interleave in the log, and `cli launch` must still tell them apart.
    {"name": "codex restart probe overlap", "instance": "codex", "model": "fixture",
     "scenario": "codex_restart_slow_initialize.json", "restart": {"after": "running"}},
    {"name": "codex restart idle", "instance": "codex", "model": "fixture",
     "scenario": "codex_restart_idle.json", "restart": {"after": "settled"}},
    # A run whose provider never took its prompt is sent again: a user's message waits in the
    # queue, held; an agent's is sent again.
    {"name": "codex restart unaccepted", "instance": "codex", "model": "fixture",
     "scenario": "codex_restart_unaccepted.json", "restart": {"after": "started"}},
    # The reference takes a client's message as a user's, wherever it came from.
    {"name": "codex restart unaccepted mcp", "instance": "codex", "model": "fixture",
     "scenario": "codex_restart_unaccepted.json", "restart": {"after": "started"},
     "dispatch": {"createdBy": "agent", "creationSource": "mcp"}},
    # Nobody reviews a scheduled prompt, so the reference sends it again instead of holding it,
    # unless continuation is off.
    {"name": "codex restart unaccepted scheduled", "instance": "codex", "model": "fixture",
     "scenario": "codex_restart_unaccepted.json", "restart": {"after": "started"},
     "dispatch": {"scheduledTaskId": "scheduled-task:fixture"}},
    {"name": "codex restart unaccepted scheduled continuation off", "instance": "codex",
     "model": "fixture", "scenario": "codex_restart_unaccepted.json",
     "restart": {"after": "started", "settings": {"continueThreadsAfterServerUpdate": False}},
     "dispatch": {"scheduledTaskId": "scheduled-task:fixture"}},
    # A command still running after its turn completed is background work the restart cuts off.
    # Queued messages wait, held, behind a cut-off turn or one put back in the queue.
    {"name": "codex restart queued", "instance": "codex", "model": "fixture",
     "scenario": "codex_restart.json", "restart": {"after": "running", "queue": ["and then this"]}},
    {"name": "codex restart unaccepted queued", "instance": "codex", "model": "fixture",
     "scenario": "codex_restart_unaccepted.json",
     "restart": {"after": "started", "queue": ["and then this"]}},
    {"name": "codex restart background", "instance": "codex", "model": "fixture",
     "scenario": "codex_restart_background.json", "restart": {"after": "settled"}},
    {"name": "codex restart continuation off", "instance": "codex", "model": "fixture",
     "scenario": "codex_restart.json",
     "restart": {"after": "running", "settings": {"continueThreadsAfterServerUpdate": False}}},
]
# The fake CLIs: version probes answer as a supported release would, and protocol sessions replay
# the scenario. Anything else exits quietly.
FAKE_CODEX = """#!/bin/sh
case "$1 $2" in
  "--version "*) echo "codex-cli 0.170.0"; exit 0;;
  "app-server --help") echo "Usage: codex app-server"; exit 0;;
  "app-server "*) exec python3 "$FAKE_PROVIDER_CLI" "$@";;
  "exec "*) exec python3 "$FAKE_PROVIDER_CLI" "$@";;
esac
case "$*" in
  *"mcp list --json") exec python3 "$FAKE_PROVIDER_CLI" "$@";;
esac
exit 0
"""
FAKE_CLAUDE = """#!/bin/sh
case "$*" in
  --version*) echo "2.1.200 (Claude Code)"; exit 0;;
  "-p "*) exec python3 "$FAKE_PROVIDER_CLI" "$@";;
  *--output-format*stream-json*) exec python3 "$FAKE_PROVIDER_CLI" "$@";;
esac
exit 0
"""
# The fake Grok: a turn's ACP session (`agent --always-approve stdio` and the other runtime modes'
# launches) replays the scenario; the status checks (version, models, skills, the plain
# `agent stdio` probe) answer from GROK_STATUS_SPEC.
FAKE_GROK = """#!/bin/sh
case "$*" in
  "agent --always-approve stdio"|"--permission-mode auto agent stdio"|"--permission-mode default agent stdio")
    exec python3 "$FAKE_PROVIDER_CLI" "$@";;
esac
FAKE_PROVIDER_LOG="$FAKE_PROVIDER_LOG.text" exec python3 "$FAKE_STATUS_CLI" "$FAKE_GROK_STATUS" "$@"
"""
GROK_STATUS_SPEC = {"commands": [command for command in GROK_TITLE_SPEC["commands"]
                                 if command.get("prefix") != ["--permission-mode", "default", "agent", "stdio"]]}
# Each driver's CLI on PATH, by the thread's provider instance.
# A local ACP Registry agent: the executable its instance names (`commandPath`), which plays the
# scenario whatever its arguments.
FAKE_ACP_AGENT = """#!/bin/sh
exec python3 "$FAKE_PROVIDER_CLI" "$@"
"""
TURN_CLIS = {"codex": "codex", "claudeAgent": "claude", "grok": "grok", "acp-local": "fixture-acp",
             # Installed in the server's home (`registryAgent`), not on PATH.
             "acp-registry": "registry-agent"}
# The ACP Registry agent a `registryAgent` scenario's servers find in their cached index, already
# downloaded into `<home>/tools` (its archive's HTTPS URL is refused), so a turn reuses it.
REGISTRY_TARGET = {("Linux", "x86_64"): "linux-x86_64", ("Linux", "aarch64"): "linux-aarch64"}.get(
    (platform.system(), platform.machine()))
REGISTRY_AGENT = {"id": "fixture-agent", "name": "Fixture Agent", "version": "1.0.0",
                  "description": "A downloaded fixture agent", "distribution": {"binary": {REGISTRY_TARGET: {
                      "archive": "https://example.invalid/fixture-agent.tar.gz", "cmd": "./run",
                      "args": ["--acp", "--registry"], "env": {"FIXTURE_AGENT_MODE": "registry"}}}}}


def seed_registry_agent(home):
    """The cached registry index and the agent's downloaded command (the fake CLI) in `home`."""
    cache = home / "caches" / "acp-registry"
    cache.mkdir(parents=True, exist_ok=True)
    (cache / "registry.json").write_text(json.dumps({"version": "1.0.0", "agents": [REGISTRY_AGENT]}))
    run = home / "tools" / "fixture-agent" / "1.0.0" / REGISTRY_TARGET / "run"
    run.parent.mkdir(parents=True, exist_ok=True)
    run.write_text('#!/bin/sh\nexec python3 "$FAKE_PROVIDER_CLI" "$@"\n')
    run.chmod(0o755)
THREAD = "thread:client-thread"
REMOTES = {
    "project:bitbucket": ("Bitbucket checkout", "git@ssh.bitbucket.org:example-org/example-repo.git"),
    "project:github": ("GitHub checkout", "https://github.com/Example-Org/Example.Repo.git"),
}
GENERATED = re.compile(r"^(\d{4}-\d\d-\d\dT[\d:.]+Z|[0-9a-f]{40}|[0-9a-f-]{36}|.*:[0-9a-f-]{36}.*)$")


# Set in main: the Git configuration both servers and the seeded checkouts use.
GIT_ISOLATION = {}
# Set in main: the hosts both servers ask instead of real ones (a loopback Bitbucket API).
HOST_FAKES = {}


def fake_bitbucket():
    """A loopback Bitbucket API that answers every request as Bitbucket answers an anonymous
    `/user`: HTTP 401. Neither server is given a Bitbucket credential, so both list a Bitbucket
    project as unauthenticated without leaving the machine."""
    from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

    class Unauthorized(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def do_GET(self):
            body = encode({"type": "error", "error": {"message": "Unauthorized"}})
            self.send_response(401)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        do_POST = do_PUT = do_DELETE = do_GET

    server = ThreadingHTTPServer(("127.0.0.1", 0), Unauthorized)
    server.daemon_threads = True
    threading.Thread(target=server.serve_forever, daemon=True).start()
    url = f"http://127.0.0.1:{server.server_port}/2.0"
    assert url.startswith("http://127.0.0.1:")
    return url


def git(*args, cwd=None):
    # parity-env: git, not a T3 server
    subprocess.run(["git", *args], cwd=cwd, check=True, capture_output=True, env={**os.environ, **GIT_ISOLATION})


def make_checkout(path, remote):
    git("init", "-qb", "main", str(path))
    git("-C", str(path), "remote", "add", "origin", remote)
    (path / "README.md").write_text("fixture\n")
    git("-C", str(path), "add", "README.md")
    git("-C", str(path), "-c", "user.email=test@example.test", "-c", "user.name=Parity", "commit", "-qm", "Fixture")


def seed(url, cookie, root):
    headers = {"Cookie": cookie, "Content-Type": "application/json"}
    for project_id, (title, remote) in REMOTES.items():
        checkout = root / project_id.split(":")[1]
        make_checkout(checkout, remote)
        status, _, body = http(url, "POST", "/api/projects/mutate", encode({
            "type": "project.create", "commandId": f"create-{project_id}", "projectId": project_id,
            "title": title, "workspaceRoot": str(checkout)}), headers)
        if status != 200:
            raise RuntimeError(f"project.create failed on {url}: HTTP {status} {body[:400]!r}")
    rpc(url, cookie, "orchestration.dispatchCommand", {
        "type": "thread.create", "commandId": "seed-thread", "threadId": THREAD, "projectId": "project:bitbucket",
        "title": "Thread", "modelSelection": {"instanceId": "codex", "model": "fixture"},
        "runtimeMode": "full-access", "interactionMode": "default", "branch": None, "worktreePath": None,
        "createdBy": "user", "creationSource": "web"})
    # No provider is installed in the sandbox, so the turn ends as soon as it starts.
    rpc(url, cookie, "orchestration.dispatchCommand", {
        "type": "message.dispatch", "commandId": "seed-message", "threadId": THREAD,
        "messageId": "message:seed", "text": "Hello from the parity fixture", "attachments": [],
        "createdBy": "user", "creationSource": "web", "dispatchMode": {"type": "start_immediately"}})
    return {"threadId": THREAD, "projectIds": list(REMOTES), "hosts": ["ssh.bitbucket.org", "github.com"]}


def run_client(args, url, cookie, seeded):
    env = {**os.environ, "T3_CLIENT_SYNC_COOKIE": cookie, "T3_CLIENT_THREAD_SEED": json.dumps(seeded),
           "T3_CLIENT_THREAD_WAIT_MS": str(args.reply_ms)}
    # parity-env: the Node client driver, not a T3 server
    result = subprocess.run([args.node, str(ROOT / "scripts/client_thread.mjs"), args.reference_root, url],
                            capture_output=True, text=True, env=env, timeout=args.reply_ms / 1000 + 120)
    lines = [line for line in result.stdout.splitlines() if line.startswith("{")]
    if not lines:
        raise RuntimeError(f"client driver produced no result (exit {result.returncode}):\n{result.stderr[-4000:]}")
    return json.loads(lines[-1])


def shape(value, path="$", out=None):
    """Every key path with its value type; array elements share one path."""
    out = {} if out is None else out
    if isinstance(value, dict):
        out.setdefault(path, set()).add("object")
        for key, item in value.items():
            shape(item, f"{path}.{key}", out)
    elif isinstance(value, list):
        out.setdefault(path, set()).add("array")
        for item in value:
            shape(item, f"{path}[]", out)
    else:
        out.setdefault(path, set()).add(type(value).__name__)
    return out


def stable(value, root):
    """The value with times, generated ids and the temporary home masked."""
    if isinstance(value, dict):
        return {key: stable(item, root) for key, item in value.items()}
    if isinstance(value, list):
        return [stable(item, root) for item in value]
    if isinstance(value, str):
        value = value.replace(str(root), "<home>")
        return "<generated>" if GENERATED.match(value) else value
    return value


def variants(projection):
    """Each projection record by collection and, for turn items, by type."""
    out = {"thread": [projection.get("thread")]}
    for key, value in projection.items():
        if not isinstance(value, list):
            continue
        for record in value:
            if key == "turnItems":
                out.setdefault(f"turnItems:{record.get('type')}", []).append(record)
            elif key == "visibleTurnItems":
                item = record.get("item", {})
                row = {k: v for k, v in record.items() if k != "item"}
                out.setdefault(f"visibleTurnItems:{item.get('type')}", []).append(row)
                out.setdefault(f"visibleTurnItems.item:{item.get('type')}", []).append(item)
            else:
                out.setdefault(key, []).append(record)
    return out


def comparable_shape(records):
    """Key paths with value types, where a null matches any type."""
    merged = {}
    for record in records:
        for path, types in shape(record).items():
            merged.setdefault(path, set()).update(types)
    return merged


# Fields a client shows that do not depend on time, generated ids or how far the run got.
STABLE_FIELDS = {
    "messages": ["role", "text", "attachments", "streaming", "createdBy", "creationSource"],
    "turnItems:user_message": ["text", "textOmitted", "inputIntent", "attachments", "createdBy",
                               "creationSource", "status", "title"],
    "visibleTurnItems:user_message": ["position", "visibility"],
    "visibleTurnItems:error": ["position", "visibility"],
    "turnItems:error": ["status"],
    "runs": ["status", "ordinal", "queuePosition"],
    "thread": ["title", "projectId", "runtimeMode", "interactionMode", "modelSelection", "branch", "worktreePath"],
}
STABLE_FIELDS["visibleTurnItems.item:user_message"] = STABLE_FIELDS["turnItems:user_message"]


def compare_thread(original, rust):
    """Problems with Rust's thread stream as the reference client decodes it.

    The reference records its provider runtime's work in the projection (attempts, nodes,
    provider threads), which Rust does not model. Each kind of record both servers send must
    have the same fields and types; values are compared where they do not depend on timing.
    """
    problems = []
    kinds = [[item.get("kind") for item in side] for side in (original, rust)]
    if kinds[0] != kinds[1]:
        return [f"items {kinds[0]} != {kinds[1]}"]
    for left, right in zip(original, rust):
        if left.get("kind") != "snapshot":
            continue
        outer = [sorted(k for k in item if k != "projection") for item in (left, right)]
        if outer[0] != outer[1]:
            problems.append(f"snapshot fields {outer[0]} != {outer[1]}")
        for field in ("historyCursor", "hasMoreHistory", "turnItemsOmitLocalVisible", "idTable"):
            if left.get(field) != right.get(field):
                problems.append(f"snapshot {field}: original {left.get(field)!r} rust {right.get(field)!r}")
        projections = left["projection"], right["projection"]
        if sorted(projections[0]) != sorted(projections[1]):
            problems.append(f"projection fields {sorted(projections[0])} != {sorted(projections[1])}")
        ours, theirs = variants(projections[0]), variants(projections[1])
        for name in sorted(set(ours) & set(theirs)):
            a, b = comparable_shape(ours[name]), comparable_shape(theirs[name])
            for path in sorted(set(a) | set(b)):
                left_types, right_types = a.get(path, set()) - {"NoneType"}, b.get(path, set()) - {"NoneType"}
                if path not in a or path not in b or (left_types and right_types and left_types != right_types):
                    problems.append(f"{name} {path}: original {sorted(a.get(path, []))} rust {sorted(b.get(path, []))}")
            for field in STABLE_FIELDS.get(name, []):
                # An attachment's id holds a generated uuid.
                values = [[json.loads(UUID.sub("<uuid>", json.dumps(record.get(field))))
                           if field == "attachments" else record.get(field)
                           for record in side[name]] for side in (ours, theirs)]
                if values[0] != values[1]:
                    problems.append(f"{name}.{field}: original {values[0]!r} rust {values[1]!r}")
        for name in sorted(set(ours) - set(theirs)):
            if name.endswith(("user_message", "error")) or name in ("messages", "runs", "thread"):
                problems.append(f"rust sends no {name}")
    return problems


def write_fake_clis(directory, drivers):
    directory.mkdir(parents=True, exist_ok=True)
    for name, script in (("codex", FAKE_CODEX), ("claude", FAKE_CLAUDE), ("grok", FAKE_GROK),
                         ("fixture-acp", FAKE_ACP_AGENT)):
        if name in drivers:
            (directory / name).write_text(script)
            (directory / name).chmod(0o755)
    if "grok" in drivers:
        (directory / "grok.status.json").write_text(json.dumps(GROK_STATUS_SPEC))


def write_text_provider(directory, provider):
    """The fake CLI of a scenario's text generation provider (`textProvider`): `status_cli.py`
    (or `fake_opencode.py` with "opencode") answering from its spec, logging to the provider log's
    `.text` files."""
    directory.mkdir(parents=True, exist_ok=True)
    spec = directory / f"{provider['cli']}.spec.json"
    spec.write_text(json.dumps(provider["spec"]))
    fake = FIXTURES / ("fake_opencode.py" if provider.get("opencode") else "status_cli.py")
    target = directory / provider["cli"]
    target.write_text(f'#!/bin/sh\nFAKE_PROVIDER_LOG="$FAKE_PROVIDER_LOG.text" exec python3 '
                      f'{shlex.quote(str(fake))} {shlex.quote(str(spec))} "$@"\n')
    target.chmod(0o755)


def seed_turn(url, cookie, root, scenario, remote=None):
    """A project on a git checkout and a thread on the scenario's provider, with the message to send."""
    checkout = root / "checkout"
    make_checkout(checkout, remote or ("https://github.com/example/parity.git" if scenario["name"] == "codex thread tools" else REMOTES["project:github"][1]))
    status, _, body = http(url, "POST", "/api/projects/mutate", encode({
        "type": "project.create", "commandId": "create-project", "projectId": "project:github",
        "title": "Provider turn", "workspaceRoot": str(checkout)}),
        {"Cookie": cookie, "Content-Type": "application/json"})
    if status != 200:
        raise RuntimeError(f"project.create failed on {url}: HTTP {status} {body[:400]!r}")
    if scenario.get("enable"):
        # A provider instance that is off by default, turned on for the thread's turns.
        # A driver name, or the instance's settings (an ACP Registry agent's `config`).
        enable = scenario["enable"]
        instance = {"driver": enable, "enabled": True} if isinstance(enable, str) else {"enabled": True, **enable}
        rpc(url, cookie, "server.updateSettings", {"patch": {"providerInstances": {scenario["instance"]: instance}}})
    if scenario.get("textProvider"):
        # Titles come from another provider's instance, which the turn's provider never runs.
        provider = scenario["textProvider"]
        rpc(url, cookie, "server.updateSettings", {"patch": {
            "providerInstances": {provider["instance"]: {"driver": provider["driver"], "enabled": True,
                                                         **({"config": provider["config"]} if provider.get("config") else {})}},
            "textGenerationModelSelection": {"instanceId": provider["instance"], "model": provider["model"],
                                             **({"options": provider["options"]} if provider.get("options") else {})}}})
    rpc(url, cookie, "orchestration.dispatchCommand", {
        "type": "thread.create", "commandId": "seed-thread", "threadId": THREAD, "projectId": "project:github",
        "title": "New thread" if scenario.get("title") else "Thread",
        "modelSelection": {"instanceId": scenario["instance"], "model": scenario["model"]},
        "runtimeMode": scenario.get("runtimeMode", "full-access"), "interactionMode": "default", "branch": None,
        "worktreePath": None, "createdBy": "user", "creationSource": "web"})
    if scenario["name"] == "codex thread tools":
        from linear_provider_parity import TOKEN
        linear = root / "linear"
        linear.mkdir()
        rpc(url, cookie, "projects.mutate", {"type":"project.create", "commandId":"add-linear",
            "projectId":"project:linear", "title":"Linear", "workspaceRoot":str(linear)})
        rpc(url, cookie, "issueTrackers.connect", {"tracker":"linear", "token":TOKEN})
        rpc(url, cookie, "issueTrackers.bindProject", {"projectId":"project:linear", "binding":{"tracker":"linear","scope":"ENG"}})
        for project, trigger in [("project:github", {"type": "interval", "everyMs": 60000}),
                                 ("project:other", {"type": "webhook"})]:
            rpc(url, cookie, "scheduledTasks.upsert", {
                "id": "task:" + project, "projectId": project, "title": "Read report", "prompt": "Read report",
                "enabled": False, "schedule": trigger, "threadMode": "fresh_session",
                "workspaceStrategy": {"type": "root"}, "modelSelection": {"instanceId": "codex", "model": "fixture"},
                "runtimeMode": "approval-required", "interactionMode": "default"})
    return {"threadId": THREAD, "turnMs": 60000 if scenario.get("change") or scenario.get("steer") else 30000,
            "interrupt": scenario.get("interrupt", False), "settleRuns": scenario.get("settleRuns", 1),
            "respond": scenario.get("respond"), "change": scenario.get("change"),
            "steer": scenario.get("steer"), "changeAfter": scenario.get("changeAfter"), "dispatch": {
        "type": "message.dispatch", "commandId": "turn-message", "threadId": THREAD,
        "messageId": "message:turn", "text": "Run echo hi and write notes.txt",
        "attachments": upload_attachments(url, cookie, scenario.get("uploads", [])),
        "createdBy": "user", "creationSource": "web", "dispatchMode": {"type": "start_immediately"},
        **scenario.get("dispatch", {})}}


def peer_calls(peer, url):
    """What a server asked its linked environment (sorted: calls made at once arrive in either order),
    with its own environment id and the
    retry keys it scoped to its caller (`fwd:<digest>`) masked."""
    status, _, body = http(url, "GET", "/.well-known/t3/environment")
    here = json.loads(body)["environmentId"] if status == 200 else None

    def mask(value):
        if isinstance(value, str):
            if here:
                value = value.replace(here, "<here>")
            return re.sub(r"fwd:[0-9a-f]{64}", "fwd:<key>", value)
        if isinstance(value, list):
            return [mask(item) for item in value]
        if isinstance(value, dict):
            return {key: mask(item) for key, item in value.items()}
        return value
    # A task's follower and a tool reading its status call at once, in either order on either server.
    return sorted((mask(call) for call in peer.calls()), key=lambda call: json.dumps(call, sort_keys=True))


def upload_attachments(url, cookie, uploads):
    """Each upload as a client sends it: an upload URL for its name, type and size, its exact bytes
    posted there, then the pending attachment the message names."""
    attachments = []
    for upload in uploads:
        content = upload["content"].encode() if isinstance(upload["content"], str) else upload["content"]
        meta = {"type": upload["type"], "name": upload["name"], "mimeType": upload["mimeType"],
                "sizeBytes": len(content)}
        issued = rpc(url, cookie, "attachments.createUploadUrl", meta)
        status, _, body = http(url, "POST", issued["relativeUrl"], content,
                               {"Cookie": cookie, "Content-Type": "application/octet-stream"})
        if status >= 300:
            raise RuntimeError(f"upload of {upload['name']} failed on {url}: HTTP {status} {body[:200]!r}")
        attachments.append({**meta, "id": issued["attachmentId"]})
    return attachments


def without_home(value, extra, paths):
    """`value` with each Claude attachment budget less `extra` bytes for each of `paths` attachment
    paths in the prompt, so a longer home path counts as a shorter one would."""
    if isinstance(value, dict):
        budget = value.get("claudeAttachmentBudget")
        if isinstance(budget, dict) and isinstance(budget.get("bytesUsed"), int):
            budget["bytesUsed"] -= extra * paths
        for item in value.values():
            without_home(item, extra, paths)
    elif isinstance(value, list):
        for item in value:
            without_home(item, extra, paths)


def turn_inputs(log, root):
    """What the provider CLI was asked: each Codex turn/start input and each Claude user message,
    with homes and ids masked and image data by its hash."""
    def mask(value):
        if isinstance(value, str):
            value = re.sub(r"data:([^;]+);base64,([A-Za-z0-9+/=]+)",
                           lambda m: f"data:{m[1]};sha256={hashlib.sha256(m[2].encode()).hexdigest()}", value)
            for server in ("original", "rust"):
                value = value.replace(f"{root}/{server}", "<home>/<server>")
            return UUID.sub("<uuid>", value.replace(str(root), "<home>"))
        if isinstance(value, list):
            return [mask(item) for item in value]
        if isinstance(value, dict):
            if value.get("type") == "base64" and isinstance(value.get("data"), str):
                value = {**value, "data": "sha256=" + hashlib.sha256(value["data"].encode()).hexdigest()}
            return {key: mask(item) for key, item in value.items()}
        return value
    messages = Path(str(log) + ".messages")
    out = []
    for line in (messages.read_text().splitlines() if messages.exists() else []):
        with contextlib.suppress(ValueError):
            message = json.loads(json.loads(line)["line"])
            if message.get("method") == "turn/start":
                out.append(f"turn/start input {json.dumps(mask(message['params'].get('input')), sort_keys=True)}")
            # A steer goes with a priority; the turn's own message without one.
            elif message.get("type") == "user" and "priority" not in message:
                out.append(f"claude user {json.dumps(mask(message.get('message')), sort_keys=True)}")
    return out


def rendered_turn(projection):
    """The turn as a client renders it, without times and generated ids."""
    items = sorted(projection.get("turnItems", []), key=lambda item: item.get("ordinal", 0))
    def item(record):
        row = [record.get("type"), record.get("status")]
        for field in ("text", "title", "command", "toolName"):
            if record.get(field) not in (None, ""):
                row.append(f"{field}={str(record[field])[:80]}")
        if record.get("failure"):
            # A provider session's id ends in a UUID each server draws.
            row.append("failure=" + UUID.sub("<uuid>", str(record["failure"].get("message")))[:120])
        return " ".join(str(part) for part in row)
    return {
        "items": [item(record) for record in items],
        "messages": [f"{m.get('role')} {m.get('createdBy')}/{m.get('creationSource')}: {str(m.get('text'))[:80]}"
                     + "".join(f" +{a.get('type')} {a.get('name')} {a.get('mimeType')} {a.get('sizeBytes')} "
                               f"{UUID.sub('<uuid>', str(a.get('id')))}" for a in m.get("attachments") or [])
                     for m in projection.get("messages", [])],
        "item attachments": [UUID.sub("<uuid>", json.dumps(record.get("attachments"), sort_keys=True))
                             for record in items if record.get("attachments")],
        "runs": [run.get("status") for run in projection.get("runs", [])],
        # A stopped run owns the rollback point at its ordinal, even with other messages queued.
        "stopped checkpoints": [
            (run["ordinal"], cp.get("appRunOrdinal"), cp.get("status"), run.get("checkpointId") == cp["id"])
            for run in projection.get("runs", []) if run.get("status") in ("interrupted", "cancelled")
            for cp in projection.get("checkpoints", []) if cp.get("runId") == run["id"]
        ],
        "runtimeRequests": [f"{r.get('kind')} {r.get('status')} {r.get('decision')} {r.get('answers')}"
                            for r in projection.get("runtimeRequests", [])],
        "attempts": [attempt.get("status") for attempt in projection.get("attempts", [])],
        "providerTurns": [turn.get("status") for turn in projection.get("providerTurns", [])],
        "providerThreads": [thread.get("status") for thread in projection.get("providerThreads", [])],
        "providerSessions": [session.get("status") for session in projection.get("providerSessions", [])],
        "plans": [plan.get("kind") for plan in projection.get("plans", [])],
        # Token usage as the client's context meter and usage views read it, without times.
        "usage": [{"model": turn.get("model"), "turnTokenUsage": turn.get("turnTokenUsage"),
                   "tokenUsage": {k: v for k, v in (turn.get("tokenUsage") or {}).items()
                                  if k not in ("updatedAt", "promptCacheExpiresAt")} or None}
                  for turn in projection.get("providerTurns", [])],
        "nativeMetadata": [thread.get("nativeMetadata") for thread in projection.get("providerThreads", [])],
    }


def live_summary(live):
    """What streamed in after the dispatch: item kinds and event types in order, repeats collapsed."""
    out = []
    for item in (live or {}).get("items", []):
        if not item.get("dispatched"):
            continue
        label = item["kind"] if item["kind"] != "event" else f"event {item['event'].get('type')}"
        if item["kind"] == "event" and item["event"].get("type") == "turn-item.updated":
            label += f" {item['event'].get('payload', {}).get('type')}"
        if not out or out[-1] != label:
            out.append(label)
    return out


# Codex request params that carry the thread's runtime mode.
CODEX_POLICY_KEYS = ("approvalPolicy", "approvalsReviewer", "sandbox", "sandboxPolicy")
# Claude Code flags that carry it.
CLAUDE_POLICY_FLAGS = ("--permission-mode", "--permission-prompt-tool", "--allow-dangerously-skip-permissions")


def cli_approvals(log):
    """What a server told the fake CLI about approvals: the runtime policy it started the thread
    and turn with, and its answer to each request the CLI made (fixture request ids are strings
    such as "approval-1" or "perm-1")."""
    out = []
    args_path = Path(str(log) + ".args")
    for line in (args_path.read_text().splitlines() if args_path.exists() else []):
        words = line.split()
        for index, word in enumerate(words):
            name = word.split("=", 1)[0]
            if name in CLAUDE_POLICY_FLAGS:
                value = word.split("=", 1)[1] if "=" in word else (
                    words[index + 1] if index + 1 < len(words) and not words[index + 1].startswith("--") else True)
                out.append(f"arg {name}={value}")
    for line in (Path(log).read_text().splitlines() if Path(log).exists() else []):
        try:
            message = json.loads(line)
        except ValueError:
            continue
        method = message.get("method")
        if method in ("thread/start", "thread/resume", "turn/start"):
            params = message.get("params") or {}
            out.append(f"{method} " + json.dumps({k: params[k] for k in CODEX_POLICY_KEYS if k in params},
                                                 sort_keys=True))
        elif method is None and isinstance(message.get("id"), str) and ("result" in message or "error" in message):
            out.append(f"reply {message['id']} " + json.dumps(message.get("result", message.get("error")),
                                                              sort_keys=True))
        elif message.get("type") == "control_response":
            response = message.get("response") or {}
            if str(response.get("request_id", "")).startswith("perm-"):
                out.append(f"control_response {response['request_id']} "
                           + json.dumps(response.get("response", response.get("error")), sort_keys=True))
    return out


UUID = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")
# A new worktree's folder (`<branch>-<random>`), named apart on each server.
WORKTREE_DIR = re.compile(r"(/worktrees/[^/\"\s]+/)t3-[0-9a-f]{8}-[0-9a-f]{16}")
BEARER = re.compile(r"Bearer [A-Za-z0-9_\-.]+")
MCP_URL = re.compile(r"http://127\.0\.0\.1:\d+/mcp")


def comparable_mcp(original, rust):
    """Both servers' agent MCP records, comparable: Rust serves some of the reference's agent
    tools, so each `tools/list` must hold only tools the reference lists in its own answer to that
    call, defined the same, and a `tool_search` finds only the tools Rust serves; a thread's latest
    run id differs in form (the shared store gives Rust the reference's)."""
    def lists(lines):
        return [json.loads(line) for line in lines if line.startswith("{")
                and json.loads(line).get("request", {}).get("method") == "tools/list"]
    listings = [[t["name"] for t in (record.get("answer") or {}).get("result", {}).get("tools", [])]
                for record in lists(rust)]
    served = {name for names in listings for name in names} | {"tool_search"}

    def shape(lines):
        out = []
        listing = iter(listings)
        for line in lines:
            if not line.startswith("{"):
                out.append(line)
                continue
            record = json.loads(line)
            answer = record.get("answer")
            request = record.get("request", {})
            if request.get("method") == "tools/list" and isinstance(answer, dict):
                tools = {t["name"]: t for t in (answer.get("result") or {}).get("tools", [])}
                names = next(listing, None)
                record["answer"] = {name: tools.get(name, "<not listed>") for name in sorted(names or tools)}
            elif request.get("tool") == "tool_search" and isinstance(answer, dict) \
                    and isinstance(answer.get("content"), dict) and not answer.get("isError"):
                answer["content"]["tools"] = [t for t in answer["content"].get("tools", [])
                                              if t.get("name") in served]
            text = re.sub(r'"latestRunId": "[^"]*"', '"latestRunId": "<run>"', json.dumps(record, sort_keys=True))
            # Each server numbers its own orchestration events.
            text = re.sub(r'"sequence": \d+', '"sequence": "<sequence>"', text)
            out.append(text)
        return out
    return shape(original), shape(rust)


def agent_mcp(log, root):
    """The control requests that hand Claude Code its MCP servers, and each call the CLI made to
    the T3 MCP server it was given, with the answer; ids, times, ports and tokens masked."""
    def mask(value):
        if isinstance(value, str):
            value = MCP_URL.sub("http://127.0.0.1:<port>/mcp", BEARER.sub("Bearer <token>", value))
            for server in ("original", "rust"):
                value = value.replace(f"{root}/{server}", "<home>/<server>")
            value = value.replace(str(root), "<home>")
            value = re.sub(r"\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(\.\d+)?Z", "<time>", value)
            value = WORKTREE_DIR.sub(r"\1<worktree>", value)
            return UUID.sub("<uuid>", value)
        if isinstance(value, list):
            return [mask(item) for item in value]
        if isinstance(value, dict):
            return {key: mask(item) for key, item in value.items()}
        return value
    out = []
    for line in (Path(log).read_text().splitlines() if Path(log).exists() else []):
        with contextlib.suppress(ValueError):
            message = json.loads(line)
            if (message.get("request") or {}).get("subtype") == "mcp_set_servers":
                out.append(f"mcp_set_servers {json.dumps(mask(message['request']), sort_keys=True)}")
    calls = Path(str(log) + ".mcp")
    for line in (calls.read_text().splitlines() if calls.exists() else []):
        record = json.loads(line)
        answer = record.get("answer")
        if isinstance(answer, dict):
            result = answer.get("result") or {}
            # A tool's answer is its text content; structuredContent repeats it.
            if isinstance(result, dict) and "content" in result:
                text = "".join(part.get("text", "") for part in result["content"])
                with contextlib.suppress(ValueError):
                    text = json.loads(text)
                answer = {"isError": result.get("isError", False), "content": text}
        out.append(json.dumps(mask({**record, "answer": answer}), sort_keys=True))
    return out


def unserved_agent_tools():
    """The reference's agent tools Rust does not serve yet: tools.json less SERVED in tools.rs."""
    names = [tool["name"] for tool in json.loads((ROOT / "src/mcp_oauth/tools.json").read_text())["tools"]]
    rust = (ROOT / "src/mcp_oauth/tools.rs").read_text()
    block = rust[rust.index("const SERVED: &[&str] = &["):]
    served = set(re.findall(r'^\s*"([a-z0-9_]+)",', block[:block.index("];")], re.M))
    return [name for name in names if name not in served]


def without_unserved(text, unserved):
    """The reference's agent instructions as Rust sends them (src/agent_instructions.rs): less each
    line that names a tool Rust does not serve, and each heading left with nothing under it."""
    word = re.compile(r"[A-Za-z0-9_]")
    def names(line):
        for tool in unserved:
            for match in re.finditer(re.escape(tool), line):
                before = line[match.start() - 1] if match.start() else ""
                after = line[match.end()] if match.end() < len(line) else ""
                if not word.match(before or " ") and not word.match(after or " "):
                    return True
        for match in re.finditer(r"([A-Za-z0-9_]+)\*", line):
            if any(tool.startswith(match.group(1)) for tool in unserved):
                return True
        return False
    lines = text.split("\n")
    keep = [not names(line) for line in lines]
    if all(keep):
        return text
    index = 0
    while index < len(lines):
        if not lines[index].startswith("#"):
            index += 1
            continue
        end = next((i for i in range(index + 1, len(lines)) if lines[i].startswith("#")), len(lines))
        span = range(index + 1, end)
        if all(not keep[i] or not lines[i].strip() for i in span) and any(not keep[i] for i in span):
            for i in range(index, end):
                keep[i] = False
        index = end
    return "\n".join(line for line, kept in zip(lines, keep) if kept)


def agent_context(context, unserved):
    """Codex's turn context of agent instructions, as Rust sends it: each entry's text less what
    names an unserved tool, and an entry with nothing left left out."""
    out = {}
    for key, entry in (context or {}).items():
        value = without_unserved(entry.get("value", ""), unserved) if isinstance(entry, dict) else entry
        if not isinstance(entry, dict) or value.strip():
            out[key] = {**entry, "value": value} if isinstance(entry, dict) else entry
    return out


def cli_launch(log, root):
    """How a server launched the fake CLI for the turn: its arguments, the environment it set,
    and the Codex initialize, thread and turn parameters, then the initialize parameters of its
    other app-servers, with generated ids, the server's temporary home, the agent MCP server's
    port and its bearer token masked. The agent instructions (Codex's turn context, Claude Code's
    system prompt append) are compared as Rust sends them: without what names a tool Rust does not
    serve yet."""
    unserved = unserved_agent_tools()
    def mask(value):
        if isinstance(value, str):
            for server in ("original", "rust"):
                value = value.replace(f"{root}/{server}", "<home>/<server>")
            value = MCP_URL.sub("http://127.0.0.1:<port>/mcp", BEARER.sub("Bearer <token>", value))
            value = WORKTREE_DIR.sub(r"\1<worktree>", value)
            return UUID.sub("<uuid>", value.replace(str(root), "<home>"))
        if isinstance(value, list):
            return [mask(item) for item in value]
        if isinstance(value, dict):
            return {key: mask(item) for key, item in value.items()}
        return value
    # A server runs other Codex app-servers beside the turn's: the provider status probe (which
    # also lists the models), which may still be starting when the turn's session does, and runs
    # again after a restart. Their lines interleave with the session's in the log, so each line is
    # attributed to the process that read it, and the turn's launch and initialize are those of
    # the process that last started or resumed the thread.
    launch = Path(str(log) + ".launch")
    launches, order = {}, []
    for line in (launch.read_text().splitlines() if launch.exists() else []):
        entry = json.loads(line)
        # Claude Code probes at startup, not for a turn.
        if "--no-session-persistence" in entry["argv"]:
            continue
        argv = entry["argv"]
        env = {k: v for k, v in entry["env"].items() if k != "T3CODE_HOME"}
        launches[entry.get("pid")] = f"launch {json.dumps(mask(argv))} env {json.dumps(mask(env), sort_keys=True)}"
        order.append(entry.get("pid"))
    messages = Path(str(log) + ".messages")
    initializes, sessions, calls, claude_initializes, acp_calls = {}, [], [], {}, []
    for line in (messages.read_text().splitlines() if messages.exists() else []):
        entry = json.loads(line)
        try:
            message = json.loads(entry["line"])
        except ValueError:
            continue
        method = message.get("method")
        acp = message.get("jsonrpc") == "2.0"
        if method == "initialize" and not acp:
            initializes[entry["pid"]] = message.get("params")
        # Claude Code's initialize: Rust sends only the system prompt append of the reference's.
        request = message.get("request") or {}
        if message.get("type") == "control_request" and request.get("subtype") == "initialize":
            claude_initializes[entry["pid"]] = without_unserved(request.get("appendSystemPrompt", ""), unserved)
        if method in ("thread/start", "thread/resume"):
            sessions.append(entry["pid"])
        if acp and method in ACP_LAUNCH_METHODS:
            acp_calls.append((entry["pid"], method,
                              f"acp {method} {json.dumps(mask(acp_params(method, message.get('params'), unserved)), sort_keys=True)}"))
        if method in ("thread/start", "thread/resume", "turn/start"):
            params = dict(message.get("params") or {})
            params.pop("input", None)
            if "additionalContext" in params:
                params["additionalContext"] = agent_context(params["additionalContext"], unserved)
            calls.append(f"{method} {json.dumps(mask(params), sort_keys=True)}")
    # An ACP agent's requests, from the processes that ran a turn (a status check's probes run
    # none, and Rust does not probe ACP Registry agents yet).
    prompted = {pid for pid, method, _ in acp_calls if method == "session/prompt"}
    calls.extend(line for pid, _, line in acp_calls if pid in prompted)
    session = sessions[-1] if sessions else None
    # Claude Code has no thread/start: its turn is the last CLI launched.
    turn_pid = session if session in launches else (order[-1] if order else None)
    out = [launches[turn_pid]] if order else []
    if turn_pid in claude_initializes:
        out.append(f"claude initialize appendSystemPrompt {json.dumps(mask(claude_initializes[turn_pid]))}")
    if session in initializes:
        out.append(f"initialize {json.dumps(mask(initializes[session]), sort_keys=True)}")
    out.extend(calls)
    # The other app-servers' initialize, once each and in no particular order, as how many run
    # and when is up to each server. The reference asks for the MCP app extension and the
    # turn/diff/updated opt-out only for a turn's session (CodexAdapterV2), not for the status
    # probe (buildCodexInitializeParams); neither server acts on turn/diff/updated.
    others = sorted({json.dumps(mask(params), sort_keys=True)
                     for pid, params in initializes.items() if pid not in sessions})
    out.extend(f"other initialize {params}" for params in others)
    return out


# The ACP requests a server sends a turn's agent, compared with their parameters.
ACP_LAUNCH_METHODS = ("initialize", "authenticate", "session/new", "session/load", "session/set_model",
                      "session/set_mode", "session/set_config_option", "session/prompt", "session/cancel",
                      "session/close")


def acp_params(method, params, unserved):
    """An ACP request's parameters as both servers can match them: the T3 MCP server each runs from
    its own executable (`<self>`), and the prompt's text as Rust sends it, without what names a tool
    Rust does not serve yet."""
    params = json.loads(json.dumps(params or {}))
    for server in params.get("mcpServers") or []:
        if server.get("name") == "t3-code":
            server["command"] = "<self>"
            server["args"] = [arg for arg in server.get("args", []) if arg.startswith("acp-mcp")]
    if method == "session/prompt":
        for block in params.get("prompt") or []:
            if block.get("type") == "text":
                block["text"] = without_unserved(block["text"], unserved)
    return params


def turn_launches(log):
    """Each turn launch of a fake CLI, in order: a new session, or one it resumes. Claude Code
    resumes with --resume; Codex with thread/resume."""
    out = []
    launch = Path(str(log) + ".launch")
    for line in (launch.read_text().splitlines() if launch.exists() else []):
        argv = json.loads(line)["argv"]
        if "--no-session-persistence" in argv or argv[:1] == ["app-server"]:
            continue
        out.append("resume" if any(arg.startswith("--resume") for arg in argv) else "new")
    for line in (Path(log).read_text().splitlines() if Path(log).exists() else []):
        with contextlib.suppress(ValueError):
            method = json.loads(line).get("method")
            if method in ("thread/start", "thread/resume"):
                out.append("resume" if method == "thread/resume" else "new")
    return out


def title_state(projection):
    """The thread's title and its last title request, without times or generated ids."""
    thread = projection.get("thread") or {}
    last = thread.get("lastTitleRegeneration") or {}
    return {"title": thread.get("title"), "titleRenamed": thread.get("titleRenamed"),
            "pending": thread.get("titleRegeneration") is not None,
            "last": {k: v for k, v in last.items() if k != "completedAt"} or None}


def title_runs(log, root):
    """Each text generation run of the fake CLIs: its arguments, prompt, output schema and
    environment, with temporary paths masked."""
    def mask(value):
        if isinstance(value, str):
            for server in ("original", "rust"):
                value = value.replace(f"{root}/{server}", "<home>/<server>")
            value = re.sub(r"/[^\s\"]*/t3code-(codex-schema|codex-output|claude-title)-[^\s\"]*",
                           lambda m: f"<tmp>/{m.group(1)}", value)
            return value.replace(str(root), "<home>")
        if isinstance(value, list):
            return [mask(item) for item in value]
        if isinstance(value, dict):
            return {key: mask(item) for key, item in value.items()}
        return value
    path = Path(str(log) + ".title")
    return [mask(json.loads(line)) for line in (path.read_text().splitlines() if path.exists() else [])]


def title_outcome(url, cookie, scenario, log, root):
    """The thread's title once its first title request settled, and with "regenerate", once a
    regenerate request settled too; then the text generation runs that produced them."""
    settled = lambda p: not (p.get("thread") or {}).get("titleRegeneration")
    out = {"initial": title_state(wait_for(url, cookie, settled, 60))}
    if scenario["title"] == "regenerate":
        rpc(url, cookie, "orchestration.dispatchCommand", {
            "type": "thread.metadata.update", "commandId": "regenerate-title", "threadId": THREAD,
            "regenerateTitle": True})
        out["regenerated"] = title_state(wait_for(url, cookie, lambda p: settled(p) and (
            (p.get("thread") or {}).get("lastTitleRegeneration") or {}).get("requestId") == "regenerate-title", 60))
    out["runs"] = title_runs(log, root)
    if scenario.get("textProvider"):
        out["textRuns"] = text_provider_runs(log, root, scenario["textProvider"])
    return out


def text_provider_runs(log, root, provider):
    """What the text generation provider's fake CLI was launched with and sent for a title: its
    distinct launches whose arguments carry `provider["launch"]`, and the records of
    `provider["records"]` types (or HTTP requests), with temporary paths and request ids masked."""
    def mask(value):
        if isinstance(value, str):
            for server in ("original", "rust"):
                value = value.replace(f"{root}/{server}", "<home>/<server>")
                # OpenCode's directory header carries the path URL-encoded.
                value = value.replace(urllib.parse.quote(f"{root}/{server}", safe=""), "<home>/<server>")
            # Temporary directories with random suffixes (Antigravity's session and runtime).
            value = re.sub(r"(t3-antigravity-text-|/run-)[A-Za-z0-9]{6}", r"\1<random>", value)
            # The reference and Rust servers may resolve Node through different safe PATH
            # entries. The executable location is irrelevant to the provider protocol.
            value = re.sub(r"/(?:[^\"']+/)*node(?=[\"'])", "<node>", value)
            return value.replace(str(root), "<home>").replace(urllib.parse.quote(str(root), safe=""), "<home>")
        if isinstance(value, list):
            return [mask(item) for item in value]
        if isinstance(value, dict):
            # The reference's RPC client adds its trace to each request.
            return {key: ("<id>" if key == "id" else mask(item)) for key, item in value.items()
                    if key not in ("sampled", "spanId", "traceId")}
        return value
    def lines(suffix):
        path = Path(str(log) + ".text" + suffix)
        return path.read_text().splitlines() if path.exists() else []
    # The status probe launches the same way, as often as its timing makes it: each distinct launch.
    launches = sorted({mask(line) for line in lines(".args") if provider["launch"] in line})
    records = []
    for line in lines(""):
        try:
            record = json.loads(line)
        except ValueError:
            continue
        kind = record.get("type") or record.get("method")
        # The status probe's own requests, as many as its timing makes.
        if provider.get("skip") and provider["skip"] in line:
            continue
        if kind in provider.get("records", []):
            records.append(mask(record))
    # The launch environment the spec asked to see ("logEnv"), for each distinct launch.
    environments = sorted({json.dumps(mask(json.loads(line)), sort_keys=True) for line in lines(".launch")
                           if provider["launch"] in " ".join(json.loads(line)["args"])})
    # A session's requests (JSON); the status probe's GETs interleave as their timing makes them.
    return {"launches": launches, "records": records,
            "http": [mask(line) for line in lines(".http") if line.startswith("{")],
            **({"environments": environments} if environments else {})}


def follow_up_turn(args, url, cookie, text):
    """A second message once the first run settled; the thread as read once it settled too."""
    runs = len(thread_projection(url, cookie).get("runs", []))
    rpc(url, cookie, "orchestration.dispatchCommand", {
        "type": "message.dispatch", "commandId": "follow-up-message", "threadId": THREAD,
        "messageId": "message:follow-up", "text": text, "attachments": [],
        "createdBy": "user", "creationSource": "web", "dispatchMode": {"type": "start_immediately"}})
    wait_for(url, cookie, lambda p: len(p.get("runs", [])) > runs
             and not any(r.get("status") in ACTIVE_RUN for r in p.get("runs", [])), 40)
    after = run_client(args, url, cookie, {"threadId": THREAD, "inspect": True})
    return {step: value for step, value in after["steps"].items() if "live" not in step}


def turn_app_servers(log):
    """How many Codex app-servers a server started for turns (its status probe's aside): the
    reference shares one per instance across turns and threads."""
    launch = Path(str(log) + ".launch")
    return sum(1 for line in (launch.read_text().splitlines() if launch.exists() else [])
               if json.loads(line)["argv"][:3] == ["app-server", "-c", "thread_unload_delay_secs=0"])


def thread_projection(url, cookie):
    return rpc(url, cookie, "orchestration.getThreadProjection", {"threadId": THREAD})


ACTIVE_RUN = {"preparing", "starting", "running", "waiting"}


def wait_for(url, cookie, done, seconds):
    deadline = time.monotonic() + seconds
    while True:
        projection = thread_projection(url, cookie)
        if done(projection) or time.monotonic() > deadline:
            return projection
        time.sleep(0.5)


def reconcile_events(live):
    """The events a watching client got from the stop's reconciliation: from the cut-off run's
    update on, with each turn item's type and status."""
    out, started = [], False
    for item in (live or {}).get("items", []):
        event = item.get("event") or {}
        payload = event.get("payload") or {}
        if event.get("type") == "run.updated" and payload.get("status") == "cancelled":
            started = True
        if not started:
            continue
        if item["kind"] != "event":
            out.append(item["kind"])
            continue
        label = event.get("type")
        if label == "turn-item.updated":
            label += f" {payload.get('type')} {payload.get('status')}"
        elif label in ("run.updated", "run-attempt.updated", "node.updated", "provider-turn.updated",
                       "provider-thread.updated", "provider-session.updated"):
            label += f" {payload.get('status')}"
        out.append(label)
    return out


def continuation_delay(projection, listening):
    """Seconds from the restarted server listening to its first restart continuation run."""
    import datetime
    for run in projection.get("runs", []):
        if run.get("restartContinuationOfRunId") is not None:
            requested = datetime.datetime.fromisoformat(run["requestedAt"].replace("Z", "+00:00"))
            return round(requested.timestamp() - listening, 2)
    return None


def startup_recorded(root, url):
    """Whether the server listening at `url` has written its `server.startup` span: the
    reference ends it, logging "V2 orchestration recovery completed", once its recovery has
    queued every continuation, and Rust once it has sent them."""
    port = int(url.rsplit(":", 1)[1])
    trace = root / "userdata/logs/server.trace.ndjson"
    with contextlib.suppress(FileNotFoundError), trace.open() as lines:
        for line in lines:
            if '"server.startup"' not in line:
                continue
            with contextlib.suppress(ValueError):
                record = json.loads(line)
                if (record.get("name") == "server.startup"
                        and record.get("attributes", {}).get("server.port") == port
                        and any("V2 orchestration recovery completed" in event.get("name", "")
                                for event in record.get("events", []))):
                    return True
    return False


def effects_due(root, clock_offset=0):
    """The thread's effects in the reference's outbox that are running or due within a few
    seconds (a retry due later is not), by type. Rust has no outbox, so none."""
    database = root / "userdata/statev2.sqlite"
    if not database.exists():
        return []
    import datetime
    soon = (datetime.datetime.now(datetime.timezone.utc)
            + datetime.timedelta(seconds=clock_offset + 3)).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    try:
        with contextlib.closing(sqlite3.connect(f"file:{database}?mode=ro", uri=True, timeout=5)) as db:
            return [row[0] for row in db.execute(
                "SELECT effect_type FROM orchestration_v2_effect_outbox WHERE thread_id = ? AND"
                " (status = 'running' OR (status = 'pending' AND available_at <= ?))",
                (THREAD, soon))]
    except sqlite3.OperationalError:
        # Not created yet, or busy: not settled.
        return ["unknown"]


def quiet(root, url, cookie, seconds=30, clock_offset=0, effects=True):
    """Wait until the thread stops changing: the same projection twice in a row, 0.5 s apart,
    and (unless `effects` is false, for a server stopped with work in flight) nothing due in the
    reference's outbox."""
    previous = None
    def done(projection):
        nonlocal previous
        settled = projection == previous and not (effects and effects_due(root, clock_offset))
        previous = projection
        return settled
    return wait_for(url, cookie, done, seconds)


class Clock:
    """A shifted clock for a server under libfaketime, which reads the offset from a file."""

    def __init__(self, library, directory):
        self.file = directory / "faketime-offset"
        self.offset = 0
        self.file.write_text("+0\n")
        self.env = {"LD_PRELOAD": str(library.resolve(strict=True)), "FAKETIME_TIMESTAMP_FILE": str(self.file),
                    "FAKETIME_CACHE_DURATION": "1"}

    def advance(self, seconds):
        self.offset += seconds
        self.file.write_text(f"+{self.offset}\n")
        # Each process rereads the file at most a second later.
        time.sleep(1.2)


def restart_turn(args, command, root, credentials, env, checkouts, fake_bin, scenario, second=None,
                 follow_up=None):
    """A turn on a server that is stopped and started again: what the client sees afterwards.

    With `second`, that server is started on the home instead, and with `follow_up` the
    restarted server is then sent that message as a new turn (shared_store_parity.py)."""
    restart = scenario["restart"]
    watched = {}
    with contextlib.ExitStack() as first:
        url, cookie = managed_server(first, command, root, *credentials, env=env)
        if scenario.get("awaitStatus"):
            # A delegated task's target must be a checked, installed provider.
            provider_status(url, cookie, scenario["instance"], root)
        seeded = seed_turn(url, cookie, checkouts, scenario)
        if restart["after"] == "created":
            # Only the thread: its first turn runs on the restarted server (follow_up).
            pass
        elif restart.get("live"):
            # A client watches the thread, sends the message, and sees what the stop does.
            watcher = threading.Thread(target=lambda: watched.update(
                run_client(args, url, cookie, {**seeded, "turnMs": 60000, "liveOnly": True})))
            watcher.start()
            wait_for(url, cookie, lambda p: bool(p.get("runs")), 30)
        else:
            rpc(url, cookie, "orchestration.dispatchCommand", seeded["dispatch"])
        for index, text in enumerate(restart.get("queue", [])):
            # Another message, queued behind the first once that one's run is there.
            wait_for(url, cookie, lambda p: len(p.get("runs", [])) > index, 30)
            rpc(url, cookie, "orchestration.dispatchCommand", {
                **seeded["dispatch"], "commandId": f"queued-message-{index}",
                "messageId": f"message:queued-{index}", "text": text,
                "dispatchMode": {"type": "queue_after_active"}})
        queued = len(restart.get("queue", []))
        if restart["after"] != "created":
            wait_for(url, cookie, lambda p: len(p.get("runs", [])) > queued, 30)
        if restart["after"] == "created":
            pass
        elif restart["after"] == "started":
            # The run started, and its provider got the prompt but has not taken it.
            log = Path(env["FAKE_PROVIDER_LOG"])
            wait_for(url, cookie, lambda p: any(r.get("status") == "running" for r in p.get("runs", []))
                     and log.exists() and '"turn/start"' in log.read_text(), 30)
        elif restart["after"] == "running":
            # The turn has run a command and holds on.
            wait_for(url, cookie, lambda p: any(i.get("type") == "command_execution"
                                                for i in p.get("turnItems", [])), 30)
        elif restart["after"] == "waiting":
            # The turn asked for an approval or an answer, which nobody gave.
            wait_for(url, cookie, lambda p: any(r.get("status") == "pending"
                                                for r in p.get("runtimeRequests", [])), 30)
        elif restart["after"] == "titling":
            # The turn settled and its title is being generated (a slow first title run).
            title_log = Path(env["FAKE_PROVIDER_LOG"] + ".title")
            wait_for(url, cookie, lambda p: not any(r.get("status") in ACTIVE_RUN for r in p["runs"])
                     and title_log.exists(), 30)
        elif restart["after"] == "settled":
            wait_for(url, cookie, lambda p: not any(r.get("status") in ACTIVE_RUN for r in p["runs"]), 30)
        # Whatever the server is still recording about it lands before the stop. A turn the
        # restart cuts off still has its effects running.
        quiet(root, url, cookie, effects=False)
        if restart.get("settings"):
            rpc(url, cookie, "server.updateSettings", {"patch": restart["settings"]})
    # Stopped as a service manager stops it.
    if restart.get("live"):
        watcher.join(90)
    if restart.get("removeCli"):
        for cli in fake_bin.iterdir():
            cli.unlink()
    clock = None
    if restart.get("restoreCli") and args.faketime_lib and not args.real_time:
        clock = Clock(args.faketime_lib, fake_bin.parent)
        env = {**env, **clock.env}
    with contextlib.ExitStack() as restarted:
        url, cookie = managed_server(restarted, second or command, root, *credentials, env=env)
        listening = time.time()
        offset = lambda: clock.offset if clock else 0
        if restart.get("restoreCli"):
            # The continuation fails without its CLI; put the CLI back for its retry, a minute
            # later.
            failed = wait_for(url, cookie, lambda p: any(
                r.get("restartContinuationOfRunId") and r.get("status") == "failed"
                for r in p.get("runs", [])), 30)
            write_fake_clis(fake_bin, scenario_drivers(scenario))
            source = next((r["id"] for r in failed.get("runs", [])
                           if r.get("restartContinuationOfRunId") and r.get("status") == "failed"), None)
            if clock:
                # Past the minute, but short of the second retry's three.
                clock.advance(62)
            wait_for(url, cookie, lambda p: any(
                r.get("restartContinuationOfRunId") == source and r.get("status") not in ACTIVE_RUN
                for r in p.get("runs", [])), 30 if clock else 100)
        # The server says its recovery has finished; let what it continued settle.
        started = time.monotonic()
        floor = 8 if args.real_time else 0
        # With `settleRuns`, also until that many runs have ended: work the restart continues
        # (a delegated child's result, say) can start the thread's next run a moment later.
        settled_runs = scenario.get("settleRuns", 0)
        wait_for(url, cookie, lambda p: time.monotonic() - started >= floor and startup_recorded(root, url)
                 and not any(r.get("status") in ACTIVE_RUN for r in p.get("runs", []))
                 and sum(r.get("status") not in ACTIVE_RUN for r in p.get("runs", [])) >= settled_runs
                 and not effects_due(root, offset()), 40)
        if follow_up is not None:
            if scenario.get("awaitStatus"):
                # The restarted server's provider check settles first: its snapshot (the
                # models it lists, say) is what the turn's agent reads.
                provider_status(url, cookie, scenario["instance"], root)
            runs = len(thread_projection(url, cookie).get("runs", []))
            rpc(url, cookie, "orchestration.dispatchCommand", {
                "type": "message.dispatch", "commandId": "follow-up-message", "threadId": THREAD,
                "messageId": "message:follow-up", "text": follow_up, "attachments": [],
                "createdBy": "user", "creationSource": "web", "dispatchMode": {"type": "start_immediately"}})
            wait_for(url, cookie, lambda p: len(p.get("runs", [])) > runs
                     and not any(r.get("status") in ACTIVE_RUN for r in p.get("runs", [])), 40)
        quiet(root, url, cookie, clock_offset=offset())
        if scenario.get("title"):
            title = title_outcome(url, cookie, scenario, Path(env["FAKE_PROVIDER_LOG"]), root.parent)
        if restart.get("liveTurn"):
            # The scenario's turn (with its steer and stop) runs on the restarted server, watched
            # live as on one server.
            outcome = run_client(args, url, cookie, {**seeded, "turnMs": 60000})
        else:
            outcome = run_client(args, url, cookie, {"threadId": THREAD, "inspect": True})
        outcome["continuationDelay"] = continuation_delay(thread_projection(url, cookie), listening)
        if scenario.get("title"):
            outcome["title"] = title
        if restart.get("live"):
            outcome["reconcileLive"] = reconcile_events(
                watched.get("steps", {}).get("orchestration.subscribeThread (live turn)", {}).get("value"))
        return outcome


PROVIDER_STATUS_TIMES = {"checkedAt", "updatedAt", "lastCheckedAt"}
# What the reference reads from the network with update checks on (these scenarios keep the
# default settings): the CLI's latest release and what follows from it. This server does not
# look the latest release up (provider_status_parity.py compares the advisory with checks off).
PROVIDER_STATUS_NETWORK = {"versionAdvisory": {"latestVersion", "status", "message", "updateCommand", "canUpdate"},
                           "compatibilityAdvisory": {"latestVersionStatus"}}


def provider_status(url, cookie, instance, root):
    """The instance's provider status from server.getConfig once its check has reported and the
    row has settled (for up to 30 seconds), without check times and network-derived fields, and
    with the temporary home masked."""
    def strip(value):
        if isinstance(value, dict):
            return {key: strip(item) for key, item in value.items() if key not in PROVIDER_STATUS_TIMES}
        if isinstance(value, list):
            return [strip(item) for item in value]
        if isinstance(value, str):
            for server in ("original", "rust"):
                value = value.replace(f"{root}/{server}", "<home>/<server>")
            return value.replace(str(root), "<home>")
        return value
    def offline(entry):
        entry = strip(entry)
        for key, fields in PROVIDER_STATUS_NETWORK.items():
            if isinstance(entry, dict) and isinstance(entry.get(key), dict):
                entry[key] = {k: v for k, v in entry[key].items() if k not in fields}
        return entry
    deadline = time.monotonic() + 30
    last, since = None, None
    while True:
        providers = rpc(url, cookie, "server.getConfig", {}).get("providers", [])
        entry = next((p for p in providers if p.get("instanceId") == instance), None)
        if entry is not None and "has not been checked" not in str(entry.get("message")):
            if offline(entry) != last:
                last, since = offline(entry), time.monotonic()
            elif time.monotonic() - since >= 1.5:
                return last
        if time.monotonic() > deadline:
            return offline(entry)
        time.sleep(0.5)


def compare_turn(original, rust):
    """Problems with Rust's provider turn as the reference client sees it, by check name."""
    problems = {}
    status = [original.get("status"), rust.get("status")]
    if status[0] != status[1]:
        problems["provider status"] = [f"original {json.dumps(status[0], sort_keys=True)}",
                                       f"rust     {json.dumps(status[1], sort_keys=True)}"]
    launch = [original.get("launch", []), rust.get("launch", [])]
    if launch[0] != launch[1]:
        problems["cli launch"] = [f"original {line}" for line in launch[0]] + [f"rust     {line}" for line in launch[1]]
    inputs = [original.get("inputs", []), rust.get("inputs", [])]
    if inputs[0] != inputs[1]:
        problems["turn input"] = [f"original {line}" for line in inputs[0]] + [f"rust     {line}" for line in inputs[1]]
    mcp = comparable_mcp(original.get("mcp", []), rust.get("mcp", []))
    if mcp[0] != mcp[1]:
        problems["agent mcp"] = mcp_diff_lines(mcp[0], mcp[1])
    cli = [original.get("cli", []), rust.get("cli", [])]
    if cli[0] != cli[1]:
        problems["cli approvals"] = [f"original {line}" for line in cli[0]] + [f"rust     {line}" for line in cli[1]]
    # The reference sends a continuation as it starts listening; so must Rust (whose times are
    # whole seconds).
    delays = [original.get("continuationDelay"), rust.get("continuationDelay")]
    if (delays[0] is None) != (delays[1] is None) or any(
            delay is not None and not -1.5 <= delay <= 3 for delay in delays):
        problems["continuation timing"] = [f"seconds after listening: original {delays[0]} rust {delays[1]}"]
    watched = [original.get("reconcileLive"), rust.get("reconcileLive")]
    if watched[0] != watched[1]:
        problems["live reconcile"] = [f"original {watched[0]}", f"rust     {watched[1]}"]
    titles = [original.get("title"), rust.get("title")]
    if titles[0] != titles[1]:
        for key in sorted(set((titles[0] or {}).keys()) | set((titles[1] or {}).keys())):
            a, b = (titles[0] or {}).get(key), (titles[1] or {}).get(key)
            if a != b:
                problems[f"title {key}"] = [f"original {json.dumps(a, sort_keys=True)[:4000]}",
                                            f"rust     {json.dumps(b, sort_keys=True)[:4000]}"]
    servers = [original.get("appServers"), rust.get("appServers")]
    if servers[0] != servers[1]:
        problems["turn app-servers"] = [f"original {servers[0]}", f"rust     {servers[1]}"]
    peer = [original.get("peer"), rust.get("peer")]
    if peer[0] != peer[1]:
        problems["peer calls"] = [f"original {json.dumps(peer[0], sort_keys=True)[:4000]}",
                                  f"rust     {json.dumps(peer[1], sort_keys=True)[:4000]}"]
    launches = [original.get("launches", []), rust.get("launches", [])]
    if launches[0] != launches[1]:
        problems["cli launches"] = [f"original {launches[0]}", f"rust     {launches[1]}"]
    failed_steps = False
    live_step = "orchestration.subscribeThread (live turn)"
    inspected = live_step not in original["steps"] and live_step not in rust["steps"]
    for step in ((() if inspected else (live_step,))
                 + ("orchestration.subscribeThread", "orchestration.getThreadProjection")):
        a, b = original["steps"].get(step), rust["steps"].get(step)
        if a is None or b is None or a["outcome"] != b["outcome"]:
            failed_steps = True
            problems[step] = [f"outcome {a and a['outcome']} != {b and b['outcome']}: {(b or {}).get('message', '')[:600]}"]
    if failed_steps:
        return problems
    # A restart scenario reads the thread afterwards; nothing streamed live.
    # A turn neither server ended in time (a change that lets it run on) has no live value.
    live = None if inspected else [original["steps"][live_step].get("value"), rust["steps"][live_step].get("value")]
    if live is None or live == [None, None]:
        pass
    elif live[0] is None or live[1] is None:
        problems["live"] = [f"run settled: original {live[0] is not None} rust {live[1] is not None}"]
    else:
        for reply in ("respondReply", "interruptReply", "changeReply", "steerReply"):
            outcomes = [None if side.get(reply) is None else sorted(side[reply]) for side in live]
            if outcomes[0] != outcomes[1]:
                problems[reply] = [f"original {live[0].get(reply)}", f"rust     {live[1].get(reply)}"]
        summaries = [live_summary(side) for side in live]
        if summaries[0] != summaries[1]:
            problems["live"] = [f"original {summaries[0]}", f"rust     {summaries[1]}"]
        # The kinds of item a client applies, whatever their order and grouping.
        kinds = [sorted(set(summary)) for summary in summaries]
        if kinds[0] != kinds[1]:
            problems["live kinds"] = [f"original only {sorted(set(kinds[0]) - set(kinds[1]))}",
                                      f"rust only     {sorted(set(kinds[1]) - set(kinds[0]))}"]
    shapes = compare_thread(original["steps"]["orchestration.subscribeThread"]["value"],
                            rust["steps"]["orchestration.subscribeThread"]["value"])
    if shapes:
        problems["shape"] = shapes
    fetched = [side["steps"].get("orchestration.getTurnItem (omitted output)", {}).get("value")
               for side in (original, rust)]
    if fetched[0] != fetched[1]:
        problems["omitted output"] = [f"original {fetched[0]}", f"rust     {fetched[1]}"]
    rendered = [rendered_turn(side["steps"]["orchestration.getThreadProjection"]["value"])
                for side in (original, rust)]
    for key in rendered[0]:
        if rendered[0][key] != rendered[1][key]:
            problems[key] = [f"original {rendered[0][key]}", f"rust     {rendered[1][key]}"]
    return problems


def mcp_record_tool(line):
    """Return the tool or protocol method named by one normalized MCP record."""
    try:
        request = json.loads(line).get("request", {})
    except (TypeError, ValueError):
        return "<non-json>"
    return request.get("tool") or request.get("method") or "<unknown>"


def mcp_line(value, limit=1900):
    """Compact one MCP record for the CI log while keeping the prefix readable."""
    if value is None:
        text = "<missing>"
    else:
        try:
            text = json.dumps(json.loads(value), sort_keys=True, separators=(",", ":"))
        except (TypeError, ValueError):
            text = str(value)
    return text if len(text) <= limit else text[:limit - 3] + "..."


def mcp_diff_lines(original, rust, truncate=True):
    """Pair only differing normalized MCP records, including length mismatches."""
    lines = []
    if len(original) != len(rust):
        lines.append(f"answer-list counts: original {len(original)} rust {len(rust)}")
    for index in range(max(len(original), len(rust))):
        a = original[index] if index < len(original) else None
        b = rust[index] if index < len(rust) else None
        if a == b:
            continue
        tool = mcp_record_tool(a if a is not None else b)
        compact = mcp_line if truncate else lambda value: mcp_line(value, limit=10**9)
        lines.append(f"[{index}] {tool} original {compact(a)}")
        lines.append(f"[{index}] {tool} rust     {compact(b)}")
    return lines


def print_mcp_diagnostics(outcomes, fake_hosts, stderr_paths):
    """Print compact MCP differences and save complete diagnostics for CI artifacts."""
    normalized = comparable_mcp(outcomes["original"].get("mcp", []), outcomes["rust"].get("mcp", []))
    differences = mcp_diff_lines(*normalized)
    print("agent mcp differing records:")
    for line in differences:
        print(f"  {line}")
    artifact = Path(os.environ.get("PARITY_DIAGNOSTICS_PATH", "agent-mcp-diff.txt"))
    artifact.parent.mkdir(parents=True, exist_ok=True)
    full = ["agent mcp normalized diff", *mcp_diff_lines(*normalized, truncate=False)]
    for name in ("original", "rust"):
        hosts = fake_hosts.get(name, {})
        for provider in ("github", "linear"):
            requests = hosts.get(provider) or []
            full.append(f"{name} fake {provider} request log ({len(requests)} requests):")
            full.extend(f"  {request}" for request in requests)
        path = stderr_paths.get(name)
        tail = "<missing>"
        if path is not None and Path(path).exists():
            tail = Path(path).read_text(errors="replace")[-4000:] or "<empty>"
        full.extend([f"{name} server stderr tail:", tail])
    artifact.write_text("\n".join(full) + "\n")
    print(f"agent mcp diagnostics: {artifact}")


# Rough seconds the seeded thread checks take on shard 1, for balancing shards.
THREAD_CHECKS_COST = 20


def scenario_cost(scenario, real_time=False):
    """Rough seconds a scenario takes on both servers in CI, for balancing shards."""
    restart = scenario.get("restart")
    if not restart:
        return 9
    if restart.get("restoreCli"):
        # The reference retries the continuation after a hard-coded 60 s, unless the clock is
        # moved on.
        return 150 if real_time else 15
    # A live subscription waits for the watcher to see the stop; real time adds the 8 s floor on
    # each server.
    return (25 if restart.get("live") else 11) + (16 if real_time else 0)


def shard_scenarios(index, count, real_time=False):
    """The provider scenarios of shard `index` (from 1) of `count`, balanced by scenario_cost.

    Shard 1 also runs the seeded thread checks, so it starts with their cost."""
    loads = [[THREAD_CHECKS_COST if n == 0 else 0, n, []] for n in range(count)]
    cost = lambda scenario: scenario_cost(scenario, real_time)
    for scenario in sorted(PROVIDER_SCENARIOS, key=cost, reverse=True):
        lightest = min(loads, key=lambda load: (load[0], load[1]))
        lightest[0] += cost(scenario)
        lightest[2].append(scenario["name"])
    return set(loads[index - 1][2])


def scenario_env(temporary, name, root, scenario, drivers):
    """The environment a scenario's server runs in, with its fake CLIs: (env, fake bin, log)."""
    if scenario.get("registryAgent"):
        seed_registry_agent(root)
    hidden = path_without(PROVIDER_CLIS | RELAY_TOOLS, temporary / "bin")
    login_shell = {}
    if scenario.get("install") == "login-shell":
        # The CLI is in the server's ~/.local/bin, which only its login shell's
        # profile puts on PATH, as for a server a service manager starts.
        fake_bin = root / ".local/bin"
        write_fake_clis(fake_bin, drivers)
        fake_bin.mkdir(parents=True, exist_ok=True)
        (root / ".profile").write_text('PATH="$HOME/.local/bin:$PATH"\nexport PATH\n')
        path = hidden
        login_shell = {"FAKE_PROVIDER_LAUNCH_PATH": "1"}
    else:
        # Each server has its own fake CLIs, which a restart scenario may remove.
        fake_bin = temporary / f"{name}-fake-bin"
        write_fake_clis(fake_bin, drivers)
        if scenario.get("textProvider"):
            write_text_provider(fake_bin, scenario["textProvider"])
        path = os.pathsep.join([str(fake_bin), hidden])
    log = temporary / f"{name}-provider.log"
    env = {**GIT_ISOLATION, **HOST_FAKES, "PATH": path, "FAKE_PROVIDER_CLI": str(FIXTURES / "fake_cli.py"),
           "FAKE_PROVIDER_LOG": str(log), **login_shell,
           "FAKE_STATUS_CLI": str(FIXTURES / "status_cli.py"), "FAKE_GROK_STATUS": str(fake_bin / "grok.status.json"),
           # "titleAfterTurn": the title CLI answers once the driver saw the turn settle.
           **({"FAKE_TITLE_GATE": str(log) + ".turn-settled"} if scenario.get("titleAfterTurn") else {}),
           "FAKE_PROVIDER_SCENARIO": str(FIXTURES / (scenario["scenario"] or "codex_turn.json"))}
    return env, fake_bin, log


def scenario_drivers(scenario):
    return set() if scenario["scenario"] is None else {TURN_CLIS[scenario["instance"]]}


def provider_turns(args, credentials):
    """Run each provider scenario on both servers; the number of unexpected results."""
    xfail = json.loads((ROOT / "scripts/client_thread_parity_xfail.json").read_text())
    failures = 0
    shard = shard_scenarios(*args.shard, args.real_time) if args.shard else None
    for scenario in PROVIDER_SCENARIOS:
        if args.scenario and scenario["name"] not in args.scenario:
            continue
        if shard is not None and scenario["name"] not in shard:
            continue
        started = time.monotonic()
        outcomes = {}
        # T3_PARITY_KEEP=1 keeps each scenario's server homes (logs, fake CLI logs) to debug it.
        keep = bool(os.environ.get("T3_PARITY_KEEP"))
        with tempfile.TemporaryDirectory(delete=not keep) as temporary, contextlib.ExitStack() as stack:
            temporary = Path(temporary)
            if keep:
                print(f"keeping {scenario['name']} in {temporary}", flush=True)
            drivers = scenario_drivers(scenario)
            fake_hosts, stderr_paths = {}, {}
            for name, command in (("original", shlex.split(args.original_command)),
                                  ("rust", shlex.split(args.rust_command))):
                root = temporary / name
                env, fake_bin, log = scenario_env(temporary, name, root, scenario, drivers)
                if scenario["name"] == "codex thread tools":
                    from github_provider_parity import github_fixture
                    from linear_provider_parity import linear_fixture
                    github, github_env = github_fixture(stack, temporary / f"{name}-gh")
                    linear = linear_fixture(stack)
                    fake_hosts[name] = {"github": github.requests, "linear": linear.requests}
                    env.update(github_env, T3CODE_LINEAR_API_URL=f"http://127.0.0.1:{linear.server_port}/graphql",
                               GIT_ALLOW_PROTOCOL="file")
                stderr_path = temporary / f"{name}-server.stderr"
                stderr_paths[name] = stderr_path
                if "restart" in scenario:
                    outcomes[name] = restart_turn(args, command, root, credentials, env,
                                                  temporary / f"{name}-checkouts", fake_bin, scenario,
                                                  follow_up=scenario["restart"].get("followUp"))
                    outcomes[name]["launches"] = turn_launches(log)
                else:
                    url, cookie = managed_server(stack, command, root, *credentials, env=env,
                                                 stderr_path=stderr_path)
                    if scenario.get("awaitStatus"):
                        # A delegated task's target must be a checked, installed provider.
                        provider_status(url, cookie, scenario["instance"], temporary)
                    peer = None
                    if "peer" in scenario:
                        # A linked environment the turn delegates to, linked with full access.
                        peer = RemoteTaskPeer(**scenario["peer"])
                        stack.callback(peer.close)
                        rpc(url, cookie, "server.updateSettings", {"patch": {"linkedEnvironmentsEnabled": True}})
                        rpc(url, cookie, "peerLinks.link", {"url": peer.url, "pairingCode": "fixture-code",
                                                            "access": "full-access"})
                    seeded = seed_turn(url, cookie, temporary / f"{name}-checkouts", scenario)
                    if scenario.get("titleAfterTurn"):
                        seeded["settledFile"] = str(log) + ".turn-settled"
                    outcomes[name] = run_client(args, url, cookie, seeded)
                    if scenario.get("uploads"):
                        # Claude's attachment budget counts the prompt's bytes, whose attachment
                        # paths hold each server's home: count them as if both homes were "<home>".
                        without_home(outcomes[name], len(str(root)) - len("<home>/<server>"),
                                     len(scenario["uploads"]))
                    if scenario.get("followUp"):
                        outcomes[name]["steps"].update(follow_up_turn(args, url, cookie, scenario["followUp"]))
                    if scenario.get("title"):
                        outcomes[name]["title"] = title_outcome(url, cookie, scenario, log, temporary)
                    if scenario.get("status"):
                        outcomes[name]["status"] = provider_status(url, cookie, scenario["instance"], temporary)
                    if peer is not None:
                        outcomes[name]["peer"] = peer_calls(peer, url)
                outcomes[name]["appServers"] = turn_app_servers(log)
                if args.dump:
                    launch_log = Path(str(log) + ".launch")
                    outcomes[name]["allLaunches"] = launch_log.read_text().splitlines() if launch_log.exists() else []
                outcomes[name]["cli"] = cli_approvals(log)
                outcomes[name]["launch"] = cli_launch(log, temporary)
                if scenario.get("uploads"):
                    outcomes[name]["inputs"] = turn_inputs(log, temporary)
                outcomes[name]["mcp"] = agent_mcp(log, temporary)
        if args.dump:
            for name, outcome in outcomes.items():
                slug = scenario["name"].replace(" ", "-")
                (args.dump / f"turn-{slug}-{name}.json").write_text(json.dumps(outcome, indent=2, sort_keys=True))
        problems = compare_turn(outcomes["original"], outcomes["rust"])
        if "agent mcp" in problems:
            print_mcp_diagnostics(outcomes, fake_hosts, stderr_paths)
        expected = set(xfail.get(scenario["name"], {}))
        for check in sorted(set(problems) | expected):
            label = f"{scenario['name']}: {check}"
            if check in problems and check in expected:
                print(f"XFAIL {label}: {xfail[scenario['name']][check]}")
            elif check in problems:
                failures += 1
                print(f"FAIL {label}")
                for problem in problems[check][:40]:
                    print(f"  {problem}")
            elif xfail[scenario["name"]][check].startswith(RACY):
                # A race either server can win: matching is no news.
                print(f"RACY {label}: matched this time")
            else:
                failures += 1
                print(f"XPASS {label}: remove it from client_thread_parity_xfail.json")
        if not problems:
            print(f"PASS {scenario['name']}")
        print(f"  ({scenario['name']}: {time.monotonic() - started:.0f} s)", flush=True)
    return failures


def thread_checks(args, credentials, temporary):
    """The seeded thread's checks; the number that failed."""
    outcomes, roots = {}, {}
    with contextlib.ExitStack() as stack:
        os.environ["PATH"] = path_without(PROVIDER_CLIS | RELAY_TOOLS, Path(temporary) / "bin")
        # Rust runs its provider command for a turn; one that fails ends the turn as the
        # reference's does without a provider CLI.
        commands = {"original": shlex.split(args.original_command),
                    "rust": shlex.split(args.rust_command) + ["--provider-command", "false"]}
        for name, command in commands.items():
            root = Path(temporary) / name
            url, cookie = managed_server(stack, command, root, *credentials, env={**GIT_ISOLATION, **HOST_FAKES})
            seeded = seed(url, cookie, Path(temporary) / f"{name}-checkouts")
            roots[name] = Path(temporary) / f"{name}-checkouts"
            # The seeded turn ends as it starts; read the thread once it has.
            wait_for(url, cookie, lambda p: bool(p.get("runs")) and not any(
                r.get("status") in ACTIVE_RUN for r in p["runs"]), 30)
            quiet(root, url, cookie)
            outcomes[name] = run_client(args, url, cookie, seeded)
    if args.dump:
        args.dump.mkdir(parents=True, exist_ok=True)
        for name, outcome in outcomes.items():
            (args.dump / f"{name}.json").write_text(json.dumps(outcome, indent=2, sort_keys=True))
    failures = total = 0
    names = sorted(set(outcomes["original"]["steps"]) | set(outcomes["rust"]["steps"]))
    for name in names:
        total += 1
        original = outcomes["original"]["steps"].get(name)
        rust = outcomes["rust"]["steps"].get(name)
        problems = []
        if original is None or rust is None:
            problems.append("missing on one server")
        elif original["outcome"] != rust["outcome"] or original.get("tag") != rust.get("tag"):
            problems.append(f"outcome {original['outcome']} {original.get('tag', '')} != "
                            f"{rust['outcome']} {rust.get('tag', '')}: {rust.get('message', '')[:600]}")
        elif name.startswith("orchestration.subscribeThread") and original["outcome"] == "reply":
            problems = compare_thread(original["value"], rust["value"])
        else:
            left, right = shape(original.get("value")), shape(rust.get("value"))
            for key in sorted(set(left) | set(right)):
                if left.get(key) != right.get(key):
                    problems.append(f"{key}: original {sorted(left.get(key, []))} rust {sorted(right.get(key, []))}")
            if not problems:
                comparable = [stable(original.get("value", original.get("error")), roots["original"]),
                              stable(rust.get("value", rust.get("error")), roots["rust"])]
                if comparable[0] != comparable[1]:
                    problems.append("values differ:\n    original " + json.dumps(comparable[0], sort_keys=True)[:3000]
                                    + "\n    rust     " + json.dumps(comparable[1], sort_keys=True)[:3000])
        if problems:
            failures += 1
            print(f"FAIL {name}")
            for problem in problems[:40]:
                print(f"  {problem}")
        else:
            print(f"PASS {name}: {rust['outcome']} {rust.get('tag', '')}")
    print(f"{total - failures}/{total} client thread checks matched")
    return failures


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--original-command", required=True)
    parser.add_argument("--rust-command", required=True)
    parser.add_argument("--reference-root", required=True,
                        help="checkout of the pinned upstream with dependencies installed")
    parser.add_argument("--node", default="node")
    parser.add_argument("--reply-ms", type=int, default=8000)
    parser.add_argument("--dump", type=Path, help="write each server's decoded results here")
    parser.add_argument("--scenario", action="append", help="run only this provider scenario (repeatable)")
    parser.add_argument("--skip-thread-checks", action="store_true", help="run only the provider scenarios")
    parser.add_argument("--shard", type=lambda text: tuple(int(n) for n in text.split("/")), metavar="I/N",
                        help="run provider scenario shard I of N (from 1); the thread checks run on shard 1 only")
    parser.add_argument("--list-shard", action="store_true", help="print the --shard's scenarios and exit")
    parser.add_argument("--faketime-lib", type=Path,
                        help="libfaketime shared object: move restarted servers' clocks on to a retry due later")
    parser.add_argument("--real-time", action="store_true",
                        help="wait out the reference's continuation retry, and give each restarted server at "
                             "least 8 s to continue anything late")
    args = parser.parse_args()
    if args.shard and not 1 <= args.shard[0] <= args.shard[1]:
        parser.error("--shard must be I/N with 1 <= I <= N")
    if args.list_shard:
        if not args.shard:
            parser.error("--list-shard needs --shard")
        print("\n".join(sorted(shard_scenarios(*args.shard, args.real_time))))
        return 0
    credentials = secrets.token_urlsafe(32), secrets.token_bytes(32)
    if args.dump:
        args.dump.mkdir(parents=True, exist_ok=True)
    failures = 0
    with tempfile.TemporaryDirectory() as temporary:
        GIT_ISOLATION.update(isolated_git_env(Path(temporary) / "git"))
        HOST_FAKES["T3CODE_BITBUCKET_API_BASE_URL"] = fake_bitbucket()
        if not args.skip_thread_checks and (not args.shard or args.shard[0] == 1):
            failures += thread_checks(args, credentials, Path(temporary) / "thread")
        failures += provider_turns(args, credentials)
    return int(failures != 0)


if __name__ == "__main__":
    raise SystemExit(main())
