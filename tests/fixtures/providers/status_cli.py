#!/usr/bin/env python3
"""A stand-in for a provider CLI in provider status tests.

Usage: status_cli.py <spec.json> <arguments...>

The spec lists how the CLI answers each command line:

  {"commands": [
     {"args": ["--version"], "stdout": "fx 0.0.13", "stderr": "", "exit": 0},
     {"args": ["status", "--json"], "sleep": 60},
     {"prefix": ["acp"], "rpc": {"initialize": {...}, "_kiro/config/template": {...}}},
     {"prefix": ["acp"], "rpc": {...}, "rpcErrors": {"session/new": {"code": -32000, "message": "..."}}}
   ]}

The first entry whose "args" equal the arguments, or whose "prefix" starts
them, answers; an unmatched command line exits 0 with no output. An entry
writes "stdout" and "stderr" (a newline is added), sleeps "sleep" seconds and
exits with "exit". An entry with "rpc" instead speaks newline-delimited
JSON-RPC 2.0 on stdin and stdout: each request is answered with the result
named by its method (or the error in "rpcErrors"), a request with neither is
left unanswered, and the CLI exits when its input closes. "notify" lists
notifications sent after a method's reply: {"initialize": [{...}]}, and
"notifyBefore" those sent before it. An entry
with "pi" speaks Pi's RPC mode instead: each {"type": ..., "id": ...} record
is answered with {"type": "response", "id", "command", "success": true,
"data"} from "pi" by type, or "success": false for a type in "piErrors"; a
type in neither is left unanswered. "piEvents" lists the events Pi writes after
answering a record of a type: {"prompt": [{"type": "agent_settled"}]}. "exitAfter": n exits after n records.
"closeOutputAfter": n closes standard output after n records and exits a
few seconds later, so a client sees the closed output, not a failed write;
"closeInputAfter": n closes standard input before the nth reply instead,
so every later write fails while the output is still open.

Every launch appends its arguments to FAKE_PROVIDER_LOG.args (and, with the
variables a spec lists in "logEnv", both as JSON to FAKE_PROVIDER_LOG.launch);
requests are appended to FAKE_PROVIDER_LOG.
"""
import json
import os
import sys
import time

spec_path, args = sys.argv[1], sys.argv[2:]
log_path = os.environ.get("FAKE_PROVIDER_LOG")
if log_path:
    with open(log_path + ".args", "a") as out:
        out.write(" ".join(args) + "\n")
with open(spec_path) as source:
    spec = json.load(source)
# The environment variables a spec names in "logEnv", with the arguments, as JSON lines in
# FAKE_PROVIDER_LOG.launch (unset ones as null).
if log_path and spec.get("logEnv"):
    with open(log_path + ".launch", "a") as out:
        out.write(json.dumps({"args": args, "env": {name: os.environ.get(name) for name in spec["logEnv"]}},
                             sort_keys=True) + "\n")


def wait_for_title_gate():
    """With FAKE_TITLE_GATE set (a scenario's "titleAfterTurn"), answer a title only once that
    file exists: the parity driver creates it when the turn settled, so the title lands after
    the same events on both servers instead of racing the turn."""
    gate = os.environ.get("FAKE_TITLE_GATE")
    deadline = time.monotonic() + 60
    while gate and not os.path.exists(gate) and time.monotonic() < deadline:
        time.sleep(0.05)


def matches(entry):
    if "args" in entry:
        return entry["args"] == args
    if "prefix" in entry:
        return args[: len(entry["prefix"])] == entry["prefix"]
    return True


entry = next((e for e in spec.get("commands", []) if matches(e)), {})


def write(stream, text):
    if text:
        stream.write(text if text.endswith("\n") else text + "\n")
        stream.flush()


if "pi" in entry:
    answered = 0
    for line in sys.stdin:
        if log_path:
            with open(log_path, "a") as out:
                out.write(line if line.endswith("\n") else line + "\n")
        try:
            record = json.loads(line)
        except ValueError:
            continue
        kind = record.get("type")
        if kind in entry.get("piErrors", []):
            reply = {"type": "response", "id": record.get("id"), "command": kind, "success": False,
                     "error": "not available"}
        elif kind in entry["pi"]:
            if kind == "prompt":
                wait_for_title_gate()
            reply = {"type": "response", "id": record.get("id"), "command": kind, "success": True,
                     "data": entry["pi"][kind]}
        else:
            continue
        answered += 1
        if entry.get("closeInputAfter") == answered:
            # Closed before the reply goes out, so no request written after it can land.
            os.close(0)
        write(sys.stdout, json.dumps(reply))
        # Events Pi emits after answering a record of this type ("piEvents").
        for event in entry.get("piEvents", {}).get(kind, []):
            write(sys.stdout, json.dumps(event))
        if entry.get("exitAfter") == answered:
            sys.exit(1)
        if entry.get("closeOutputAfter") == answered:
            sys.stdout.flush()
            os.close(1)
            time.sleep(3)
            os._exit(1)
        if entry.get("closeInputAfter") == answered:
            time.sleep(3)
            os._exit(1)
    sys.exit(0)

if "rpc" not in entry:
    write(sys.stdout, entry.get("stdout", ""))
    write(sys.stderr, entry.get("stderr", ""))
    if entry.get("sleep"):
        time.sleep(entry["sleep"])
    sys.exit(entry.get("exit", 0))

results, errors, notify = entry["rpc"], entry.get("rpcErrors", {}), entry.get("notify", {})
for line in sys.stdin:
    if log_path:
        with open(log_path, "a") as out:
            out.write(line if line.endswith("\n") else line + "\n")
    try:
        message = json.loads(line)
    except ValueError:
        continue
    method = message.get("method")
    if "id" not in message or method is None:
        continue
    if method == "session/prompt":
        wait_for_title_gate()
    # Notifications that go out before a method's reply ("notifyBefore"), as an agent streams a
    # prompt's updates before answering it.
    for notification in entry.get("notifyBefore", {}).get(method, []):
        write(sys.stdout, json.dumps({"jsonrpc": "2.0", **notification}))
    if method in errors:
        reply = {"jsonrpc": "2.0", "id": message["id"], "error": errors[method]}
    elif method in results:
        reply = {"jsonrpc": "2.0", "id": message["id"], "result": results[method]}
    else:
        continue
    write(sys.stdout, json.dumps(reply))
    for notification in notify.get(method, []):
        write(sys.stdout, json.dumps({"jsonrpc": "2.0", **notification}))
