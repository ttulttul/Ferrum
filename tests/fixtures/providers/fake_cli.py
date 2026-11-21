#!/usr/bin/env python3
"""A stand-in for the Codex and Claude Code CLIs in provider turn tests.

It plays the scenario named by FAKE_PROVIDER_SCENARIO: messages recorded from
a real `codex app-server` or `claude --output-format stream-json` session,
with steps that wait for what the client sends. Every line it receives is
appended to FAKE_PROVIDER_LOG, and its arguments to FAKE_PROVIDER_LOG.args.

A scenario is {"steps": [...]} or {"variants": [{"argsContain": "...",
"steps": [...]}, ...]}, where the first variant whose argsContain is in the
arguments (or that has none) plays. Steps:

  {"expect": "turn/start"}   read until a message whose method, type or
                             request.subtype matches; remember it
  {"expectReply": "id"}      read until the reply to request "id"
  {"send": {...}}            write a message; "$id" becomes the remembered
                             message's id and "$params.x" its params.x
  {"stderr": "text"}         write a line to standard error
  {"sleep": seconds}
  {"exit": code}

After the last step the CLI waits for its input to close, as the real ones do.
"""
import json
import os
import sys
import time

args = sys.argv[1:]
log_path = os.environ["FAKE_PROVIDER_LOG"]
with open(log_path + ".args", "a") as out:
    out.write(" ".join(args) + "\n")
with open(os.environ["FAKE_PROVIDER_SCENARIO"]) as source:
    scenario = json.load(source)

if "variants" in scenario:
    steps = next(
        v["steps"]
        for v in scenario["variants"]
        if "argsContain" not in v or v["argsContain"] in " ".join(args)
    )
else:
    steps = scenario["steps"]

remembered = {}


def receive():
    line = sys.stdin.readline()
    if not line:
        sys.exit(0)
    with open(log_path, "a") as out:
        out.write(line if line.endswith("\n") else line + "\n")
    try:
        return json.loads(line)
    except ValueError:
        return {}


def fill(value):
    if isinstance(value, str):
        if value == "$id":
            return remembered.get("id")
        if value.startswith("$params."):
            return remembered.get("params", {}).get(value[len("$params."):])
        return value
    if isinstance(value, list):
        return [fill(v) for v in value]
    if isinstance(value, dict):
        return {k: fill(v) for k, v in value.items()}
    return value


for step in steps:
    if "expect" in step:
        while True:
            message = receive()
            names = (
                message.get("method"),
                message.get("type"),
                (message.get("request") or {}).get("subtype"),
            )
            if step["expect"] in names:
                remembered = message
                break
    elif "expectReply" in step:
        while True:
            message = receive()
            if message.get("id") == step["expectReply"] and "method" not in message:
                break
    elif "send" in step:
        sys.stdout.write(json.dumps(fill(step["send"])) + "\n")
        sys.stdout.flush()
    elif "stderr" in step:
        sys.stderr.write(step["stderr"] + "\n")
        sys.stderr.flush()
    elif "sleep" in step:
        time.sleep(step["sleep"])
    elif "exit" in step:
        sys.exit(step["exit"])

while True:
    receive()
