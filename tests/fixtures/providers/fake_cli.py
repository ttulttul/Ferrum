#!/usr/bin/env python3
"""A stand-in for the Codex and Claude Code CLIs in provider turn tests.

It plays the scenario named by FAKE_PROVIDER_SCENARIO: messages recorded from
a real `codex app-server` or `claude --output-format stream-json` session,
with steps that wait for what the client sends. Every line it receives is
appended to FAKE_PROVIDER_LOG (and, with its process id, to
FAKE_PROVIDER_LOG.messages), its arguments to FAKE_PROVIDER_LOG.args, and its
arguments, launch environment and process id as JSON to FAKE_PROVIDER_LOG.launch.

A scenario is {"steps": [...]} or {"variants": [{"argsContain": "...",
"steps": [...]}, ...]}, where the first variant whose argsContain is in the
arguments (or that has none), and whose envContains {NAME: part} parts are
all in those variables (a delegated child's T3CODE_THREAD_ID, say), plays. A variant may name another scenario file
in this directory ("scenario": "claude_turn.json") to play its steps instead.
With "repeat": true (on the scenario or the variant) the steps play again
from the start after the last, so one long-lived CLI answers every prompt.
Steps:

  {"expect": "turn/start"}   read until a message whose method, type or
                             request.subtype matches; remember it
  {"expectReply": "id"}      read until the reply to request "id"
  {"branch": {"thread/start": "codex_turn.json",
              "thread/resume": "codex_resume.json"}}
                             read until a message one of the keys matches,
                             then play the named scenario from the step after
                             its first {"expect"} of that key (so one Codex
                             session can start or resume its thread)
  {"branchOnUser": [{"contains": "text", "steps": [...]}, {"steps": [...]}]}
                             read until a user message, then play the steps
                             of the first case whose text is in its content
                             (or that has none), so one Claude CLI can take a
                             first prompt or a later delivery
  {"send": {...}}            write a message; "$id" becomes the remembered
                             message's id ("$request_id" its control
                             request_id), "$params.x" its params.x, and
                             "$session" the Claude session the CLI was
                             started on
  {"sendTogether": [...]}    write several protocol lines together so a control callback
                             and stream output are available in the same read
  {"stderr": "text"}         write a line to standard error
  {"sleep": seconds}
  {"exit": code}
  {"mcp": {"tool": "t3_thread_list", "arguments": {...}}}
                             call a tool on the T3 MCP server the client gave
                             this CLI (Codex: `mcp_servers.t3-code` in
                             thread/start; Claude: `mcp_set_servers`), as the
                             real CLI would; {"mcp": {"method": "tools/list"}}
                             lists its tools. The request and the answer are
                             appended to FAKE_PROVIDER_LOG.mcp as JSON. With
                             "as": "name", the tool's answer is kept, and a
                             later argument "$mcp.name.a.b" is its field a.b;
                             "$env.NAME" is an environment variable, and
                             "headers" are sent with that request only.
                             With "background": true the call runs while
                             later steps play; {"await": "<as>"} waits for it.

A request the scenario does not expect is answered from SIDE_REPLIES (or the
scenario's own "replies" by method), so a client that reads its config or
account first still reaches the scripted steps. A Claude control request is
answered from CONTROL_REPLIES (or "controlReplies" by subtype). A scenario
with "silent": true answers nothing, as a CLI that hangs.

After the last step the CLI waits for its input to close, as the real ones do.
"""
import json
import os
import re
import sys
import threading
import time
import urllib.error
import urllib.request

args = sys.argv[1:]
log_path = os.environ["FAKE_PROVIDER_LOG"]


def title_generation():
    """Answer a one-shot text generation run (a thread title) and exit.

    `codex ... mcp list --json` prints the scenario's "mcpServers" (default
    none). `codex exec ... -` and `claude -p ...` read the prompt from standard
    input, append {argv, prompt, schema, env} to FAKE_PROVIDER_LOG.title, and
    answer the scenario's "title" output ({"title", "needsRefinement"}); a
    scenario without one fails at once, as a CLI that cannot run. With
    "titleDelayFirst", the first run answers after that many seconds.
    """
    with open(os.environ["FAKE_PROVIDER_SCENARIO"]) as source:
        scenario = json.load(source)
    if "mcp" in args and args[args.index("mcp") + 1:args.index("mcp") + 3] == ["list", "--json"]:
        sys.stdout.write(json.dumps(scenario.get("mcpServers", [])) + "\n")
        sys.exit(0)
    prompt = sys.stdin.read()
    schema = None
    for flag in ("--output-schema", "--json-schema"):
        if flag in args:
            value = args[args.index(flag) + 1]
            schema = open(value).read() if flag == "--output-schema" else value
    env = {k: v for k, v in os.environ.items()
           if k.startswith(("CODEX_INTERNAL_", "CODEX_HOME", "CLAUDE_CODE_ENTRYPOINT", "CLAUDE_CONFIG_DIR"))}
    # "titleDelayFirst": the first run takes that many seconds, so a server stopped meanwhile
    # leaves its title request to the next.
    first = not os.path.exists(log_path + ".title")
    with open(log_path + ".title", "a") as out:
        out.write(json.dumps({"argv": args, "prompt": prompt, "schema": schema, "env": env}) + "\n")
    if first and scenario.get("titleDelayFirst"):
        time.sleep(scenario["titleDelayFirst"])
    output = scenario.get("title")
    if output is None:
        sys.stderr.write("fake provider: this scenario generates no title\n")
        sys.exit(1)
    if args[0] == "exec":
        with open(args[args.index("--output-last-message") + 1], "w") as out:
            out.write(json.dumps(output))
    else:
        sys.stdout.write(json.dumps({"type": "result", "subtype": "success", "is_error": False,
                                     "result": "", "structured_output": output}) + "\n")
    sys.exit(0)


if args[:1] in (["exec"], ["-p"]) or ("mcp" in args and "--json" in args and "app-server" not in args):
    title_generation()

with open(log_path + ".args", "a") as out:
    out.write(" ".join(args) + "\n")
# The launch as JSON: the arguments and the environment a server sets for its CLIs.
LAUNCH_ENV = ("T3CODE_THREAD_ID", "CLAUDE_CODE_", "CLAUDE_AGENT_SDK_", "CODEX_INTERNAL_", "ENABLE_CLAUDEAI_")
with open(log_path + ".launch", "a") as out:
    env = {k: v for k, v in os.environ.items() if k.startswith(LAUNCH_ENV)}
    # A login-shell scenario also records the PATH the server gave its CLI.
    if os.environ.get("FAKE_PROVIDER_LAUNCH_PATH"):
        env.update({k: os.environ[k] for k in ("PATH", "__MISE_DIFF", "JAVA_HOME") if k in os.environ})
    out.write(json.dumps({"argv": args, "env": env, "pid": os.getpid()}) + "\n")
with open(os.environ["FAKE_PROVIDER_SCENARIO"]) as source:
    scenario = json.load(source)

repeat = bool(scenario.get("repeat"))
if "variants" in scenario:
    variant = next(
        v
        for v in scenario["variants"]
        if ("argsContain" not in v or v["argsContain"] in " ".join(args))
        and all(part in os.environ.get(name, "") for name, part in v.get("envContains", {}).items())
    )
    repeat = bool(variant.get("repeat", repeat))
    if "scenario" in variant:
        directory = os.path.dirname(os.path.abspath(os.environ["FAKE_PROVIDER_SCENARIO"]))
        with open(os.path.join(directory, variant["scenario"])) as source:
            played = json.load(source)
        steps = played["steps"]
        scenario = {**played, **{k: v for k, v in scenario.items() if k != "variants"}}
    else:
        steps = variant["steps"]
else:
    steps = scenario["steps"]

remembered = {}
# The T3 MCP server the client configured: {"url", "authorization"}, and its session.
mcp_server = {}

# Answers to requests a client may make between the scripted ones.
SIDE_REPLIES = {
    "account/read": {"account": {"type": "apiKey"}, "requiresOpenaiAuth": False},
    "config/read": {"config": {}, "origins": {}},
    "thread/unsubscribe": {},
    "turn/interrupt": {},
    **scenario.get("replies", {}),
}
# Answers to Claude control requests by subtype; any other succeeds empty.
CONTROL_REPLIES = {
    "initialize": {"commands": [], "agents": [], "output_style": "default", "available_output_styles": ["default"],
                   "models": [], "account": {}},
    "mcp_set_servers": {"added": [], "removed": [], "errors": {}},
    **scenario.get("controlReplies", {}),
}


def answer(message):
    """Reply to an unscripted request this CLI knows an answer to."""
    if scenario.get("silent"):
        return
    method = message.get("method")
    if "id" in message and method in SIDE_REPLIES:
        sys.stdout.write(json.dumps({"id": message["id"], "result": SIDE_REPLIES[method]}) + "\n")
        sys.stdout.flush()
    subtype = (message.get("request") or {}).get("subtype")
    if message.get("type") == "control_request":
        reply = {"type": "control_response", "response": {
            "subtype": "success", "request_id": message.get("request_id"),
            "response": CONTROL_REPLIES.get(subtype, {})}}
        sys.stdout.write(json.dumps(reply) + "\n")
        sys.stdout.flush()


def receive():
    line = sys.stdin.readline()
    if not line:
        sys.exit(0)
    with open(log_path, "a") as out:
        out.write(line if line.endswith("\n") else line + "\n")
    # Each line again with the process that read it: a server runs several CLIs at once (a
    # status probe beside a turn's session), and their lines interleave in the log.
    with open(log_path + ".messages", "a") as out:
        out.write(json.dumps({"pid": os.getpid(), "line": line.rstrip("\n")}) + "\n")
    try:
        message = json.loads(line)
    except ValueError:
        return {}
    note_mcp_server(message)
    return message


def note_mcp_server(message):
    """Remember the T3 MCP server a client hands its CLI."""
    if not isinstance(message, dict):
        return
    if message.get("method") in ("thread/start", "thread/resume"):
        server = ((message.get("params") or {}).get("config") or {}).get("mcp_servers.t3-code")
        # A shared app-server keeps each thread's MCP server apart; the steps act as the first
        # thread, so a later thread (a delegated child) does not take its credential.
        if isinstance(server, dict) and not mcp_server.get("url"):
            mcp_server.update(url=server.get("url"),
                              authorization=(server.get("http_headers") or {}).get("Authorization"))
    request = message.get("request") or {}
    if message.get("type") == "control_request" and request.get("subtype") == "mcp_set_servers":
        server = (request.get("servers") or {}).get("t3-code")
        if isinstance(server, dict):
            mcp_server.update(url=server.get("url"), authorization=(server.get("headers") or {}).get("Authorization"))


def mcp_post(body, extra=None):
    headers = {"content-type": "application/json", "accept": "application/json, text/event-stream"}
    headers.update(extra or {})
    if mcp_server.get("authorization"):
        headers["authorization"] = mcp_server["authorization"]
    if mcp_server.get("session"):
        headers["mcp-session-id"] = mcp_server["session"]
        headers["mcp-protocol-version"] = "2025-06-18"
    request = urllib.request.Request(mcp_server["url"], data=json.dumps(body).encode(), headers=headers, method="POST")
    try:
        with urllib.request.urlopen(request, timeout=60) as response:
            status, received, text = response.status, dict(response.headers), response.read().decode()
    except urllib.error.HTTPError as error:
        status, received, text = error.code, dict(error.headers), error.read().decode()
    received = {k.lower(): v for k, v in received.items()}
    if "text/event-stream" in received.get("content-type", ""):
        # One message per event: notifications the call raised, then its answer.
        messages = []
        for line in text.splitlines():
            if line.startswith("data:"):
                try:
                    messages.append(json.loads(line[5:].strip()))
                except ValueError:
                    messages.append(line[5:].strip())
        answers = [m for m in messages if not (isinstance(m, dict) and "method" in m and "id" not in m)]
        notes = [m["method"] for m in messages if isinstance(m, dict) and "method" in m and "id" not in m]
        answer = answers[-1] if answers else None
        if notes:
            received["x-notifications"] = notes
        return status, received, answer
    try:
        answer = json.loads(text) if text else None
    except ValueError:
        answer = text
    return status, received, answer


def mcp_call(step):
    """One call to the client's T3 MCP server, opening a session first."""
    record = {"request": step}
    if not mcp_server.get("url"):
        record["error"] = "no T3 MCP server was configured"
    else:
        if not mcp_server.get("session"):
            status, headers, answer = mcp_post({"jsonrpc": "2.0", "id": 0, "method": "initialize", "params": {
                "protocolVersion": "2025-06-18", "capabilities": {}, "clientInfo": {"name": "fake-cli", "version": "1"}}})
            record["initialize"] = status
            if status == 200:
                mcp_server["session"] = headers.get("mcp-session-id")
                mcp_post({"jsonrpc": "2.0", "method": "notifications/initialized"})
        method = step.get("method", "tools/call")
        arguments = fill_mcp(step.get("arguments", {}))
        params = {"name": step["tool"], "arguments": arguments} if method == "tools/call" else {}
        status, received, answer = mcp_post({"jsonrpc": "2.0", "id": 1, "method": method, "params": params},
                                            step.get("headers"))
        record.update(status=status, answer=answer)
        if received.get("x-notifications"):
            record["notifications"] = received["x-notifications"]
        if "as" in step:
            result = (answer or {}).get("result") if isinstance(answer, dict) else None
            text = "".join(part.get("text", "") for part in (result or {}).get("content", []))
            try:
                mcp_answers[step["as"]] = json.loads(text)
            except ValueError:
                mcp_answers[step["as"]] = None
    with mcp_log_lock, open(log_path + ".mcp", "a") as out:
        out.write(json.dumps(record) + "\n")


mcp_answers = {}
mcp_calls = {}
mcp_log_lock = threading.Lock()


def fill_mcp(value):
    """Arguments with "$mcp.name.path", "$env.NAME" and "$match:REGEX" (the first group
    REGEX finds in the last expected message's user content, such as a task id in a
    delivery prompt) filled in."""
    if isinstance(value, str):
        if value.startswith("$match:"):
            content = (remembered.get("message") or {}).get("content")
            found = re.search(value[len("$match:"):], content if isinstance(content, str) else json.dumps(content))
            return found.group(1) if found else value
        if value.startswith("$env."):
            return os.environ.get(value[len("$env."):])
        if value.startswith("$mcp."):
            # Left as written when the answer lacks it, so a failed step cannot
            # turn a later call into one on the caller's own thread.
            name, *path = value[len("$mcp."):].split(".")
            found = mcp_answers.get(name)
            for key in path:
                found = found.get(key) if isinstance(found, dict) else None
            return value if found is None else found
        return value
    if isinstance(value, list):
        return [fill_mcp(v) for v in value]
    if isinstance(value, dict):
        return {k: fill_mcp(v) for k, v in value.items()}
    return value


def claude_session():
    """The Claude session this process was started on (--session-id or --resume)."""
    for arg in args:
        for flag in ("--session-id=", "--resume="):
            if arg.startswith(flag):
                return arg[len(flag):]
    return None


def fill(value):
    if isinstance(value, str):
        if value == "$id":
            return remembered.get("id")
        if value.startswith("$params."):
            return remembered.get("params", {}).get(value[len("$params."):])
        if value == "$session":
            return claude_session()
        if value == "$request_id":
            return remembered.get("request_id")
        return value
    if isinstance(value, list):
        return [fill(v) for v in value]
    if isinstance(value, dict):
        return {k: fill(v) for k, v in value.items()}
    return value


def load(name):
    directory = os.path.dirname(os.path.abspath(os.environ["FAKE_PROVIDER_SCENARIO"]))
    with open(os.path.join(directory, name)) as source:
        return json.load(source)


index = 0
while index < len(steps):
    step = steps[index]
    index += 1
    if index >= len(steps) and repeat:
        index = 0
    if "branch" in step:
        while True:
            message = receive()
            names = (
                message.get("method"),
                message.get("type"),
                (message.get("request") or {}).get("subtype"),
            )
            matched = next((name for name in names if name in step["branch"]), None)
            if matched is not None:
                remembered = message
                break
            answer(message)
        played = load(step["branch"][matched])
        SIDE_REPLIES.update(played.get("replies", {}))
        CONTROL_REPLIES.update(played.get("controlReplies", {}))
        after = next(i for i, s in enumerate(played["steps"]) if s.get("expect") == matched)
        steps, index = played["steps"], after + 1
    elif "branchOnUser" in step:
        # Read until a user message, then play the steps of the first case whose
        # "contains" is in its content (or that has none).
        while True:
            message = receive()
            if message.get("type") == "user":
                remembered = message
                break
            answer(message)
        content = json.dumps((message.get("message") or {}).get("content"))
        case = next(c for c in step["branchOnUser"] if c.get("contains", "") in content)
        steps, index = case["steps"], 0
    elif "expect" in step:
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
            answer(message)
    elif "expectReply" in step:
        while True:
            message = receive()
            if message.get("id") == step["expectReply"] and "method" not in message:
                break
            answer(message)
    elif "send" in step or "sendTogether" in step:
        frames = [step["send"]] if "send" in step else step["sendTogether"]
        sys.stdout.write("".join(json.dumps(fill(frame)) + "\n" for frame in frames))
        sys.stdout.flush()
    elif "stderr" in step:
        sys.stderr.write(step["stderr"] + "\n")
        sys.stderr.flush()
    elif "sleep" in step:
        time.sleep(step["sleep"])
    elif "exit" in step:
        sys.exit(step["exit"])
    elif "mcp" in step:
        if step["mcp"].get("background"):
            # The server may need this CLI to answer it before the call returns, as a real
            # CLI does while a tool call is in flight; {"await": "<as>"} waits for it.
            call = threading.Thread(target=mcp_call, args=(step["mcp"],), daemon=True)
            call.start()
            mcp_calls[step["mcp"].get("as", "")] = call
        else:
            mcp_call(step["mcp"])
    elif "await" in step:
        mcp_calls.pop(step["await"]).join(timeout=120)

while True:
    answer(receive())
