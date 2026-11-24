"""A linked environment on loopback for delegated tasks that run there (`target.environmentId`).

It answers the link's sign-in, the descriptor and `/mcp`, the way a T3 Code environment would for
a task launched there: `t3_thread_launch` starts its thread, `t3_thread_wait` reports its run
ended, `t3_thread_read` holds its reply, `t3_thread_interrupt` stops it and `t3_thread_send` starts its
next run (a follow-up). Each call is recorded
(tool, arguments, the caller's mode limit and sender thread) so both servers' calls can be
compared. All data is fake: loopback addresses and fixture ids, labels and tokens.
"""
import json
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlencode

LINK_REDIRECT = "http://127.0.0.1/t3code-peer-link"
PEER_ID = "env-fixture-peer"
THREAD_THERE = "thread:there"
AT = "2026-01-01T00:00:00.000Z"
REPLY = "Hi from the linked machine."


def run_id(ordinal):
    return f"run:thread:there:ordinal:{ordinal}"


RUN_THERE = run_id(1)
FOLLOW_UP_REPLY = "Hi again from the linked machine."


def thread_row(status, runs, items):
    return dict(threadId=THREAD_THERE, projectId="project:there", title="Greeter", createdBy="agent",
                creationSource="mcp", status=status, latestRunId=runs[-1]["id"], activeRunId=None,
                providerInstanceId="claudeAgent", model="claude-opus-5-5", runtimeMode="full-access",
                interactionMode="default", linkedPullRequest=None, titleRegeneration=None, branch=None,
                worktreePath=None, parentThreadId=None, relationshipToParent=None, runCount=len(runs),
                itemCount=len(items), pendingRequestCount=0, archived=False, settled=False, settledAt=None,
                snoozed=False, snoozedUntil=None, createdAt=AT, updatedAt=AT)


def item(position, kind, text, created_by, run):
    return dict(position=position, visibility="local", sourceThreadId=THREAD_THERE,
                itemId=f"item:there:{position}", runId=run,
                messageId=f"message:there:{position}", createdBy=created_by,
                creationSource="mcp" if created_by == "agent" else "provider", type=kind, status="completed",
                title=None, text=text, textTruncated=False, updatedAt=AT)


def run_status(run):
    return "interrupted" if run["stopped"] else "completed" if run["finished"] else "running"


def answers(state, arguments):
    """What each tool answers there, as the task's thread there is now."""
    runs = state["runs"]
    wanted = next((r for r in runs if r["id"] == arguments.get("runId")), runs[-1])
    rows = [dict(runId=r["id"], ordinal=r["ordinal"], status=run_status(r), providerInstanceId="claudeAgent",
                 model="claude-opus-5-5", requestedAt=AT, startedAt=AT,
                 completedAt=AT if run_status(r) != "running" else None) for r in runs]
    items = []
    for r in runs:
        items.append(item(len(items), "user_message", r["prompt"], "agent", r["id"]))
        if r["finished"] and not r["stopped"]:
            items.append(item(len(items), "assistant_message", r["reply"], None, r["id"]))
    latest = run_status(runs[-1])
    return {
        "t3_thread_launch": dict(threadId=THREAD_THERE, projectId="project:there",
                                 modelSelection=dict(instanceId="claudeAgent", model="claude-opus-5-5"),
                                 runId=RUN_THERE, status="running"),
        "t3_thread_wait": dict(threadId=THREAD_THERE, runId=wanted["id"], status=run_status(wanted),
                               timedOut=run_status(wanted) == "running"),
        "t3_thread_read": dict(thread=thread_row(latest, runs, items), recentRuns=rows[::-1], items=items,
                               nextPosition=None, hasMore=False),
        "t3_thread_interrupt": dict(threadId=THREAD_THERE, runId=runs[-1]["id"] if latest == "running" else None,
                                    status="interrupt_requested" if latest == "running" else "no_active_run"),
        "t3_thread_send": dict(threadId=THREAD_THERE, messageId=f"message:there:send:{len(runs)}",
                               runId=runs[-1]["id"], status="running", delivery="started"),
    }


class RemoteTaskPeer:
    """The linked environment. `finish_after` waits answer that many times before its run ends
    (None: never, so a cancel lands while it runs); a stop ends the run interrupted."""

    def __init__(self, finish_after=0, follow_up_after=0):
        self.lock = threading.Lock()
        self.changed = threading.Condition(self.lock)
        self.log = []
        self.sessions = set()
        # `finish_after` None: the run there goes on until it is stopped, and a wait on it waits
        # (up to 10 s) as a wait there does.
        self.finish_after = finish_after
        # A follow-up's run there ends this many seconds after its send (its waits wait for it),
        # so it ends after the parent's turn here, whichever server follows it.
        self.follow_up_after = follow_up_after
        self.state = dict(runs=[self.new_run(1, "Say hi to the parent.", REPLY)])
        peer = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def reply(self, status, body=None, headers=()):
                data = b"" if body is None else json.dumps(body).encode()
                self.send_response(status)
                for name, value in headers:
                    self.send_header(name, value)
                if body is not None:
                    self.send_header("content-type", "application/json")
                self.send_header("content-length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)

            def do_GET(self):
                if self.path == "/.well-known/t3/environment":
                    return self.reply(200, dict(
                        environmentId=PEER_ID, label="Fixture peer", platform=dict(os="linux", arch="x64"),
                        serverVersion="0.0.0-fixture",
                        capabilities=dict(peerLinks=True, mcpModeLimitHeader=True, linkFence=True)))
                self.reply(404, {})

            def do_POST(self):
                raw = self.rfile.read(int(self.headers.get("content-length") or 0)).decode()
                if self.path == "/oauth/mcp/register":
                    return self.reply(201, dict(client_id="fixture-client", client_name=json.loads(raw)["client_name"],
                                                redirect_uris=[LINK_REDIRECT], grant_types=["authorization_code"],
                                                response_types=["code"], token_endpoint_auth_method="none"))
                if self.path == "/oauth/mcp/decision":
                    state = json.loads(raw)["authorization"]["state"]
                    return self.reply(200, dict(redirectTo=f"{LINK_REDIRECT}?{urlencode(dict(code='fixture-code', state=state))}"))
                if self.path == "/oauth/mcp/token":
                    return self.reply(200, dict(access_token="fixture-link-token", token_type="Bearer",
                                                expires_in=2592000, scope="orchestration:read orchestration:operate"))
                if self.path == "/mcp":
                    return self.mcp(json.loads(raw))
                self.reply(404, {})

            def mcp(self, body):
                session = self.headers.get("mcp-session-id")
                if body.get("method") == "initialize":
                    with peer.lock:
                        session = f"fixture-session-{len(peer.sessions) + 1}"
                        peer.sessions.add(session)
                    return self.reply(200, dict(jsonrpc="2.0", id=body["id"], result=dict(
                        protocolVersion="2025-06-18", capabilities={}, serverInfo=dict(name="T3 Code", version="0"))),
                        [("mcp-session-id", session)])
                if session not in peer.sessions:
                    return self.reply(404)
                if body.get("method", "").startswith("notifications/"):
                    return self.reply(202)
                name = body["params"]["name"]
                arguments = body["params"].get("arguments") or {}
                with peer.lock:
                    peer.log.append(dict(tool=name, arguments=arguments,
                                         modeLimit=self.headers.get("t3-mode-limit"),
                                         sender=self.headers.get("t3-sender-thread")))
                    runs = peer.state["runs"]
                    latest = runs[-1]
                    if name == "t3_thread_send":
                        # A follow-up starts the thread's next run there.
                        run = peer.new_run(len(runs) + 1, arguments.get("message", ""), FOLLOW_UP_REPLY)
                        if peer.follow_up_after:
                            run.update(finished=False, waits_left=None,
                                       finish_at=time.monotonic() + peer.follow_up_after)
                        runs.append(run)
                    if name == "t3_thread_interrupt" and not latest["finished"]:
                        latest["stopped"] = True
                        peer.changed.notify_all()
                    run = next((r for r in runs if r["id"] == arguments.get("runId")), runs[-1])
                    if name == "t3_thread_wait" and run.get("finish_at") is not None:
                        peer.changed.wait_for(lambda: run["stopped"],
                                              timeout=max(0, run["finish_at"] - time.monotonic()))
                        run["finished"] = not run["stopped"]
                    elif name == "t3_thread_wait" and run["waits_left"] is None:
                        peer.changed.wait_for(lambda: run["stopped"], timeout=10)
                    answer = answers(peer.state, arguments).get(name)
                    if name == "t3_thread_wait" and run["waits_left"] is not None \
                            and not run["finished"] and not run["stopped"]:
                        run["waits_left"] -= 1
                        if run["waits_left"] <= 0:
                            run["finished"] = True
                if answer is None:
                    text = json.dumps(dict(_tag="OrchestratorMcpFailure", code="invalid_request",
                                           message=f"The fixture peer does not serve {name}."))
                    result = dict(content=[dict(type="text", text=text)], isError=True)
                else:
                    result = dict(content=[dict(type="text", text=json.dumps(answer))],
                                  structuredContent=answer, isError=False)
                self.reply(200, dict(jsonrpc="2.0", id=body["id"], result=result))

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.url = f"http://127.0.0.1:{self.server.server_address[1]}"
        threading.Thread(target=self.server.serve_forever, daemon=True).start()

    def new_run(self, ordinal, prompt, reply):
        return dict(id=run_id(ordinal), ordinal=ordinal, prompt=prompt, reply=reply, stopped=False,
                    finished=self.finish_after == 0, waits_left=self.finish_after)

    def calls(self):
        with self.lock:
            return list(self.log)

    def close(self):
        self.server.shutdown()
