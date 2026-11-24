"""A linked environment on loopback for delegated tasks that run there (`target.environmentId`).

It answers the link's sign-in, the descriptor and `/mcp`, the way a T3 Code environment would for
a task launched there: `t3_thread_launch` starts its thread, `t3_thread_wait` reports its run
ended, `t3_thread_read` holds its reply and `t3_thread_interrupt` stops it. Each call is recorded
(tool, arguments, the caller's mode limit and sender thread) so both servers' calls can be
compared. All data is fake: loopback addresses and fixture ids, labels and tokens.
"""
import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlencode

LINK_REDIRECT = "http://127.0.0.1/t3code-peer-link"
PEER_ID = "env-fixture-peer"
THREAD_THERE = "thread:there"
RUN_THERE = "run:thread:there:ordinal:1"
AT = "2026-01-01T00:00:00.000Z"
REPLY = "Hi from the linked machine."


def thread_row(status):
    return dict(threadId=THREAD_THERE, projectId="project:there", title="Greeter", createdBy="agent",
                creationSource="mcp", status=status, latestRunId=RUN_THERE, activeRunId=None,
                providerInstanceId="claudeAgent", model="claude-opus-5-5", runtimeMode="full-access",
                interactionMode="default", linkedPullRequest=None, titleRegeneration=None, branch=None,
                worktreePath=None, parentThreadId=None, relationshipToParent=None, runCount=1, itemCount=2,
                pendingRequestCount=0, archived=False, settled=False, settledAt=None, snoozed=False,
                snoozedUntil=None, createdAt=AT, updatedAt=AT)


def item(position, kind, text, created_by):
    return dict(position=position, visibility="local", sourceThreadId=THREAD_THERE,
                itemId=f"item:there:{position}", runId=RUN_THERE,
                messageId=f"message:there:{position}", createdBy=created_by,
                creationSource="mcp" if created_by == "agent" else "provider", type=kind, status="completed",
                title=None, text=text, textTruncated=False, updatedAt=AT)


def answers(state):
    """What each tool answers there, as the task's thread there is now."""
    ended = state["stopped"] or state["finished"]
    status = "interrupted" if state["stopped"] else "completed"
    run = dict(runId=RUN_THERE, ordinal=1, status=status if ended else "running",
               providerInstanceId="claudeAgent", model="claude-opus-5-5", requestedAt=AT, startedAt=AT,
               completedAt=AT if ended else None)
    items = [item(0, "user_message", "Say hi to the parent.", "agent")]
    if state["finished"] and not state["stopped"]:
        items.append(item(1, "assistant_message", REPLY, None))
    return {
        "t3_thread_launch": dict(threadId=THREAD_THERE, projectId="project:there",
                                 modelSelection=dict(instanceId="claudeAgent", model="claude-opus-5-5"),
                                 runId=RUN_THERE, status="running"),
        "t3_thread_wait": dict(threadId=THREAD_THERE, runId=RUN_THERE,
                               status=run["status"], timedOut=not ended),
        "t3_thread_read": dict(thread=thread_row(run["status"]), recentRuns=[run], items=items,
                               nextPosition=None, hasMore=False),
        "t3_thread_interrupt": dict(threadId=THREAD_THERE, runId=RUN_THERE if not ended else None,
                                    status="interrupt_requested" if not ended else "no_active_run"),
    }


class RemoteTaskPeer:
    """The linked environment. `finish_after` waits answer that many times before its run ends,
    so a cancel can land while it runs; a stop ends the run interrupted."""

    def __init__(self, finish_after=0):
        self.lock = threading.Lock()
        self.log = []
        self.sessions = set()
        self.state = dict(stopped=False, finished=finish_after == 0, waits_left=finish_after)
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
                    state = peer.state
                    if name == "t3_thread_interrupt" and not state["finished"]:
                        state["stopped"] = True
                    answer = answers(state).get(name)
                    if name == "t3_thread_wait" and not state["finished"] and not state["stopped"]:
                        state["waits_left"] -= 1
                        if state["waits_left"] <= 0:
                            state["finished"] = True
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

    def calls(self):
        with self.lock:
            return list(self.log)

    def close(self):
        self.server.shutdown()
