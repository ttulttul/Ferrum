#!/usr/bin/env python3
"""Compare a real public-client OAuth grant and MCP project reads on two isolated servers."""
import argparse
import base64
from contextlib import ExitStack
import hashlib
import json
from pathlib import Path
import secrets
import shlex
from tempfile import TemporaryDirectory
from urllib.parse import parse_qs, urlencode, urlsplit

from client_initial_sync_parity import rpc
from parity_harness import encode, http, managed_server


def observations(url, cookie, root):
    def post(path, data):
        status, _, body = http(url, "POST", path, encode(data), {"Cookie":cookie, "Content-Type":"application/json"})
        assert status in (200,201), (path,status,body)
        return json.loads(body)

    redirect = "http://localhost/callback"
    client = post("/oauth/mcp/register", {"client_name":"Fixture","redirect_uris":[redirect]})["client_id"]
    verifier = secrets.token_urlsafe(48)
    params = dict(client_id=client, response_type="code", redirect_uri=redirect, code_challenge_method="S256",
                  code_challenge=base64.urlsafe_b64encode(hashlib.sha256(verifier.encode()).digest()).rstrip(b"=").decode())
    code = post("/api/auth/pairing-token", {})["credential"]
    decision = post("/oauth/mcp/decision", dict(authorization=params, decision={"_tag":"pairing-code","access":"read-only","code":code}))
    grant = parse_qs(urlsplit(decision["redirectTo"]).query)["code"][0]
    form = urlencode(dict(grant_type="authorization_code",client_id=client,code=grant,code_verifier=verifier,redirect_uri=redirect))
    status, _, response = http(url, "POST", "/oauth/mcp/token", form, {"content-type":"application/x-www-form-urlencoded"})
    assert status == 200, status
    headers = {"authorization":"Bearer "+json.loads(response)["access_token"],"content-type":"application/json",
               "accept":"application/json, text/event-stream"}
    results=[]
    def call(method, params):
        status, h, body = http(url, "POST", "/mcp", encode(dict(jsonrpc="2.0",id=1,method=method,params=params)),headers)
        assert status==200, (method,status,body)
        # Each server has its own home, which a themes directory names.
        body = body.replace(str(root).encode(), b"<home>")
        results.append((method,status,h.get("content-type"),body))
        return h
    h=call("initialize",dict(protocolVersion="2025-06-18",capabilities={},clientInfo=dict(name="Fixture",version="1")))
    headers["mcp-session-id"]=h["mcp-session-id"]
    call("ping",{})
    call("tools/call",dict(name="t3_project_list",arguments={}))
    call("tools/call",dict(name="t3_project_read",arguments=dict(projectId="missing")))
    # A read-only outside client can list across projects, including disabled tasks.
    task = dict(title="Read the report", prompt="Read this report", enabled=False,
                workspaceStrategy={"type":"root"}, modelSelection={"instanceId":"codex","model":"fixture"},
                runtimeMode="approval-required", interactionMode="default")
    for project, schedule in [("project:a", {"type":"interval","everyMs":60000}),
                              ("project:b", {"type":"webhook"})]:
        rpc(url, cookie, "scheduledTasks.upsert", dict(task, id="task:"+project, projectId=project, schedule=schedule))
    for filters in [{}, {"projectId":"project:a"}, {"scheduledTaskId":"task:project:b"},
                    {"projectId":"project:a","scheduledTaskId":"task:project:b"}, {"projectId":17}]:
        call("tools/call", dict(name="list_scheduled_tasks", arguments=filters))
    for name, arguments in [("schedule_task", {"projectId":"project:a","prompt":"run","schedule":{"type":"once","runAt":"2027-01-01T00:00:00Z"}}),
                            ("delete_scheduled_task", {"scheduledTaskId":"task:project:a"}),
                            ("run_scheduled_task_now", {"taskId":"task:project:a"})]:
        call("tools/call", dict(name=name, arguments=arguments))
    # Pull-request and issue toolkit calls from a read-only outside client.
    # The write calls exercise the caller gate without contacting a provider.
    for name, arguments in [
        ("list_thread_pull_requests", {"threadId":"thread-missing"}),
        ("list_thread_issues", {"threadId":"thread-missing"}),
        ("read_pull_request_guide", {"url":"https://github.com/example/parity/pull/3"}),
        ("link_pull_request", {"url":"https://github.com/example/parity/pull/3"}),
        ("link_pull_requests", {"urls":["https://github.com/example/parity/pull/3"]}),
        ("unlink_pull_request", {"url":"https://github.com/example/parity/pull/3"}),
        ("unlink_pull_requests", {"states":["open"]}),
        ("watch_pull_request", {"url":"https://github.com/example/parity/pull/3"}),
        ("unwatch_pull_request", {"url":"https://github.com/example/parity/pull/3"}),
        ("write_pull_request_guides", {"pullRequests":[{"url":"https://github.com/example/parity/pull/3"}]}),
        ("list_issues", {"projectId":"project:missing"}),
        ("read_issue", {"url":"https://github.com/example/parity/issues/12"}),
        ("create_issue", {"projectId":"project:missing","title":"Fixture"}),
        ("comment_on_issue", {"url":"https://github.com/example/parity/issues/12","body":"Fixture"}),
        ("set_issue_state", {"url":"https://github.com/example/parity/issues/12","state":"Open"}),
        ("link_issue", {"url":"https://github.com/example/parity/issues/12","threadId":"thread-missing"}),
        ("unlink_issue", {"url":"https://github.com/example/parity/issues/12","threadId":"thread-missing"}),
    ]:
        call("tools/call", dict(name=name, arguments=arguments))
    # The thread tools for a client outside any thread, approved read-only.
    for name, arguments in [
        ("t3_thread_configure", {}),
        ("t3_thread_configure", dict(threadId="thread-missing")),
        ("t3_queue", dict(action="list", threadId="thread-missing")),
        ("t3_queue", dict(action="bogus")),
        ("t3_pending_request", dict(action="list")),
        ("t3_pending_request", dict(action="respond", requestId="request-missing", answers={})),
        ("t3_thread_transfer", dict(action="status")),
        ("t3_thread_transfer", dict(action="fork")),
        ("t3_thread_organize", dict(action="pin", threadId="thread-missing")),
        ("t3_thread_update", dict(action="rename", title="Renamed")),
        ("t3_workflow_stop", dict(threadId="thread-missing", subagentId="workflow-missing")),
        ("t3_thread_search", dict(query="anything")),
        ("t3_thread_search", dict(query="anything", projectId="project-missing")),
        ("t3_project_update", dict(projectId="project-missing", title="Renamed")),
        ("t3_project_delete", dict(projectId="project-missing")),
        ("t3_project_clone", dict(remoteUrl="/nonexistent/t3-parity.git", destinationPath="/nonexistent/clone")),
        ("t3_environment_preferences_update", dict(newWorktreesStartFromOrigin=False)),
        ("t3_worktree_list", {}),
        ("t3_worktree_list", dict(threadId="thread-missing")),
        ("t3_theme_read", {}),
        ("t3_theme_set", dict(appearance="dark")),
        ("send_to_user", dict(message="Hello")),
        ("t3_background_wait", dict(taskIds=["bg-1"])),
        ("tool_search", dict(query="theme")),
    ]:
        call("tools/call", dict(name=name, arguments=arguments))
    call("tools/call",dict(name="missing-tool",arguments={}))
    status, _, body = http(url, "DELETE", "/mcp", headers=headers)
    results.append(("close",status,body))
    status, _, body = http(url, "POST","/mcp",encode(dict(jsonrpc="2.0",id=1,method="ping",params={})),headers)
    results.append(("closed session",status,body))
    return results


if __name__=="__main__":
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--original-command",required=True)
    parser.add_argument("--rust-command",required=True)
    args=parser.parse_args()
    with TemporaryDirectory() as tmp, ExitStack() as stack:
        key, secret=secrets.token_bytes(32), secrets.token_urlsafe(32)
        servers=[managed_server(stack,shlex.split(cmd),Path(tmp)/str(i),secret,key) for i,cmd in enumerate((args.original_command,args.rust_command))]
        expected,actual=[observations(*s, Path(tmp)/str(i)) for i,s in enumerate(servers)]
        for a,b in zip(expected,actual,strict=True):
            assert a==b,(a,b)
        print(f"{len(expected)}/{len(expected)} MCP transport fixtures matched after single-use OAuth approval and PKCE exchange")
