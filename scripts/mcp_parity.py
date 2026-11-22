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
