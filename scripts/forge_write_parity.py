#!/usr/bin/env python3
"""Compare a thread's forge tool answers and the operations received by isolated GitHub and Bitbucket fixtures."""
import argparse
from contextlib import ExitStack
import json
from pathlib import Path
import re
import secrets
import shlex
import tempfile
import threading
import time
from http.server import ThreadingHTTPServer

import bitbucket_pull_request_parity as bb
import github_provider_parity as gh
import parity_harness as harness
from client_thread_parity import FIXTURES, git, rpc, scenario_env, seed_turn
from peer_forwarding_parity import McpClient


def thread_client(server, log):
    # Only this fixture's CLI receives this short-lived credential.
    end = time.monotonic() + 45
    while time.monotonic() < end:
        for line in log.read_text().splitlines() if log.exists() else []:
            try:
                msg = json.loads(line)
            except ValueError:
                continue
            if msg.get("method") == "thread/start":
                cfg = msg["params"]["config"]["mcp_servers.t3-code"]
                assert cfg["url"] == server + "/mcp"
                return McpClient(server, cfg["http_headers"]["Authorization"].removeprefix("Bearer "))
        time.sleep(.1)
    raise RuntimeError("fake Codex did not receive its thread credential")


def cases(github):
    num = 3 if github else 7
    sha = gh.PR["headRefOid"] if github else bb.pull_request(bb.WRITE, num)["source"]["commit"]["hash"]
    create = dict(title="Fixture change", body="Fixture body", base="main")
    return [
        ("respond no branch PR", "respond", dict(body="comment"), False),
        ("stack auto", "merge", dict(number=num, expectedHeadSha=sha, stack=True, auto=True), False),
        ("update empty", "update", {}, False),
        ("respond empty", "respond", {}, False),
        ("respond needs thread", "respond", dict(body="note", resolved=False), False),
        ("create private", "create", dict(**create, private=True), False),
        ("update fields", "update", dict(number=num, title="Retitled", body="Updated"), True),
        ("labels", "update", dict(number=num, addLabels=["bug"], removeLabels=["old"]), github),
        ("reviewers", "update", dict(number=num, requestReviewers=["reviewer-one"]), True),
        ("missing reviewer", "update", dict(number=num, requestReviewers=["unknown"]), False),
        *([] if github else [("reply and resolve", "respond",
            dict(number=num, reviewThreadId="201", body="Follow up", resolved=True), True)]),
        ("respond", "respond", dict(number=num, body="Top level"), True),
        ("review", "review", dict(number=num, verdict="comment", body="Looks good"), True),
        ("inline review", "review", dict(number=num, verdict="comment",
            comments=[dict(path="lib.rs" if github else "src/a.rs", line=1 if github else 2, body="inspect")]), True),
        ("review absent line", "review", dict(number=num, verdict="comment",
            comments=[dict(path="absent.txt", line=4, body="missing")]), False),
        ("review cached diff", "review", dict(number=num, verdict="comment",
            comments=[dict(path="absent.txt", line=5, body="missing")]), False),
        ("merge moved head", "merge", dict(number=num, expectedHeadSha="bbbbbbb"), False),
        ("merge", "merge", dict(number=num, expectedHeadSha=sha, method="squash"), True),
        ("create", "create", create, True),
        ("return existing", "create", create, True),
    ]


def run(command, root, github):
    with ExitStack() as stack:
        root.mkdir()
        home = root / "server"
        scenario = dict(name="forge writes", instance="codex", model="fixture", scenario="codex_turn.json")
        env, _, log = scenario_env(root, "forge", home, scenario, {"codex"})
        if github:
            host, credentials = gh.github_fixture(stack, root / "gh")
            seen = host.forge_calls = []
            host.status_rows, host.status_reads = [], []
            remote = f"https://github.com/{gh.REPO}.git"
        else:
            seen = []
            host = stack.enter_context(ThreadingHTTPServer(("127.0.0.1", 0), bb.fake_bitbucket(seen)))
            host.forge_rows = []
            threading.Thread(target=host.serve_forever, daemon=True).start()
            stack.callback(host.shutdown)
            credentials = {"T3CODE_BITBUCKET_API_BASE_URL": f"http://127.0.0.1:{host.server_port}/2.0",
                           "T3CODE_BITBUCKET_ACCESS_TOKEN": bb.TOKEN}
            remote = f"https://bitbucket.org/{bb.WRITE}.git"
        fixture = json.loads((FIXTURES / "codex_turn.json").read_text())
        i = next(i for i, s in enumerate(fixture["steps"]) if s.get("send", {}).get("method") == "turn/started")
        fixture["steps"].insert(i + 1, {"expect": "turn/interrupt"})
        playback = root / "turn.json"
        playback.write_text(json.dumps(fixture))
        env.update(credentials, GIT_ALLOW_PROTOCOL="file", FAKE_PROVIDER_SCENARIO=str(playback))
        url, cookie = harness.managed_server(stack, shlex.split(command), home, secrets.token_urlsafe(32),
                                            secrets.token_bytes(32), env=env)
        seed = seed_turn(url, cookie, root, scenario, remote=remote)
        checkout, bare = root / "checkout", root / "fixture.git"
        git("init", "-q", "--bare", str(bare))
        git("-C", str(checkout), "remote", "set-url", "--push", "origin", str(bare))
        git("-C", str(checkout), "switch", "-qc", "work")
        rpc(url, cookie, "orchestration.dispatchCommand", seed["dispatch"])
        agent = thread_client(url, log)
        result = []
        for label, tool, args, success in cases(github):
            seen.clear()
            answer = agent.call("pull_request_" + tool, args)
            # Include the snapshot sync scheduled by a new link before advancing to the next call.
            if label == "create" and not answer.get("isError"):
                until = time.monotonic() + 5
                while time.monotonic() < until:
                    projection = rpc(url, cookie, "orchestration.getThreadProjection", {"threadId": seed["threadId"]})
                    if any(link.get("snapshot") for link in projection["thread"].get("pullRequests", [])):
                        break
                    time.sleep(.1)
                else:
                    raise AssertionError("Created pull request did not acquire a snapshot")
            requests = list(seen)
            result.append((label, answer, requests, success))
        assert bare.joinpath("refs/heads/work").is_file(), "branch did not reach the isolated bare repository"
        return result


def github_request(request):
    method, path, body = request
    if path != "/graphql":
        return request
    # Compare the operation and all its selections and variables. The reference batches
    # singleton repository queries, aliasing their root and prefixing variables with _0_.
    query = re.sub(r"#[^\n]*", "", body["query"])
    variables = body["variables"]
    if re.search(r"\b_0_repository\s*:\s*repository\(", query):
        query = re.sub(r"\b_0_repository\s*:\s*", "", query)
        query = query.replace("$_0_", "$")
        variables = {k.removeprefix("_0_"): v for k, v in variables.items()}
    tokens = re.findall(r'"(?:\\.|[^"\\])*"|\w+|[^\s,]', query)
    name = body.get("operationName", tokens[1])
    return (method, path, {**body, "query": tokens, "operationName": name, "variables": variables})


def bitbucket_requests(requests):
    # A change-request snapshot fans out five reads concurrently. Its second diffstat
    # page makes six requests in this fixture. Keep each snapshot and each write in order.
    ordered = []
    i = 0
    while i < len(requests):
        group = requests[i:i + 6]
        ends = sorted(r["path"].rsplit("/", 1)[-1] for r in group)
        if ends == ["7", "conflicts", "diffstat", "diffstat", "repositories", "statuses"]:
            size = 6
        elif ([r["method"] for r in requests[i:i + 2]] == ["GET", "GET"]
              and {r["path"].rsplit("/", 1)[-1] for r in requests[i:i + 2]}
              in ({"example-repo", "branching-model"}, {"7", "members"})):
            size = 2
        else:
            size = 1
        ordered.extend(sorted(requests[i:i + size], key=lambda r: json.dumps(r, sort_keys=True)))
        i += size
    return ordered


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--original-command", required=True)
    parser.add_argument("--rust-command", required=True)
    parser.add_argument("--dump", type=Path, help="Save answers and fake host operations")
    args = parser.parse_args()
    failures = 0
    with tempfile.TemporaryDirectory() as directory:
        for forge in ("github", "bitbucket"):
            rows = [run(cmd, Path(directory) / (forge + side), forge == "github") for side, cmd in
                    [("original", args.original_command), ("rust", args.rust_command)]]
            if args.dump:
                args.dump.mkdir(exist_ok=True, parents=True)
                (args.dump / (forge + ".json")).write_text(json.dumps(rows, indent=2))
            for original, rust in zip(*rows, strict=True):
                label, answer, requests, success = original
                good = answer == rust[1] and answer.get("isError") == (not success)
                if forge == "github":
                    requests_ok = list(map(github_request, requests)) == list(map(github_request, rust[2]))
                else:
                    requests_ok = bitbucket_requests(requests) == bitbucket_requests(rust[2])
                print(f"{'PASS' if good and requests_ok else 'FAIL'} {forge} {label}", flush=True)
                if not good:
                    print(f"  original {answer}\n  rust {rust[1]}")
                if not requests_ok:
                    for side, logs in [("original", requests), ("rust", rust[2])]:
                        print(" ", side, [str(r)[:200] for r in logs])
                failures += not (good and requests_ok)
    harness.assert_nothing_escaped()
    return int(failures != 0)


if __name__ == "__main__":
    raise SystemExit(main())
