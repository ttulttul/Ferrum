#!/usr/bin/env python3
"""Compare ci_rerun tool answers, watch state, and every request to local forge fixtures."""
import argparse
from contextlib import ExitStack
from copy import deepcopy
from http.server import ThreadingHTTPServer
import json
from pathlib import Path
import re
import secrets
import shlex
import tempfile
import threading
import time

import bitbucket_pull_request_parity as bb
import github_provider_parity as gh
import parity_harness as harness
from client_thread_parity import FIXTURES, rpc, scenario_env, seed_turn
from peer_forwarding_parity import McpClient

ROOT = f"/repos/{gh.REPO}/actions"
URL = f"https://github.com/{gh.REPO}"
CHECKS = [
    ("one", "FAILURE", 10, 11), ("two", "CANCELLED", 10, 12),
    ("other", "SUCCESS", 20, 21), ("external", "FAILURE", None, None),
]


class Actions(gh.Provider):
    def do_GET(self):
        self.server.calls.append(("GET", self.path, None))
        if self.path.startswith(ROOT + "/jobs/"):
            job = int(self.path.rsplit("/", 1)[-1])
            if self.server.mode == "missing job":
                return self.reply(404, {"message": "No such job"})
            if self.server.mode == "malformed job":
                return self.reply(200, {})
            return self.reply(200, {"run_id": job // 10 * 10})
        if self.path.startswith(ROOT + "/runs/10/jobs?"):
            ids = [11, 12, 13] if self.server.mode == "subset" else [11, 12]
            return self.reply(200, {"total_count": 101 if self.server.mode == "paged" else len(ids),
                                    "jobs": [{"id": i, "conclusion":"failure"} for i in ids]})
        return super().do_GET()

    def do_POST(self):
        if self.path == "/graphql":
            data = self.read_json()
            self.server.calls.append(("POST", self.path, data))
            repository = deepcopy(gh.REPOSITORY)
            checks = repository["pullRequest"]["commits"]["nodes"][0]["commit"]["statusCheckRollup"]["contexts"]
            sample = checks["nodes"][0]
            checks["nodes"] = [
                {**sample, "name": name, "conclusion": status if self.server.mode != "green" else "SUCCESS",
                 "detailsUrl": (f"{URL}/actions/runs/{run}/job/{job}" if run else "https://example.invalid/build")}
                for name, status, run, job in CHECKS]
            return self.reply(200, {"data": {"repository": repository}})
        self.server.calls.append(("POST", self.path, self.rfile.read(int(self.headers.get("Content-Length", 0))).decode()))
        if self.path in (ROOT + "/runs/10/rerun-failed-jobs", ROOT + "/jobs/11/rerun", ROOT + "/jobs/21/rerun"):
            if self.server.mode == "refusal details":
                return self.reply(422, {"message": "Run in progress", "errors": [
                    "Try again later", {"field": "run_id", "code": "busy"}]})
            return self.reply(422 if self.server.mode == "refusal" else 201, {})
        return self.reply(404, {"message": "Unknown fixture write"})


def connect(url, log):
    deadline = time.monotonic() + 45
    while time.monotonic() < deadline:
        for line in log.read_text().splitlines() if log.exists() else ():
            try:
                record = json.loads(line)
            except ValueError:
                continue
            if record.get("method") == "thread/start":
                mcp = record["params"]["config"]["mcp_servers.t3-code"]
                assert mcp["url"] == url + "/mcp"
                return McpClient(url, mcp["http_headers"]["Authorization"].removeprefix("Bearer "))
        time.sleep(.1)
    raise RuntimeError("fake Codex did not receive its thread credential")


def run(command, root, github):
    root.mkdir()
    with ExitStack() as stack:
        scenario = dict(name="rerun", instance="codex", model="fixture", scenario="codex_turn.json")
        home = root / "server"
        env, _, log = scenario_env(root, "rerun", home, scenario, {"codex"})
        fixture = json.loads((FIXTURES / "codex_turn.json").read_text())
        i = next(i for i, step in enumerate(fixture["steps"]) if step.get("send", {}).get("method") == "turn/started")
        fixture["steps"].insert(i + 1, {"expect":"turn/interrupt"})
        playback = root / "turn.json"
        playback.write_text(json.dumps(fixture))
        calls = []
        if github:
            host, credentials = gh.github_fixture(stack, root / "github", Actions)
            host.calls, host.mode = calls, ""
            remote = URL + ".git"
        else:
            host = stack.enter_context(ThreadingHTTPServer(("127.0.0.1", 0), bb.fake_bitbucket(calls)))
            threading.Thread(target=host.serve_forever, daemon=True).start()
            stack.callback(host.shutdown)
            credentials = {"T3CODE_BITBUCKET_API_BASE_URL": f"http://127.0.0.1:{host.server_port}/2.0",
                           "T3CODE_BITBUCKET_ACCESS_TOKEN": bb.TOKEN}
            remote = f"https://bitbucket.org/{bb.WRITE}.git"
        env.update(credentials, FAKE_PROVIDER_SCENARIO=str(playback), GIT_ALLOW_PROTOCOL="file")
        url, cookie = harness.managed_server(stack, shlex.split(command), home, secrets.token_urlsafe(32),
                                            secrets.token_bytes(32), env=env)
        seed = seed_turn(url, cookie, root, scenario, remote=remote)
        rpc(url, cookie, "orchestration.dispatchCommand", seed["dispatch"])
        client = connect(url, log)
        cases = [("branch default", {}), ("unsupported", {"number":7})]
        if github:
            cases = [("branch default", {}), ("green", {}), ("all", {}), ("subset", {}), ("paged", {}),
                     ("named", {"check":" OtHeR "}), ("unknown", {"check":"absent"}),
                     ("external", {"check":"external"}), ("missing job", {"check":"one"}),
                     ("malformed job", {"check":"one"}), ("refusal", {"check":"one"}),
                     ("refusal details", {"check":"one"}), ("watched", {"check":"one"})]
        rows = []
        for label, extra in cases:
            host.mode = label
            args = ({"number":3, **extra} if github and label != "branch default" else extra)
            if label == "refusal":
                rpc(url, cookie, "orchestration.dispatchCommand",
                    {"type":"thread.pull-request.watch", "threadId":seed["threadId"], "commandId":"seed-watch",
                     "host":"unconfigured.invalid", "repository":"other/project", "number":7, "watching":True, "checksPending":True,
                     "link":{"url":"https://unconfigured.invalid/other/project/pull/7", "source":"agent"}})
            calls.clear()
            answer = client.call("ci_rerun", args)
            current = list(calls)
            watch = None
            if label in ("refusal", "watched"):
                projection = rpc(url, cookie, "orchestration.getThreadProjection", {"threadId":seed["threadId"]})
                link = next(x for x in projection["thread"]["pullRequests"] if x["number"] == 7)
                watch = link["watch"].get("checksPending", "reopened")
            rows.append((label, answer, current, watch))
        return rows


def comparable(requests):
    # Job lookups may complete in either order. Keep every read and all write boundaries.
    result, reads = [], []
    for method, path, data in requests:
        if path == "/graphql":
            query = re.sub(r"#[^\n]*", "", data["query"])
            tokens = re.findall(r"\w+|[^\s,]", query)
            data = {**data, "query":tokens, "operationName":data.get("operationName", tokens[1])}
        request = (method, path, data)
        if method == "GET" and re.fullmatch(ROOT + r"/jobs/\d+", path):
            reads.append(request)
        else:
            result.extend(sorted(reads))
            reads.clear()
            result.append(request)
    return result + sorted(reads)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--original-command", required=True)
    parser.add_argument("--rust-command", required=True)
    parser.add_argument("--dump", type=Path)
    args = parser.parse_args()
    failed = 0
    with tempfile.TemporaryDirectory() as directory:
        for github in (True, False):
            forge = "github" if github else "bitbucket"
            rows = [run(cmd, Path(directory) / (forge + str(i)), github)
                    for i, cmd in enumerate((args.original_command, args.rust_command))]
            if args.dump:
                args.dump.mkdir(exist_ok=True, parents=True)
                (args.dump / (forge + ".json")).write_text(json.dumps(rows, indent=2))
            for (label, a, ar, aw), (_, b, br, bw) in zip(*rows, strict=True):
                requests_match = comparable(ar) == comparable(br) if github else ar == br == []
                ok = a == b and aw == bw and requests_match
                if label in ("refusal", "watched"):
                    ok = ok and aw == (True if label == "refusal" else "reopened")
                print("PASS" if ok else "FAIL", forge, label, flush=True)
                if not ok:
                    print("  answers", a, b, "watch", aw, bw)
                    print("  requests", [[r[:2] for r in side] for side in (ar, br)])
                failed += not ok
    harness.assert_nothing_escaped()
    return int(bool(failed))


if __name__ == "__main__":
    raise SystemExit(main())
