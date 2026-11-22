#!/usr/bin/env python3
"""Compare config snapshot wire fields and complete settings/keybinding change frames."""
import argparse
from contextlib import ExitStack
import json
from pathlib import Path
import secrets
import shlex
import subprocess
from tempfile import TemporaryDirectory
from time import monotonic

from config_parity import frame, identical, socket
from parity_harness import encode, managed_server


def parts(raw):
    """Spans come from the original RPC frame. Preserve number and string spellings."""
    decoder = json.JSONDecoder()
    if raw.startswith("["):
        out = []
    elif raw.startswith("{"):
        out = {}
    else:
        raise ValueError("Expected array or object.")
    i = 1
    while raw[i] not in "}]":
        if isinstance(out, dict):
            key, i = decoder.raw_decode(raw, i)
            assert raw[i] == ":"
            i += 1
        _, end = decoder.raw_decode(raw, i)
        if isinstance(out, list):
            out.append(raw[i:end])
        else:
            out[key] = raw[i:end]
        i = end
        if raw[i] == ",":
            i += 1
    return out


def snapshot_fields(frame, stream):
    root = parts(frame.decode())
    return (parts(parts(root["values"])[0])["config"] if stream else parts(root["exit"])["value"])


def select(label, raws, keys):
    objects = [parts(raw) for raw in raws]
    for key in keys:
        identical(f"{label}.{key}", [obj[key].encode() for obj in objects])


def snapshot_pair(frames, stream):
    raw = [snapshot_fields(f, stream) for f in frames]
    label = "stream" if stream else "unary"
    select(label, raw, ("hostname", "keybindings", "issues", "availableEditors", "directEndpoints",
        "settings", "shellResumeCompletionMarker","shellThreadFamilies","threadLinkMetadata","shellOlderSettled",
        "threadResumeCompletionMarker","threadSnapshotPagination","threadFind","threadFindProgressive",
        "secretProviderSettings","secretReferences","secretRequestReferences"))
    a,b = [parts(x) for x in raw]
    if not stream:
        select(label, raw, ["remoteOpenTargets"])
    select(label+".auth", [a["auth"],b["auth"]], ("policy","bootstrapMethods","sessionMethods","serverUpdateScope"))
    select(label+".observability", [a["observability"],b["observability"]],
           ("localTracingEnabled","otlpTracesEnabled","otlpMetricsEnabled","otlpLogsEnabled"))
    rows = [parts(obj["providers"]) for obj in (a,b)]
    assert len(rows[0]) == len(rows[1]) == 10
    for pair in zip(*rows):
        provider = json.loads(pair[0])
        keys = ["instanceId","driver","displayName","showInteractionModeToggle","setup","enabled",
                "models","slashCommands","skills"]
        for k in ("reportsContextWindow","supportedRuntimeModes","supportsConversationRollback","supportsTextGeneration","catalogDigest"):
            if k in provider:
                keys.append(k)
        if not provider["enabled"]:
            keys += ["installed","version","status","auth","message"]
            if provider["driver"] not in ("codex","claudeAgent"):
                keys.append("continuation")
        select(label+"."+provider["driver"],pair,keys)
    return json.loads(raw[1])


def receive(ws, kind):
    deadline = monotonic() + 15
    while deadline > monotonic():
        packet = ws.recv(timeout=deadline-monotonic()).encode()
        frame = json.loads(packet)
        if frame.get("_tag") != "Chunk":
            raise AssertionError(f"expected stream data, got {frame}")
        ws.send(encode({"_tag":"Ack","requestId":"config-stream"}).decode())
        if frame["values"][0]["type"] == kind:
            return packet
    raise AssertionError(f"missing {kind}")


def verify_schema(root, node, samples):
    program = """
import {createRequire} from "node:module";
import {pathToFileURL} from "node:url";
import path from "node:path";
import fs from "node:fs";
const root=process.argv[1];
const require=createRequire(path.join(root,"apps/server/package.json"));
const schema=await import(pathToFileURL(require.resolve("effect/Schema")));
const contracts=await import(pathToFileURL(path.join(root,"packages/contracts/src/index.ts")));
for(const [type,data] of JSON.parse(fs.readFileSync(0,"utf8"))){
  const value=schema.decodeUnknownSync(contracts[type])(data);
  // Provider arrays are forward-compatible. A silently dropped entry is not success.
  const config=type==="ServerConfig"?value:value.config;
  if(config && config.providers.length!==10) throw new Error("schema dropped a provider");
}
"""
    # parity-env: Node reading the reference's config, not a T3 server
    subprocess.run([node, "--input-type=module", "-e", program, str(root.resolve())],
                   input=encode(samples),check=True)


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--original-command",required=True)
    parser.add_argument("--rust-command",required=True)
    parser.add_argument("--reference-root",type=Path)
    parser.add_argument("--node",default="node")
    args=parser.parse_args()
    with TemporaryDirectory() as directory,ExitStack() as stack:
        root=Path(directory)
        homes=[root/"original",root/"rust"]
        # Keep CLI account status and changing model catalogs out of this deterministic scenario.
        for home in homes:
            state=home/"userdata"
            state.mkdir(parents=True)
            (state/"settings.json").write_bytes(encode({"providerInstances":{
                "codex":{"driver":"codex","enabled":False},"claudeAgent":{"driver":"claudeAgent","enabled":False}}}))
        credential,key=secrets.token_urlsafe(32),secrets.token_bytes(32)
        servers=[managed_server(stack,shlex.split(cmd),home,credential,key)
                 for cmd,home in zip((args.original_command,args.rust_command),homes)]
        # Neither server reaches the upstream model manifest (parity_harness.offline_env), so both
        # list models from the bundled copy, which is the same file.
        clients=[socket(stack,*s,True) for s in servers]
        # The reference builds a provider row's models from the catalog as of that row's last
        # check, which can race its start-up manifest fetch. The fetch is refused here, but
        # refresh both servers anyway so each row reflects the manifest it now holds.
        for ws in clients:
            frame(ws,"refresh-providers","server.refreshProviders",{})
        streams=[socket(stack,*s,True) for s in servers]
        unary=[frame(ws,"config","server.getConfig",{}) for ws in clients]
        samples=[["ServerConfig",snapshot_pair(unary,False)]]
        options={"workspaceSnapshotsOnDemand":True,"providerCatalogsOnDemand":True,"providerCatalogDigests":True}
        initial=[frame(ws,"config-stream","subscribeServerConfig",options) for ws in streams]
        samples.append(["ServerConfigStreamEvent",{"version":1,"type":"snapshot","config":snapshot_pair(initial,True)}])
        for ws in streams:
            ws.send(encode({"_tag":"Ack","requestId":"config-stream"}).decode())
        for tag in ("work","personal"):
            responses=[frame(ws,tag,"server.updateSettings",{"patch":{"hiddenThreadTags":[tag]}}) for ws in clients]
            identical(tag+" RPC response",responses)
        # Each RPC emits immediately. Both atomic saves also wake the debounced file watcher.
        for label in ("work","personal","personal"):
            packets=[receive(ws,"settingsUpdated") for ws in streams]
            identical(label+" full event",packets)
            event=json.loads(packets[1])["values"][0]
            assert event["payload"]["settings"]["hiddenThreadTags"] == [label]
            samples.append(["ServerConfigStreamEvent",event])
        # A repeated patch and the ensuing file change each publish even when the public view stays equal.
        identical("no-op response",[frame(ws,"again","server.updateSettings",{"patch":{"hiddenThreadTags":["personal"]}}) for ws in clients])
        for _ in range(2):
            packets=[receive(ws,"settingsUpdated") for ws in streams]
            identical("no-op publication",packets)
            samples.append(["ServerConfigStreamEvent",json.loads(packets[1])["values"][0]])
        for home in homes:
            (home/"userdata/settings.json").write_bytes(encode({"hiddenThreadTags":["from-file"]}))
        packets=[receive(ws,"settingsUpdated") for ws in streams]
        identical("external settings edit full event",packets)
        event=json.loads(packets[1])["values"][0]
        assert event["payload"]["settings"]["hiddenThreadTags"]==["from-file"]
        samples.append(["ServerConfigStreamEvent",event])
        for home in homes:
            (home/"userdata/keybindings.json").write_bytes(encode([{"key":"ctrl+esc","command":"terminal.toggle",
                                                                    "when":"(composerFocus || !chatFocus) && active"}]))
        packets=[receive(ws,"keybindingsUpdated") for ws in streams]
        identical("keybinding edit full event",packets)
        samples.append(["ServerConfigStreamEvent",json.loads(packets[1])["values"][0]])
        if args.reference_root:
            verify_schema(args.reference_root,args.node,samples)
    print("Matched deterministic unary/stream config fields and seven full event frames.")
    if args.reference_root:
        print("Upstream TypeScript schemas accepted all nine Rust samples.")


if __name__=="__main__":
    main()
