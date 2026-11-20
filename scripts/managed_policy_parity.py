#!/usr/bin/env python3
"""Compare managed settings reads, refusals, saved files and transport guards against upstream."""
import argparse
import os
import shutil
from contextlib import ExitStack
import json
from pathlib import Path
import secrets
import shlex
import subprocess
import tempfile
import time

from websockets.sync.client import connect

from parity_harness import encode, free_port, http, managed_server

def exercise(prefix, root, policy_path, seed, signing):
    with ExitStack() as stack:
        state=root/"userdata"
        state.mkdir(parents=True)
        (state/"settings.json").write_bytes(encode({"defaultAutoPull":True,"projectSettingsFolded":True,"defaultProjectScripts":[],
            "providerInstances":{"codex_work":{"driver":"codex","config":{"launchArgs":"--local"}}}}))
        # Never rely on the host's Tailscale configuration when exercising local-only policies.
        tools=root/"bin"
        tools.mkdir()
        cli=tools/"tailscale"
        cli.write_text("#!/bin/sh\ncase \"$1 $2\" in 'serve status') echo '{}' ;; *) exit 1 ;; esac\n")
        cli.chmod(0o755)
        prefix=["env","T3CODE_DEV_MANAGED_POLICY_PATH="+str(policy_path),"PATH="+str(tools)+":"+os.environ["PATH"]]+prefix
        url,cookie=managed_server(stack,prefix,root,seed,signing)
        ws=stack.enter_context(connect(url.replace("http:","ws:")+"/ws?orchestrationProtocol=2",additional_headers={"Cookie":cookie},proxy=None))
        observations={}
        def rpc(name,tag,body):
            ws.send(encode({"_tag":"Request","id":name,"tag":tag,"payload":body,"headers":[]}).decode())
            wire=ws.recv(timeout=12)
            observations[name]=wire
            return json.loads(wire)["exit"]
        def post(name,path,body):
            status,_,data=http(url,"POST",path,encode(body),{"Cookie":cookie,"content-type":"application/json"})
            observations[name]=(status,data)
            return status
        settings=rpc("managed settings","server.getSettings",{})["value"]
        assert settings["defaultAutoPull"] is False and settings["keepAwakeWhileAgentsWork"] is False
        assert settings["defaultRuntimeMode"]=="auto"
        assert settings["providerInstances"]["codex_work"]["config"]["launchArgs"]=="--corp"
        # The rest of getConfig includes live provider status and process-specific values.
        label="managed config summary"
        config=rpc(label,"server.getConfig",{})["value"]
        assert config["managedPolicy"]["organization"]=="Fixture Org"
        key='"managedPolicy":'
        field=observations[label].split(key,1)[1]
        _,end=json.JSONDecoder().raw_decode(field)
        observations[label]=field[:end]
        assert rpc("locked global","server.updateSettings",{"patch":{"defaultAutoPull":True}})["_tag"]=="Failure"
        assert rpc("locked project","server.updateSettings",{"patch":{"projectSettingsOverrides":{"p":{"defaultAutoPull":True}}}})["_tag"]=="Failure"
        for label, operation, instance_id, instance in [
            ("pinned provider", "upsert", "codex_work", {"driver":"codex","config":{"launchArgs":"--changed"}}),
            ("disallowed provider", "create", "pi-work", {"driver":"pi","enabled":True,"config":{}}),
        ]:
            mutation={"operation":operation,"instanceId":instance_id,"instance":instance}
            assert rpc(label,"server.updateSettings",{"patch":{},"providerInstanceMutation":mutation})["_tag"]=="Failure"
        create={"type":"thread.create","commandId":"managed-create","threadId":"policy-thread",
                "projectId":"policy-project","title":"Managed thread",
                "modelSelection":{"instanceId":"codex","model":"fixture"},"runtimeMode":"full-access",
                "interactionMode":"default","branch":None,"worktreePath":None,
                "createdBy":"user","creationSource":"web"}
        assert rpc("runtime ceiling","orchestration.dispatchCommand",create)["_tag"]=="Failure"
        assert rpc("echo policy","server.updateSettings",{"patch":{"defaultAutoPull":False,"keepAwakeWhileAgentsWork":True}})["_tag"]=="Success"
        observations["sparse settings"]=(state/"settings.json").read_bytes()
        assert json.loads(observations["sparse settings"])["defaultAutoPull"] is True
        assert post("managed pairing","/api/auth/pairing-token",{})==403
        assert post("managed Connect","/api/connect/preferences",{"publishAgentActivity":True})==403
        # server-runtime.json: same keys in the same order, and the same loaded
        # policy (source, organization and document fingerprint) byte for byte.
        record_path=state/"server-runtime.json"
        deadline=time.monotonic()+10
        while not record_path.exists() and time.monotonic()<deadline:
            time.sleep(0.05)
        raw=record_path.read_text()
        record=json.loads(raw)
        observations["runtime record keys"]=list(record)
        observations["runtime record host and origin"]=(record.get("host"),record["origin"].replace(str(record["port"]),"PORT"))
        observations["runtime record serverVersion"]=record["serverVersion"]
        policy_field=raw.split('"managedPolicy":',1)[1]
        _,end=json.JSONDecoder().raw_decode(policy_field)
        observations["runtime record managedPolicy"]=policy_field[:end]
        return observations

def run_policy(original_command,rust_command):
    with tempfile.TemporaryDirectory() as directory:
        base=Path(directory)
        path=base/"policy.json"
        path.write_bytes(encode({"schemaVersion":1,"organization":"Fixture Org","settings":{
            "defaultAutoPull":{"locked":True,"value":False},"defaultProjectScripts":{"locked":False,"value":[]},"keepAwakeWhileAgentsWork":{"locked":False,"value":False}},
            "limits":{"maxRuntimeMode":"auto","clients":["desktop"],"providers":{"allowedDrivers":["codex"],
                "instances":{"codex":{"locked":True,"launchArgs":"--corp"}}}}}))
        seed,sign=secrets.token_urlsafe(32),secrets.token_bytes(32)
        outputs=[]
        root=base/"server"
        for cmd in [original_command,rust_command]:
            outputs.append(exercise(shlex.split(cmd),root,path,seed,sign))
            shutil.rmtree(root)
        for name,expected in outputs[0].items():
            actual=outputs[1][name]
            if expected!=actual:
                index=next((i for i,(a,b) in enumerate(zip(expected,actual)) if a!=b),min(len(expected),len(actual)))
                raise AssertionError((name,index,expected[max(0,index-30):index+180],actual[max(0,index-30):index+180]))
            print("PASS",name)
        port=free_port()
        org={"schemaVersion":1,"organization":"Fixture Org"}
        for label,doc,tailscale,mode,reason,code in [
            ("invalid policy exit",{**org,"settings":{"compactIdleThreads":{"locked":True,"value":False}}},
             False,"desktop",'"compactIdleThreads" follows "idleCompaction"',77),
            ("client policy exit",{**org,"limits":{"clients":["desktop"]}},False,"web",
             "Fixture Org's policy does not allow browsers on this machine.",1),
            ("bootstrap Tailscale exit",{**org,"limits":{"clients":["desktop"]}},True,"desktop",
             "Fixture Org's policy does not allow remote clients, so Tailscale Serve cannot be turned on.",1),
        ]:
            path.write_bytes(encode(doc))
            for cmd in [original_command,rust_command]:
                root.mkdir()
                # Mask the host CLI even if a guard accidentally attempts to start Tailscale.
                cli=root/"tailscale"
                cli.write_text("#!/bin/sh\nexit 1\n")
                cli.chmod(0o755)
                envelope=dict(mode="desktop",noBrowser=True,host="127.0.0.1",port=port,t3Home=str(root),
                              desktopBootstrapToken=seed,tailscaleServeEnabled=tailscale,tailscaleServePort=443)
                with tempfile.TemporaryFile() as fd:
                    fd.write(encode(envelope))
                    fd.seek(0)
                    result=subprocess.run(shlex.split(cmd)+["--mode",mode,"--bootstrap-fd",str(fd.fileno())],
                        cwd=root,env={"HOME":str(root),"PATH":str(root)+":"+os.environ["PATH"],
                            "T3CODE_HOME":str(root),"T3CODE_DEV_MANAGED_POLICY_PATH":str(path)},
                        pass_fds=(fd.fileno(),),capture_output=True,text=True,timeout=35)
                assert result.returncode==code and reason in result.stdout+result.stderr, (label,result.returncode,result.stdout,result.stderr)
                shutil.rmtree(root)
            print("PASS",label)
def main():
    parser=argparse.ArgumentParser(description=__doc__)
    for k in ["original","rust"]:
        parser.add_argument("--"+k+"-command",required=True)
    args=parser.parse_args()
    run_policy(args.original_command,args.rust_command)

if __name__=="__main__":
    main()
