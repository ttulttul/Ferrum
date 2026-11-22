#!/usr/bin/env python3
"""Run the original and Rust servers against independent local, stateful Linear APIs."""
import argparse
from contextlib import ExitStack
from copy import deepcopy
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
import re
import secrets
import shlex
import tempfile
from threading import Thread

from websockets.sync.client import connect
import parity_harness as harness

TOKEN = "synthetic-linear-key"
STAMP = "2026-10-02T12:00:00.000Z"
USER = {"id":"U1","displayName":"sam","name":"Sam T.","email":"sam@example.test","avatarUrl":None}
LABEL = {"id":"L1","name":"bug","color":"#abcdef","description":"A regression"}
OPEN = {"id":"S1","name":"Todo","type":"unstarted","position":0}
DONE = {"id":"S2","name":"Done","type":"completed","position":1}


def page(rows):
    return {"nodes":rows,"pageInfo":{"hasNextPage":False,"endCursor":None}}


class Provider(BaseHTTPRequestHandler):
    def log_message(self, *_args):
        pass

    def answer(self, body, status=200):
        data=harness.encode(body)
        self.send_response(status)
        self.send_header("Content-Type","application/json")
        self.send_header("Content-Length",str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_POST(self):
        self.server.requests.append(f"POST {self.path}")
        if self.path != "/graphql" or self.headers.get("authorization")!=TOKEN:
            self.answer({"errors":[{"message":"Invalid token"}]},401)
            return
        request=json.loads(self.rfile.read(int(self.headers["content-length"])))
        name=request.get("operationName") or re.search(r"(?:query|mutation)\s+(\w+)", request["query"])[1]
        v=request.get("variables",{})
        team=self.server.team
        issue=self.server.issue
        if name=="T3Viewer":
            data={"viewer":USER,"teams":page([team])}
        elif name=="T3Team":
            data={"teams":page([team] if v["key"]=="ENG" else []),"issueLabels":page([])}
        elif name=="T3IssueCreate":
            self.server.writes.append((name,v))
            fields=v["input"]
            issue.update({"id":"I1","identifier":"ENG-1","number":1,"url":"https://linear.app/workspace/issue/ENG-1",
                "title":fields["title"],"description":fields["description"],"state":OPEN,"team":team,"creator":USER,
                "botActor":None,"assignee":USER if fields.get("assigneeId")=="U1" else None,"labels":page([LABEL] if "L1" in fields["labelIds"] else []),
                "createdAt":STAMP,"updatedAt":STAMP,"completedAt":None,"canceledAt":None,"comments":page([]),"reactions":[],
                "attachments":page([]),"history":page([])})
            data={"issueCreate":{"success":True,"issue":issue}}
        elif name=="T3Issue":
            if v["id"]=="ENG-98":
                self.answer({"errors":[{"message":"Issue not found"}]},400)
                return
            if v["id"]=="ENG-97":
                self.answer({"errors":[{"message":"Busy","extensions":{"code":"RATELIMITED"}}]},429)
                return
            if v["id"]=="ENG-96":
                self.answer({"errors":[{"message":"Unauthorized"}]},401)
                return
            if v["id"]=="ENG-95":
                self.answer({"errors":[{"message":"Denied for fixture"}]},400)
                return
            if v["id"]=="ENG-94":
                broken=deepcopy(issue)
                broken["comments"]["nodes"][0]["url"]=None
                self.answer({"data":{"issue":broken}})
                return
            if v["id"]=="OTHER-2":
                moved=deepcopy(issue)
                moved.update(identifier="OTHER-2",team={**team,"key":"OTHER"})
                data={"issue":moved}
            else:
                data={"issue":issue if v["id"]==issue.get("identifier") else None}
        elif name=="T3Issues":
            self.server.filters.append(v)
            data={"issues":page([issue] if issue else [])}
        elif name=="T3IssueUpdate":
            self.server.writes.append((name,v))
            fields=v["input"]
            assert v["id"]==issue["identifier"]
            for k in ("title","description"):
                if k in fields:
                    issue[k]=fields[k]
            if "stateId" in fields:
                issue["state"]=DONE if fields["stateId"]=="S2" else OPEN
                issue["completedAt"]=STAMP if issue["state"]==DONE else None
            if "labelIds" in fields:
                issue["labels"]=page([LABEL] if "L1" in fields["labelIds"] else [])
            if "assigneeId" in fields:
                issue["assignee"]=USER if fields["assigneeId"]=="U1" else None
            data={"issueUpdate":{"success":True}}
        elif name=="T3CommentCreate":
            self.server.writes.append((name,v))
            issue["comments"]["nodes"].append({"id":"C1","user":USER,"body":v["input"]["body"],"botActor":None,"createdAt":STAMP,"url":"https://linear.app/comment/C1","reactions":[]})
            data={"commentCreate":{"success":True}}
        elif name=="T3Comments":
            data={"issue":{"comments":issue["comments"]}}
        elif name in ("T3CommentReactions","T3IssueReactions"):
            if name=="T3IssueReactions":
                data={"issue":{"reactions":issue["reactions"]}}
            else:
                comment=issue["comments"]["nodes"][0]
                data={"comment":{"issue":{"id":issue["id"] if v["id"]==comment["id"] else "I9"},"reactions":comment["reactions"]}}
        elif name=="T3ReactionCreate":
            self.server.writes.append((name,v))
            fields=v["input"]
            target=issue if "issueId" in fields else issue["comments"]["nodes"][0]
            target["reactions"].append({"id":"R"+("1" if target is issue else "2"),"emoji":"heart","user":USER})
            data={"reactionCreate":{"success":True}}
        elif name=="T3ReactionDelete":
            self.server.writes.append((name,v))
            for target in [issue, *issue["comments"]["nodes"]]:
                target["reactions"]=[r for r in target["reactions"] if r["id"]!=v["id"]]
            data={"reactionDelete":{"success":True}}
        else:
            self.answer({"errors":[{"message":"Unknown fixture operation"}]},400)
            return
        self.answer({"data":data})


def linear_fixture(stack):
    host=stack.enter_context(ThreadingHTTPServer(("127.0.0.1",0),Provider))
    host.team={"id":"T1","key":"ENG","name":"Engineering","states":page([DONE,OPEN]),"members":page([USER]),
               "labels":page([LABEL]),"templates":page([{"id":"TEMPLATE","name":"Bug","description":"File a bug",
               "content":"How to reproduce","type":"issue","hasFormFields":False,"templateData":{"title":"Bug", "labelIds":["L1"]}}])}
    host.issue,host.writes,host.filters,host.requests={},[],[],[]
    thread=Thread(target=host.serve_forever,daemon=True)
    thread.start()
    stack.callback(thread.join)
    stack.callback(host.shutdown)
    return host


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--original-command",required=True)
    parser.add_argument("--rust-command",required=True)
    args=parser.parse_args()
    with ExitStack() as stack:
        root=Path(stack.enter_context(tempfile.TemporaryDirectory(prefix="t3-linear-parity-")))
        checkout=root/"project"
        checkout.mkdir()
        providers=[]
        sockets=[]
        secret=secrets.token_urlsafe(32)
        signing=secrets.token_bytes(32)
        for who,command in (("original",args.original_command),("rust",args.rust_command)):
            host=linear_fixture(stack)
            providers.append(host)
            prefix=["env",f"T3CODE_LINEAR_API_URL=http://127.0.0.1:{host.server_port}/graphql"]
            url,cookie=harness.managed_server(stack,prefix+shlex.split(command),root/who,secret,signing)
            cmd={"type":"project.create","commandId":"add","projectId":"test-project","title":"Work","workspaceRoot":str(checkout)}
            code,_,body=harness.http(url,"POST","/api/projects/mutate",harness.encode(cmd),{"Cookie":cookie,"Content-Type":"application/json"})
            if code!=200:
                raise RuntimeError(f"registering project failed: {code} {body!r}")
            sockets.append(stack.enter_context(connect(url.replace("http","ws",1)+"/ws?orchestrationProtocol=2",
                additional_headers={"Cookie":cookie},proxy=None,compression=None)))
        proj={"projectId":"test-project"}
        scope={**proj,"tracker":"linear","scope":"ENG"}
        ref={**scope,"key":"ENG-1"}
        body={"title":"First","body":"Description","labels":["bug"],"assignees":["sam"]}
        reaction={**ref,"content":"heart"}
        scenarios=[
            ("issueTrackers.connect",{"tracker":"linear","token":TOKEN},True),
            ("issueTrackers.connect",{"tracker":"linear","token":"rejected-fixture-key"},False),
            ("issueTrackers.bindProject",{**proj,"binding":{"tracker":"linear","scope":"ENG"}},True),
            ("issueTrackers.list",{},True),
            ("issues.candidates",scope,True),("issues.templates",scope,True),
            ("issues.create",{**scope,**body},True),("issues.detail",ref,True),
            ("issues.list",{**proj,"state":"open","involvement":"assigned","query":"#1","labels":["bug"]},True),
            ("issues.comment",{**ref,"body":"Discussion"},True),
            ("issues.comments",{**ref,"cursor":"more"},True),
            ("issues.update",{**ref,"title":"Revised","body":"Changed","labels":[],"assignees":[]},True),
            ("issues.setState",{**ref,"state":"Done"},True),
            ("issues.react",{**reaction,"reacted":True},True),("issues.react",{**reaction,"reacted":True,"subjectId":"C1"},True),
            ("issues.detail",ref,True),
            ("issues.react",{**reaction,"reacted":False},True),("issues.react",{**reaction,"reacted":False,"subjectId":"C1"},True),
            ("issues.react",{**reaction,"reacted":False,"subjectId":"C1"},True),
            ("issues.react",{**reaction,"reacted":True,"subjectId":"other"},False),
            ("issues.create",{**scope,**body,"scope":"OTHER"},False),
            ("issues.update",{**ref,"key":"OTHER-2","title":"Wrong team"},False),
            ("issues.setState",{**ref,"state":"Unknown"},False),
            ("issues.update",{**ref,"labels":["missing"]},False),("issues.update",{**ref,"assignees":["a","b"]},False),
            *[("issues.detail",{**ref,"key":f"ENG-{n}"},False) for n in range(94,100)],("issues.detail",{**ref,"key":"bad"},False),
            ("issues.detail",ref,True),("issueTrackers.connect",{"tracker":"linear","token":None},True),
            ("issues.detail",ref,False),
        ]
        failures=0
        for i,(method,payload,success) in enumerate(scenarios):
            data=harness.encode({"_tag":"Request","tag":method,"id":str(i),"headers":[],"payload":payload}).decode()
            for ws in sockets:
                ws.send(data)
            results=[ws.recv(timeout=20) for ws in sockets]
            ok=results[0]==results[1] and json.loads(results[0])["exit"]["_tag"]==("Success" if success else "Failure")
            print(("PASS" if ok else "FAIL"),method,i)
            if not ok:
                failures+=1
                print(*results,sep="\n")
        expected=[
            ("T3IssueCreate",{"input":{"teamId":"T1","title":"First","description":"Description","labelIds":["L1"],"assigneeId":"U1"}}),
            ("T3CommentCreate",{"input":{"issueId":"I1","body":"Discussion"}}),
            ("T3IssueUpdate",{"id":"ENG-1","input":{"title":"Revised","description":"Changed","labelIds":[],"assigneeId":None}}),
            ("T3IssueUpdate",{"id":"ENG-1","input":{"stateId":"S2"}}),
            ("T3ReactionCreate",{"input":{"issueId":"I1","emoji":"❤️"}}),("T3ReactionCreate",{"input":{"commentId":"C1","emoji":"❤️"}}),
            ("T3ReactionDelete",{"id":"R1"}),("T3ReactionDelete",{"id":"R2"})]
        for provider in providers:
            if provider.writes!=expected:
                failures+=1
                print("FAIL provider mutation log",provider.writes)
        if not providers[0].filters or providers[0].filters!=providers[1].filters:
            failures+=1
            print("FAIL list filters",*(p.filters for p in providers))
        print(f"{len(scenarios)} client comparisons, both provider write logs and list filters checked")
        return int(bool(failures))

if __name__=="__main__":
    raise SystemExit(main())
