use super::{error, failure};
use crate::provider_rpc::{Failure, Github, Reply};
use serde_json::{json, Value};

pub(super) fn handles(op: &str) -> bool {
    matches!(
        op,
        "labelCandidates"
            | "reviewerCandidates"
            | "update"
            | "comment"
            | "updateComment"
            | "submitReview"
            | "replyToThread"
            | "setThreadResolution"
            | "setReaction"
            | "requestReviewers"
            | "setLabels"
    )
}
fn nodes(v: &Value) -> impl Iterator<Item = &Value> {
    v["nodes"]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|n| !n.is_null())
}
fn trimmed(v: &Value) -> Option<&str> {
    v.as_str().map(str::trim).filter(|s| !s.is_empty())
}
fn position(v: &Value) -> Option<(u64, &'static str)> {
    let left = match (v["kind"].as_str()?, v["side"].as_str()) {
        ("added", _) => false,
        ("deleted", _) => true,
        ("context", Some("left")) => true,
        ("context", Some("right")) => false,
        _ => return None,
    };
    let key = if left { "oldLine" } else { "newLine" };
    Some((
        v[key].as_u64().filter(|n| *n > 0)?,
        if left { "LEFT" } else { "RIGHT" },
    ))
}
fn review_submission(input: &Value, own: bool) -> Reply {
    let op = "submitReview";
    let comments = input["comments"]
        .as_array()
        .ok_or_else(|| error(op, "Invalid review comments."))?;
    let body = input["body"]
        .as_str()
        .ok_or_else(|| error(op, "Invalid review summary."))?;
    let verdict = input["verdict"].as_str().unwrap_or("");
    if verdict != "approve" && body.trim().is_empty() && comments.is_empty() {
        return Err(error(
            op,
            "A review needs a summary or at least one comment.",
        ));
    }
    let event = match verdict {
        "comment" => "COMMENT",
        "approve" => "APPROVE",
        "request-changes" => "REQUEST_CHANGES",
        _ => return Err(error(op, "Invalid review verdict.")),
    };
    if own && verdict != "comment" {
        return Err(error(
            op,
            &format!(
                "You need write access on this repository to {} a change request.",
                if verdict == "approve" {
                    "approve"
                } else {
                    "request changes on"
                }
            ),
        ));
    }
    let mut drafts = Vec::new();
    for c in comments {
        let invalid = || error(op, "Invalid review comment.");
        let (line, side) = position(&c["position"]).ok_or_else(invalid)?;
        let path = trimmed(&c["path"]).ok_or_else(invalid)?;
        let body = c["body"]
            .as_str()
            .filter(|s| !s.is_empty() && s.len() <= 65536)
            .ok_or_else(invalid)?;
        let mut draft = json!({"path":path});
        if let Some(start) = c.get("startPosition") {
            let (s, where_) = position(start).ok_or_else(invalid)?;
            if (s, where_) != (line, side) {
                draft["start_line"] = json!(s);
                draft["start_side"] = json!(where_)
            }
        }
        draft["line"] = json!(line);
        draft["side"] = json!(side);
        draft["body"] = json!(body);
        drafts.push(draft)
    }
    Ok(json!({"event":event,"body":body,"comments":drafts}))
}
fn encoded(s: &str) -> String {
    let mut out = String::new();
    for b in s.bytes() {
        if b.is_ascii_alphanumeric() || b"-_.!~*'()".contains(&b) {
            out.push(b as char)
        } else {
            out.push_str(&format!("%{b:02X}"))
        }
    }
    out
}

pub(super) async fn run(
    gh: &Github<'_>,
    op: &str,
    repo: &str,
    number: u64,
    input: &Value,
) -> Reply {
    let (owner, name) = repo.split_once('/').unwrap();
    let mut vars = json!({"owner":owner,"name":name,"number":number});
    let fail = |e| failure(op, e);
    let base=gh.graphql("query($owner:String!,$name:String!,$number:Int!){repository(owner:$owner,name:$name){viewerPermission pullRequest(number:$number){id viewerDidAuthor}}}",vars.clone()).await.map_err(fail)?;
    let pr = &base["repository"]["pullRequest"];
    let id = pr["id"].as_str().ok_or_else(|| fail(Failure::NotFound))?;
    let own = pr["viewerDidAuthor"] == true;
    let permission = base["repository"]["viewerPermission"]
        .as_str()
        .unwrap_or("");
    let can_write = super::detail::can_write(&base["repository"]);
    let can_triage = can_write || permission == "TRIAGE";
    if op == "reviewerCandidates" && !can_write {
        return Err(error(
            op,
            "You need write access on this repository to ask for a review.",
        ));
    }
    if op == "labelCandidates" && !can_triage {
        return Err(error(
            op,
            "You need triage access on this repository to change its labels.",
        ));
    }
    if matches!(op, "reviewerCandidates" | "labelCandidates") {
        let query = if op == "labelCandidates" {
            include_str!("LabelCandidates.graphql")
        } else {
            vars["query"] = input["query"].clone();
            include_str!("ReviewerCandidates.graphql")
        };
        let data = gh.graphql(query, vars).await.map_err(fail)?;
        let repository = &data["repository"];
        if repository.is_null() {
            return Err(fail(Failure::NotFound));
        }
        return if op == "labelCandidates" {
            Ok(labels(repository))
        } else {
            Ok(reviewers(repository, input["query"].as_str()))
        };
    }
    let api = format!("repos/{repo}/pulls/{number}");
    let mutation=match op{
        "comment"=>("mutation($id:ID!,$body:String!){addComment(input:{subjectId:$id,body:$body}){clientMutationId}}",json!({"id":id,"body":input["body"]})),
        "update"=>{
            let mut args=json!({"id":id});
            if let Some(title)=input.get("title"){args["title"]=json!(trimmed(title).filter(|v|v.len()<=1024).ok_or_else(||error(op,"Invalid title."))?)}
            if let Some(body)=input.get("body"){args["body"]=json!(body.as_str().filter(|v|v.len()<=65536).ok_or_else(||error(op,"Invalid body."))?)}
            if let Some(flag)=input.get("maintainerCanModify"){args["maintainerCanModify"]=json!(flag.as_bool().ok_or_else(||error(op,"Invalid maintainer edit setting."))?)}
            if args.as_object().unwrap().len()==1{return Err(error(op,"Nothing was changed."))}
            ("mutation($id:ID!,$title:String,$body:String,$maintainerCanModify:Boolean){updatePullRequest(input:{pullRequestId:$id,title:$title,body:$body,maintainerCanModify:$maintainerCanModify}){clientMutationId}}",args)
        }
        "submitReview"=>{
            let data=review_submission(input,own)?;
            gh.request("POST",&format!("{api}/reviews"),"application/vnd.github+json",Some(data)).await.map_err(fail)?;
            return Ok(Value::Null)
        }
        "setLabels"=>{
            if !can_triage{return Err(error(op,"You need triage access on this repository to change its labels."))}
            let labels=names(&input["labels"]).ok_or_else(||error(op,"Invalid labels."))?;
            let url=format!("repos/{repo}/issues/{number}/labels");
            match input["applied"].as_bool(){
                Some(true)=>{gh.request("POST",&url,"application/vnd.github+json",Some(json!({"labels":labels}))).await.map_err(fail)?;}
                Some(false)=>{for label in labels{if matches!(label,"."|".."){return Err(error(op,"Invalid label path."))}gh.request("DELETE",&format!("{url}/{}",encoded(label)),"application/vnd.github+json",None).await.map_err(fail)?;}}
                None=>return Err(error(op,"Invalid label change."))
            }return Ok(Value::Null)
        }
        "requestReviewers"=>{
            if !can_write{return Err(error(op,"You need write access on this repository to ask for a review."))}
            let mut users=Vec::new();let mut teams=Vec::new();
            let items=input["reviewers"].as_array().filter(|v|(1..=25).contains(&v.len())).ok_or_else(||error(op,"Invalid reviewer list."))?;
            for reviewer in items{let id=trimmed(&reviewer["id"]).ok_or_else(||error(op,"Invalid reviewer."))?;match reviewer["kind"].as_str(){Some("team")=>teams.push(id),Some("user")=>users.push(id),_=>return Err(error(op,"Invalid reviewer."))}}
            let method=match input["requested"].as_bool(){Some(true)=>"POST",Some(false)=>"DELETE",None=>return Err(error(op,"Invalid reviewer change."))};
            let body=json!({"reviewers":users,"team_reviewers":teams});
            gh.request(method,&format!("{api}/requested_reviewers"),"application/vnd.github+json",Some(body)).await.map_err(fail)?;
            return Ok(Value::Null)
        }
        _=>{
            let key=match op{"replyToThread"|"setThreadResolution"=>"threadId","updateComment"=>"commentId",_=>"subjectId"};
            let subject=match input.get(key){Some(value)=>trimmed(value).ok_or_else(||error(op,"Invalid subject."))?,None if op=="setReaction"=>id,_=>return Err(error(op,"Invalid subject."))};
            if subject!=id||op!="setReaction"{
                vars["subjectId"]=json!(subject);
                let result=gh.graphql(include_str!("PullRequestSubject.graphql"),vars).await.map_err(fail)?;
                let node=&result["node"];
                let valid=match (op,node["__typename"].as_str()){
                    ("setReaction",Some("PullRequest"))=>node["id"]==id,
                    ("setReaction",Some("PullRequestReview"))=>node["reviewPullRequest"]["id"]==id,
                    ("setReaction"|"updateComment",Some("IssueComment"))=>node["pullRequest"]["id"]==id&&(op=="setReaction"||input["kind"]=="issue-comment"),
                    ("setReaction"|"updateComment",Some("PullRequestReviewComment"))=>node["reviewCommentPullRequest"]["id"]==id&&(op=="setReaction"||input["kind"]=="review-comment"),
                    ("replyToThread"|"setThreadResolution",Some("PullRequestReviewThread"))=>node["threadPullRequest"]["id"]==id,
                    _=>false
                };
                if !valid{return Err(error(op,"The named subject did not belong to the named pull request."))}
            }
            match op{
                "setReaction"=>{
                    let content=match input["content"].as_str(){Some("thumbs-up")=>"THUMBS_UP",Some("thumbs-down")=>"THUMBS_DOWN",Some("laugh")=>"LAUGH",Some("hooray")=>"HOORAY",Some("heart")=>"HEART",Some("confused")=>"CONFUSED",Some("rocket")=>"ROCKET",Some("eyes")=>"EYES",_=>return Err(error(op,"Invalid reaction."))};
                    let action=match input["reacted"].as_bool(){Some(true)=>"addReaction",Some(false)=>"removeReaction",None=>return Err(error(op,"Invalid reaction."))};
                    let q=format!("mutation($id:ID!,$content:ReactionContent!){{{action}(input:{{subjectId:$id,content:$content}}){{clientMutationId}}}}");
                    gh.graphql(&q,json!({"id":subject,"content":content})).await.map_err(fail)?;return Ok(Value::Null)
                }
                "updateComment"=>(if input["kind"]=="issue-comment"{"mutation($id:ID!,$body:String!){updateIssueComment(input:{id:$id,body:$body}){clientMutationId}}"}else{"mutation($id:ID!,$body:String!){updatePullRequestReviewComment(input:{pullRequestReviewCommentId:$id,body:$body}){clientMutationId}}"},json!({"id":subject,"body":input["body"]})),
                "replyToThread"=>("mutation($id:ID!,$body:String!){addPullRequestReviewThreadReply(input:{pullRequestReviewThreadId:$id,body:$body}){clientMutationId}}",json!({"id":subject,"body":input["body"]})),
                "setThreadResolution"=>{
                    if !own&&!can_write{return Err(error(op,"You need write access on this repository, or to have opened this change request, to resolve a review conversation."))}
                    (match input["resolved"].as_bool(){Some(true)=>"mutation($id:ID!){resolveReviewThread(input:{threadId:$id}){clientMutationId}}",Some(false)=>"mutation($id:ID!){unresolveReviewThread(input:{threadId:$id}){clientMutationId}}",None=>return Err(error(op,"Invalid thread resolution."))},json!({"id":subject}))
                }
                _=>unreachable!()
            }
        }
    };
    gh.graphql(mutation.0, mutation.1).await.map_err(fail)?;
    Ok(Value::Null)
}
fn names(v: &Value) -> Option<Vec<&str>> {
    v.as_array()
        .filter(|a| (1..=25).contains(&a.len()))?
        .iter()
        .map(trimmed)
        .collect()
}

fn labels(repo: &Value) -> Value {
    let applied = nodes(&repo["pullRequest"]["labels"])
        .filter_map(|l| trimmed(&l["name"]))
        .collect::<Vec<_>>();
    let mut list = indexmap::IndexMap::new();
    for label in nodes(&repo["labels"]) {
        if let Some(name) = trimmed(&label["name"]) {
            list.insert(name,json!({"name":name,"color":trimmed(&label["color"]),"description":trimmed(&label["description"]),"isApplied":applied.contains(&name)}));
        }
    }
    let mut candidates = applied
        .iter()
        .filter(|key| !list.contains_key(**key))
        .map(|key| json!({"name":key,"color":null,"description":null,"isApplied":true}))
        .collect::<Vec<_>>();
    candidates.extend(list.into_values());
    json!({"candidates":candidates,"truncated":repo["labels"]["pageInfo"]["hasNextPage"]==true})
}
fn reviewers(repo: &Value, query: Option<&str>) -> Value {
    let pr = &repo["pullRequest"];
    let mut list = indexmap::IndexMap::new();
    for request in nodes(&pr["reviewRequests"]) {
        let r = &request["requestedReviewer"];
        let slug = trimmed(&r["slug"]);
        let Some(id) = slug.or_else(|| trimmed(&r["login"])) else {
            continue;
        };
        let kind = if slug.is_some() { "team" } else { "user" };
        let login = if slug.is_some() {
            trimmed(&r["combinedSlug"]).unwrap_or(id)
        } else {
            id
        };
        let (name, avatar) = if slug.is_some() {
            (&r["teamName"], &r["teamAvatarUrl"])
        } else {
            (&r["name"], &r["avatarUrl"])
        };
        list.insert((kind,id),json!({"login":login,"name":trimmed(name),"avatarUrl":trimmed(avatar),"id":id,"kind":kind,"isRequested":true}));
    }
    for user in nodes(&repo["assignableUsers"]) {
        if let Some(id) = trimmed(&user["login"]) {
            if pr["author"]["login"] != id {
                list.entry(("user",id)).or_insert_with(||json!({"login":id,"name":trimmed(&user["name"]),"avatarUrl":trimmed(&user["avatarUrl"]),"id":id,"kind":"user","isRequested":false}));
            }
        }
    }
    let host_matches = nodes(&repo["assignableUsers"])
        .filter_map(|u| trimmed(&u["login"]))
        .collect::<std::collections::HashSet<_>>();
    let candidates = list
        .into_values()
        .filter(|c| {
            query.is_none_or(|q| q.trim().is_empty())
                || (c["kind"] == "user"
                    && c["id"].as_str().is_some_and(|id| host_matches.contains(id)))
        })
        .collect::<Vec<_>>();
    json!({"candidates":candidates,"truncated":repo["assignableUsers"]["pageInfo"]["hasNextPage"]==true})
}
