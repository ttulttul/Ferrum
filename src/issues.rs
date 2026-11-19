use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::BTreeMap;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct IssueRef { pub project_id: String, pub tracker: String, pub scope: String, pub key: String, pub host: Option<String> }
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Issue { pub reference: IssueRef, pub title: String, pub body: String, pub state: String, pub labels: Vec<String>, pub assignees: Vec<String>, pub comments: Vec<Value> }
#[derive(Clone, Debug, Default)]
pub struct IssueService { issues: BTreeMap<String, Issue> }

impl IssueService {
    pub fn new() -> Self { Self::default() }
    pub fn list(&self, tracker: Option<&str>, state: Option<&str>) -> Value {
        let entries = self.issues.values().filter(|issue| tracker.is_none_or(|value| value == issue.reference.tracker)).filter(|issue| state.is_none_or(|value| value == "all" || (value == "open" && issue.state != "closed") || (value == "closed" && issue.state == "closed"))).map(|issue| json!({"tracker":issue.reference.tracker,"scope":issue.reference.scope,"key":issue.reference.key,"title":issue.title,"state":{"category":if issue.state == "closed" {"closed"} else {"open"},"name":issue.state},"labels":issue.labels,"assignees":issue.assignees})).collect::<Vec<_>>();
        json!({"trackers":[],"entries":entries,"errors":[],"nextCursors":{}})
    }
    pub fn create(&mut self, reference: IssueRef, title: String, body: String, labels: Vec<String>, assignees: Vec<String>) -> Value {
        let key = reference.key.clone(); let issue = Issue { reference: reference.clone(), title, body, state:"open".into(), labels, assignees, comments:vec![] }; self.issues.insert(key.clone(), issue); json!({"key":key,"displayKey":key,"url":format!("https://{}/issues/{}", reference.host.clone().unwrap_or_else(|| "issues.local".into()), reference.key)})
    }
    pub fn update(&mut self, key: &str, title: Option<String>, body: Option<String>, labels: Option<Vec<String>>, assignees: Option<Vec<String>>) -> Result<Value, Value> {
        let Some(issue) = self.issues.get_mut(key) else { return Err(self.unavailable("not-found", None)); }; if let Some(value)=title { issue.title=value; } if let Some(value)=body { issue.body=value; } if let Some(value)=labels { issue.labels=value; } if let Some(value)=assignees { issue.assignees=value; } Ok(json!({"updated":true}))
    }
    pub fn set_state(&mut self, key: &str, state: String) -> Result<Value, Value> { let Some(issue)=self.issues.get_mut(key) else { return Err(self.unavailable("not-found",None)); }; issue.state=state; Ok(json!({"updated":true})) }
    pub fn comment(&mut self, key: &str, body: String) -> Result<Value, Value> { let Some(issue)=self.issues.get_mut(key) else { return Err(self.unavailable("not-found",None)); }; let comment=json!({"id":issue.comments.len()+1,"body":body,"author":Value::Null}); issue.comments.push(comment.clone()); Ok(comment) }
    pub fn unavailable(&self, reason: &str, tracker: Option<&str>) -> Value { json!({"_tag":"IssueUnavailableError","reason":reason,"tracker":tracker,"message":"Issues cannot be browsed for this project yet."}) }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn reference() -> IssueRef { IssueRef { project_id:"p".into(), tracker:"github".into(), scope:"org/repo".into(), key:"1".into(), host:None } }
    #[test] fn issue_crud_is_deterministic() { let mut service=IssueService::new(); service.create(reference(),"Title".into(),"Body".into(),vec![],vec![]); assert_eq!(service.list(Some("github"),Some("open"))["entries"].as_array().unwrap().len(),1); service.set_state("1","closed".into()).unwrap(); assert_eq!(service.list(Some("github"),Some("open"))["entries"].as_array().unwrap().len(),0); }
    #[test] fn missing_issue_has_contract_error() { let service=IssueService::new(); assert_eq!(service.unavailable("not-found",None)["_tag"],"IssueUnavailableError"); }
}
