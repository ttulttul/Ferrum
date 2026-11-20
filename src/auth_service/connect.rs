use super::*;
use crate::dpop_proof::CheckedProof;
impl AuthService {
    pub(super) fn save_session(records: &mut Credentials, s: Session, at: i64) -> (Value, String) {
        let subject = if s.owner {
            "desktop-bootstrap"
        } else if s.connect {
            "cloud-connect"
        } else {
            "one-time-token"
        };
        let mut claims = json!({"v":1,"kind":"session","sid":s.id,"sub":subject,"scopes":s.permissions,"method":s.method,"iat":at,"exp":s.expires});
        if let Some(proof) = &s.proof_key {
            claims["jkt"] = json!(proof);
        }
        if s.renewable_until.is_some() {
            claims["rnw"] = json!(1);
        }
        let token = signed(records.signing_key.get_or_insert_with(random_key), &claims);
        let response = if s.method == BROWSER {
            s.metadata()
        } else {
            json!({"access_token":token,"issued_token_type":"urn:ietf:params:oauth:token-type:access_token","token_type":if s.proof_key.is_some(){"DPoP"}else{"Bearer"},"expires_in":(s.expires-at)/1000,"scope":s.permissions.join(" ")})
        };
        records.sessions.insert(key(&token), s);
        (response, token)
    }
    fn consume(records: &mut Credentials, proof: &CheckedProof) -> Result<(), &'static str> {
        let at = now() / 1000;
        records.used_proofs.retain(|_, expires| *expires > at);
        if records
            .used_proofs
            .insert(
                key(&format!("{}:{}", proof.thumbprint, proof.jti)),
                proof.expires,
            )
            .is_some()
        {
            return Err("replay");
        }
        Ok(())
    }
    pub(crate) fn consume_proof(&self, proof: &CheckedProof) -> Result<(), &'static str> {
        self.records.update(|r| Self::consume(r, proof))
    }
    pub(crate) fn exchange_bound(
        &self,
        code: &str,
        scopes: Option<Vec<String>>,
        proof: &CheckedProof,
        renew: bool,
    ) -> Result<Value, &'static str> {
        if !renew {
            return self
                .issue(
                    code,
                    "dpop-access-token",
                    scopes,
                    None,
                    Some(&proof.thumbprint),
                )
                .map(|pair| pair.0);
        }
        if scopes.is_some() {
            return Err("invalid_scope");
        }
        self.update(|r| {
            let at = now();
            let mut s = r
                .sessions
                .get(&key(code))
                .filter(|s| {
                    s.proof_key.as_ref() == Some(&proof.thumbprint)
                        && s.renewable_until.is_some_and(|t| t > at)
                })
                .cloned()
                .ok_or("invalid_credential")?;
            Self::consume(r, proof)?;
            let renewed = Some(at + 30 * DAY);
            for old in r.sessions.values_mut().filter(|old| old.id == s.id) {
                old.renewable_until = renewed;
            }
            s.expires = at + 3_600_000;
            s.renewable_until = renewed;
            Ok(Self::save_session(r, s, at).0)
        })
    }
    pub(crate) fn issue_connect(
        &self,
        binding: &str,
        require_approval: bool,
    ) -> Result<(String, i64), &'static str> {
        self.update(|r| {
            let at = now();
            r.links.retain(|_, g| g.expires > at);
            let wait = require_approval && !r.devices.contains(binding);
            if wait {
                if let Some((code, g)) = r
                    .links
                    .values()
                    .filter(|g| g.binding.as_deref() == Some(binding))
                    .find_map(|g| Some((g.pending_code.as_ref()?, g)))
                {
                    return Ok((code.clone(), g.expires));
                }
            }
            let expires = at + if wait { 600_000 } else { 120_000 };
            let alphabet = b"23456789ABCDEFGHJKLMNPQRSTUVWXYZ";
            let code: String = (0..12)
                .map(|_| alphabet[OsRng.gen_range(0..alphabet.len())] as char)
                .collect();
            r.links.insert(
                key(&code),
                Grant {
                    id: Uuid::new_v4().to_string(),
                    created: at,
                    label: Some("T3 Connect".into()),
                    requested_client: None,
                    scopes: STANDARD.iter().map(|p| p.to_string()).collect(),
                    expires,
                    purpose: None,
                    access: None,
                    binding: Some(binding.into()),
                    pending_code: wait.then(|| code.clone()),
                },
            );
            Ok((code, expires))
        })
    }
    pub(crate) fn approve_device(&self, id: &str) -> Result<bool, &'static str> {
        self.update(|r| {
            let Some(bound) = r
                .links
                .values()
                .find(|g| g.id == id && g.expires > now() && g.pending_code.is_some())
                .and_then(|g| g.binding.as_ref())
            else {
                return Ok(false);
            };
            r.device_rows.insert(
                bound.clone(),
                (
                    now(),
                    r.links
                        .values()
                        .find(|g| g.id == id)
                        .and_then(|g| g.requested_client.clone()),
                ),
            );
            r.devices.insert(bound.clone());
            Ok(true)
        })
    }
    pub(super) fn remove_device(r: &mut Credentials, binding: &str) -> bool {
        r.sessions
            .retain(|_, s| s.proof_key.as_deref() != Some(binding));
        r.links.retain(|_, g| g.binding.as_deref() != Some(binding));
        r.device_rows.remove(binding);
        r.devices.remove(binding)
    }
    pub(crate) fn revoke_device(&self, binding: &str) -> Result<bool, &'static str> {
        let removed = self.update(|r| Ok(Self::remove_device(r, binding)));
        if removed.is_ok() {
            self.invalidations.send_replace(());
        }
        removed
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bound_device_waits_for_approval_and_revocation_survives_restart() {
        let dir = tempfile::tempdir().unwrap();
        let auth = AuthService::new().open(dir.path()).unwrap();
        let proof = CheckedProof {
            thumbprint: "device".into(),
            jti: "proof".into(),
            expires: now() / 1000 + 60,
        };
        let (code, expires) = auth.issue_connect("device", true).unwrap();
        assert_eq!(
            auth.issue_connect("device", true).unwrap(),
            (code.clone(), expires)
        );
        assert_eq!(
            auth.exchange_bound(&code, None, &proof, false),
            Err("connect_approval_pending")
        );
        assert!(auth.exchange(&code, None).is_err());
        assert!(auth
            .consume_approval_code(&code, "full-access", false)
            .is_err());
        let id = auth.records.read(|r| r.links[&key(&code)].id.clone());
        assert!(auth.approve_device(&id).unwrap());
        let t = auth.exchange_bound(&code, None, &proof, false).unwrap();
        let credential = t["access_token"].as_str().unwrap();
        assert_eq!(
            auth.principal(credential).unwrap().proof_key.as_deref(),
            Some("device")
        );
        let (outstanding, _) = auth.issue_connect("device", true).unwrap();
        let auth = AuthService::new().open(dir.path()).unwrap();
        assert!(auth.revoke_device("device").unwrap());
        assert!(auth
            .exchange_bound(&outstanding, None, &proof, false)
            .is_err());
        assert!(auth.principal(credential).is_none());
        let (new, _) = auth.issue_connect("device", true).unwrap();
        assert_eq!(
            auth.exchange_bound(&new, None, &proof, false),
            Err("connect_approval_pending")
        );
    }
}
