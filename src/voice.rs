use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum CloudProvider {
    Openai,
    Xai,
    Openrouter,
}

impl CloudProvider {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Openai => "openai",
            Self::Xai => "xai",
            Self::Openrouter => "openrouter",
        }
    }
    pub fn display_name(self) -> &'static str {
        match self {
            Self::Openai => "OpenAI",
            Self::Xai => "Grok Voice Transcribe",
            Self::Openrouter => "OpenRouter",
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct VoiceService {
    keys: [bool; 3],
    models: Vec<(String, String)>,
}

impl VoiceService {
    pub fn new() -> Self {
        Self {
            keys: [false, false, false],
            models: Vec::new(),
        }
    }
    pub fn set_key_configured(&mut self, provider: CloudProvider, configured: bool) {
        self.keys[index(provider)] = configured;
    }
    pub fn add_model(&mut self, id: impl Into<String>, name: impl Into<String>) {
        self.models.push((id.into(), name.into()));
    }
    pub fn cloud_keys(&self) -> Value {
        json!({"openai":self.keys[0],"xai":self.keys[1],"openrouter":self.keys[2]})
    }
    pub fn cloud_models(&self) -> Value {
        json!({"models":self.models.iter().map(|(id,name)| json!({"id":id,"name":name})).collect::<Vec<_>>()})
    }
    pub fn validate_provider_model(
        &self,
        provider: CloudProvider,
        model: Option<&str>,
    ) -> Result<(), Value> {
        if !self.keys[index(provider)] {
            return Err(cloud_error(provider, "missing-key"));
        }
        if let Some(model) = model {
            if !self.models.iter().any(|(id, _)| id == model) {
                return Err(cloud_error(provider, "model-unavailable"));
            }
        }
        Ok(())
    }
    pub fn post_process(&self, text: &str, custom_words: &[String]) -> String {
        let mut output = text.to_owned();
        for filler in ["um", "uh", "erm", "hmm"] {
            output = remove_word(&output, filler);
        }
        for word in custom_words {
            output = remove_word(&output, word);
        }
        output.split_whitespace().collect::<Vec<_>>().join(" ")
    }
    pub fn transcribe_unavailable(&self, provider: CloudProvider) -> Value {
        cloud_error(provider, "unavailable")
    }
}

fn index(provider: CloudProvider) -> usize {
    match provider {
        CloudProvider::Openai => 0,
        CloudProvider::Xai => 1,
        CloudProvider::Openrouter => 2,
    }
}

fn cloud_error(provider: CloudProvider, reason: &str) -> Value {
    let message = match reason {
        "missing-key" => format!(
            "Add your {} API key in Settings → Voice.",
            provider.display_name()
        ),
        "model-unavailable" => format!(
            "The chosen model isn’t available on {}. Choose another model in Settings → Voice.",
            provider.display_name()
        ),
        "unavailable" => format!(
            "{} couldn’t transcribe this recording. Try again later.",
            provider.display_name()
        ),
        _ => format!("{} speech request failed.", provider.display_name()),
    };
    json!({"_tag":"CloudSpeechError","provider":provider.as_str(),"reason":reason,"message":message})
}

fn remove_word(input: &str, word: &str) -> String {
    input
        .split_whitespace()
        .filter(|part| {
            !part
                .trim_matches(|c: char| !c.is_alphanumeric())
                .eq_ignore_ascii_case(word)
        })
        .fold(String::new(), |mut output, part| {
            if !output.is_empty() {
                output.push(' ');
            }
            if !part
                .trim_matches(|c: char| !c.is_alphanumeric())
                .eq_ignore_ascii_case(word)
            {
                output.push_str(part);
            }
            output
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn reports_missing_key() {
        let service = VoiceService::new();
        let error = service
            .validate_provider_model(CloudProvider::Openai, None)
            .unwrap_err();
        assert_eq!(error["reason"], "missing-key");
    }
    #[test]
    fn filters_fillers_and_custom_words() {
        let service = VoiceService::new();
        assert_eq!(
            service.post_process("um hello, uh world", &["world".into()]),
            "hello,"
        );
    }
}
