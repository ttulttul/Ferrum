//! JSON.stringify uses ECMAScript number formatting and enumerates array-index
//! property names before other properties, even in ordinary objects.
use serde::{
    ser::{SerializeMap, SerializeSeq},
    Serialize, Serializer,
};
use serde_json::{value::RawValue, Value};

struct Js<'a>(&'a Value);

fn array_index(key: &str) -> Option<u32> {
    let n = key.parse::<u32>().ok()?;
    (n < u32::MAX && n.to_string() == key).then_some(n)
}
impl Serialize for Js<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self.0 {
            Value::Array(values) => {
                let mut seq = serializer.serialize_seq(Some(values.len()))?;
                for v in values {
                    seq.serialize_element(&Js(v))?
                }
                seq.end()
            }
            Value::Object(obj) => {
                let mut map = serializer.serialize_map(Some(obj.len()))?;
                let mut indexed = obj
                    .iter()
                    .filter_map(|(k, v)| array_index(k).map(|n| (n, k, v)))
                    .collect::<Vec<_>>();
                if indexed.is_empty() {
                    for (k, v) in obj {
                        map.serialize_entry(k, &Js(v))?
                    }
                } else {
                    indexed.sort_unstable_by_key(|e| e.0);
                    for (_, k, v) in indexed {
                        map.serialize_entry(k, &Js(v))?
                    }
                    for (k, v) in obj {
                        if array_index(k).is_none() {
                            map.serialize_entry(k, &Js(v))?
                        }
                    }
                }
                map.end()
            }
            Value::Number(n)
                if n.is_f64() || n.as_f64().is_some_and(|x| x.abs() > 9_007_199_254_740_991.) =>
            {
                let mut buffer = ryu_js::Buffer::new();
                let digits = buffer.format_finite(n.as_f64().expect("JSON number"));
                let raw: &RawValue =
                    serde_json::from_str(digits).map_err(serde::ser::Error::custom)?;
                raw.serialize(serializer)
            }
            v => v.serialize(serializer),
        }
    }
}
pub(crate) fn compact(value: &Value) -> String {
    serde_json::to_string(&Js(value)).expect("JSON value")
}
pub(crate) fn pretty(writer: impl std::io::Write, value: &Value) -> serde_json::Result<()> {
    serde_json::to_writer_pretty(writer, &Js(value))
}
