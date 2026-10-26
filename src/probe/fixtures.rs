//! Keeps one raw frame of each kind the probe sees, to save as test fixtures.

use std::collections::BTreeMap;
use std::path::Path;

use anyhow::Context;
use serde_json::Value;

/// The smallest frame seen of each kind, keyed by kind name.
#[derive(Default)]
pub struct Fixtures {
    smallest: BTreeMap<String, String>,
}

impl Fixtures {
    /// Keeps `text` as the example of its kind if it is the first or the
    /// smallest so far. Frames of no interesting kind are skipped.
    pub fn offer(&mut self, frame: &Value, text: &str) {
        let Some(kind) = fixture_kind(frame) else {
            return;
        };
        let kept = self.smallest.entry(kind).or_insert_with(|| text.to_owned());
        if text.len() < kept.len() {
            text.clone_into(kept);
        }
    }

    /// The kinds kept so far.
    pub fn kinds(&self) -> impl Iterator<Item = &str> {
        self.smallest.keys().map(String::as_str)
    }

    /// Writes each kept frame, pretty-printed, to `<dir>/<kind>.json`.
    pub fn write(&self, dir: &Path) -> anyhow::Result<()> {
        std::fs::create_dir_all(dir).with_context(|| format!("failed to create {dir:?}"))?;
        for (kind, text) in &self.smallest {
            let frame: Value = serde_json::from_str(text)?;
            let path = dir.join(format!("{kind}.json"));
            let pretty = serde_json::to_string_pretty(&frame)? + "\n";
            std::fs::write(&path, pretty).with_context(|| format!("failed to write {path:?}"))?;
        }
        Ok(())
    }
}

/// The fixture name for a frame, such as `delta_no` or `lifecycle_created`.
fn fixture_kind(frame: &Value) -> Option<String> {
    let msg = &frame["msg"];
    Some(match frame["type"].as_str()? {
        "orderbook_snapshot" => snapshot_kind(frame, msg).to_owned(),
        "orderbook_delta" => format!("delta_{}", msg["side"].as_str()?),
        "error" => format!("error_code_{}", msg["code"].as_i64()?),
        "market_lifecycle_v2" => format!("lifecycle_{}", msg["event_type"].as_str()?),
        other => other.to_owned(),
    })
}

fn snapshot_kind(frame: &Value, msg: &Value) -> &'static str {
    let yes = msg.get("yes_dollars_fp").is_some();
    let no = msg.get("no_dollars_fp").is_some();
    match (frame.get("id").is_some(), yes, no) {
        (true, _, _) => "snapshot_reply_to_get_snapshot",
        (false, true, true) => "snapshot_both_sides",
        (false, false, false) => "snapshot_empty",
        (false, _, _) => "snapshot_one_side_absent",
    }
}
