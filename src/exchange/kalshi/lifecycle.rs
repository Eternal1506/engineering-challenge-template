//! Maps Kalshi `market_lifecycle_v2` messages onto [`LifecycleEvent`]s.

use std::sync::Arc;

use anyhow::anyhow;
use chrono::{DateTime, Utc};
use serde::Deserialize;

use crate::core::events::LifecycleEvent;

/// The `msg` of a `market_lifecycle_v2` message, as far as the core uses it.
#[derive(Deserialize)]
pub(super) struct LifecycleMsg<'a> {
    market_ticker: &'a str,
    event_type: &'a str,
    open_ts: Option<i64>,
    close_ts: Option<i64>,
    is_deactivated: Option<bool>,
}

impl LifecycleMsg<'_> {
    /// The lifecycle event, or `None` for event types the core does not use,
    /// such as `metadata_updated`.
    ///
    /// Kalshi reports both pausing and resuming as `deactivated`, telling
    /// them apart with `is_deactivated`, so `is_deactivated: false` maps to
    /// [`LifecycleEvent::Activated`].
    pub(super) fn into_event(self) -> anyhow::Result<Option<LifecycleEvent>> {
        let symbol: Arc<str> = self.market_ticker.into();
        Ok(Some(match self.event_type {
            "created" => LifecycleEvent::Created {
                symbol,
                open_ts: self.open_ts.and_then(from_seconds),
                close_ts: self.close_ts.and_then(from_seconds),
            },
            "activated" => LifecycleEvent::Activated { symbol },
            "deactivated" if self.is_deactivated == Some(false) => {
                LifecycleEvent::Activated { symbol }
            }
            "deactivated" => LifecycleEvent::Deactivated { symbol },
            "close_date_updated" => LifecycleEvent::CloseChanged {
                close_ts: self.required_close_ts()?,
                symbol,
            },
            "determined" => LifecycleEvent::Determined { symbol },
            "settled" => LifecycleEvent::Settled { symbol },
            _ => return Ok(None),
        }))
    }

    fn required_close_ts(&self) -> anyhow::Result<DateTime<Utc>> {
        self.close_ts.and_then(from_seconds).ok_or_else(|| {
            anyhow!(
                "market_lifecycle_v2 {} for {} has no valid close_ts",
                self.event_type,
                self.market_ticker
            )
        })
    }
}

fn from_seconds(seconds: i64) -> Option<DateTime<Utc>> {
    DateTime::from_timestamp(seconds, 0)
}

#[cfg(test)]
mod tests {
    use serde_json::{Value, json};

    use super::*;
    use crate::core::events::{Decoded, Inbound, StreamPosition, SubId};
    use crate::exchange::FeedCodec;
    use crate::exchange::kalshi::KalshiCodec;

    /// A frame captured from the live feed by `probe --fixtures-dir`.
    macro_rules! fixture {
        ($name:literal) => {
            include_str!(concat!("../../../tests/fixtures/kalshi/", $name, ".json"))
        };
    }

    fn decode(text: &str) -> anyhow::Result<Decoded> {
        let received_at = DateTime::from_timestamp_millis(1_800_000_000_000).unwrap();
        let mut out = Vec::new();
        KalshiCodec.decode(text, received_at, &mut out)?;
        Ok(out.remove(0))
    }

    fn lifecycle(text: &str) -> LifecycleEvent {
        match decode(text).unwrap().message {
            Inbound::Lifecycle(event) => event,
            other => panic!("expected a lifecycle event, got {other:?}"),
        }
    }

    fn lifecycle_frame(msg: Value) -> String {
        json!({ "type": "market_lifecycle_v2", "sid": 1, "seq": 2, "msg": msg }).to_string()
    }

    fn symbol(name: &str) -> Arc<str> {
        name.into()
    }

    #[test]
    fn captured_created_event_carries_open_and_close_times() {
        let expected = LifecycleEvent::Created {
            symbol: symbol("KXSNLHOST-27-TIN"),
            open_ts: from_seconds(1_793_030_400),
            close_ts: from_seconds(1_830_265_200),
        };
        assert_eq!(lifecycle(fixture!("lifecycle_created")), expected);
    }

    #[test]
    fn captured_lifecycle_event_keeps_its_stream_position() {
        let decoded = decode(fixture!("lifecycle_created")).unwrap();
        let position = StreamPosition {
            sub: SubId(1),
            seq: 97,
        };
        assert_eq!(decoded.position, Some(position));
    }

    #[test]
    fn captured_close_date_update_carries_the_new_close_time() {
        let expected = LifecycleEvent::CloseChanged {
            symbol: symbol("KXATPGTOTAL-26OCT25FRITSI-20"),
            close_ts: from_seconds(1_793_028_728).unwrap(),
        };
        assert_eq!(
            lifecycle(fixture!("lifecycle_close_date_updated")),
            expected
        );
    }

    #[test]
    fn captured_deactivated_determined_and_settled_events_decode() {
        let events = [
            lifecycle(fixture!("lifecycle_deactivated")),
            lifecycle(fixture!("lifecycle_determined")),
            lifecycle(fixture!("lifecycle_settled")),
        ];
        let expected = [
            LifecycleEvent::Deactivated {
                symbol: symbol("KXITFMATCH-26OCT26GARBEN-GAR"),
            },
            LifecycleEvent::Determined {
                symbol: symbol("KXITFMATCH-26OCT26BINESC-BIN"),
            },
            LifecycleEvent::Settled {
                symbol: symbol("KXATPMATCH-26OCT25FRITSI-FRI"),
            },
        ];
        assert_eq!(events, expected);
    }

    #[test]
    fn activated_event_decodes() {
        let frame = lifecycle_frame(json!({ "market_ticker": "T", "event_type": "activated" }));
        let expected = LifecycleEvent::Activated {
            symbol: symbol("T"),
        };
        assert_eq!(lifecycle(&frame), expected);
    }

    #[test]
    fn deactivated_with_is_deactivated_false_is_a_resume() {
        let msg =
            json!({ "market_ticker": "T", "event_type": "deactivated", "is_deactivated": false });
        let expected = LifecycleEvent::Activated {
            symbol: symbol("T"),
        };
        assert_eq!(lifecycle(&lifecycle_frame(msg)), expected);
    }

    #[test]
    fn close_date_update_without_close_ts_is_an_error() {
        let msg = json!({ "market_ticker": "KXTEST-1", "event_type": "close_date_updated" });
        let error = format!("{:#}", decode(&lifecycle_frame(msg)).unwrap_err());
        assert!(error.contains("KXTEST-1"), "{error}");
    }

    #[test]
    fn event_types_the_core_does_not_use_are_ignored() {
        let msg = json!({ "market_ticker": "T", "event_type": "metadata_updated" });
        let decoded = decode(&lifecycle_frame(msg)).unwrap();
        assert_eq!(decoded.message, Inbound::Ignored);
    }
}
