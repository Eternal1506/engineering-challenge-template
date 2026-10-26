//! Log output: `tracing` events to stderr, filtered by `RUST_LOG`, with a
//! fixed cap that keeps the signed websocket handshake out of the logs.

use tracing::{Level, Subscriber};
use tracing_subscriber::filter::{LevelFilter, filter_fn};
use tracing_subscriber::fmt::MakeWriter;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::{EnvFilter, fmt};

/// A subscriber that writes to `writer` the events `rust_log` selects
/// (default `info`), never letting tungstenite log at `trace`.
///
/// tungstenite traces the raw handshake request, signature header included.
/// The cap is a separate global filter that every event must also pass, so
/// no `RUST_LOG` directive, span directives included, can lift it. It gives
/// no level hint, so it never raises the subscriber's maximum level, and
/// with it `log`'s, above what `RUST_LOG` selects.
pub fn subscriber<W>(rust_log: &str, writer: W, ansi: bool) -> impl Subscriber + Send + Sync
where
    W: for<'w> MakeWriter<'w> + Send + Sync + 'static,
{
    let selected = EnvFilter::builder()
        .with_default_directive(LevelFilter::INFO.into())
        .parse_lossy(rust_log);
    let cap = filter_fn(|meta| {
        !(meta.target().starts_with("tungstenite") && *meta.level() == Level::TRACE)
    });
    let output = fmt::layer().with_writer(writer).with_ansi(ansi);
    tracing_subscriber::registry()
        .with(selected)
        .with(cap)
        .with(output)
}

#[cfg(test)]
mod tests {
    use std::io::{self, Write};
    use std::sync::{Arc, Mutex, PoisonError};

    use super::*;

    /// A writer that keeps everything written to it.
    #[derive(Clone, Default)]
    struct Captured(Arc<Mutex<Vec<u8>>>);

    impl Captured {
        fn text(&self) -> String {
            let bytes = self.0.lock().unwrap_or_else(PoisonError::into_inner);
            String::from_utf8_lossy(&bytes).into_owned()
        }
    }

    impl Write for Captured {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            let mut bytes = self.0.lock().unwrap_or_else(PoisonError::into_inner);
            bytes.extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    /// Everything logged while emitting a handshake trace inside a `conn`
    /// span, both as a `tracing` event and through the `log` bridge the way
    /// tungstenite emits it, then a tungstenite debug event and an info
    /// event of our own.
    fn logged_with(rust_log: &str) -> String {
        let captured = Captured::default();
        let writer = captured.clone();
        let subscriber = subscriber(rust_log, move || writer.clone(), false);
        tracing::subscriber::with_default(subscriber, || {
            tracing::trace_span!("conn").in_scope(|| {
                tracing::trace!(target: "tungstenite::handshake::client", "SECRET");
                bridge(
                    "tungstenite::handshake::client",
                    log::Level::Trace,
                    "SECRET",
                );
            });
            tracing::debug!(target: "tungstenite::protocol", "tungstenite debug");
            tracing::info!("control");
            bridge("bridge_control", log::Level::Info, "bridged control");
        });
        captured.text()
    }

    /// Emits `message` through the `log` bridge, as `log`-based crates such
    /// as tungstenite do.
    fn bridge(target: &str, level: log::Level, message: &str) {
        tracing_log::format_trace(
            &log::Record::builder()
                .target(target)
                .level(level)
                .args(format_args!("{message}"))
                .build(),
        )
        .unwrap();
    }

    #[test]
    fn the_cap_leaves_the_default_level_hint_at_info() {
        let hint = Subscriber::max_level_hint(&subscriber("", io::sink, false));
        assert_eq!(hint, Some(LevelFilter::INFO));
    }

    #[test]
    fn no_rust_log_directive_lets_the_handshake_trace_through() {
        let leaked: Vec<&str> = [
            "trace",
            "tungstenite=trace",
            "tungstenite::handshake=trace",
            "tungstenite::handshake::client=trace",
            "[conn]=trace",
        ]
        .into_iter()
        .filter(|rust_log| logged_with(rust_log).contains("SECRET"))
        .collect();
        assert!(leaked.is_empty(), "leaked with RUST_LOG {leaked:?}");
    }

    #[test]
    fn tungstenite_debug_still_logs_when_asked_for() {
        assert!(logged_with("tungstenite=trace").contains("tungstenite debug"));
    }

    #[test]
    fn default_level_logs_nothing_from_tungstenite() {
        assert!(!logged_with("").contains("tungstenite"));
    }

    #[test]
    fn default_level_logs_our_info_events() {
        assert!(logged_with("").contains("control"));
    }

    #[test]
    fn default_level_logs_info_events_from_the_log_bridge() {
        assert!(logged_with("").contains("bridged control"));
    }
}
