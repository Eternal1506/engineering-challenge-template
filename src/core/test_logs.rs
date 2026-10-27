//! Capturing what code under test logs, for unit tests that check a log
//! line.

use std::io;
use std::sync::{Arc, Mutex, PoisonError};

/// Runs `run` on this thread, returning everything it logged at any level,
/// formatted as the recorder's logs are, without colour.
pub(crate) fn logs_of(run: impl FnOnce()) -> String {
    let buffer = Arc::new(Mutex::new(Vec::new()));
    let sink = buffer.clone();
    let subscriber = tracing_subscriber::fmt()
        .with_writer(move || Sink(sink.clone()))
        .with_ansi(false)
        .with_max_level(tracing::Level::TRACE)
        .finish();
    tracing::subscriber::with_default(subscriber, run);
    let logged = buffer.lock().unwrap_or_else(PoisonError::into_inner);
    String::from_utf8_lossy(&logged).into_owned()
}

/// A writer into a shared buffer.
struct Sink(Arc<Mutex<Vec<u8>>>);

impl io::Write for Sink {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let mut buffer = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        buffer.extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
