//! The writer writing to `/dev/stdout` when stdout is a pipe.
//!
//! This test has no libtest harness, because libtest prints its progress to
//! stdout, which here carries the SBE output. The test re-runs its own
//! executable as a child that writes a recording to `/dev/stdout`, copies the
//! child's stdout pipe into a file, and reads that file back.

mod common;

use std::io::Read;
use std::path::Path;
use std::process::{Command, Stdio};

use anyhow::Context;
use common::{
    bid, feed_level, feed_snapshot, read_recording, snapshot_message, stamped, update_message,
    write_all,
};
use engineering_challenge::Side;
use engineering_challenge::core::events::QuantityChange::Delta;
use engineering_challenge::core::writer::CHUNK_SIZE;

/// Set in the child's environment to make it write instead of test.
const CHILD_MARKER: &str = "WRITER_STDOUT_PIPE_CHILD";

fn main() -> anyhow::Result<()> {
    if std::env::var_os(CHILD_MARKER).is_some() {
        return write_to_stdout();
    }
    read_through_a_pipe()?;
    eprintln!("writer_stdout_pipe: ok");
    Ok(())
}

/// The child: writes a snapshot and an update to `/dev/stdout`.
fn write_to_stdout() -> anyhow::Result<()> {
    let messages = vec![
        stamped(feed_snapshot("A", &[(400_000, 1)], &[]), 1, Some(1)),
        stamped(feed_level("A", bid(400_000, Delta(2))), 2, Some(2)),
    ];
    let (stats, _) = write_all(Path::new("/dev/stdout"), messages, |_| {})?;
    anyhow::ensure!(stats.snapshots_written == 1 && stats.updates_written == 1);
    Ok(())
}

/// The parent: pipes the child's stdout into a file and reads it back.
fn read_through_a_pipe() -> anyhow::Result<()> {
    let mut child = Command::new(std::env::current_exe()?)
        .env(CHILD_MARKER, "1")
        .stdout(Stdio::piped())
        .spawn()?;
    let mut piped = Vec::new();
    let mut stdout = child.stdout.take().context("no stdout pipe")?;
    stdout.read_to_end(&mut piped)?;
    anyhow::ensure!(child.wait()?.success(), "the child failed");
    anyhow::ensure!(piped.len() == CHUNK_SIZE, "piped {} bytes", piped.len());
    let file = tempfile::NamedTempFile::new()?;
    std::fs::write(file.path(), &piped)?;
    let expected = vec![
        snapshot_message("A", (1, 1), (&[(400_000, 1)], &[])),
        update_message("A", (2, 2), (Side::Buy, 400_000, 3)),
    ];
    let recorded = read_recording(file.path())?;
    anyhow::ensure!(recorded == expected, "read back {recorded:?}");
    Ok(())
}
