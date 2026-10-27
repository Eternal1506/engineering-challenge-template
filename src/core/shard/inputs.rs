//! [`Inputs`]: everything a shard's supervisor waits on, in priority order.

use std::pin::Pin;

use tokio::sync::mpsc;
use tokio::time::{Instant, Sleep};

use super::ShardCommand;
use super::failover::{ReplicaInput, ReplicaSet};

/// The next thing a supervisor has to handle.
pub(super) enum Input {
    /// A command from the coordinator.
    Command(ShardCommand),
    /// The coordinator dropped the shard's handle.
    Uncommanded,
    /// An event or the end of one of the replicas' connections.
    Replica(ReplicaInput),
    /// The running resync's deadline passed.
    ResyncDeadline,
    /// The standbys are due to start.
    StandbysDue,
    /// A standby has warmed up while the primary is down.
    PromotionDue,
}

/// The supervisor's timers' deadlines; a timer without one never fires.
pub(super) struct Deadlines {
    pub(super) resync: Option<Instant>,
    pub(super) standbys: Option<Instant>,
    pub(super) promotion: Option<Instant>,
}

/// What a supervisor waits on besides its replicas' events.
pub(super) struct Inputs {
    /// The coordinator's commands, until it drops the shard's handle.
    commands: Option<mpsc::UnboundedReceiver<ShardCommand>>,
    resync_timer: Timer,
    standby_timer: Timer,
    promotion_timer: Timer,
}

impl Inputs {
    pub(super) fn new(commands: mpsc::UnboundedReceiver<ShardCommand>) -> Self {
        Self {
            commands: Some(commands),
            resync_timer: Timer::new(),
            standby_timer: Timer::new(),
            promotion_timer: Timer::new(),
        }
    }

    /// Waits for the next command, passed deadline, or input of `replicas`,
    /// in that order of priority; `None` once every connection has ended.
    pub(super) async fn next(
        &mut self,
        replicas: &mut ReplicaSet,
        due: Deadlines,
    ) -> Option<Input> {
        let Self {
            commands,
            resync_timer,
            standby_timer,
            promotion_timer,
        } = self;
        tokio::select! {
            biased;
            command = next_command(commands) => Some(command),
            () = resync_timer.wait(due.resync) => Some(Input::ResyncDeadline),
            () = standby_timer.wait(due.standbys) => Some(Input::StandbysDue),
            () = promotion_timer.wait(due.promotion) => Some(Input::PromotionDue),
            input = replicas.next_input() => input.map(Input::Replica),
        }
    }

    /// Stops waiting for commands: the coordinator dropped the shard's
    /// handle.
    pub(super) fn stop_commands(&mut self) {
        self.commands = None;
    }
}

/// The coordinator's next command, or `Uncommanded` once it dropped the
/// shard's handle; never resolves after that.
async fn next_command(commands: &mut Option<mpsc::UnboundedReceiver<ShardCommand>>) -> Input {
    match commands {
        Some(commands) => commands
            .recv()
            .await
            .map_or(Input::Uncommanded, Input::Command),
        None => std::future::pending().await,
    }
}

/// A reusable timer, reset only when its deadline changes.
struct Timer(Pin<Box<Sleep>>);

impl Timer {
    fn new() -> Self {
        Self(Box::pin(tokio::time::sleep_until(Instant::now())))
    }

    /// Waits until `deadline`; never resolves without one.
    async fn wait(&mut self, deadline: Option<Instant>) {
        let Some(deadline) = deadline else {
            return std::future::pending().await;
        };
        if deadline != self.0.deadline() {
            self.0.as_mut().reset(deadline);
        }
        self.0.as_mut().await;
    }
}
