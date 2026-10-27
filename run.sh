#!/usr/bin/env bash
# Records Kalshi order books into $HOME/recordings/kalshi-<start>.sbe,
# restarting the recorder whenever it crashes. Arguments go to `record`.
#
# SIGINT and SIGTERM go to the recorder, which shuts down cleanly, and
# SIGHUP and SIGQUIT go to it as SIGTERM; the loop then ends. It also ends
# when the recorder exits 0 or 130 on its own, or with a status that a
# restart cannot fix: 2 (bad arguments), 126 or 127 (no binary to run). Any
# other exit restarts it after a pause that doubles from 1 s up to 30 s,
# starting again from 1 s after a run of more than 5 minutes.
#
# The recorder runs in its own process group, so signals reach it only
# through this script. Run the script where nothing kills its whole
# process group at once: under nohup, tmux or setsid, or as a systemd
# service with KillMode=mixed, which signals only this script and kills
# what is left after the stop timeout.
#
# RECORDER_BIN overrides the binary, for tests.

set -u

readonly BINARY="${RECORDER_BIN:-$(dirname "$0")/target/release/engineering-challenge}"
readonly RECORDINGS="$HOME/recordings"
readonly OUTPUT="$RECORDINGS/kalshi-{start}.sbe"
readonly FIRST_DELAY=1
readonly MAX_DELAY=30
readonly LONG_RUN=300

child=0
signalled=0
stopping=0

log() {
    printf '%s run.sh: %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$*" >&2
}

# Passes signal $1 on to the running recorder and ends the loop after it.
forward() {
    stopping=1
    if ((child != 0)); then
        kill -s "$1" "$child" 2>/dev/null
        signalled=1
    fi
}

# Whether exit status $1 ends the loop: a deliberate shutdown or a failure
# a restart cannot fix.
is_final() {
    case $1 in
        0 | 2 | 126 | 127 | 130) return 0 ;;
        *) return 1 ;;
    esac
}

# The pause before the next restart: $2, the pause due, unless the run
# lasted $1 seconds, more than LONG_RUN, which starts the doubling again.
restart_delay() {
    if (($1 > LONG_RUN)); then
        echo "$FIRST_DELAY"
    else
        echo "$2"
    fi
}

# The pause after $1: double it, at most MAX_DELAY.
next_delay() {
    local doubled=$(($1 * 2))
    echo $((doubled > MAX_DELAY ? MAX_DELAY : doubled))
}

# Runs the recorder once with the given arguments, waiting through
# forwarded signals until it exits; returns its exit status. A signal that
# came before the recorder's pid was known is passed on as SIGTERM, once.
record_once() {
    signalled=0
    "$BINARY" record --output "$OUTPUT" "$@" &
    child=$!
    if ((stopping && !signalled)); then
        kill -s TERM "$child" 2>/dev/null
        signalled=1
    fi
    while kill -0 "$child" 2>/dev/null; do
        wait "$child"
    done
    wait "$child"
    local status=$?
    child=0
    return "$status"
}

# Sleeps $1 seconds, waking early on a signal.
pause() {
    sleep "$1" &
    local sleeper=$!
    wait "$sleeper"
    kill "$sleeper" 2>/dev/null
}

main() {
    # Job control puts the recorder in its own process group, so a Ctrl-C
    # reaches it once, through forward, not twice.
    set -m
    trap 'forward INT' INT
    trap 'forward TERM' TERM HUP QUIT
    mkdir -p "$RECORDINGS" || exit 1
    local delay=$FIRST_DELAY status started lasted
    while :; do
        started=$SECONDS
        record_once "$@"
        status=$?
        lasted=$((SECONDS - started))
        if ((stopping)) || is_final "$status"; then
            log "recorder exited with status $status after ${lasted}s, stopping"
            exit "$status"
        fi
        delay=$(restart_delay "$lasted" "$delay")
        log "recorder exited with status $status after ${lasted}s, restarting in ${delay}s"
        ((stopping)) || pause "$delay"
        ((stopping)) && exit "$status"
        delay=$(next_delay "$delay")
    done
}

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
    main "$@"
fi
