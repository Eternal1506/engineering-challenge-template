#!/usr/bin/env bash
# Checks run.sh against tests/run_sh/fake-recorder: its restart delays, the
# recordings directory, restarts with backoff after a crash, stops on a
# deliberate or permanent exit, and the signals it passes to the recorder.
# Exits 1 if any check fails. Run it from anywhere:
# bash tests/run_sh/check.sh

set -u
set -m

here=$(cd "$(dirname "$0")" && pwd)
run_sh="$here/../../run.sh"
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
mkdir "$work/home"
export RECORDER_BIN="$here/fake-recorder"
export FAKE_LOG="$work/log" FAKE_SCRIPT="$work/script" HOME="$work/home"
failures=0
status=0
pid=0

# Passes if $2, what happened, is $3, what check $1 expects.
check() {
    if [[ "$2" == "$3" ]]; then
        echo "ok   $1"
    else
        echo "FAIL $1: expected [$3], got [$2]"
        failures=$((failures + 1))
    fi
}

# Empties the fake's log and gives it the steps $@, one per run.
script_fake() {
    : >"$FAKE_LOG"
    printf '%s\n' "$@" >"$FAKE_SCRIPT"
}

runs() { grep -c '^run ' "$FAKE_LOG"; }

logged() { grep -c -- "$2" "$1"; }

# Runs run.sh to the end with the fake taking steps $@; sets status.
run_through() {
    script_fake "$@"
    timeout 20 bash "$run_sh" --shard-size 5 2>"$work/stderr"
    status=$?
}

# Starts run.sh in its own process group, as pid, with the fake taking
# steps $2 onwards, and waits until line $1 shows in the fake's log or
# run.sh's stderr.
start_until() {
    local line=$1 tries=0
    shift
    script_fake "$@"
    : >"$work/stderr"
    bash "$run_sh" 2>"$work/stderr" &
    pid=$!
    until grep -q -- "$line" "$FAKE_LOG" "$work/stderr" || ((tries++ > 100)); do
        sleep 0.05
    done
}

# Sends signal $1 to $2 and waits up to 10 s for run.sh to end; sets
# status, or "timed out" after killing run.sh's group and the fake.
signal_and_wait() {
    local tries=0
    kill -s "$1" -- "$2"
    while kill -0 "$pid" 2>/dev/null && ((tries++ < 200)); do
        sleep 0.05
    done
    if kill -0 "$pid" 2>/dev/null; then
        pkill -KILL -f fake-recorder
        kill -KILL -- "-$pid"
        wait "$pid"
        status="timed out"
        return
    fi
    wait "$pid"
    status=$?
}

check_delays() {
    check "delay: due pause kept" "$(source "$run_sh"; restart_delay 10 4)" 4
    check "delay: reset after a long run" "$(source "$run_sh"; restart_delay 301 16)" 1
    check "delay: doubles" "$(source "$run_sh"; next_delay 1)" 2
    check "delay: capped at 30" "$(source "$run_sh"; next_delay 16)" 30
    check "delay: stays at 30" "$(source "$run_sh"; next_delay 30)" 30
}

check_recordings_directory_is_made() {
    run_through 0
    check "recordings: directory made" "$([[ -d $HOME/recordings ]] && echo made)" made
}

check_crash_restarts_with_backoff() {
    run_through 1 1 0
    check "crash: final status" "$status" 0
    check "crash: runs" "$(runs)" 3
    check "crash: first pause" "$(logged "$work/stderr" 'status 1 after [0-9]*s, restarting in 1s')" 1
    check "crash: second pause doubles" "$(logged "$work/stderr" 'restarting in 2s')" 1
    check "crash: stop logged" "$(logged "$work/stderr" 'status 0 after [0-9]*s, stopping')" 1
}

check_record_arguments() {
    run_through 0
    local expected="run record --output $HOME/recordings/kalshi-{start}.sbe --shard-size 5"
    check "args: passed to record" "$(head -n 1 "$FAKE_LOG")" "$expected"
}

check_final_exits_stop() {
    local exit
    for exit in 0 2 126 127 130; do
        run_through "$exit" 0
        check "exit $exit: status" "$status" "$exit"
        check "exit $exit: no restart" "$(runs)" 1
    done
}

check_a_missing_binary_stops() {
    RECORDER_BIN="$work/missing" run_through 0
    check "missing binary: status" "$status" 127
    check "missing binary: stop logged" "$(logged "$work/stderr" 'status 127 after [0-9]*s, stopping')" 1
}

check_term_is_forwarded() {
    start_until waiting wait 0
    signal_and_wait TERM "$pid"
    check "TERM: status" "$status" 0
    check "TERM: forwarded once" "$(logged "$FAKE_LOG" 'got TERM')" 1
    check "TERM: no restart" "$(runs)" 1
}

check_hangup_and_quit_stop_the_recorder() {
    local signal
    for signal in HUP QUIT; do
        start_until waiting wait 0
        signal_and_wait "$signal" "$pid"
        check "$signal: status" "$status" 0
        check "$signal: passed on as TERM" "$(logged "$FAKE_LOG" 'got TERM')" 1
        check "$signal: no restart" "$(runs)" 1
    done
}

# A terminal's Ctrl-C signals its foreground process group, run.sh's. The
# recorder, in a group of its own, must get SIGINT once, from run.sh: a
# second SIGINT would make it exit at once with its output unfinished.
check_sigint_to_run_sh_group_reaches_the_recorder_once() {
    start_until waiting wait 0
    signal_and_wait INT "-$pid"
    check "group SIGINT: status" "$status" 0
    check "group SIGINT: recorder got it once" "$(logged "$FAKE_LOG" 'got INT')" 1
    check "group SIGINT: no restart" "$(runs)" 1
}

check_term_during_a_pause_stops() {
    start_until 'restarting in 1s' 1 0
    signal_and_wait TERM "$pid"
    check "TERM in pause: status of the crash" "$status" 1
    check "TERM in pause: no restart" "$(runs)" 1
}

check_delays
check_recordings_directory_is_made
check_crash_restarts_with_backoff
check_record_arguments
check_final_exits_stop
check_a_missing_binary_stops
check_term_is_forwarded
check_hangup_and_quit_stop_the_recorder
check_sigint_to_run_sh_group_reaches_the_recorder_once
check_term_during_a_pause_stops

if ((failures > 0)); then
    echo "$failures checks failed"
    exit 1
fi
echo "all run.sh checks passed"
