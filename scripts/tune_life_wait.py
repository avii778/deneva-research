#!/usr/bin/env python3
"""Find the LIFE help delay with the highest median YCSB throughput.

The default search stays in the microsecond-scale range, but --waits can sweep
up to two seconds.  config.h is restored after the search unless --apply-best
is used.
"""

import argparse
import atexit
import csv
import os
from pathlib import Path
import re
import statistics
import subprocess
import sys
import time


ROOT = Path(__file__).resolve().parents[1]
CONFIG = ROOT / "config.h"
RESULTS = ROOT / "results" / "life_wait_tuning"
DEFAULT_WAITS_US = (0, 1, 2, 5, 10, 20, 50, 100)
MAX_WAIT_US = 2 * 1000 * 1000
DEFAULT_ZIPF_THETA = 0.99
DEFAULT_SYNTH_TABLE_SIZE = 1024
DEFAULT_TXN_WRITE_PERC = 1.0
DEFAULT_TUP_WRITE_PERC = 0.5


def replace_define(config, name, value):
    pattern = re.compile(r"^\s*#define\s+" + re.escape(name) + r"\b.*$", re.MULTILINE)
    replacement = "#define {} {}".format(name, value)
    updated, count = pattern.subn(replacement, config, count=1)
    if count != 1:
        raise RuntimeError("expected one #define {} in config.h".format(name))
    return updated


def configure(original, algorithm, wait_us, duration_s, warmup_s,
              zipf_theta, synth_table_size, txn_write_perc, tup_write_perc):
    config = original
    config = replace_define(config, "WORKLOAD", "YCSB")
    config = replace_define(config, "NODE_CNT", 1)
    config = replace_define(config, "PART_CNT", 1)
    config = replace_define(config, "CLIENT_NODE_CNT", 1)
    config = replace_define(config, "SERVER_GENERATE_QUERIES", "false")
    config = replace_define(config, "CC_ALG", algorithm)
    config = replace_define(config, "LIFE_HELP_WAIT_US", wait_us)
    config = replace_define(config, "SKEW_METHOD", "ZIPF")
    config = replace_define(config, "ZIPF_THETA", zipf_theta)
    config = replace_define(config, "SYNTH_TABLE_SIZE", synth_table_size)
    config = replace_define(config, "TXN_WRITE_PERC", txn_write_perc)
    config = replace_define(config, "TUP_WRITE_PERC", tup_write_perc)
    config = replace_define(config, "DONE_TIMER", "{} * BILLION".format(duration_s))
    config = replace_define(config, "WARMUP_TIMER", "{} * BILLION".format(warmup_s))
    return config


def parse_summary(path):
    summaries = []
    with path.open(errors="replace") as output:
        for line in output:
            if line.startswith("[summary] "):
                values = {}
                for field in line[len("[summary] "):].strip().split(","):
                    if "=" in field:
                        key, value = field.split("=", 1)
                        values[key] = value
                summaries.append(values)
    if not summaries:
        raise RuntimeError("no [summary] line in {}".format(path))
    return summaries[-1]


def build(jobs):
    subprocess.run(["make", "clean"], cwd=ROOT, check=True,
                   stdout=subprocess.DEVNULL)
    subprocess.run(["make", "-j{}".format(jobs)], cwd=ROOT, check=True)


def stop_process(process):
    if process is None or process.poll() is not None:
        return
    process.terminate()
    try:
        process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait()


def database_processes_running():
    for name in ("rundb", "runcl"):
        result = subprocess.run(["pgrep", "-x", name], stdout=subprocess.DEVNULL,
                                stderr=subprocess.DEVNULL, check=False)
        if result.returncode == 0:
            return True
        if result.returncode != 1:
            raise RuntimeError("could not check for running {} processes".format(name))
    return False


def clean_ipc_endpoints():
    for endpoint in ROOT.glob("node_*.ipc"):
        endpoint.unlink()


def read_latest_counter(path, names):
    try:
        data = path.read_text(errors="replace")
    except OSError:
        return None
    for name in names:
        matches = re.findall(r"(?:^|,)" + re.escape(name) + r"=([0-9]+)", data)
        if matches:
            return int(matches[-1])
    return None


def print_transaction_progress(server_path, client_path, started,
                               previous_snapshot=None):
    value = read_latest_counter(client_path, ("txn_cnt",))
    source = "client txn_cnt"
    if value is None:
        value = read_latest_counter(server_path, (
            "total_txn_commit_cnt", "local_txn_commit_cnt", "txn_cnt"))
        source = "server commits"
    elapsed = int(time.monotonic() - started)
    if value is not None:
        snapshot = (source, value)
        freshness = "unchanged" if snapshot == previous_snapshot else "updated"
        print("  [progress] elapsed={}s completed_transactions={} source={} "
              "snapshot={}".format(elapsed, value, source, freshness), flush=True)
        return snapshot
    elif server_path.exists() and server_path.stat().st_size > 0:
        print("  [progress] elapsed={}s completed_transactions=0 source=no stats yet".format(
            elapsed), flush=True)
    else:
        print("  [progress] elapsed={}s completed_transactions=unknown".format(
            elapsed), flush=True)
    return previous_snapshot


def process_reached_run_phase(path):
    try:
        data = path.read_text(errors="replace")
    except OSError:
        return False
    return re.search(r"^Running \d+:\d+", data, re.MULTILINE) is not None


def run_trial(label, duration_s, warmup_s, startup_timeout_s, trial):
    RESULTS.mkdir(parents=True, exist_ok=True)
    server_path = RESULTS / "{}_trial{}_server.out".format(label, trial)
    client_path = RESULTS / "{}_trial{}_client.out".format(label, trial)
    processes = []
    if database_processes_running():
        raise RuntimeError("rundb or runcl is already running; stop it before tuning")
    clean_ipc_endpoints()
    try:
        commands = (
            ("rundb", [str(ROOT / "rundb"), "-nid0"], server_path),
            ("runcl", [str(ROOT / "runcl"), "-nid1"], client_path),
        )
        for name, command, output_path in commands:
            with output_path.open("w") as output:
                process = subprocess.Popen(command, cwd=ROOT,
                                           stdout=output, stderr=subprocess.STDOUT)
            processes.append((name, process, output_path))
            print("  {} started (pid {}, log: {})".format(
                name, process.pid, output_path), flush=True)

        started = time.monotonic()
        run_deadline = None
        next_progress = started
        progress_snapshot = None
        while any(process.poll() is None for _, process, _ in processes):
            now = time.monotonic()
            if run_deadline is None:
                if all(process_reached_run_phase(path)
                       for _, _, path in processes):
                    run_deadline = now + duration_s + warmup_s + 120
                    print("  database and client entered the run phase", flush=True)
                elif now - started >= startup_timeout_s:
                    raise subprocess.TimeoutExpired(
                        "rundb/runcl initialization", startup_timeout_s)
            if now >= next_progress:
                progress_snapshot = print_transaction_progress(
                    server_path, client_path, started, progress_snapshot)
                next_progress = now + 5
            if run_deadline is not None and now >= run_deadline:
                raise subprocess.TimeoutExpired(
                    "rundb/runcl trial", duration_s + warmup_s + 120)
            time.sleep(1)
        print_transaction_progress(server_path, client_path, started,
                                   progress_snapshot)

        failures = [(name, process.returncode, path)
                    for name, process, path in processes if process.returncode != 0]
        if failures:
            raise RuntimeError("trial process failure: {}".format(
                ", ".join("{}={} (see {})".format(name, code, path)
                          for name, code, path in failures)))
    finally:
        for _, process, _ in reversed(processes):
            stop_process(process)
        clean_ipc_endpoints()

    summary = parse_summary(client_path)
    return float(summary["tput"])


def parse_waits(value):
    try:
        waits = sorted(set(int(item) for item in value.split(",")))
    except ValueError:
        raise argparse.ArgumentTypeError("waits must be comma-separated integers")
    if not waits or waits[0] < 0 or waits[-1] > MAX_WAIT_US:
        raise argparse.ArgumentTypeError(
            "waits must be between 0 and {} microseconds".format(MAX_WAIT_US))
    return waits


def arguments():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--waits", type=parse_waits,
                        default=list(DEFAULT_WAITS_US),
                        help="comma-separated microsecond delays (default: %(default)s)")
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--duration", type=int, default=30,
                        help="measured seconds per trial")
    parser.add_argument("--warmup", type=int, default=5,
                        help="warmup seconds per trial")
    parser.add_argument("--jobs", type=int, default=max(1, os.cpu_count() or 1))
    parser.add_argument("--startup-timeout", type=int, default=600,
                        help="seconds allowed for database/client initialization")
    parser.add_argument("--zipf-theta", type=float, default=DEFAULT_ZIPF_THETA,
                        help="YCSB Zipf theta used during tuning (default: %(default)s)")
    parser.add_argument("--table-size", type=int, default=DEFAULT_SYNTH_TABLE_SIZE,
                        help="YCSB table size used during tuning (default: %(default)s)")
    parser.add_argument("--txn-write-perc", type=float,
                        default=DEFAULT_TXN_WRITE_PERC,
                        help="fraction of update transactions during tuning "
                             "(default: %(default)s)")
    parser.add_argument("--tup-write-perc", type=float,
                        default=DEFAULT_TUP_WRITE_PERC,
                        help="fraction of writes within update transactions "
                             "(default: %(default)s)")
    parser.add_argument("--apply-best", action="store_true",
                        help="leave config.h set to LIFE and the winning delay")
    parser.add_argument("--dry-run", action="store_true",
                        help="validate and print the planned search without building")
    args = parser.parse_args()
    if (args.repeats < 1 or args.duration < 1 or args.warmup < 0 or
            args.jobs < 1 or args.startup_timeout < 1):
        parser.error("repeats, duration, jobs, and startup-timeout must be positive; "
                     "warmup cannot be negative")
    if not 0.0 <= args.zipf_theta < 1.0:
        parser.error("zipf-theta must be in [0.0, 1.0)")
    if args.table_size < 64:
        parser.error("table-size must be at least 64 to avoid pathological retries")
    if not 0.0 <= args.txn_write_perc <= 1.0:
        parser.error("txn-write-perc must be in [0.0, 1.0]")
    if not 0.0 <= args.tup_write_perc <= 1.0:
        parser.error("tup-write-perc must be in [0.0, 1.0]")
    if args.txn_write_perc == 0.0:
        parser.error("txn-write-perc=0.0 creates read-only trials with little or no contention")
    return args


def main():
    args = arguments()
    original = CONFIG.read_text()
    restored = [False]

    def restore_config():
        if not restored[0]:
            CONFIG.write_text(original)
            restored[0] = True

    print("LIFE waits (us): {}".format(", ".join(map(str, args.waits))),
          flush=True)
    print("Trials: {} x {}s, {}s warmup".format(
        args.repeats, args.duration, args.warmup), flush=True)
    print("Contention profile: zipf_theta={}, table_size={}, txn_write_perc={}, "
          "tup_write_perc={}".format(
              args.zipf_theta, args.table_size, args.txn_write_perc,
              args.tup_write_perc), flush=True)
    if args.dry_run:
        return 0

    atexit.register(restore_config)
    rows = []
    for wait_us in args.waits:
        print("\nTesting LIFE_HELP_WAIT_US={} us: configuring and building...".format(
            wait_us), flush=True)
        CONFIG.write_text(configure(original, "LIFE", wait_us,
                                    args.duration, args.warmup,
                                    args.zipf_theta, args.table_size,
                                    args.txn_write_perc, args.tup_write_perc))
        build(args.jobs)
        throughputs = []
        for trial in range(1, args.repeats + 1):
            print("LIFE {:>4} us trial {}/{}: running ({}s warmup + {}s measured)...".format(
                wait_us, trial, args.repeats, args.warmup, args.duration),
                flush=True)
            throughput = run_trial("life_{}us".format(wait_us), args.duration,
                                   args.warmup, args.startup_timeout, trial)
            throughputs.append(throughput)
            print("LIFE {:>4} us trial {}/{}: {:.2f} txn/s".format(
                wait_us, trial, args.repeats, throughput), flush=True)
        rows.append((wait_us, statistics.median(throughputs), throughputs))

    best_wait, best_median, _ = max(rows, key=lambda row: row[1])
    RESULTS.mkdir(parents=True, exist_ok=True)
    csv_path = RESULTS / "summary.csv"
    with csv_path.open("w", newline="") as output:
        writer = csv.writer(output)
        writer.writerow(("wait_us", "median_tput", "trial_tputs"))
        for wait_us, median, throughputs in rows:
            writer.writerow((wait_us, median, " ".join(map(str, throughputs))))

    restore_config()
    if args.apply_best:
        applied = replace_define(original, "CC_ALG", "LIFE")
        applied = replace_define(applied, "LIFE_HELP_WAIT_US", best_wait)
        CONFIG.write_text(applied)
    print("Best LIFE_HELP_WAIT_US: {} ({:.2f} median txn/s)".format(
        best_wait, best_median), flush=True)
    print("Results: {}".format(csv_path), flush=True)
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        sys.exit("interrupted; config.h restored")
