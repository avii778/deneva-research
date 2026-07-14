#!/usr/bin/env python
"""Find the LIFE help delay with the highest median YCSB throughput.

The default search stays in the microsecond-scale range, but --waits can sweep
up to two seconds.  config.h is restored after the search unless --apply-best
is used.
"""

import argparse
import atexit
import csv
import glob
import io
import multiprocessing
import os
import re
import sys
import time

try:
    import subprocess32 as subprocess
except ImportError:
    import subprocess


PY2 = sys.version_info[0] == 2
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
CONFIG = os.path.join(ROOT, "config.h")
RESULTS = os.path.join(ROOT, "results", "life_wait_tuning")
DEFAULT_WAITS_US = (0, 1, 2, 5, 10, 20, 50, 100)
MAX_WAIT_US = 2 * 1000 * 1000
YCSB_SCALING_BASE_TABLE_SIZE = 2097152 * 8
DEFAULT_NODE_CNT = 2
DEFAULT_CLIENT_NODE_CNT = DEFAULT_NODE_CNT
DEFAULT_THREAD_CNT = 4
DEFAULT_MAX_TXN_IN_FLIGHT = 10000
DEFAULT_ZIPF_THETA = 0.4
DEFAULT_TXN_WRITE_PERC = 0.5
DEFAULT_TUP_WRITE_PERC = 0.5
DEVNULL = open(os.devnull, "wb")

try:
    monotonic = time.monotonic
except AttributeError:
    monotonic = time.time

try:
    TimeoutExpired = subprocess.TimeoutExpired
except AttributeError:
    class TimeoutExpired(Exception):
        def __init__(self, cmd, timeout):
            super(TimeoutExpired, self).__init__(
                "Command '{}' timed out after {} seconds".format(cmd, timeout))
            self.cmd = cmd
            self.timeout = timeout


class CompletedProcess(object):
    def __init__(self, args, returncode):
        self.args = args
        self.returncode = returncode


def run_command(args, cwd=None, check=False, stdout=None, stderr=None):
    process = subprocess.Popen(args, cwd=cwd, stdout=stdout, stderr=stderr)
    returncode = process.wait()
    if check and returncode != 0:
        raise subprocess.CalledProcessError(returncode, args)
    return CompletedProcess(args, returncode)


def wait_process(process, timeout=None):
    if timeout is None:
        return process.wait()
    deadline = time.time() + timeout
    while process.poll() is None:
        if time.time() >= deadline:
            raise TimeoutExpired(getattr(process, "args", None), timeout)
        time.sleep(0.1)
    return process.returncode


def read_text(path):
    with io.open(path, "r", encoding="utf-8", errors="replace") as handle:
        return handle.read()


def write_text(path, data):
    with io.open(path, "w", encoding="utf-8") as handle:
        handle.write(data)


def mkdir_p(path):
    if not os.path.isdir(path):
        os.makedirs(path)


def median(values):
    ordered = sorted(values)
    midpoint = len(ordered) // 2
    if len(ordered) % 2:
        return ordered[midpoint]
    return (ordered[midpoint - 1] + ordered[midpoint]) / 2.0


def cpu_count():
    try:
        return multiprocessing.cpu_count()
    except NotImplementedError:
        return 1


def log(message=""):
    print(message)
    sys.stdout.flush()


def replace_define(config, name, value):
    pattern = re.compile(r"^\s*#define\s+" + re.escape(name) + r"\b.*$", re.MULTILINE)
    replacement = "#define {} {}".format(name, value)
    updated, count = pattern.subn(replacement, config, count=1)
    if count != 1:
        raise RuntimeError("expected one #define {} in config.h".format(name))
    return updated


def ycsb_scaling_table_size(node_cnt):
    return YCSB_SCALING_BASE_TABLE_SIZE * node_cnt


def configure(original, algorithm, wait_us, duration_s, warmup_s,
              node_cnt, client_node_cnt, thread_cnt, max_txn_in_flight,
              zipf_theta, synth_table_size, txn_write_perc, tup_write_perc):
    config = original
    config = replace_define(config, "WORKLOAD", "YCSB")
    config = replace_define(config, "NODE_CNT", node_cnt)
    config = replace_define(config, "PART_CNT", node_cnt)
    config = replace_define(config, "CLIENT_NODE_CNT", client_node_cnt)
    config = replace_define(config, "THREAD_CNT", thread_cnt)
    config = replace_define(config, "MAX_TXN_IN_FLIGHT", max_txn_in_flight)
    config = replace_define(config, "SERVER_GENERATE_QUERIES", "false")
    config = replace_define(config, "CC_ALG", algorithm)
    config = replace_define(config, "LIFE_HELP_WAIT_US", wait_us)
    config = replace_define(config, "SKEW_METHOD", "ZIPF")
    config = replace_define(config, "ZIPF_THETA", zipf_theta)
    config = replace_define(config, "SYNTH_TABLE_SIZE", synth_table_size)
    config = replace_define(config, "TXN_WRITE_PERC", txn_write_perc)
    config = replace_define(config, "TUP_WRITE_PERC", tup_write_perc)
    config = replace_define(config, "PART_PER_TXN", "PART_CNT")
    config = replace_define(config, "NUM_WH", "PART_CNT")
    config = replace_define(config, "DONE_TIMER", "{} * BILLION".format(duration_s))
    config = replace_define(config, "WARMUP_TIMER", "{} * BILLION".format(warmup_s))
    return config


def parse_summary(path):
    summaries = []
    with io.open(path, "r", encoding="utf-8", errors="replace") as output:
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


def parse_client_throughput(paths):
    return sum(float(parse_summary(path)["tput"]) for path in paths)


def build(jobs):
    run_command(["make", "clean"], cwd=ROOT, check=True, stdout=DEVNULL)
    run_command(["make", "-j{}".format(jobs)], cwd=ROOT, check=True)


def stop_process(process):
    if process is None or process.poll() is not None:
        return
    process.terminate()
    try:
        wait_process(process, timeout=5)
    except TimeoutExpired:
        process.kill()
        process.wait()


def database_processes_running():
    for name in ("rundb", "runcl"):
        result = run_command(["pgrep", "-x", name], stdout=DEVNULL,
                             stderr=DEVNULL, check=False)
        if result.returncode == 0:
            return True
        if result.returncode != 1:
            raise RuntimeError("could not check for running {} processes".format(name))
    return False


def clean_ipc_endpoints():
    for endpoint in glob.glob(os.path.join(ROOT, "node_*.ipc")):
        os.unlink(endpoint)


def write_local_ifconfig(total_node_cnt):
    with open(os.path.join(ROOT, "ifconfig.txt"), "w") as output:
        for _ in range(total_node_cnt):
            output.write("127.0.0.1\n")


def read_latest_counter(path, names):
    try:
        data = read_text(path)
    except OSError:
        return None
    for name in names:
        matches = re.findall(r"(?:^|,)" + re.escape(name) + r"=([0-9]+)", data)
        if matches:
            return int(matches[-1])
    return None


def print_transaction_progress(server_paths, client_paths, started,
                               previous_snapshot=None):
    counters = []
    for path in client_paths:
        value = read_latest_counter(path, ("txn_cnt",))
        if value is not None:
            counters.append(value)
    source = "client txn_cnt"
    if not counters:
        source = "server commits"
        for path in server_paths:
            value = read_latest_counter(path, (
                "total_txn_commit_cnt", "local_txn_commit_cnt", "txn_cnt"))
            if value is not None:
                counters.append(value)
    elapsed = int(monotonic() - started)
    if counters:
        value = sum(counters)
        snapshot = (source, value)
        freshness = "unchanged" if snapshot == previous_snapshot else "updated"
        log("  [progress] elapsed={}s completed_transactions={} source={} "
            "snapshot={}".format(elapsed, value, source, freshness))
        return snapshot
    elif any(os.path.exists(path) and os.path.getsize(path) > 0
             for path in server_paths):
        log("  [progress] elapsed={}s completed_transactions=0 source=no stats yet".format(
            elapsed))
    else:
        log("  [progress] elapsed={}s completed_transactions=unknown".format(
            elapsed))
    return previous_snapshot


def process_reached_run_phase(path):
    try:
        data = read_text(path)
    except OSError:
        return False
    return re.search(r"^Running \d+:\d+", data, re.MULTILINE) is not None


def run_trial(label, duration_s, warmup_s, startup_timeout_s, trial,
              node_cnt, client_node_cnt):
    mkdir_p(RESULTS)
    server_paths = [
        os.path.join(RESULTS, "{}_trial{}_server{}.out".format(label, trial, node_id))
        for node_id in range(node_cnt)
    ]
    client_paths = [
        os.path.join(RESULTS, "{}_trial{}_client{}.out".format(
            label, trial, node_id))
        for node_id in range(node_cnt, node_cnt + client_node_cnt)
    ]
    processes = []
    if database_processes_running():
        raise RuntimeError("rundb or runcl is already running; stop it before tuning")
    clean_ipc_endpoints()
    write_local_ifconfig(node_cnt + client_node_cnt)
    try:
        commands = []
        for node_id in range(node_cnt):
            commands.append((
                "rundb",
                [os.path.join(ROOT, "rundb"), "-nid{}".format(node_id)],
                server_paths[node_id],
            ))
        for index, node_id in enumerate(range(node_cnt, node_cnt + client_node_cnt)):
            commands.append((
                "runcl",
                [os.path.join(ROOT, "runcl"), "-nid{}".format(node_id)],
                client_paths[index],
            ))
        for name, command, output_path in commands:
            with open(output_path, "w") as output:
                process = subprocess.Popen(command, cwd=ROOT,
                                           stdout=output, stderr=subprocess.STDOUT)
            processes.append((name, process, output_path))
            log("  {} started (pid {}, log: {})".format(
                name, process.pid, output_path))

        started = monotonic()
        run_deadline = None
        next_progress = started
        progress_snapshot = None
        while any(process.poll() is None for _, process, _ in processes):
            now = monotonic()
            if run_deadline is None:
                if all(process_reached_run_phase(path)
                       for _, _, path in processes):
                    run_deadline = now + duration_s + warmup_s + 120
                    log("  database and client entered the run phase")
                elif now - started >= startup_timeout_s:
                    raise TimeoutExpired(
                        "rundb/runcl initialization", startup_timeout_s)
            if now >= next_progress:
                progress_snapshot = print_transaction_progress(
                    server_paths, client_paths, started, progress_snapshot)
                next_progress = now + 5
            if run_deadline is not None and now >= run_deadline:
                raise TimeoutExpired(
                    "rundb/runcl trial", duration_s + warmup_s + 120)
            time.sleep(1)
        print_transaction_progress(server_paths, client_paths, started,
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

    return parse_client_throughput(client_paths)


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
    parser.add_argument("--jobs", type=int, default=max(1, cpu_count()))
    parser.add_argument("--startup-timeout", type=int, default=600,
                        help="seconds allowed for database/client initialization")
    parser.add_argument("--node-count", type=int, default=DEFAULT_NODE_CNT,
                        help="server node count; defaults to ycsb_scaling's first point")
    parser.add_argument("--client-node-count", type=int,
                        default=DEFAULT_CLIENT_NODE_CNT,
                        help="client node count; defaults to matching server nodes")
    parser.add_argument("--thread-count", type=int, default=DEFAULT_THREAD_CNT,
                        help="worker threads per server node")
    parser.add_argument("--max-txn-in-flight", type=int,
                        default=DEFAULT_MAX_TXN_IN_FLIGHT,
                        help="MAX_TXN_IN_FLIGHT used during tuning")
    parser.add_argument("--zipf-theta", type=float, default=DEFAULT_ZIPF_THETA,
                        help="YCSB Zipf theta used during tuning (default: %(default)s)")
    parser.add_argument("--table-size", type=int, default=None,
                        help="YCSB table size; default is 2097152 * 8 * node-count")
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
    if (args.node_count < 1 or args.client_node_count < 1 or
            args.thread_count < 1 or args.max_txn_in_flight < 1):
        parser.error("node-count, client-node-count, thread-count, and "
                     "max-txn-in-flight must be positive")
    if args.table_size is None:
        args.table_size = ycsb_scaling_table_size(args.node_count)
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
    original = read_text(CONFIG)
    restored = [False]

    def restore_config():
        if not restored[0]:
            write_text(CONFIG, original)
            restored[0] = True

    log("LIFE waits (us): {}".format(", ".join(map(str, args.waits))))
    log("Trials: {} x {}s, {}s warmup".format(
        args.repeats, args.duration, args.warmup))
    log("YCSB scaling profile: node_count={}, client_node_count={}, "
        "thread_count={}, max_txn_in_flight={}".format(
            args.node_count, args.client_node_count, args.thread_count,
            args.max_txn_in_flight))
    log("Contention profile: zipf_theta={}, table_size={}, txn_write_perc={}, "
        "tup_write_perc={}".format(
            args.zipf_theta, args.table_size, args.txn_write_perc,
            args.tup_write_perc))
    if args.dry_run:
        return 0

    atexit.register(restore_config)
    rows = []
    for wait_us in args.waits:
        log("\nTesting LIFE_HELP_WAIT_US={} us: configuring and building...".format(
            wait_us))
        write_text(CONFIG, configure(original, "LIFE", wait_us,
                                     args.duration, args.warmup,
                                     args.node_count, args.client_node_count,
                                     args.thread_count, args.max_txn_in_flight,
                                     args.zipf_theta, args.table_size,
                                     args.txn_write_perc, args.tup_write_perc))
        build(args.jobs)
        throughputs = []
        for trial in range(1, args.repeats + 1):
            log("LIFE {:>4} us trial {}/{}: running ({}s warmup + {}s measured)...".format(
                wait_us, trial, args.repeats, args.warmup, args.duration))
            throughput = run_trial("life_{}us".format(wait_us), args.duration,
                                   args.warmup, args.startup_timeout, trial,
                                   args.node_count, args.client_node_count)
            throughputs.append(throughput)
            log("LIFE {:>4} us trial {}/{}: {:.2f} txn/s".format(
                wait_us, trial, args.repeats, throughput))
        rows.append((wait_us, median(throughputs), throughputs))

    best_wait, best_median, _ = max(rows, key=lambda row: row[1])
    mkdir_p(RESULTS)
    csv_path = os.path.join(RESULTS, "summary.csv")
    csv_mode = "wb" if PY2 else "w"
    with open(csv_path, csv_mode) as output:
        writer = csv.writer(output)
        writer.writerow(("wait_us", "median_tput", "trial_tputs"))
        for wait_us, median_tput, throughputs in rows:
            writer.writerow((wait_us, median_tput, " ".join(map(str, throughputs))))

    restore_config()
    if args.apply_best:
        applied = replace_define(original, "CC_ALG", "LIFE")
        applied = replace_define(applied, "LIFE_HELP_WAIT_US", best_wait)
        write_text(CONFIG, applied)
    log("Best LIFE_HELP_WAIT_US: {} ({:.2f} median txn/s)".format(
        best_wait, best_median))
    log("Results: {}".format(csv_path))
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        sys.exit("interrupted; config.h restored")
