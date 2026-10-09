import argparse
import csv
import json
import math
import platform
import statistics
import subprocess
import sys
import threading
import time
import uuid
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import grpc

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from client import CounterClient, QuorumClient


def summarize_tests():
    logs = ROOT / "logs"
    root = ET.parse(logs / "test_results.xml").getroot()
    purposes = {
        "test_increment_applies_delta": "Increment applies +5 to a fresh counter",
        "test_duplicate_key_not_reapplied": "Duplicate returns original 5; current state stays 8",
        "test_get_missing_counter": "Missing counter has found=False and value=0",
        "test_concurrent_increments_exact": "Two clients x 1000 increments produce exactly 2000",
        "test_retry_after_timeout_is_safe": "Observed timeout; same-key retry; value=7; applies=1",
        "test_majority_commit_two_acks": "One replica stopped; committed=True and acks=2",
        "test_no_commit_below_majority": "Two replicas stopped; committed=False; surviving value=1",
        "test_replicas_converge": "30 delivered writes; all three values equal 30",
        "test_lamport_rules": "Clock transitions equal 1, 11, 12",
        "test_counter_isolation": "x=5 and y=3 remain independent",
        "test_key_conflict_is_rejected": "Reused key with another payload is INVALID_ARGUMENT",
        "test_replica_crash_mid_request": "Killed after RECV; two acknowledgements; values=[1,1]",
        "test_request_duplication": "Second reply duplicate=True; current value=5",
        "test_induced_timeout_with_retry": "Delayed first reply; timeout then safe retry",
        "test_protocol_lamport_round_trip": "Reply clock exceeds request; receiving client advances",
    }
    rows = []
    for case in root.iter("testcase"):
        failed = case.find("failure") is not None or case.find("error") is not None
        skipped = case.find("skipped") is not None
        status = "FAIL" if failed else "SKIP" if skipped else "PASS"
        expected = purposes.get(case.attrib["name"], "See executable assertions")
        actual = "All listed assertions passed" if status == "PASS" else "See XML failure or skip details"
        rows.append([case.attrib["name"], expected, actual, status])
    with (logs / "test_case_results.csv").open("w", encoding="utf-8", newline="") as stream:
        writer = csv.writer(stream)
        writer.writerow(["Test ID", "Purpose and expected result", "Actual result", "Status"])
        writer.writerows(rows)
    counts = {status: sum(row[3] == status for row in rows) for status in ["PASS", "FAIL", "SKIP"]}
    (logs / "test_summary.json").write_text(json.dumps(counts, indent=2), encoding="utf-8")
    print(json.dumps(counts))
    print("Saved logs/test_case_results.csv")


def benchmark(requests):
    if requests < 2000 or requests % 16:
        raise ValueError("Use at least 2000 requests, divisible by 16; default is 2000")
    logs = ROOT / "logs"
    logs.mkdir(exist_ok=True)
    processes, targets, streams = [], [], []
    try:
        for index in range(3):
            path = logs / f"benchmark-server-{index}.log"
            stream = path.open("w", encoding="utf-8")
            streams.append(stream)
            process = subprocess.Popen([sys.executable, "-u", str(ROOT / "server.py"),
                                        "--port", "0", "--name", f"replica-{index}", "--quiet"],
                                       cwd=ROOT, stdout=stream, stderr=subprocess.STDOUT)
            processes.append(process)
            deadline = time.monotonic() + 10
            target = None
            while time.monotonic() < deadline:
                text = path.read_text(encoding="utf-8")
                ready = next((line for line in text.splitlines() if line.startswith("READY ")), None)
                if ready:
                    target = ready.split()[1]
                    break
                if process.poll() is not None:
                    raise RuntimeError(text)
                time.sleep(0.01)
            if target is None:
                raise RuntimeError("Replica startup timed out")
            targets.append(target)
            channel = grpc.insecure_channel(target)
            try:
                grpc.channel_ready_future(channel).result(timeout=5)
            finally:
                channel.close()
        rows = []
        raw_rows = []
        for quorum_mode, workers in [(False, 1), (False, 16), (True, 1), (True, 16)]:
            label = ("Quorum (3 replicas)" if quorum_mode else "Single replica")
            label += f", {workers} " + ("client" if workers == 1 else "clients")
            clients = []
            try:
                for index in range(workers):
                    if quorum_mode:
                        client = QuorumClient(targets, name=f"bench-{index}", quiet=True)
                        for child in client.clients:
                            grpc.channel_ready_future(child.channel).result(timeout=5)
                    else:
                        client = CounterClient(targets[0], name=f"bench-{index}", quiet=True)
                        grpc.channel_ready_future(client.channel).result(timeout=5)
                    clients.append(client)
                warm = clients[0]
                for number in range(50):
                    warm.incr("warmup", 1, f"warm-{uuid.uuid4()}")
                barrier = threading.Barrier(workers)
                run_id = str(uuid.uuid4())

                def worker(index):
                    local = []
                    barrier.wait(timeout=10)
                    for number in range(requests // workers):
                        key = f"{run_id}-{index}-{number}"
                        start = time.perf_counter()
                        result = clients[index].incr(run_id, 1, key)
                        elapsed = (time.perf_counter() - start) * 1000
                        if quorum_mode and not result["committed"]:
                            raise RuntimeError("Benchmark write did not commit")
                        local.append(elapsed)
                    return local

                start = time.perf_counter()
                with ThreadPoolExecutor(max_workers=workers) as pool:
                    batches = list(pool.map(worker, range(workers)))
                seconds = time.perf_counter() - start
                samples = [value for batch in batches for value in batch]
                ordered = sorted(samples)
                p95 = ordered[math.ceil(0.95 * len(ordered)) - 1]
                median = statistics.median(samples)
                rows.append([label, median, p95, len(samples), len(samples) / seconds])
                raw_rows.extend([label, index, value] for index, value in enumerate(samples))
                print(f"{label}: median={median:.3f} ms p95={p95:.3f} ms requests={len(samples)}")
            finally:
                for client in clients:
                    client.close()
        with (logs / "performance.csv").open("w", encoding="utf-8", newline="") as stream:
            writer = csv.writer(stream)
            writer.writerow(["Configuration", "Median latency (ms)", "p95 latency (ms)", "Requests", "Throughput (ops/s)"])
            writer.writerows(rows)
        with (logs / "latencies.csv").open("w", encoding="utf-8", newline="") as stream:
            writer = csv.writer(stream)
            writer.writerow(["Configuration", "Sample", "Latency (ms)"])
            writer.writerows(raw_rows)
        environment = {"OS": platform.platform(), "Python": sys.version,
                       "Machine": platform.machine(), "Processor": platform.processor(),
                       "Method": "50 warmup writes per configuration; reused channels; logs quiet; all bounded replica calls awaited",
                       "p95": "nearest rank: sorted[ceil(0.95*n)-1]",
                       "Requests per configuration": requests}
        (logs / "environment.json").write_text(json.dumps(environment, indent=2), encoding="utf-8")
        frozen = subprocess.check_output([sys.executable, "-m", "pip", "freeze"], text=True)
        (logs / "pip-freeze.txt").write_text(frozen, encoding="utf-8")
    finally:
        for process in processes:
            if process.poll() is None:
                process.kill()
            process.wait(timeout=5)
        for stream in streams:
            stream.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--requests", type=int, default=2000)
    parser.add_argument("--summarize-tests", action="store_true")
    args = parser.parse_args()
    if args.summarize_tests:
        summarize_tests()
    else:
        benchmark(args.requests)


if __name__ == "__main__":
    main()