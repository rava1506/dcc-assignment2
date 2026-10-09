import argparse
import json
import re
import subprocess
import sys
import tempfile
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import grpc
import counter_pb2 as pb
import counter_pb2_grpc as rpc
from clocks import LamportClock


class CounterClient:
    def __init__(self, target="127.0.0.1:50051", name="client-1", timeout=2.0,
                 retries=3, backoff=0.2, log_file=None, quiet=False, clock=None,
                 first_send_barrier=None):
        self.target = target
        self.channel = grpc.insecure_channel(target)
        self.stub = rpc.CounterStub(self.channel)
        self.timeout = timeout
        self.retries = retries
        self.backoff = backoff
        self.clock = clock if clock is not None else LamportClock(name, log_file, quiet)
        self.first_send_barrier = first_send_barrier
        self.attempts = 0
        self.errors = []

    def close(self):
        self.channel.close()

    def _call(self, method, request, description):
        for attempt in range(self.retries + 1):
            request.lamport_time = self.clock.event("SEND", description)
            if self.first_send_barrier is not None:
                barrier = self.first_send_barrier
                self.first_send_barrier = None
                barrier.wait(timeout=10)
            self.attempts += 1
            try:
                reply = method(request, timeout=self.timeout)
                if isinstance(reply, pb.IncrementReply):
                    details = f"IncrementReply(new_value={reply.new_value})"
                else:
                    details = f"GetReply(value={reply.value}, found={reply.found})"
                self.clock.event("RECV", details, received=reply.lamport_time)
                return reply
            except grpc.RpcError as error:
                self.errors.append(error.code())
                self.clock.event("ERROR", error.code().name)
                retryable = error.code() in (
                    grpc.StatusCode.DEADLINE_EXCEEDED,
                    grpc.StatusCode.UNAVAILABLE,
                )
                if not retryable or attempt == self.retries:
                    raise
                self.clock.event("RETRY", f"attempt={attempt + 2}")
                time.sleep(self.backoff * (2 ** attempt))

    def incr(self, counter_id, delta=1, key=None):
        key = key if key is not None else str(uuid.uuid4())
        request = pb.IncrementRequest(counter_id=counter_id, delta=delta,
                                      idempotency_key=key)
        return self._call(self.stub.Increment, request,
                          f"Increment(counter={counter_id}, delta={delta})")

    def get(self, counter_id):
        return self._call(self.stub.Get, pb.GetRequest(counter_id=counter_id),
                          f"Get(counter={counter_id})")


def run_scenario():
    # Self-contained B2 scenario with three independent client processes.
    from server import start_server
    logs = Path("logs")
    logs.mkdir(exist_ok=True)
    names = ["client-1", "client-2", "client-3", "replica-A"]
    for name in names:
        (logs / f"{name}.log").write_text("", encoding="utf-8")
    server, _, target = start_server(name="replica-A", log_file=logs / "replica-A.log", quiet=True)
    clients = []
    try:
        with tempfile.TemporaryDirectory() as sync_dir:
            processes = []
            try:
                for index in range(3):
                    command = [sys.executable, str(Path(__file__).resolve()),
                               "--target", target, "scenario-worker", "--index", str(index),
                               "--sync-dir", sync_dir, "--logs-dir", str(logs.resolve())]
                    processes.append(subprocess.Popen(command))
                for process in processes:
                    if process.wait(timeout=20) != 0:
                        raise RuntimeError("A scenario client failed")
            finally:
                for process in processes:
                    if process.poll() is None:
                        process.kill()
                    process.wait(timeout=5)
        checker = CounterClient(target, name="checker", quiet=True)
        clients.append(checker)
        assert checker.get("x").value == 2
        assert checker.get("y").value == 2
    finally:
        for client in clients:
            client.close()
        server.stop(0).wait()
    # Preserve each participant's local order. This is an excerpt, not a wall-clock timeline.
    excerpt = []
    for name in names[:3]:
        lines = (logs / f"{name}.log").read_text(encoding="utf-8").splitlines()
        excerpt.extend(lines[:4] if name != "client-2" else lines[:6])
    server_lines = (logs / "replica-A.log").read_text(encoding="utf-8").splitlines()
    selected = [index for index, line in enumerate(server_lines)
                if "Increment(" in line or "IncrementReply(" in line or "] APPLY" in line]
    extra = [index for index in range(len(server_lines)) if index not in selected]
    selected += extra[:max(0, 16 - len(selected))]
    excerpt.extend(server_lines[index] for index in sorted(selected))
    (logs / "event_excerpt.log").write_text("\n".join(excerpt) + "\n", encoding="utf-8")

    def first_send(name):
        line = next(line for line in excerpt if line.startswith(f"[{name}] SEND"))
        value = int(re.search(r"L=(\d+)", line).group(1))
        return line, value

    first, a = first_send("client-1")
    second, b = first_send("client-2")
    analysis = f"""B2 scenario generated from this run
Pair 1: client-1 first SEND of Increment(x) -> replica-A RECV of that request.
Pair 2: client-1 first RECV of IncrementReply -> its second SEND of Increment(x).
Proof: pair 1 has a message edge; pair 2 has local program order.
Concurrent pair: {first}
                {second}
The launcher barrier prevents either first request from reaching the server
until all three client processes have logged their first SEND. Neither chosen SEND has
an incoming reply before it. Thus neither causally precedes the other.
Their times {a} and {b} differ; this does not establish causal or physical order.
Lamport clocks cannot distinguish concurrency solely from scalar timestamps.
Vector clocks can detect concurrency by incomparable timestamp vectors.
The merged excerpt groups participants and preserves their local order.
Diagram edges: client-1 SEND1 -> replica-A RECV1 -> APPLY1 -> SEND1 ->
client-1 RECV1 -> client-1 SEND2. No causal edge between the two first SENDs.
"""
    (logs / "ordering_analysis.txt").write_text(analysis, encoding="utf-8")
    print("Scenario OK: x=2 y=2. See logs/event_excerpt.log and logs/ordering_analysis.txt")


class FileBarrier:
    def __init__(self, folder, index):
        self.folder = Path(folder)
        self.index = index

    def wait(self, timeout=10):
        (self.folder / f"{self.index}.sent").write_text("sent", encoding="utf-8")
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if all((self.folder / f"{index}.sent").exists() for index in range(3)):
                return
            time.sleep(0.01)
        raise RuntimeError("Scenario barrier timed out")


def scenario_worker(args):
    index = args.index
    client = CounterClient(args.target, name=f"client-{index + 1}", quiet=True,
                           log_file=Path(args.logs_dir) / f"client-{index + 1}.log",
                           first_send_barrier=FileBarrier(args.sync_dir, index))
    try:
        grpc.channel_ready_future(client.channel).result(timeout=5)
        if index == 1:
            client.clock.event("START", "scenario initialization")
            client.clock.event("START", "counter y selected")
        if index < 2:
            counter = "x" if index == 0 else "y"
            for number in range(2):
                client.incr(counter, 1, f"scenario-{index}-{number}")
        else:
            for counter in ["x", "y", "x", "y"]:
                client.get(counter)
    finally:
        client.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--target", default="127.0.0.1:50051")
    parser.add_argument("--name", default="client-1")
    parser.add_argument("--timeout", type=float, default=2.0)
    parser.add_argument("--quiet", action="store_true")
    sub = parser.add_subparsers(dest="command", required=True)
    incr = sub.add_parser("incr")
    incr.add_argument("counter_id")
    incr.add_argument("--by", type=int, default=1)
    incr.add_argument("--key")
    get = sub.add_parser("get")
    get.add_argument("counter_id")
    sub.add_parser("scenario")
    worker = sub.add_parser("scenario-worker")
    worker.add_argument("--index", type=int, choices=[0, 1, 2], required=True)
    worker.add_argument("--sync-dir", required=True)
    worker.add_argument("--logs-dir", required=True)
    args = parser.parse_args()
    if args.command == "scenario-worker":
        scenario_worker(args)
        return
    if args.command == "scenario":
        run_scenario()
        return
    client = CounterClient(args.target, args.name, args.timeout, quiet=args.quiet)
    try:
        if args.command == "incr":
            result = client.incr(args.counter_id, args.by, args.key)
            print(f"new_value={result.new_value} was_duplicate={result.was_duplicate}")
        else:
            result = client.get(args.counter_id)
            print(f"value={result.value} found={result.found}")
    except grpc.RpcError as error:
        print(f"RPC failed: {error.code().name}: {error.details()}")
        raise SystemExit(1)
    finally:
        client.close()


if __name__ == "__main__":
    main()