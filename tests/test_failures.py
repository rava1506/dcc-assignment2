import re
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import grpc

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from client import CounterClient, QuorumClient
import test_counter as helpers

ROOT = Path(__file__).resolve().parents[1]


def wait_until(predicate, timeout=10):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.01)
    raise AssertionError("Timed out waiting for injected fault")


def test_replica_crash_mid_request(tmp_path):
    processes, streams, targets = [], [], []
    marker = tmp_path / "received.txt"
    quorum = None
    try:
        for index in range(3):
            output = tmp_path / f"replica-{index}.log"
            stream = output.open("w", encoding="utf-8")
            streams.append(stream)
            command = [sys.executable, "-u", str(ROOT / "server.py"),
                       "--port", "0", "--name", f"replica-{index}"]
            if index == 2:
                command += ["--fault", "pause-after-recv", "--delay-ms", "5000",
                            "--marker-file", str(marker)]
            process = subprocess.Popen(command, cwd=ROOT, stdout=stream,
                                       stderr=subprocess.STDOUT)
            processes.append(process)

            def ready():
                if process.poll() is not None:
                    raise AssertionError(output.read_text(encoding="utf-8"))
                return "READY " in output.read_text(encoding="utf-8")

            wait_until(ready)
            target = re.search(r"READY (127\.0\.0\.1:\d+)",
                               output.read_text(encoding="utf-8")).group(1)
            targets.append(target)
            channel = grpc.insecure_channel(target)
            try:
                grpc.channel_ready_future(channel).result(timeout=5)
            finally:
                channel.close()
        quorum = QuorumClient(targets, timeout=2, retries=0, quiet=True)
        with ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(quorum.incr, "x", 1, "crash-key")
            wait_until(marker.exists)
            processes[2].kill()
            processes[2].wait(timeout=5)
            result = future.result(timeout=10)
        assert result["committed"] and result["acks"] == 2
        for target in targets[:2]:
            client = CounterClient(target, quiet=True)
            try:
                assert client.get("x").value == 1
            finally:
                client.close()
        logs = ROOT / "logs"
        logs.mkdir(exist_ok=True)
        evidence = (tmp_path / "replica-2.log").read_text(encoding="utf-8")
        evidence += "\nKILLED after RECV; committed=True; acks=2; surviving values=[1,1]\n"
        (logs / "failure-crash.log").write_text(evidence, encoding="utf-8")
        print("CRASH: killed after RECV; committed=True; acks=2; values=[1,1]")
    finally:
        if quorum:
            quorum.close()
        for process in processes:
            if process.poll() is None:
                process.kill()
            process.wait(timeout=5)
        for stream in streams:
            stream.close()


def test_request_duplication():
    path = Path("logs/failure-duplicate.log")
    path.parent.mkdir(exist_ok=True)
    path.write_text("", encoding="utf-8")
    node = helpers.Node(log_file=path)
    try:
        client = node.client()
        first = client.incr("x", 5, "duplicate-key")
        second = client.incr("x", 5, "duplicate-key")
        assert first.new_value == second.new_value == 5
        assert second.was_duplicate
        assert client.get("x").value == 5
        print("DUPLICATE: second reply was_duplicate=True; value=5")
    finally:
        node.close()


def test_induced_timeout_with_retry():
    helpers.timeout_case()