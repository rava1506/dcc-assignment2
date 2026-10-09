import sys
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from pathlib import Path

import grpc
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from client import CounterClient, QuorumClient
from clocks import LamportClock
from server import start_server


class Node:
    def __init__(self, **options):
        self.server, self.service, self.target = start_server(quiet=True, **options)
        self.clients = []
        self.probe = grpc.insecure_channel(self.target)
        try:
            grpc.channel_ready_future(self.probe).result(timeout=5)
        except Exception:
            self.close()
            raise

    def client(self, **options):
        client = CounterClient(self.target, quiet=True, **options)
        self.clients.append(client)
        return client

    def stop(self):
        self.server.stop(0).wait()

    def close(self):
        for client in self.clients:
            client.close()
        self.probe.close()
        self.stop()


@contextmanager
def group(count=3):
    nodes = []
    try:
        for index in range(count):
            nodes.append(Node(name=f"replica-{index}"))
        yield nodes
    finally:
        for node in nodes:
            node.close()


@pytest.fixture
def running_server():
    with group(1) as nodes:
        yield nodes[0]


def test_increment_applies_delta(running_server):
    client = running_server.client()
    reply = client.incr("x", 5, "single")
    assert reply.new_value == 5
    assert not reply.was_duplicate
    assert client.get("x").value == 5


def test_duplicate_key_not_reapplied(running_server):
    client = running_server.client()
    first = client.incr("x", 5, "duplicate")
    client.incr("x", 3, "another")
    second = client.incr("x", 5, "duplicate")
    assert first.new_value == second.new_value == 5
    assert second.was_duplicate
    assert client.get("x").value == 8


def test_get_missing_counter(running_server):
    client = running_server.client()
    reply = client.get("missing")
    assert not reply.found
    assert reply.value == 0


def test_concurrent_increments_exact(running_server):
    clients = [running_server.client(name=f"client-{index}") for index in range(2)]

    def worker(index):
        for number in range(1000):
            clients[index].incr("x", 1, f"worker-{index}-{number}")

    with ThreadPoolExecutor(max_workers=2) as pool:
        list(pool.map(worker, range(2)))
    assert clients[0].get("x").value == 2000


def timeout_case():
    path = Path("logs/failure-timeout.log")
    path.parent.mkdir(exist_ok=True)
    path.write_text("", encoding="utf-8")
    node = Node(fault="delay-first", delay_ms=1500, log_file=path)
    try:
        client = node.client(timeout=0.3, backoff=0.05)
        reply = client.incr("x", 7, "timeout-key")
        assert grpc.StatusCode.DEADLINE_EXCEEDED in client.errors
        assert client.attempts >= 2
        assert reply.was_duplicate
        assert reply.new_value == 7
        assert client.get("x").value == 7
        assert node.service._applied == 1
        assert node.service.timeout_event.wait(2)
        print("TIMEOUT: deadline observed; same-key retry succeeded; value=7; applies=1")
    finally:
        node.close()


def test_retry_after_timeout_is_safe():
    timeout_case()


def test_majority_commit_two_acks():
    with group() as nodes:
        nodes[2].stop()
        client = QuorumClient([node.target for node in nodes], timeout=0.3,
                              retries=0, quiet=True)
        try:
            result = client.incr("x", 1, "majority")
            assert result["committed"]
            assert result["acks"] == 2
            assert all(node.client().get("x").value == 1 for node in nodes[:2])
        finally:
            client.close()


def test_no_commit_below_majority():
    with group() as nodes:
        nodes[1].stop()
        nodes[2].stop()
        client = QuorumClient([node.target for node in nodes], timeout=0.3,
                              retries=0, quiet=True)
        try:
            result = client.incr("x", 1, "partial")
            assert not result["committed"]
            assert result["acks"] == 1
            assert nodes[0].client().get("x").value == 1
        finally:
            client.close()


def test_replicas_converge():
    with group() as nodes:
        client = QuorumClient([node.target for node in nodes], quiet=True)
        try:
            for number in range(30):
                result = client.incr("x", 1, f"batch-{number}")
                assert result["committed"] and result["acks"] == 3
            assert [node.client().get("x").value for node in nodes] == [30, 30, 30]
        finally:
            client.close()


def test_lamport_rules():
    clock = LamportClock("test", quiet=True)
    assert clock.event("SEND", "a") == 1
    assert clock.event("RECV", "b", received=10) == 11
    assert clock.event("RECV", "c", received=2) == 12


def test_protocol_lamport_round_trip(running_server):
    client = running_server.client()
    reply = client.incr("x", 1, "clock-key")
    assert reply.lamport_time > 1
    assert client.clock.value == reply.lamport_time + 1
    before = client.clock.value
    read = client.get("x")
    assert read.lamport_time > before + 1
    assert client.clock.value == read.lamport_time + 1


def test_counter_isolation(running_server):
    client = running_server.client()
    client.incr("x", 5, "x-key")
    client.incr("y", 3, "y-key")
    assert client.get("x").value == 5
    assert client.get("y").value == 3


def test_key_conflict_is_rejected(running_server):
    client = running_server.client()
    client.incr("x", 5, "key")
    with pytest.raises(grpc.RpcError) as error:
        client.incr("y", 1, "key")
    assert error.value.code() == grpc.StatusCode.INVALID_ARGUMENT
    assert not client.get("y").found