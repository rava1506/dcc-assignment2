import sys
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import grpc
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import counter_pb2_grpc as rpc
from client import CounterClient
from server import CounterServicer


@pytest.fixture
def running_server():
    server = grpc.server(ThreadPoolExecutor(max_workers=8))
    rpc.add_CounterServicer_to_server(CounterServicer(), server)
    port = server.add_insecure_port("127.0.0.1:0")
    server.start()
    client = CounterClient(f"127.0.0.1:{port}")
    try:
        grpc.channel_ready_future(client.channel).result(timeout=5)
        yield client
    finally:
        client.close()
        server.stop(0).wait()


def test_increment_applies_delta(running_server):
    reply = running_server.incr("x", 5, "single")
    assert reply.new_value == 5
    assert not reply.was_duplicate
    assert running_server.get("x").value == 5


def test_duplicate_key_not_reapplied(running_server):
    first = running_server.incr("x", 5, "duplicate")
    second = running_server.incr("x", 5, "duplicate")
    assert first.new_value == second.new_value == 5
    assert second.was_duplicate
    assert running_server.get("x").value == 5


def test_get_missing_counter(running_server):
    reply = running_server.get("missing")
    assert not reply.found
    assert reply.value == 0


def test_concurrent_increments_exact(running_server):
    clients = [CounterClient(running_server.target) for _ in range(2)]

    def worker(index):
        for number in range(1000):
            clients[index].incr("x", 1, f"worker-{index}-{number}")

    try:
        with ThreadPoolExecutor(max_workers=2) as pool:
            list(pool.map(worker, range(2)))
        assert running_server.get("x").value == 2000
    finally:
        for client in clients:
            client.close()        