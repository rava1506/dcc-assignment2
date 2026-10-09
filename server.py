import argparse
import threading
import time
from concurrent import futures
from pathlib import Path

import grpc
import counter_pb2 as pb
import counter_pb2_grpc as rpc
from clocks import LamportClock


class CounterServicer(rpc.CounterServicer):
    def __init__(self, name="replica-A", log_file=None, quiet=False,
                 fault="none", delay_ms=0, marker_file=None):
        self._lock = threading.Lock()
        self._values = {}
        self._seen = {}
        self.clock = LamportClock(name, log_file, quiet)
        self.fault = fault
        self.delay = delay_ms / 1000.0
        self.marker_file = Path(marker_file) if marker_file else None
        self.received_event = threading.Event()
        self.retry_event = threading.Event()
        self.timeout_event = threading.Event()
        self._delayed = False
        self._applied = 0

    def Increment(self, request, context):
        self.received_event.set()
        delay_this = False
        with self._lock:
            self.clock.event("RECV", f"Increment(counter={request.counter_id}, delta={request.delta})",
                             received=request.lamport_time)
            if not request.counter_id or not request.idempotency_key:
                context.abort(grpc.StatusCode.INVALID_ARGUMENT, "ID and key are required")
            if self.fault == "pause-after-recv":
                if self.marker_file:
                    self.marker_file.parent.mkdir(parents=True, exist_ok=True)
                    self.marker_file.write_text("request received", encoding="utf-8")
            else:
                old = self._seen.get(request.idempotency_key)
                duplicate = old is not None
                if duplicate:
                    counter_id, delta, value = old
                    if (counter_id, delta) != (request.counter_id, request.delta):
                        context.abort(grpc.StatusCode.INVALID_ARGUMENT,
                                      "Key reused for another operation")
                    self.retry_event.set()
                    self.clock.event("DUP", f"counter={counter_id} -> {value}")
                else:
                    value = self._values.get(request.counter_id, 0) + request.delta
                    if not -(2 ** 63) <= value < 2 ** 63:
                        context.abort(grpc.StatusCode.OUT_OF_RANGE, "int64 overflow")
                    self._values[request.counter_id] = value
                    self._seen[request.idempotency_key] = (request.counter_id, request.delta, value)
                    self._applied += 1
                    self.clock.event("APPLY", f"counter={request.counter_id} -> {value}")
                    if self.fault == "delay-first" and not self._delayed:
                        self._delayed = True
                        delay_this = True
        if self.fault == "pause-after-recv":
            time.sleep(self.delay or 10.0)
            context.abort(grpc.StatusCode.UNAVAILABLE, "Injected pause")
        if delay_this:
            # The mutation has happened, but the first reply exceeds its deadline.
            deadline = time.monotonic() + self.delay
            while time.monotonic() < deadline:
                if not context.is_active():
                    self.timeout_event.set()
                    context.abort(grpc.StatusCode.DEADLINE_EXCEEDED,
                                  "Injected delay after apply")
                time.sleep(0.005)
        sent = self.clock.event("SEND", f"IncrementReply(new_value={value})")
        return pb.IncrementReply(new_value=value, was_duplicate=duplicate,
                                  lamport_time=sent)

    def Get(self, request, context):
        with self._lock:
            self.clock.event("RECV", f"Get(counter={request.counter_id})",
                             received=request.lamport_time)
            value = self._values.get(request.counter_id, 0)
            found = request.counter_id in self._values
            self.clock.event("READ", f"counter={request.counter_id} -> {value}, found={found}")
            sent = self.clock.event("SEND", f"GetReply(value={value}, found={found})")
        return pb.GetReply(value=value, found=found, lamport_time=sent)


def start_server(port=0, **options):
    service = CounterServicer(**options)
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=8))
    rpc.add_CounterServicer_to_server(service, server)
    bound = server.add_insecure_port(f"127.0.0.1:{port}")
    if not bound:
        raise RuntimeError(f"Cannot bind port {port}")
    server.start()
    return server, service, f"127.0.0.1:{bound}"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=50051)
    parser.add_argument("--name", default="replica-A")
    parser.add_argument("--log-file")
    parser.add_argument("--quiet", action="store_true")
    parser.add_argument("--fault", choices=["none", "delay-first", "pause-after-recv"], default="none")
    parser.add_argument("--delay-ms", type=int, default=0)
    parser.add_argument("--marker-file")
    args = parser.parse_args()
    server, _, target = start_server(args.port, name=args.name, log_file=args.log_file,
                                     quiet=args.quiet, fault=args.fault,
                                     delay_ms=args.delay_ms, marker_file=args.marker_file)
    print(f"READY {target}", flush=True)
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        server.stop(0).wait()


if __name__ == "__main__":
    main()