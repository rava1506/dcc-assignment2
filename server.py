import argparse
import threading
from concurrent import futures

import grpc
import counter_pb2 as pb
import counter_pb2_grpc as rpc


class CounterServicer(rpc.CounterServicer):
    def __init__(self):
        self._lock = threading.Lock()
        self._values = {}
        self._seen = {}

    def Increment(self, request, context):
        if not request.counter_id or not request.idempotency_key:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "ID and key are required")
        with self._lock:
            old = self._seen.get(request.idempotency_key)
            if old is not None:
                counter_id, delta, value = old
                if (counter_id, delta) != (request.counter_id, request.delta):
                    context.abort(grpc.StatusCode.INVALID_ARGUMENT, "Key reused for another operation")
                return pb.IncrementReply(new_value=value, was_duplicate=True)
            value = self._values.get(request.counter_id, 0) + request.delta
            if not -(2 ** 63) <= value < 2 ** 63:
                context.abort(grpc.StatusCode.OUT_OF_RANGE, "int64 overflow")
            self._values[request.counter_id] = value
            self._seen[request.idempotency_key] = (request.counter_id, request.delta, value)
            return pb.IncrementReply(new_value=value, was_duplicate=False)

    def Get(self, request, context):
        with self._lock:
            return pb.GetReply(value=self._values.get(request.counter_id, 0),
                               found=request.counter_id in self._values)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=50051)
    args = parser.parse_args()
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=8))
    rpc.add_CounterServicer_to_server(CounterServicer(), server)
    port = server.add_insecure_port(f"127.0.0.1:{args.port}")
    if not port:
        raise RuntimeError("Cannot bind port")
    server.start()
    print(f"Listening on 127.0.0.1:{port}", flush=True)
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        server.stop(0).wait()


if __name__ == "__main__":
    main()