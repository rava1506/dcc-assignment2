import argparse
import time
import uuid

import grpc
import counter_pb2 as pb
import counter_pb2_grpc as rpc


class CounterClient:
    def __init__(self, target="127.0.0.1:50051", timeout=2.0):
        self.target = target
        self.channel = grpc.insecure_channel(target)
        self.stub = rpc.CounterStub(self.channel)
        self.timeout = timeout

    def close(self):
        self.channel.close()

    def _call(self, method, request):
        for attempt in range(4):
            try:
                return method(request, timeout=self.timeout)
            except grpc.RpcError as error:
                retryable = error.code() in (
                    grpc.StatusCode.DEADLINE_EXCEEDED,
                    grpc.StatusCode.UNAVAILABLE,
                )
                if not retryable or attempt == 3:
                    raise
                time.sleep(0.2 * (2 ** attempt))

    def incr(self, counter_id, delta=1, key=None):
        key = key if key is not None else str(uuid.uuid4())
        request = pb.IncrementRequest(counter_id=counter_id, delta=delta,
                                      idempotency_key=key)
        return self._call(self.stub.Increment, request)

    def get(self, counter_id):
        return self._call(self.stub.Get, pb.GetRequest(counter_id=counter_id))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--target", default="127.0.0.1:50051")
    parser.add_argument("--timeout", type=float, default=2.0)
    sub = parser.add_subparsers(dest="command", required=True)
    incr = sub.add_parser("incr")
    incr.add_argument("counter_id")
    incr.add_argument("--by", type=int, default=1)
    incr.add_argument("--key")
    get = sub.add_parser("get")
    get.add_argument("counter_id")
    args = parser.parse_args()
    client = CounterClient(args.target, args.timeout)
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