# Replicated Counter Service

Python gRPC service with idempotent mutations, Lamport clocks and independent
three-replica majority writes. No leader election, log shipping or persistence.

## Setup: Windows PowerShell

Open a terminal in the repository root. Python and Git must already be installed.

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install --upgrade pip
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
.\.venv\Scripts\python.exe -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. counter.proto
```

If `python` is unavailable but the Windows launcher exists, use `py -m venv .venv`.
The direct Python path avoids requiring PowerShell script activation.

## Single replica

Terminal 1:

```powershell
.\.venv\Scripts\python.exe server.py --port 50051 --name replica-A
```

Terminal 2:

```powershell
.\.venv\Scripts\python.exe client.py incr x --by 5 --key demo-001
.\.venv\Scripts\python.exe client.py incr x --by 5 --key demo-001
.\.venv\Scripts\python.exe client.py get x
```

On a fresh server the value remains 5 and the second Increment is a duplicate.
Stop a server with Ctrl+C. An in-memory restart resets both values and seen keys.

## Three replicas

Run each command in a separate terminal:

```powershell
.\.venv\Scripts\python.exe server.py --port 50051 --name replica-A
.\.venv\Scripts\python.exe server.py --port 50052 --name replica-B
.\.venv\Scripts\python.exe server.py --port 50053 --name replica-C
```

In a fourth terminal:

```powershell
.\.venv\Scripts\python.exe client.py incr q --by 1 --key quorum-001 --replicas 127.0.0.1:50051 127.0.0.1:50052 127.0.0.1:50053
.\.venv\Scripts\python.exe client.py --target 127.0.0.1:50051 get q
```

The client fans out in parallel, waits for all bounded attempts, and reports
committed=true only for at least two distinct acknowledging replicas.
One stopped replica still permits a commit. Two stopped replicas cause failure;
the remaining replica may nevertheless have applied the operation.
Default: 2-second deadline, initial attempt plus up to 3 retries, backoffs
0.2/0.4/0.8 seconds. All attempts reuse the logical operation's UUID.

## Tests and evidence

Tests launch their own servers on ephemeral ports and perform cleanup.
No manually running servers are required.

```powershell
.\.venv\Scripts\python.exe -m pytest tests -v -s --junitxml=logs/test_results.xml
.\.venv\Scripts\python.exe tests/perf_benchmark.py --summarize-tests
```

Eight required counter tests, four extra checks and three failure scenarios.
The crash test kills a separate replica process after its received marker.
Failure logs and XML are saved under logs/.

## Lamport scenario

```powershell
.\.venv\Scripts\python.exe client.py scenario
```

Starts one fresh server and three client processes: client-1 increments x twice,
client-2 increments y twice, client-3 reads only. See logs/event_excerpt.log,
the individual logs and logs/ordering_analysis.txt. The excerpt preserves local
order and is grouped by participant; it is not a global physical-time ordering.

## Benchmark

```powershell
.\.venv\Scripts\python.exe tests/perf_benchmark.py
```

Runs four configurations, 2000 logical writes each, 50 excluded warmups per
configuration. Reused channels, quiet logging, 1 or 16 client workers.
Replicas are independent processes; 8 gRPC server worker threads each.
Median and nearest-rank p95 use milliseconds. p95 index:
ceil(0.95*n)-1 in Python. Quorum latency covers the implemented full operation,
including waiting for all bounded replica attempts.
See logs/performance.csv, logs/latencies.csv, logs/environment.json and
logs/pip-freeze.txt. Requests count logical operations, not individual replica RPCs.

## Fault flags

```powershell
.\.venv\Scripts\python.exe server.py --port 50051 --fault delay-first --delay-ms 3000
.\.venv\Scripts\python.exe server.py --port 50051 --fault pause-after-recv --delay-ms 5000 --marker-file logs/received.txt
```

delay-first delays only the first newly applied Increment reply; a retry can use
the saved result. pause-after-recv writes the marker and pauses without applying,
allowing deterministic process killing. --log-file PATH captures event lines;
--quiet suppresses console event lines while retaining clock instrumentation.

## Semantics and limitations

At-most-once mutation per key per replica while its in-memory dedup state lives.
Duplicate keys with conflicting payloads are rejected. State and dedup check
share one lock. Scalar Lamport timestamps establish a necessary condition for
causality; they do not detect concurrency on their own.

No durable storage, no catch-up for restarted replicas, no atomic rollback of
failed quorum writes and no linearizable reads. A single-replica Get may be
stale. Quorum intersection alone does not turn this design into consensus.
Missing operation delivery can leave replicas divergent; duplicate suppression
does not deliver missed writes. Quorum is fixed at 2 of the original 3 nodes.

## Linux or macOS

Use `python3 -m venv .venv`, install with `.venv/bin/python -m pip install -r requirements.txt`,
then replace each `.\.venv\Scripts\python.exe` above with `.venv/bin/python`.
The Python code is platform-independent; the commands above target PowerShell.

## Submission

Source, incremental Git history, generated stubs, tests, captured logs, report.pdf.
For defense: deck PDF, repository link and ZIP in Canvas.
The report must contain local measured results and B2 ordering analysis.
