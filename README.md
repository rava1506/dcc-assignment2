# Replicated Counter Service

Individual distributed systems assignment implemented in Python and gRPC.
The service provides named counters with Increment and Get operations.
Development stages: single replica, Lamport clocks, three-replica quorum writes.

## Setup on Windows PowerShell

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install --upgrade pip
.\.venv\Scripts\python.exe -m pip install grpcio grpcio-tools pytest
```

## Development status

Environment initialized. Parts A, B and C will be committed as they are implemented.
