import threading
from pathlib import Path


class LamportClock:
    def __init__(self, name, log_file=None, quiet=False):
        self.name = name
        self.value = 0
        self._lock = threading.Lock()
        self.quiet = quiet
        self.path = Path(log_file) if log_file else None
        if self.path:
            self.path.parent.mkdir(parents=True, exist_ok=True)

    def event(self, kind, description, received=None):
        with self._lock:
            if received is None:
                self.value += 1
            else:
                self.value = max(self.value, received) + 1
            line = f"[{self.name}] {kind:<6} {description}  L={self.value}"
            if received is not None:
                line += f"  (received L={received})"
            if not self.quiet:
                print(line, flush=True)
            if self.path:
                with self.path.open("a", encoding="utf-8") as stream:
                    stream.write(line + "\n")
            return self.value