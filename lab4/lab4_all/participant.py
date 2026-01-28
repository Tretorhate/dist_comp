#!/usr/bin/env python3
"""
Lab 4 - Participant (2PC/3PC) with Recovery and Durability
"""

from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import argparse
import json
import threading
import time
import os
from typing import Dict, Any, Optional

lock = threading.Lock()

NODE_ID: str = ""
PORT: int = 8001
kv: Dict[str, str] = {}
TX: Dict[str, Dict[str, Any]] = {}
WAL_PATH: Optional[str] = None

def jdump(obj: Any) -> bytes:
    return json.dumps(obj).encode("utf-8")

def jload(b: bytes) -> Any:
    return json.loads(b.decode("utf-8"))

def wal_append(line: str) -> None:
    """Durably writes a record to the WAL[cite: 68]."""
    if not WAL_PATH:
        return
    with open(WAL_PATH, "a", encoding="utf-8") as f:
        f.write(line.rstrip("\n") + "\n")
        f.flush()
        os.fsync(f.fileno()) # Forces write to physical disk [cite: 33]

def validate_op(op: dict) -> bool:
    t = str(op.get("type", "")).upper()
    if t == "SET":
        return bool(str(op.get("key", "")).strip())
    if t == "TRANSFER":
        return "from" in op and "to" in op and "amount" in op
    return False

def apply_op(op: dict) -> None:
    """Applies the committed operation to the local state[cite: 67]."""
    t = str(op.get("type", "")).upper()
    if t == "SET":
        kv[str(op["key"])] = str(op.get("value", ""))
    elif t == "TRANSFER":
        f, t_to, amt = op["from"], op["to"], float(op["amount"])
        kv[f] = str(float(kv.get(f, 0)) - amt)
        kv[t_to] = str(float(kv.get(t_to, 0)) + amt)

class Handler(BaseHTTPRequestHandler):
    def _send(self, code: int, obj: dict):
        data = jdump(obj)
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        if self.path.startswith("/status"):
            with lock:
                self._send(200, {"ok": True, "node": NODE_ID, "kv": kv, "tx": TX})
            return
        self._send(404, {"ok": False})

    def do_POST(self):
        length = int(self.headers.get("Content-Length", "0"))
        raw = self.rfile.read(length)
        body = jload(raw)

        # 2PC / 3PC Common Phases
        if self.path in ("/prepare", "/can_commit"):
            txid, op = body.get("txid"), body.get("op")
            vote = "YES" if validate_op(op) else "NO"
            state = "READY" if vote == "YES" else "ABORTED"
            with lock:
                TX[txid] = {"state": state, "op": op}
            wal_append(f"{txid} {self.path[1:].upper()} {vote} {json.dumps(op)}")
            self._send(200, {"ok": True, "vote": vote})
            
        elif self.path == "/precommit":
            txid = body.get("txid")
            with lock:
                if txid in TX and TX[txid]["state"] == "READY":
                    TX[txid]["state"] = "PRECOMMIT"
                    wal_append(f"{txid} PRECOMMIT")
                    self._send(200, {"ok": True})
                else:
                    self._send(409, {"ok": False})

        elif self.path == "/commit":
            txid = body.get("txid")
            with lock:
                rec = TX.get(txid)
                if rec and rec["state"] in ("READY", "PRECOMMIT"):
                    apply_op(rec["op"])
                    rec["state"] = "COMMITTED"
                    wal_append(f"{txid} COMMIT")
                    self._send(200, {"ok": True})
                else:
                    self._send(409, {"ok": False})

        elif self.path == "/abort":
            txid = body.get("txid")
            with lock:
                TX[txid] = {"state": "ABORTED", "op": body.get("op")}
                wal_append(f"{txid} ABORT")
            self._send(200, {"ok": True})

def main():
    global NODE_ID, PORT, WAL_PATH
    ap = argparse.ArgumentParser()
    ap.add_argument("--id", required=True)
    ap.add_argument("--port", type=int, default=8001)
    ap.add_argument("--wal", default="")
    args = ap.parse_args()

    NODE_ID, PORT, WAL_PATH = args.id, args.port, args.wal.strip()

    # RECOVERY LOGIC: Replay the WAL to restore state [cite: 16, 68]
    if WAL_PATH and os.path.exists(WAL_PATH):
        with open(WAL_PATH, "r") as f:
            for line in f:
                parts = line.strip().split(" ", 3)
                if len(parts) < 2: continue
                tid, action = parts[0], parts[1]
                if action in ("PREPARE", "CAN_COMMIT"):
                    TX[tid] = {"state": "READY" if parts[2] == "YES" else "ABORTED", "op": json.loads(parts[3])}
                elif action == "PRECOMMIT":
                    TX[tid]["state"] = "PRECOMMIT"
                elif action == "COMMIT":
                    if tid in TX:
                        apply_op(TX[tid]["op"])
                        TX[tid]["state"] = "COMMITTED"
                elif action == "ABORT":
                    TX[tid] = {"state": "ABORTED"}
        print(f"[{NODE_ID}] Recovered state from WAL.")

    server = ThreadingHTTPServer(("0.0.0.0", PORT), Handler)
    print(f"[{NODE_ID}] Participant active on {PORT}")
    server.serve_forever()

if __name__ == "__main__":
    main()