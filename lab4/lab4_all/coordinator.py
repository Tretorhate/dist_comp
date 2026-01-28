#!/usr/bin/env python3
"""
Lab 4 - Coordinator (2PC/3PC) with Robust Propagation
"""

from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib import request
import argparse, json, threading, time, os
from typing import List, Tuple

lock = threading.Lock()
PARTICIPANTS: List[str] = []
COORD_WAL = "coordinator.wal"

def post_json(url: str, payload: dict) -> Tuple[int, dict]:
    data = json.dumps(payload).encode("utf-8")
    req = request.Request(url, data=data, headers={"Content-Type":"application/json"}, method="POST")
    with request.urlopen(req, timeout=2.0) as resp:
        return resp.status, json.loads(resp.read().decode("utf-8"))

def log_decision(txid: str, decision: str):
    """Persists the global decision before propagation[cite: 79]."""
    with open(COORD_WAL, "a") as f:
        f.write(f"{txid} {decision}\n")
        f.flush()
        os.fsync(f.fileno())

def robust_send(endpoint: str, payload: dict):
    """Retries communication until all participants acknowledge[cite: 130]."""
    for p in PARTICIPANTS:
        url = p.rstrip("/") + endpoint
        success = False
        while not success:
            try:
                post_json(url, payload)
                success = True
            except Exception:
                print(f"Retrying {endpoint} for {p}...")
                time.sleep(1)

def two_pc(txid: str, op: dict):
    # Phase 1: Prepare [cite: 44]
    votes_yes = 0
    for p in PARTICIPANTS:
        try:
            _, resp = post_json(p.rstrip("/") + "/prepare", {"txid": txid, "op": op})
            if resp.get("vote") == "YES": votes_yes += 1
        except Exception: pass

    # Phase 2: Decision & Propagation [cite: 49]
    decision = "COMMIT" if votes_yes == len(PARTICIPANTS) else "ABORT"
    log_decision(txid, decision)
    robust_send("/" + decision.lower(), {"txid": txid})
    return {"ok": True, "decision": decision}

def three_pc(txid: str, op: dict):
    # Phase 1: CanCommit [cite: 54]
    votes_yes = 0
    for p in PARTICIPANTS:
        try:
            _, resp = post_json(p.rstrip("/") + "/can_commit", {"txid": txid, "op": op})
            if resp.get("vote") == "YES": votes_yes += 1
        except Exception: pass

    if votes_yes < len(PARTICIPANTS):
        log_decision(txid, "ABORT")
        robust_send("/abort", {"txid": txid})
        return {"ok": True, "decision": "ABORT"}

    # Phase 2: PreCommit [cite: 55]
    robust_send("/precommit", {"txid": txid})
    
    # Phase 3: DoCommit [cite: 56]
    log_decision(txid, "COMMIT")
    robust_send("/commit", {"txid": txid})
    return {"ok": True, "decision": "COMMIT"}

class Handler(BaseHTTPRequestHandler):
    def do_POST(self):
        if self.path == "/tx/start":
            length = int(self.headers.get("Content-Length", "0"))
            body = json.loads(self.rfile.read(length))
            func = two_pc if body.get("protocol") == "2PC" else three_pc
            res = func(body["txid"], body["op"])
            self.send_response(200)
            self.end_headers()
            self.wfile.write(json.dumps(res).encode())

def main():
    global PARTICIPANTS
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, default=8000)
    ap.add_argument("--participants", required=True)
    args = ap.parse_args()
    PARTICIPANTS = [p.strip() for p in args.participants.split(",")]
    print(f"Coordinator started on {args.port}")
    ThreadingHTTPServer(("0.0.0.0", args.port), Handler).serve_forever()

if __name__ == "__main__":
    main()