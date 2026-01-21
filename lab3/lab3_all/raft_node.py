#!/usr/bin/env python3
"""
Lab 3: Raft-Lite Leader Election Implementation
Distributed Computing • 3-5 nodes

This node implements the Raft consensus leader election logic including:
- Randomized election timeouts [cite: 62]
- Majority-based voting [cite: 14]
- Heartbeat-driven leader maintenance [cite: 19]
"""

from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib import request
import argparse
import json
import threading
import time
import random
from enum import Enum
from typing import List, Optional

# ─────────────────────────────────────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────────────────────────────────────

ELECTION_TIMEOUT_MIN = 150  # ms [cite: 62]
ELECTION_TIMEOUT_MAX = 300  # ms [cite: 62]
HEARTBEAT_INTERVAL = 50     # ms [cite: 62]

# ─────────────────────────────────────────────────────────────────────────────
# Node State
# ─────────────────────────────────────────────────────────────────────────────

class Role(Enum):
    FOLLOWER = "follower"
    CANDIDATE = "candidate"
    LEADER = "leader"

lock = threading.Lock()

NODE_ID: str = ""
PEERS: List[str] = []

# Persistent state
current_term: int = 0
voted_for: Optional[str] = None

# Volatile state
role: Role = Role.FOLLOWER
current_leader: Optional[str] = None
last_heartbeat: float = 0.0

# ─────────────────────────────────────────────────────────────────────────────
# Helper Functions
# ─────────────────────────────────────────────────────────────────────────────

def random_election_timeout() -> float:
    """Return random timeout in seconds."""
    return random.randint(ELECTION_TIMEOUT_MIN, ELECTION_TIMEOUT_MAX) / 1000.0

def log(msg: str) -> None:
    """Print timestamped log message."""
    print(f"[{NODE_ID}] term={current_term} role={role.value} | {msg}")

def become_follower(new_term: int, leader: Optional[str] = None) -> None:
    """Transition to follower state[cite: 17, 30]."""
    global role, current_term, voted_for, current_leader, last_heartbeat
    if new_term > current_term:
        current_term = new_term
        voted_for = None
    role = Role.FOLLOWER
    current_leader = leader
    last_heartbeat = time.time()
    log(f"became FOLLOWER (leader={leader})")

def become_candidate() -> None:
    """Transition to candidate state and start election[cite: 18, 22]."""
    global role, current_term, voted_for, current_leader, last_heartbeat
    role = Role.CANDIDATE
    current_term += 1
    voted_for = NODE_ID  # Vote for self [cite: 22]
    current_leader = None
    last_heartbeat = time.time() # Reset timer to allow election to run
    log("became CANDIDATE, starting election")

def become_leader() -> None:
    """Transition to leader state[cite: 19, 23]."""
    global role, current_leader
    role = Role.LEADER
    current_leader = NODE_ID
    log("became LEADER")

# ─────────────────────────────────────────────────────────────────────────────
# RPC: Vote Request
# ─────────────────────────────────────────────────────────────────────────────

def handle_vote_request(term: int, candidate_id: str) -> dict:
    """Handle incoming vote request from a candidate[cite: 48]."""
    global current_term, voted_for, last_heartbeat
    
    with lock:
        if term > current_term:
            become_follower(term)
            
        vote_granted = False
        # Grant vote if term is current and haven't voted or already voted for this candidate [cite: 28, 29]
        if term == current_term and (voted_for is None or voted_for == candidate_id):
            vote_granted = True
            voted_for = candidate_id
            last_heartbeat = time.time()  # Reset election timeout on granting vote
            
        log(f"vote request from {candidate_id} term={term} -> granted={vote_granted}")
        return {"term": current_term, "vote_granted": vote_granted}

def request_votes() -> int:
    """Send vote requests to all peers[cite: 18, 22]."""
    votes = 1  # self-vote
    my_term = current_term
    
    for peer in PEERS:
        url = peer.rstrip("/") + "/vote"
        payload = json.dumps({"term": my_term, "candidate_id": NODE_ID}).encode()
        
        try:
            req = request.Request(url, data=payload, 
                                  headers={"Content-Type": "application/json"}, 
                                  method="POST")
            with request.urlopen(req, timeout=0.1) as resp:
                data = json.loads(resp.read().decode())
                
                # Check response term - if higher, step down [cite: 30]
                resp_term = data.get("term", 0)
                if resp_term > my_term:
                    with lock:
                        become_follower(resp_term)
                    return votes

                if data.get("vote_granted"):
                    votes += 1
                    
        except Exception as e:
            pass # Peer unreachable
    
    return votes

# ─────────────────────────────────────────────────────────────────────────────
# RPC: Heartbeat
# ─────────────────────────────────────────────────────────────────────────────

def handle_heartbeat(term: int, leader_id: str) -> dict:
    """Handle incoming heartbeat from leader[cite: 48]."""
    global current_term, role, current_leader, last_heartbeat
    
    with lock:
        # Reject if leader term is outdated [cite: 29]
        if term < current_term:
            log(f"heartbeat from {leader_id} term={term} -> REJECTED (current={current_term})")
            return {"term": current_term, "success": False}
        
        # Accept heartbeat: update term and stay/become follower [cite: 24, 30]
        success = True
        become_follower(term, leader_id)
        
        log(f"heartbeat from {leader_id} term={term} -> success={success}")
        return {"term": current_term, "success": success}

def send_heartbeats() -> None:
    """Send heartbeat to all peers[cite: 19]."""
    my_term = current_term
    
    for peer in PEERS:
        url = peer.rstrip("/") + "/heartbeat"
        payload = json.dumps({"term": my_term, "leader_id": NODE_ID}).encode()
        
        try:
            req = request.Request(url, data=payload,
                                  headers={"Content-Type": "application/json"},
                                  method="POST")
            with request.urlopen(req, timeout=0.1) as resp:
                data = json.loads(resp.read().decode())
                
                # If peer has higher term, step down immediately [cite: 19, 30]
                if data.get("term", 0) > my_term:
                    with lock:
                        become_follower(data["term"])
                    return
                    
        except Exception as e:
            pass  # Peer unreachable, continue

# ─────────────────────────────────────────────────────────────────────────────
# Background Loops
# ─────────────────────────────────────────────────────────────────────────────

def election_loop() -> None:
    """Monitors heartbeat timeout and triggers elections[cite: 51]."""
    global last_heartbeat
    
    last_heartbeat = time.time()
    timeout = random_election_timeout()
    
    while True:
        time.sleep(0.01)
        
        with lock:
            current_role = role
            elapsed = time.time() - last_heartbeat
        
        if current_role == Role.LEADER:
            continue
        
        # Follower times out -> starts election [cite: 21]
        if current_role == Role.FOLLOWER and elapsed > timeout:
            with lock:
                become_candidate()
            timeout = random_election_timeout()
        
        # Candidate logic: request votes and check majority [cite: 23]
        if role == Role.CANDIDATE:
            votes = request_votes()
            majority = (len(PEERS) + 1) // 2 + 1
            log(f"got {votes}/{len(PEERS)+1} votes (need {majority})")
            
            with lock:
                if role == Role.CANDIDATE:
                    if votes >= majority:
                        become_leader()
                    else:
                        # Wait remaining timeout or a random one before checking again
                        remaining = timeout - (time.time() - last_heartbeat)
                        if remaining > 0:
                            time.sleep(remaining)
                        if role == Role.CANDIDATE:  # Recheck after sleep
                            become_candidate()  # New term
                            timeout = random_election_timeout()

def leader_loop() -> None:
    """Sends heartbeats periodically when in leader state[cite: 19, 51]."""
    while True:
        time.sleep(HEARTBEAT_INTERVAL / 1000.0)
        
        with lock:
            if role != Role.LEADER:
                continue
        
        send_heartbeats()

# ─────────────────────────────────────────────────────────────────────────────
# HTTP Handler
# ─────────────────────────────────────────────────────────────────────────────

class Handler(BaseHTTPRequestHandler):
    def _send(self, code: int, obj: dict) -> None:
        data = json.dumps(obj).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)
    
    def do_GET(self):
        if self.path.startswith("/status"):
            with lock:
                self._send(200, {
                    "ok": True, "node": NODE_ID, "term": current_term,
                    "role": role.value, "leader": current_leader,
                    "voted_for": voted_for, "peers": PEERS
                })
            return
        self._send(404, {"ok": False, "error": "not found"})
    
    def do_POST(self):
        length = int(self.headers.get("Content-Length", "0"))
        raw = self.rfile.read(length) if length > 0 else b"{}"
        try:
            body = json.loads(raw.decode())
        except:
            self._send(400, {"ok": False, "error": "invalid json"})
            return
        
        if self.path == "/vote":
            result = handle_vote_request(int(body.get("term", 0)), str(body.get("candidate_id", "")))
            self._send(200, result)
        elif self.path == "/heartbeat":
            result = handle_heartbeat(int(body.get("term", 0)), str(body.get("leader_id", "")))
            self._send(200, result)
        else:
            self._send(404, {"ok": False, "error": "not found"})

    def log_message(self, fmt, *args): pass

# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def main():
    global NODE_ID, PEERS, last_heartbeat
    parser = argparse.ArgumentParser(description="Raft-Lite Node")
    parser.add_argument("--id", required=True)
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument("--peers", default="")
    args = parser.parse_args()
    
    NODE_ID = args.id
    PEERS = [p.strip() for p in args.peers.split(",") if p.strip()]
    last_heartbeat = time.time()
    
    threading.Thread(target=election_loop, daemon=True).start()
    threading.Thread(target=leader_loop, daemon=True).start()
    
    log(f"starting on {args.host}:{args.port} peers={PEERS}")
    ThreadingHTTPServer((args.host, args.port), Handler).serve_forever()

if __name__ == "__main__":
    main()