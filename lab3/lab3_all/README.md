# LAB 3 — Leader Election and Consensus (Raft-Lite)

**Distributed Computing • AWS EC2-based • 3–5 node topology**  
**Estimated time:** 2 hours  
**Submission:** Demo or report + code repository  
**Required topology:** 3–5 nodes on EC2

---

## 1. Objectives

By completing this lab, students will:

- Implement leader election using timeouts and heartbeats
- Understand the Raft consensus model (simplified)
- Observe leader failover and re-election behavior
- Run experiments on AWS EC2 with 3–5 nodes

---

## 2. What You Will Build

A simplified Raft cluster where nodes elect a leader, the leader sends periodic heartbeats, and followers detect leader failure via timeout. When the leader fails, a new election occurs.

**Scope:** This lab focuses on leader election only (no log replication).

---

## 3. System Model

| Aspect | Description |
|--------|-------------|
| **Roles** | Leader, Follower, Candidate |
| **Communication** | HTTP/JSON (like Lab 2) |
| **Election trigger** | Follower timeout (no heartbeat received) |
| **Leader wins** | Majority votes (>N/2 for N nodes) |

---

## 4. Raft-Lite Rules

### Node States
- **Follower:** Waits for heartbeats; starts election on timeout
- **Candidate:** Requests votes from peers; becomes leader if majority
- **Leader:** Sends heartbeats; steps down if higher term seen

### Election Logic

1. Follower times out → becomes **Candidate**
2. Candidate increments **term**, votes for self, requests votes
3. If majority votes received → becomes **Leader**
4. If another leader's heartbeat with ≥ term → becomes **Follower**
5. If election times out → restart election with new term

### Term Rules

- Each node tracks current `term` (starts at 0)
- Vote only once per term
- Reject requests with lower term
- On seeing higher term → update term, become Follower

---

## 5. EC2 Setup

Reuse your 3 EC2 instances from Lab 2 (or create 5 for extended experiments).

| Node | Port | Example Command |
|------|------|-----------------|
| A | 8000 | `python3 raft_node.py --id A --port 8000 --peers http://<B>:8001,http://<C>:8002` |
| B | 8001 | `python3 raft_node.py --id B --port 8001 --peers http://<A>:8000,http://<C>:8002` |
| C | 8002 | `python3 raft_node.py --id C --port 8002 --peers http://<A>:8000,http://<B>:8001` |

---

## 6. Required Scenarios

### Scenario A — Normal Election
Start all nodes simultaneously. Observe which node becomes leader first.

**Record:** Which node won? What term? Why that node?

### Scenario B — Leader Failure
1. Identify the current leader (via `/status`)
2. Stop the leader (Ctrl+C)
3. Observe new election on remaining nodes

**Record:** How long until new leader elected? New term number?

### Scenario C — Network Partition (Optional)
1. Block traffic between leader and one follower
2. Observe if/when partition triggers election
3. Restore connectivity and observe behavior

---

## 7. Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/status` | GET | Returns node state, term, leader info |
| `/vote` | POST | `{term, candidate_id}` → `{term, vote_granted}` |
| `/heartbeat` | POST | `{term, leader_id}` → `{term, success}` |

---

## 8. Where to Add Code

Search for `# YOUR CODE HERE` in `raft_node.py`:

| Location | Task |
|----------|------|
| `request_vote()` | Implement vote granting logic |
| `handle_heartbeat()` | Update state on valid heartbeat |
| `election_loop()` | Start election when timeout expires |
| `leader_loop()` | Send periodic heartbeats |

---

## 9. Starter Code Files

- `raft_node.py` — Main node implementation (fill in marked sections)
- `raft_client.py` — Utility to query node status

---

## 10. Deliverables

1. **Demo or report:** Show 3+ nodes, leader election, and failover (Scenarios A & B)
2. **Code:** Updated `raft_node.py` with working election logic

---

## 11. Grading (100 points)

| Criterion | Points |
|-----------|--------|
| Nodes deployed and communicating (3+) | 15 |
| Correct term handling | 15 |
| Vote request/response logic | 20 |
| Heartbeat mechanism | 15 |
| Leader election demonstrated | 15 |
| Leader failover demonstrated | 15 |
| Code quality and logs | 5 |

---

## 12. Tips

- Use randomized election timeouts (150–300ms) to avoid split votes
- Heartbeat interval should be shorter than election timeout (e.g., 50ms)
- Print clear logs: `[A] term=2 state=CANDIDATE requesting votes`
- Test with 3 nodes first, then try 5 for more realistic behavior

---

## References

- Ongaro & Ousterhout, "In Search of an Understandable Consensus Algorithm" (Raft paper)
- [Raft Visualization](https://raft.github.io/)

