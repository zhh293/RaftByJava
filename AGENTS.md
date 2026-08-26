# AGENTS.md

This file provides guidance to Codex (Codex.ai/code) when working with code in this repository.

## Project overview

Java implementation of the Raft distributed consensus algorithm. Currently in pre-implementation research phase — no source code exists yet.

The primary reference is `Raft算法-写代码前研究提纲.md`, a comprehensive Chinese-language research document covering: CAP theory tradeoffs, Raft's core mechanisms (leader election, log replication, two-phase commit, membership changes), request flow for every internal/external RPC, edge cases, and Q&A.

## Build tool

Not yet established. Use Maven (pom.xml) or Gradle (build.gradle) — standard Java project tooling. Confirm with the user before initializing the build system.

## Architecture reference

The implementation follows standard Raft architecture:

- **Node roles**: Leader, Follower, Candidate — with state machine transitions defined in the research doc
- **Core data structures**: Term (monotonic), Log entries (`{term, index, command}`), Write-Ahead Log (append-only array)
- **Key RPCs**: AppendEntries (log replication + heartbeat), RequestVote (leader election)
- **Safety properties**: Majority-based commit, log matching via `prevLogIndex`/`prevLogTerm` validation, election restriction (only candidates with up-to-date logs can win)
- **Read consistency**: Default model provides eventual consistency; linearizable reads require `appliedIndex` verification or read-leasing through leader with quorum heartbeat

## Suggested implementation order

Per the research document's checklist (section at end of file):

1. Node state machine — three roles
2. Term, vote records, heartbeat/election timeouts (with randomized jitter)
3. Log entry structure: `{term, index, command}`
4. AppendEntries RPC: `prevLogIndex`, `prevLogTerm`, `entries[]`, `leaderCommit`
5. Commit advancement: advance `commitIndex` on majority replication
6. State machine application: advance `appliedIndex` in order
7. Leader election: vote granting based on `lastLogTerm`/`lastLogIndex` comparison
8. Log conflict repair: catch-up for lagging followers, truncation for conflicting entries
9. Read consistency: implement eventual consistency first, then linearizable reads
10. Engineering enhancements: no-op entry on election, pre-vote, membership changes, client idempotency

## Language

The research document and all discussion context are in Chinese. Code identifiers (class names, method names, variables) should be in English.
