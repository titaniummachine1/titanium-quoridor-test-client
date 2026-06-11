# titanium-quoridor-test-client

Repo: [github.com/titaniummachine1/titanium-quoridor-test-client](https://github.com/titaniummachine1/titanium-quoridor-test-client)

Worker client for distributed Quoridor engine testing. Contributors run one
command; the client polls the coordinator, builds/downloads engines, plays
games, and reports results.

## Run

```bash
cargo build --release
./target/release/quoridor-test-client \
    --coordinator https://YOUR-WORKER.workers.dev \
    --worker-id yourname-laptop
```

Flags: `--once` (process one job / one poll then exit, good for cloud
free-tier invocations).

## What it does per job

1. Claim job from `GET /api/job`.
2. Acquire NEW engine (job commit) and BASE engine: local cache →
   sha256-verified prebuilt download → `cargo build --release` from the
   engine repo at the pinned commit (`-C target-cpu=native`, built once,
   cached under `~/.cache/quoridor-fishtest/engines/<sha8>/`).
3. Play `game_count` games NEW vs BASE, alternating who moves first,
   fixed movetime per move, draw at 300 plies.
4. `POST /api/result` with W/L/D (failed submissions saved and retried).

Requires: `git`, `cargo` (for source builds). Nothing else.
