# Notes for coding agents

celld-cdc turns the LTX files [celld](https://github.com/denoland/celld)
writes to S3 into a Parquet change log and SCD1/SCD2 Delta tables, with
stateless workers. Read `docs/architecture.md` before changing behavior: it
holds the design, the guarantees, and the failure-case tables that the code
must keep true.

## Building and testing

Rust builds run **only in Docker**, through `scripts/cargo.sh` (Rust 1.98.1,
matching celld). The host may have no native toolchain or linker.

```sh
scripts/cargo.sh test --workspace
scripts/cargo.sh clippy --workspace --all-targets -- -D warnings
scripts/cargo.sh test -p celld-cdc-core --test pagediff_oracle   # one test file
```

- Build output lives in the Docker volume `celld-cdc-target`, not `./target`.
- `CARGO_BUILD_JOBS` defaults to 3. Higher values have run Docker out of
  memory while linking the deltalake test binaries (`linking with cc failed`).
  If that happens, retry with `CARGO_BUILD_JOBS=2`.
- **Disk is tight on the main dev machine.** A full build of the workspace
  (deltalake, datafusion, the AWS SDK) is many GB. Check
  `df -h /System/Volumes/Data` before heavy builds and avoid parallel
  `test` + `clippy` runs. A full disk wedges Docker Desktop. Ask before
  deleting anything outside this repo's own volumes.
- The first build compiles a lot; later ones are incremental. Run long
  builds in the background rather than waiting on a short timeout.

End-to-end (LocalStack S3+SQS, two celld nodes, the workers, injected
failures; see the README's quickstart):

```sh
scripts/build-images.sh   # needs a celld checkout at ../celld, or CELLD_SRC
scripts/e2e.sh            # must end with "== all phases passed" and exit 0
```

Run the e2e test after any change to the lane, catalog, diff, sink, or
materializer. Unit tests alone have missed real-fleet behavior before.

## Layout

| Path | What |
| --- | --- |
| `crates/celld-cdc-core` | keys, bucket, cursor/lease (`cursor.rs`), segment chain and planner (`segment.rs`), catalog of cells/bundles/archive (`catalog.rs`), paged databases (`paged.rs`), page diff (`pagediff.rs`) and its full-table oracle (`diff.rs`), V8 values, the lane (`lane.rs`) |
| `crates/celld-cdc-parquet` | change-log schema, codec, and key layout |
| `crates/celld-cdc-materialize` | SCD1/SCD2 Delta merges |
| `crates/celld-cdc` | the binary: `decode`, `materialize`, `sweep`, `dump`, `events` |
| `deploy/` | docker-compose stack, LocalStack init, image Dockerfiles |
| `examples/counter` | the actor the e2e test drives (KV + SQL) |
| `scripts/` | `cargo.sh`, `build-images.sh`, `e2e.sh`, `e2e_check.py` |

`celld-ltx` (LTX codec, page maps, paged VFS) comes from celld at tag
`v0.6.3`. Read its source in `../celld/crates/ltx` rather than guessing its
API; a commented-out path dependency in `Cargo.toml` switches to the local
checkout.

## Invariants: do not break these

1. **Workers are stateless.** All state lives in the `celld-cdc` bucket.
   In-memory caches are fine; they may only save time, never be needed for
   correctness.
2. **Order comes from `(epoch, txid)` in keys plus sorted, strongly
   consistent S3 LIST, never from the queue.** Notifications are doorbells.
   Don't add FIFO queues, sequence numbers, or "trust the notification's
   key" shortcuts.
3. **A lane applies only the segment starting at `cursor + 1`.** It never
   skips a txid; a missing one is a `Gap` and a retry.
4. **Every write is deterministic.** Change-file keys and bytes and
   `event_id`s are functions of `(scope, epoch, txid, table, pk)`. No
   wall-clock times, random ids, or hash-map iteration order in output.
5. **The cursor advances only after the change file (and bundle archive)
   is written**, with `If-Match` on its ETag.
6. **The materializer is order-independent and idempotent.** Applying the
   same change files in any order, any number of times, gives the same
   tables.
7. **The page diff must agree with the full diff event for event.**
   `tests/pagediff_oracle.rs` enforces this. When you touch either diff,
   extend the random workloads rather than weakening the check, and keep
   the assertion that most steps take the fast path.
8. **Follow celld's chain rule** (`segment.rs`) for epochs and takeovers;
   when in doubt, read celld's own restore code.

## Gotchas

- **SQLite page faults block.** The paged VFS fetches pages synchronously
  through `Handle::block_on`, so every SQLite call on a `PagedDb`, opening
  the connection included, must run in `spawn_blocking`. Calling one on a
  runtime thread panics with "Cannot start a runtime from within a runtime".
- **Fault granularity matters.** Diffs fetch single pages; only whole-table
  scans (`PagedDb::scan(true)`) use celld-ltx's 1 MB read-ahead. Turning
  read-ahead on for diffs made a one-row change read 13.8 MB.
- **Actor ids are HMACs.** The name in `idFromName` is not recoverable from
  the key; it comes from `_cf_METADATA.actor_name` inside the database.
- **One L0 file is one capture, not one transaction.** With several nodes,
  txids arrive in node-log bundles (`.ltxb`) first and reach `cells/`
  later.
- **S3 notification keys are URL-encoded** (`:` is `%3A`); decode them with
  `key::decode_event_key`.
- **DuckDB's `->>` binds looser than `AND`.** Use `json_extract_string` in
  examples.
- Test fixture paths contain `:` (celld's real key format). Windows is not
  supported, on purpose.

## Design decisions already made

Don't re-propose these without a new reason: processing in Rust with
celld-ltx; plain SQS doorbells; per-actor cursor and lease in S3 via
conditional writes; Parquet change log and Delta SCD tables; reading bundles
directly. Running the CDC processing itself as Durable Objects, or adding a
database such as Postgres for state, was considered and rejected as more
complex and more costly.

## Conventions

- Match the surrounding code: doc comments explain *why*, short module
  headers, `anyhow` errors with `.context(...)`, `tracing` for logs.
- Keep `docs/architecture.md` and the README in step with behavior changes,
  including the failure-case tables and the cost numbers.
- Commit to a branch and open a PR against `main`; don't push to `main`.
