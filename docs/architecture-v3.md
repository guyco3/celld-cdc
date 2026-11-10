# celld-cdc v3.1 architecture

**Status:** proposed; revises v3 for scale (target: 5M change events per
second, at a cost well below the celld fleet's own). Supersedes the v2
proposal. Replaces v1's per-actor cursor and lease objects in S3, the
`cdc-changes` queue, and the materializer rescan. Decoding (paged databases,
page diff, V8 values) is unchanged. **celld is not changed.**

**The idea in one line:** actors are hashed into a fixed set of groups; each
group has one append-only log in S3, and **a batch is committed by creating
the next log file with `If-None-Match`**, exactly as Delta Lake commits a
table version. Every worker reads each source object **once** and forwards
each group's part to the group's owner, like a reverse proxy with sticky
routing. Only S3, one SQS queue, and the workers are needed.

**What changed from v3, and why:**

| v3 | v3.1 | Why |
| --- | --- | --- |
| SNS fans every doorbell out to G queues; each owner reads every bundle's footer and GETs its own rows | **one SQS queue**; any worker receives a message, GETs the object once, and **forwards each group's rows to that group's owner** | A bundle mixes every group's rows in no useful order (celld can't change). Reading it per group costs G × its bytes or a ranged GET per row; at about 2M segments/s that's millions of dollars a month. Read once, it's about $300. |
| messages deleted by the owner | deleted by the worker that received them, once every owner replied **committed** | the owner no longer sees the message |
| batch window 5 s | **30 s** (or sooner at 64 MiB) | fewer, larger log entries and Delta commits |
| G = 32 | **G = 128** | caps active workers; 5M events/s needs more than 32 |
| one materializer for all groups | **one materializer per group**, each committing to its own partition | one merge loop can't keep up |
| SCD2 recomputed per touched key | **SCD2 is append-only**; validity from a view, materialized by compaction | a recompute reads every version of every touched key |

**Why groups at all**, when correctness comes from S3: they're how the cost
stays low, not how the output stays correct.

- **Warm state.** Diffing a segment needs the actor's database just before
  it. A worker that keeps an actor applies its next segment with almost no
  reads; a worker that sees it cold rebuilds it from S3 (a LIST, the page
  index of every file since the last snapshot, the pages). At ~2M
  segments/s, cold rebuilds every time would be ~10M GETs/s.
- **Batched positions.** One log entry per group per 30 s records every
  actor's position. One cursor per actor (v1) would be one PUT per active
  actor per batch.

Ownership is only a hint about where an actor's warm state is (D7). Two
owners, or none for a while, cost time, never correctness.

## 1. Invariants

Everything below is argued from these. **I** are facts about the systems we
build on; **D** are properties this design maintains.

| # | Invariant | Source |
| --- | --- | --- |
| I1 | **An actor has one writer at a time and runs single threaded**, so its history is one sequence of positions `(epoch, txid)`. | celld ownership records (`own.json`, conditional writes) |
| I2 | **Every segment names its position.** A cell key names `scope, epoch, level, min, max`; a bundle footer row names `cell, epoch, txid, offset, len`. | celld key and bundle formats |
| I3 | **S3 is strongly consistent**: a GET, LIST, or 404 after a PUT reflects it. **A PUT with `If-None-Match: *` is atomic**: of concurrent creates of one key, exactly one succeeds; the others get 412 (or 409 while one is in flight). | S3 |
| I4 | **Segments are immutable.** celld never rewrites a key with other content. A bundle is deleted only after its rows are drained to `cells/` (as L1 compactions or a handoff snapshot), and each object it drains to gets its own notification. | celld (`ltx_repl.rs`: compaction "stays pure per-cell L1s, which is the continuous drain") |
| I5 | **celld's chain rule** decides which epochs are history: a non-paged epoch opens with a snapshot; a paged epoch continues the one below it exactly at its cut; txids past a cut in a superseded epoch are not history. | celld restore |
| I6 | **S3 notifications** can be late, duplicated, reordered, or (rarely) lost. Each names one object key. | S3 |
| I7 | **Network requests between workers** can fail, time out, or be duplicated; a timeout says nothing about whether the receiver acted. | design assumption |
| I8 | **SQS standard queues** are at-least-once and unordered. A received message is hidden for its visibility timeout (≤ 12 h) and removed only by a delete with its receipt handle. Retention ≤ 14 days. | SQS |
| I9 | **S3 Inventory** periodically (daily) delivers a listing of every object in the bucket at that time. | S3 |
| I10 | **Decoding is deterministic**: the same segment over the same before-state gives the same events, and `event_id = H(scope, epoch, txid, table, pk)`. | celld-cdc |
| I11 | **SCD merges are idempotent and order-independent**: SCD1 keeps the greatest `seq` per key; SCD2's versions are keyed by `event_id` and ordered by `seq`. | celld-cdc |
| I12 | **A Delta commit is atomic and can carry application transactions** (`appId → version`), read back with the table state. | Delta protocol (`txn` action); delta-rs 1.1: `CommitProperties::with_application_transactions`, `DeltaTableState::transaction_version` |
| D1 | **The group count G is immutable.** It's in `config.json`, created once with `If-None-Match` and never rewritten; workers take G only from there. So each actor belongs to exactly one group. | I3 |
| D2 | **Each group's log is gapless and has one entry per `seq`.** Entry `n` is created only by a writer that has read entry `n − 1` (or the checkpoint at it), and only one create of `n` succeeds. | I3 |
| D3 | **Entry `n` is a function of the log up to `n − 1` and the segments it applies.** Its positions continue the positions folded from entries `≤ n − 1`. So the state at any `seq` is `fold(checkpoint, later entries)`, the same for every reader. | D2, I10 |
| D4 | **An actor's position only moves forward, one contiguous segment at a time.** | design, I1, I2 |
| D5 | **A worker deletes a message from the queue only after every segment in it got a `committed` or `duplicate` reply from its group's owner.** | design, I8 |
| D6 | **A snapshot marker for an actor at `seq` S is in the same entry as that actor's complete set of rows at S**, each a `r` event carrying `seq` S. | design (§5.10) |
| D7 | **Leases are advisory, and so is routing.** They send each group's segments to one worker, for efficiency; correctness never depends on them, only on D2 and D9. | design |
| D8 | **Wall-clock time is used only for liveness** (leases, batch and gap timers), never for correctness. | design |
| D9 | **An owner replies `committed` for a segment only when the committed log it has folded puts the actor's `pos` at or past the segment's end.** The reply is a fact about the log (D2, D3), true whoever sends it. | design |

## 2. Services

| Service | Count | State | Role |
| --- | --- | --- | --- |
| **celld fleet** | existing | its bucket | writes `.ltx` and `.ltxb` (unchanged) |
| **S3 `celld`** | 1 | celld's data | source, read only; notifies the queue; S3 Inventory enabled |
| **SQS `cdc-doorbell`** | 1 queue | in-flight doorbells | every doorbell and hint |
| **workers** | up to G owning, any number receiving | none durable | one binary. Each **receives** from the queue and forwards (§5.1), **owns** groups and decodes their actors (§5.2), and **materializes** the groups it owns (§5.4). One also runs the reconciler. |
| **S3 `celld-cdc`** | 1 | config, leases, group logs, checkpoints, Delta tables | output and all durable state |

**Workers run in one availability zone.** Forwarding moves every byte of
every bundle between workers (about 5–10 GB/s at the target). Within a zone
that's free; across zones it costs about $0.02/GB, which at this rate is
hundreds of thousands of dollars a month. Workers keep no durable state, so
a zone outage means starting them in another zone; they recover from S3
(§5.5).

Singleton roles, each behind an advisory lease object (D7):

| Role | Job |
| --- | --- |
| **reconciler** | daily: reads the source bucket's S3 Inventory and sends hints for anything a doorbell missed |

## 3. Groups and positions

- `group(scope) = xxh3_64(scope, seed 0) mod G`, default **G = 128**. The
  group is the unit of ownership: one owner, one log, one checkpoint, one
  materializer. A worker can own several groups and decodes their actors in
  parallel. Small deployments run all groups on a few workers.
- `pos(actor) = (epoch, txid)`: the end of the last segment applied. An owner
  keeps `pos` for every actor in its groups in memory (about 100 B each).

## 4. Data

### 4.1 S3 `celld-cdc` keys

| Key | Content | Written |
| --- | --- | --- |
| `config.json` | `{version: 3, groups: G}` | once, `If-None-Match` |
| `leases/group-<g>.json`, `leases/<role>.json` | `{holder, expires_ms}` | CAS (`If-Match`), renewed every 10 s |
| `workers/<id>.json` | `{expires_ms, addr}`: heartbeat, and the address other workers forward to | every 10 s |
| `log/<g>/<seq:020>.parquet` | **one log entry** (below) | **`If-None-Match`: this create is the commit** |
| `log/<g>/_last_checkpoint` | `{seq}`: a hint | unconditional PUT |
| `checkpoints/<g>/<seq:020>.json.zst` | the group's `pos` map at `seq`, plus where each actor's needed archived segments are (`log` seq and row) | unconditional PUT; deterministic bytes (D3) |
| `tables/scd1/`, `tables/scd2/` | Delta tables partitioned by `grp`; each group's materializer offset is an application transaction in them (I12) | Delta commits |

### 4.2 A log entry

One Parquet file per group per batch window:

- **Events row group**: the v1 change-event schema. Rows are `c`/`u`/`d`
  events, `r` events (rows read from a snapshot), and **snapshot markers**
  (`op = snapshot`, no table or key: "the `r` rows at this `seq` are the
  actor's complete state").
- **Segments row group**: `scope, epoch, min, max, ltx` for each bundle
  segment applied, so a state stays rebuildable after celld drains the bundle
  (I4). This replaces v1's separate archive objects.
- **Footer metadata**: `group, seq, writer, nonce`, `positions` (the new
  `pos` of each actor the entry advanced), and `segments` (the segment row
  group's `scope, epoch, min, max`, without bytes), so recovery reads only
  footers.

A Parquet file has one schema, so both row groups share it: the v1
event columns (nullable) followed by `seg_scope, seg_epoch, seg_min,
seg_max, seg_ltx`. A row is a segment exactly when `seg_scope` is set;
query the events with `WHERE seg_scope IS NULL`.

### 4.3 Messages

| Message | From → to | Content |
| --- | --- | --- |
| **Doorbell** | S3 notification (source bucket: `cells/**.ltx`, `log/**.ltxb`) → `cdc-doorbell` | S3 event JSON, `Records[].s3.object.key` (URL-encoded). A `.ltx` names one actor; a `.ltxb` names a bundle holding many. |
| **Hint** | reconciler → `cdc-doorbell` | `{keys: [newest segment key per actor]}`, up to 256 KB each. Treated as doorbells. |
| **Forward** | receiving worker → group owner, HTTP `POST /v1/groups/<g>/segments` | the segments of one message for group g: for a `.ltx`, its key; for a bundle, each row's `scope, epoch, min, max` and its bytes |
| **Reply** | owner → receiving worker, the response to a Forward | per segment: `committed`, `duplicate`, or `pending`; or `421 not owner`, or `503 busy` |

## 5. Flows

### 5.1 Receiving and forwarding

Every worker polls `cdc-doorbell` (receives of up to 10, long polling). For
each message:

1. Decode the key.
   - **`.ltx`:** one actor, one group, one segment. Nothing is read; the
     owner GETs it only if it needs it.
   - **`.ltxb`:** GET the **whole bundle, once**, parse its footer, and split
     its rows by `group(scope)`.
   - **Bundle 404:** celld drained and deleted it, so its rows are in
     `cells/` objects with their own doorbells (I4). Delete the message.
2. For each group in the message, find the owner: the holder of
   `leases/group-<g>.json`, at the `addr` in its `workers/<id>.json` (both
   cached; refreshed on a `421` or a connection error). Forward that group's
   segments. A group this worker owns is an in-process call.
3. Collect the replies. The owner answers when every segment is `committed`
   or `duplicate`, so a reply can take up to a batch window plus a commit
   (about 30–40 s). Meanwhile the worker extends the message's visibility.
4. **Every segment `committed` or `duplicate`:** delete the message (D5).
5. **`pending`, `421`, `503`, a timeout, or an error:** retry, re-resolving
   the owner, with backoff, for up to a few minutes; then stop extending the
   visibility and let the message reappear for any worker. It's never deleted
   on these replies (D5).

Duplicated forwards are harmless: the owner drops segments at or before
`pos` (§5.2). A forward to a worker that just lost the group is answered with
`421`, or, if it doesn't know yet, decoded and lost in a `412` (§5.3), after
which it answers by D9 from the log it folded.

### 5.2 Owner decode loop

An owner accepts forwards only for groups whose lease it holds (otherwise
`421`), keeps the segments in memory with the open request, and every
**batch window** (default 30 s, or sooner at 64 MiB of events) processes
every actor with pending segments:

```
for each segment K of actor A, in (epoch, min) order:
  K ends at or before pos(A)                      → duplicate: drop
  K.epoch == pos.epoch and K.min == pos.txid + 1  → apply: diff, pos(A) = K.end
  K.epoch == pos.epoch and K.min >  pos.txid + 1  → hold: a predecessor is missing
  K.epoch >  pos.epoch                            → epoch change (below)
```

- **Apply** uses the forwarded bytes for a bundle row, or a GET for a `.ltx`,
  then the page diff over the actor's paged state (v1).
- **Hold** waits up to the **gap timeout** (default 60 s), then probes, once
  per timeout: a HEAD of the guessed key `…/{pos+1}-{pos+1}.ltx`; then one
  LIST of the epoch's level-0 directory starting after `pos` (the missing
  file covers several txids); then the **full view** (below), which finds
  txids that now live only in an L1 or a snapshot. A held segment's forward
  is answered `pending` when the hold outlasts the request's limit; its bytes
  stay in memory until it applies or is evicted.
- **Memory is bounded.** Past a limit on pending bytes, an owner answers new
  forwards with `503`; the messages wait in the queue.
- **LIST is used only where celld's keys can't be predicted**, all on the
  source bucket and all off the steady-state path. The **full view** is one
  LIST of the actor's `cells/<scope>/ltx/`, plus its segments archived in the
  log, plus its pending segments, walked with the chain rule (I5). It's used
  for:
  - the last gap probe, above;
  - an epoch change: the chain rule needs the old epoch up to the cut and
    any epochs between, not only the new `e<epoch>/` directory;
  - a cold actor (not in the owner's cache): to plan its rebuild at `pos`.
- **Re-snapshot** (§5.10) when the full view shows the state at `pos` can no
  longer be rebuilt: no plan ends at `pos`, no segment starts at `pos + 1`,
  and a segment covers `pos + 1` but starts at or before `pos` (celld folded
  the txids into a coarser segment and deleted the finer ones). A chain with
  no base at all is held instead: its segments aren't uploaded yet.

### 5.3 Commit

1. Write the entry `n = head + 1` with `PUT log/<g>/<n> If-None-Match: *`.
2. **Success:** `head = n`; update `pos`; answer every open forward whose
   segments are now all committed or duplicates (D9).
3. **412 (taken):** another writer committed `n`. GET it, fold its positions
   into `pos` (D3), discard the pending batch, and rebuild it from the held
   segments; anything that entry already applied is now a duplicate. If the
   group's lease is held by someone else, answer the open forwards by D9
   (`committed`, `duplicate`, or `421` for the rest) and stop working the
   group.
4. **409, 5xx, or timeout:** retry the same create. If it then gets 412, GET
   entry `n`: a footer with this writer's `nonce` means the earlier attempt
   committed.
5. **Nothing to commit, but segments held:** GET `log/<g>/<head + 1>` and fold
   any entries another writer committed. Two writers can share a group (D7),
   and the one that's stuck must see the other's progress to answer its
   forwards. It's one GET per batch window, only while stuck.

### 5.4 Materializer

Each group has a materializer, behind `leases/materialize-<g>.json`. A worker
takes the materializer lease of the groups it owns, so it usually has the
entries it just committed in memory; otherwise it GETs them.

1. Read the group's offset from the SCD tables' application transactions
   (`appId = celld-cdc/group-<g>`, `version = seq`).
2. Poll: GET `log/<g>/<offset + 1>` (or take it from memory). A 404 means
   nothing new (I3).
3. Merge the entries found into **one Delta commit per table**, touching only
   partition `grp = g`, that also sets the group's application transaction to
   its new offset. Offset and data land atomically (I12). Groups commit to
   disjoint partitions, so their commits don't conflict.
4. **SCD1:** MERGE on `(scope, table, pk)`, taking the greater `seq` (I11).
   Deletes are kept as `is_deleted = true`.
5. **SCD2 is append-only:** one row per event, `valid_from = seq`, keyed by
   `event_id`. `valid_to` and `is_current` come from a view (`LEAD(seq)`
   over `(scope, table, pk)`). A periodic compaction per partition rewrites
   its files with them filled in and sorted by `(scope, table, pk, seq)`.
6. A **snapshot marker** for actor A at `seq` S adds, in the same commit, the
   standard full-load reconciliation scoped to one actor:
   - SCD1: rows with `scope = A`, `seq < S`, and `is_deleted = false` get
     `is_deleted = true` and `seq = S`. The marker's own `r` rows carry `seq`
     S, so exactly the rows missing from the snapshot match.
   - SCD2: for the same keys, append a deleted version at S.

### 5.5 Group acquire and recovery

1. **Acquire:** take `leases/group-<g>.json` if it's absent (`If-None-Match`)
   or expired (`If-Match` on its ETag).
2. **Recover:** GET `_last_checkpoint`, then the checkpoint it names, then GET
   `log/<g>/<seq + 1>`, `<seq + 2>`, … until a 404, folding each footer's
   positions. That gives `head` and the exact committed `pos` map (D3). A
   stale hint only means a longer walk. If the hint is missing, LIST
   `checkpoints/<g>/` (our own bucket, recovery only).
3. Start accepting forwards for g. Messages forwarded to the previous owner
   and not yet deleted reappear when their visibility expires, and are
   forwarded here.

### 5.6 Rebalancing

Every 30 s, each worker LISTs `workers/` (heartbeats, a handful of keys) and
computes `target = ceil(G / live workers)`. Below target, it acquires free
or expired groups. Above target, it releases one group per round: commit
pending work, answer its open forwards, stop accepting forwards for it, and
expire its lease by CAS. A plain delete could remove a successor's lease.

### 5.7 Checkpoints

Per group, when `head` crosses a multiple of 12, or every 60 s: PUT
`checkpoints/<g>/<seq>`, then PUT `_last_checkpoint`, then delete
checkpoints older than the newest two. A checkpoint's bytes are a function
of the log up to `seq` (D3), so a second writer would write the same object,
and the multiple-of-12 rule makes writers checkpoint at the same `seq`.

A checkpoint drops an actor's archived refs once they're no longer needed:
refs ending at or before the start of the newest `cells/` segment the owner
has seen for that actor that reaches `pos` (an L0, L1, or snapshot that a plan
to `pos` can use instead).

### 5.8 Reconciler

Daily, it reads the source bucket's S3 Inventory, keeps each actor's newest
segment key, and sends **Hints** to `cdc-doorbell`. Workers forward them like
doorbells, and owners drop anything at or before `pos`.

### 5.9 Retention

An S3 lifecycle rule expires `log/` entries after R days (default 7). R must
exceed the materializer's worst lag and the checkpoint interval by a wide
margin. The materializer's compaction clusters SCD1 and SCD2 by `scope`
(Delta z-order or liquid clustering), so a query or reconciliation for one
actor reads about one file per `(grp, class, table_name)` partition.

### 5.10 Resync (re-snapshot)

When an actor's state at `pos` can't be rebuilt (celld's retention removed
its segments, or an archived segment it needs has expired), its owner
re-snapshots it, the way a Debezium connector does when its log position is
gone:

1. builds the first state of the actor's current chain (I5);
2. emits every row of every captured table as an `r` event, then one
   **snapshot marker**, all at the entry's `seq` S, in the same entry (D6);
3. commits as usual and continues from there.

Deletes that happened in the gap are not in the change log; the tables get
them from the marker's reconciliation (§5.4). Other consumers of the change
log treat a marker the same way, as Debezium consumers treat the end of a
snapshot.

### 5.11 Monitoring

- **Source lag**, per group: the age (by `commit_ts`) of the oldest segment
  pending or held. **Alert well before celld's retention**
  (`CELLD_LTX_RETENTION_SECS`): for example, at one hour against a retention
  of days. A resync should never happen in normal operation.
- **Materializer lag**, per group: `head − offset` and its age. Alert well
  before R.
- The queue's oldest message age; forward replies by kind (`pending`,
  `421`, `503`, timeouts); held segments past the gap timeout; resyncs; 412s
  on commits; lease changes.

## 6. Optional celld change: retention hold

The standard way to rule out a resync entirely is a **retention hold**, as a
Postgres replication slot keeps WAL until its consumer confirms it: celld
would not delete an actor's epochs past a watermark that celld-cdc publishes.
That's a change to celld, so it's listed here, not assumed. The cost: celld
keeps data longer for an actor celld-cdc is stuck on.

## 7. Cost (rough)

At the target of **5M events/s**, assuming about 2M segments/s, about 250
bundles/s (5–10 GB/s of page data), and on the order of 10k `cells/` objects/s
(L1 compactions and snapshots). These are estimates to check against a real
fleet, not measurements.

| Item | ~$/month | Notes |
| --- | --- | --- |
| S3 GETs, bundles | ~300 | each bundle read once |
| S3 GETs, `.ltx` and page reads | small to ~1k | an L1 is usually a duplicate and not read; page reads only for cold actors |
| SQS | ~2–5k | one copy per object; receives and deletes batched by 10, visibility extensions batched |
| S3 PUTs, log entries and checkpoints | < 100 | 128 groups every 30 s |
| Worker-to-worker transfer | 0 | one zone (§2) |
| Workers, decode | ~8–15k | ~32 × 8 vCPU |
| Workers, materialize | ~10–20k | per group, append-only SCD2 |
| Storage | ~5–10k | change log for R = 7 days, plus SCD2 history |
| **Total** | **~25–50k** | |

Requests grow with source objects (one GET and one SQS copy each) and with
groups × time (PUTs), not with actors × wake-ups as in v1, nor with G × bundles
as in v3. There is no cluster to run.

## 8. Failures

| Failure | What happens | Why it is safe |
| --- | --- | --- |
| **Doorbell late, duplicated, or reordered** | The owner holds, applies, or drops each segment by the rule in §5.2. | Positions come from keys (I2), not arrival order (I6, I8); applying only `pos + 1` keeps one contiguous history (D4, I1). |
| **Doorbell lost, a later one arrives** | The later segment is held; after the gap timeout the owner HEADs the guessed key, then LISTs. | S3 shows every object that exists (I3), so the missing one is found. |
| **Doorbell lost for an actor's newest segment** | Nothing reveals it until the reconciler's hint, up to a day later. | The inventory lists every object (I9); a hint at or before `pos` is dropped (D4). |
| **Bundle deleted before it's read** | GET 404; the message is deleted. | Its rows are in `cells/` objects, each with its own doorbell (I4); a lost one is a lost doorbell (rows above). |
| **A predecessor isn't in S3 yet** (multi-node fleet: on peer disks, not uploaded) | The segment is held; its own doorbell or the gap probe finds it once uploaded. | Holding never skips a txid (D4). |
| **Receiving worker dies mid-forward** | The message isn't deleted; it reappears and another worker forwards it again. Owners drop what they already have. | D5; duplicates are dropped by `pos` (D4). |
| **Forward or reply lost, or times out** | The receiver retries, then lets the message reappear. | It deletes only on `committed`/`duplicate` replies (D5); a repeated forward is a duplicate (I7, D4). |
| **Stale routing** (the receiver forwards to a worker that lost the group) | `421`, re-resolve, retry. If the old owner doesn't know yet, it decodes, loses the commit with a `412`, and replies from the folded log. | Replies are facts about the log (D9); only one create per `seq` wins (D2). |
| **A group has no owner** | Forwards fail; messages wait in the queue until a worker acquires the group. | Nothing deletes them (D5); retention is 14 days (I8). |
| **Owner overloaded** | `503`; messages wait in the queue. | D5. |
| **Owner dies before committing** | Its lease expires; another worker acquires the group and recovers. The messages reappear and are forwarded to it. | The uncommitted batch was never created (I3); recovery restores exactly the committed positions (§5.5, D3); redoing gives the same events (I10). |
| **Owner dies after committing, before replying** | The messages reappear; the new owner replies `duplicate`. | `pos` after recovery includes the entry (§5.5, D3, D4). |
| **Two writers on one group** (a paused owner wakes past its lease, or clock skew makes a lease look free) | Both try to create entry `n`; exactly one succeeds. The loser GETs the winner's entry, folds it, and stops if it no longer holds the lease. | Only one create of `n` succeeds (I3, D2), and each writer built its entry from the log at `n − 1`, so either entry is a valid continuation (D3). Leases don't matter for this (D7). |
| **Commit outcome unknown** (timeout, 5xx) | Retry the same create; on 412, compare entry `n`'s nonce with ours. | The create is idempotent per key (I3); the nonce identifies the writer. |
| **Concurrent create in flight** (409) | Retry. | I3. |
| **Checkpoint written twice, or by a stale writer** | Same bytes either way. | A checkpoint is a function of the log up to its `seq` (D3). |
| **`_last_checkpoint` stale, backwards, or missing** | Recovery walks forward further, or LISTs `checkpoints/<g>/`. | The hint is only a starting point; the walk always ends at the true head (D2, I3). |
| **A worker configured with a different G** | Impossible: G comes only from `config.json`. | D1. |
| **Materializer crashes mid-merge** | The Delta commit either happened, with its offset, or didn't. The next run resumes from the offset in the table. | Data and offset commit atomically (I12); merges are idempotent (I11). |
| **Two materializers for one group** | Delta's optimistic concurrency makes one commit win; the other re-reads the offset and skips entries already applied. | I12, I11. The lease is only for efficiency (D7). |
| **Materializer behind log retention** | Entries it hasn't read have expired. | **Must not happen**: the lag alert (§5.11) fires long before R. Recovery is a re-snapshot of the affected group's actors (§5.10). |
| **celld drains a bundle before an owner applies it** | Its txids appear in `cells/`, possibly as an L1; the planner applies that, coarser. | I4, I5. The before-state comes from archived segments in our log (§4.2). |
| **An archived segment an actor needs has expired** (the actor was idle past R) | The actor is re-snapshotted (§5.10). | The same argument as the row below. |
| **celld takeover, new epoch, or a fenced owner's late writes** | The epoch-change path applies the chain rule, and rejoins from the old state if the history moved. | I5, as in v1. |
| **Actor state no longer rebuildable** (celld's retention removed its segments while celld-cdc was far behind) | The actor is re-snapshotted (§5.10): its current rows as `r` events plus a marker. The tables reconcile on the marker; the change log has no delete events for the gap, as with a Debezium re-snapshot. | The snapshot is celld's current history (I5). The marker and its rows are in one entry (D6), so the actor's rows older than the marker's `seq` are exactly those the snapshot no longer has. A later event has a greater `seq` and is untouched, and replaying an older entry can't revive a row, since SCD1 only takes a newer `seq` (I11). |
| **A worker's cache is lost** | Cold actors are rebuilt from S3. | Caches only save time (I4, D8). |
| **Zone outage** | Workers start in another zone and recover from S3. | No durable state on workers (§2, §5.5). |
| **Clock skew** | Leases expire early or late. | Leases are advisory (D7); conditional creates decide (I3); timers affect liveness only (D8). |
| **S3 throttling** (503 SlowDown) | Back off and retry. | Request rates are low (§7); a retried create is safe (I3). |

## 9. Open questions

- **Concurrent Delta commits to disjoint partitions:** §5.4 relies on
  delta-rs letting per-group commits (MERGE on `grp = g`, appends) land
  concurrently without conflicts. Step 3 must test this first. If it doesn't
  hold, each group gets its own SCD1 and SCD2 tables under
  `tables/scd1/grp=<g>/`, read through a union view.
- **Epoch change with an unseen bundle:** if a new epoch's first segment is
  still only in a bundle the owner hasn't seen, the chain may first pick the
  old epoch's later txids; the rejoin path (as in v1) diffs back once the new
  epoch appears. This needs an explicit test.
- **Inventory delay:** the reconciler finds a lost newest-segment doorbell
  within a day. If that's too slow, add a weekly or daily flat LIST, or run
  the inventory more often.
- **G:** 128 by default; it caps owning workers and can't change later.
- **Bundle size:** a receiving worker holds a whole bundle in memory while it
  forwards. celld's bundle size bounds this; measure it on a real fleet.
