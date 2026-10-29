# celld-cdc v3 architecture

**Status:** proposed; supersedes the v2 proposal. Replaces v1's per-actor
cursor and lease objects in S3, the `cdc-changes` queue, and the materializer
rescan. Decoding (paged databases, page diff, V8 values) and the SCD merges
are unchanged.

**The idea in one line:** actors are hashed into a fixed set of groups; each
group has one append-only log in S3, and **a batch is committed by creating
the next log file with `If-None-Match`**, exactly as Delta Lake commits a
table version. Only S3, SNS, and SQS are needed.

## 1. Invariants

Everything below is argued from these. **I** are facts about the systems we
build on; **D** are properties this design maintains.

| # | Invariant | Source |
| --- | --- | --- |
| I1 | **An actor has one writer at a time and runs single threaded**, so its history is one sequence of positions `(epoch, txid)`. | celld ownership records (`own.json`, conditional writes) |
| I2 | **Every segment names its position.** A cell key names `scope, epoch, level, min, max`; a bundle footer row names `cell, epoch, txid, offset, len`. | celld key and bundle formats |
| I3 | **S3 is strongly consistent**: a GET, LIST, or 404 after a PUT reflects it. **A PUT with `If-None-Match: *` is atomic**: of concurrent creates of one key, exactly one succeeds; the others get 412 (or 409 while one is in flight). | S3 |
| I4 | **Segments are immutable.** celld never rewrites a key with other content. A bundle is deleted only after its rows are drained to `cells/`. | celld |
| I5 | **celld's chain rule** decides which epochs are history: a non-paged epoch opens with a snapshot; a paged epoch continues the one below it exactly at its cut; txids past a cut in a superseded epoch are not history. | celld restore |
| I6 | **S3 notifications** can be late, duplicated, reordered, or (rarely) lost. Each names one object key. | S3 |
| I7 | **SNS delivers every published message to every subscribed SQS queue**, at least once, retrying failed deliveries. | SNS |
| I8 | **SQS standard queues** are at-least-once and unordered. A received message is hidden for its visibility timeout (≤ 12 h) and removed only by a delete with its receipt handle. Retention ≤ 14 days. Each queue's copy of a message is independent. | SQS |
| I9 | **S3 Inventory** periodically (daily) delivers a listing of every object in the bucket at that time. | S3 |
| I10 | **Decoding is deterministic**: the same segment over the same before-state gives the same events, and `event_id = H(scope, epoch, txid, table, pk)`. | celld-cdc |
| I11 | **SCD merges are idempotent and order-independent**: SCD1 keeps the greatest `seq` per key, SCD2 recomputes validity for touched keys. | celld-cdc (v1, unchanged) |
| I12 | **A Delta commit is atomic and can carry application transactions** (`appId → version`), read back with the table state. | Delta protocol (`txn` action) |
| D1 | **The group count G is immutable.** It's in `config.json`, created once with `If-None-Match` and never rewritten; workers take G only from there. So each actor belongs to exactly one group. | I3 |
| D2 | **Each group's log is gapless and has one entry per `seq`.** Entry `n` is created only by a writer that has read entry `n − 1` (or the checkpoint at it), and only one create of `n` succeeds. | I3 |
| D3 | **Entry `n` is a function of the log up to `n − 1` and the segments it applies.** Its positions continue the positions folded from entries `≤ n − 1`. So the state at any `seq` is `fold(checkpoint, later entries)`, the same for every reader. | D2, I10 |
| D4 | **An actor's position only moves forward, one contiguous segment at a time.** | design, I1, I2 |
| D5 | **An owner deletes a message from its queue only after every segment in it for its group is committed or is a duplicate.** | design, I8 |
| D6 | **A snapshot marker for an actor at `seq` S is in the same entry as that actor's complete set of rows at S**, each a `r` event carrying `seq` S. | design (§5.10) |
| D7 | **Leases are advisory.** They keep two workers from working one group at once, for efficiency; correctness never depends on them, only on D2. | design |
| D8 | **Wall-clock time is used only for liveness** (leases, batch and gap timers), never for correctness. | design |

## 2. Services

| Service | Count | State | Role |
| --- | --- | --- | --- |
| **celld fleet** | existing | its bucket | writes `.ltx` and `.ltxb` (unchanged) |
| **S3 `celld`** | 1 | celld's data | source, read only; notifies SNS; S3 Inventory enabled |
| **SNS `cdc-doorbell`** | 1 topic | none | fans every doorbell out to every group queue |
| **SQS `cdc-q-<g>`** | G queues (default 32) | in-flight doorbells | one per group; every queue gets every message |
| **workers** | up to G active, any number standing by | none durable | one binary; each owns groups and decodes their actors; one also runs the materializer and one the reconciler |
| **S3 `celld-cdc`** | 1 | config, leases, group logs, checkpoints, Delta tables | output and all durable state |

Singleton roles, each behind an advisory lease object (D7):

| Role | Job |
| --- | --- |
| **materializer** | reads every group's log and merges it into the SCD1/SCD2 Delta tables |
| **reconciler** | daily: reads the source bucket's S3 Inventory and publishes hints for anything a doorbell missed |

## 3. Groups and positions

- `group(scope) = xxh3_64(scope, seed 0) mod G`, default **G = 32**. The group
  is the unit of everything: one owner, one queue, one log, one checkpoint.
  A worker can own several groups and decodes their actors in parallel.
- `pos(actor) = (epoch, txid)`: the end of the last segment applied. An owner
  keeps `pos` for every actor in its groups in memory (about 100 B each).

## 4. Data

### 4.1 S3 `celld-cdc` keys

| Key | Content | Written |
| --- | --- | --- |
| `config.json` | `{version: 3, groups: G}` | once, `If-None-Match` |
| `leases/group-<g>.json`, `leases/<role>.json` | `{holder, expires_ms}` | CAS (`If-Match`), renewed every 10 s |
| `workers/<id>.json` | `{expires_ms}` | heartbeat every 10 s |
| `log/<g>/<seq:020>.parquet` | **one log entry** (below) | **`If-None-Match`: this create is the commit** |
| `log/<g>/_last_checkpoint` | `{seq}`: a hint | unconditional PUT |
| `checkpoints/<g>/<seq:020>.json.zst` | the group's `pos` map at `seq`, plus where each actor's needed archived segments are (`log` seq and row) | unconditional PUT; deterministic bytes (D3) |
| `tables/scd1/`, `tables/scd2/` | Delta tables; the materializer's offsets are application transactions in them (I12) | Delta commits |

### 4.2 A log entry

One Parquet file per group per batch window:

- **Events row group**: the v1 change-event schema. Rows are `c`/`u`/`d`
  events, `r` events (rows read from a snapshot), and **snapshot markers**
  (`op = snapshot`, no table or key: "the `r` rows at this `seq` are the
  actor's complete state").
- **Segments row group**: `scope, epoch, min, max, ltx` for each bundle
  segment applied, so a state stays rebuildable after celld drains the bundle
  (I4). This replaces v1's separate archive objects.
- **Footer metadata**: `group, seq, writer, nonce`, and `positions`, the new
  `pos` of each actor the entry advanced.

### 4.3 Messages

All messages travel S3 or the reconciler → SNS → every `cdc-q-<g>` → that
group's owner. There is no worker-to-worker traffic.

| Message | Published by | Content |
| --- | --- | --- |
| **Doorbell** | S3 notification (source bucket: `cells/**.ltx`, `log/**.ltxb`) | S3 event JSON, `Records[].s3.object.key` (URL-encoded). A `.ltx` names one actor; a `.ltxb` names a bundle holding many. |
| **Hint** | reconciler | `{keys: [newest segment key per actor]}`, up to 256 KB each. Treated as doorbells. |

## 5. Flows

### 5.1 Receiving doorbells

The owner of group g polls `cdc-q-<g>`. For each message:

1. Decode the key. For a bundle, GET its footer (a ranged read, cached).
2. Keep the segments whose actors are in group g; ignore the rest.
3. Nothing for g: delete the message now. Otherwise, hold it (extending its
   visibility) until every kept segment is committed or a duplicate, then
   delete it (D5).

### 5.2 Owner decode loop

Every **batch window** (default 5 s, or sooner at 64 MiB of events), the
owner processes every actor with pending segments:

```
for each segment K of actor A, in (epoch, min) order:
  K ends at or before pos(A)                      → duplicate: drop
  K.epoch == pos.epoch and K.min == pos.txid + 1  → apply: GET K, diff, pos(A) = K.end
  K.epoch == pos.epoch and K.min >  pos.txid + 1  → hold: a predecessor is missing
  K.epoch >  pos.epoch                            → epoch change (below)
```

- **Apply** is a GET (a ranged GET for a bundle row), then the page diff over
  the actor's paged state (v1).
- **Hold** waits up to the **gap timeout** (default 30 s), then GETs the
  guessed key `…/{pos+1}-{pos+1}.ltx`.
- **LIST is used only where celld's keys can't be predicted**, all on the
  source bucket and all off the steady-state path:
  - a gap the guessed GET doesn't fill (the missing file covers several
    txids): one LIST of the epoch's level-0 directory, starting after `pos`;
  - an epoch change: one LIST of the new `e<epoch>/` directory, then the chain
    rule (I5);
  - a cold actor (not in the owner's cache): one LIST of `cells/<scope>/` to
    plan its rebuild at `pos`, plus its archived segments.
- If the state at `pos` can no longer be rebuilt, the actor is re-snapshotted
  (§5.10).

### 5.3 Commit

1. Write the entry `n = head + 1` with `PUT log/<g>/<n> If-None-Match: *`.
2. **Success:** `head = n`; update `pos`; delete every held message whose
   segments are now all committed or duplicates (D5).
3. **412 (taken):** another writer committed `n`. GET it, fold its positions
   into `pos` (D3), discard the pending batch, and rebuild it from the held
   segments; anything that entry already applied is now a duplicate. If the
   group's lease is held by someone else, stop working the group.
4. **409, 5xx, or timeout:** retry the same create. If it then gets 412, GET
   entry `n`: a footer with this writer's `nonce` means the earlier attempt
   committed.

### 5.4 Materializer

1. Read its offset per group from the SCD tables' application transactions
   (`appId = celld-cdc/group-<g>`, `version = seq`).
2. Poll: GET `log/<g>/<offset + 1>` for each group, every few seconds. A 404
   means nothing new (I3).
3. Merge the entries found, for many groups, into **one Delta commit** that
   also sets each group's application transaction to its new offset. Offset
   and data land atomically (I12).
4. A **snapshot marker** for actor A at `seq` S adds, in the same commit, the
   standard full-load MERGE scoped to one actor:
   - SCD1: rows with `scope = A`, `seq < S`, and `is_deleted = false` get
     `is_deleted = true` and `seq = S`. The marker's own `r` rows carry `seq`
     S, so exactly the rows missing from the snapshot match.
   - SCD2: the current versions of those rows close at S, and a deleted
     version opens at S.

### 5.5 Group acquire and recovery

1. **Acquire:** take `leases/group-<g>.json` if it's absent (`If-None-Match`)
   or expired (`If-Match` on its ETag).
2. **Recover:** GET `_last_checkpoint`, then the checkpoint it names, then GET
   `log/<g>/<seq + 1>`, `<seq + 2>`, … until a 404, folding each footer's
   positions. That gives `head` and the exact committed `pos` map (D3). A
   stale hint only means a longer walk. If the hint is missing, LIST
   `checkpoints/<g>/` (our own bucket, recovery only).
3. Start polling `cdc-q-<g>`. Messages in flight under the previous owner
   reappear when their visibility expires.

### 5.6 Rebalancing

Every 30 s, each worker LISTs `workers/` (heartbeats, a handful of keys) and
computes `target = ceil(G / live workers)`. Below target, it acquires free
or expired groups. Above target by more than one, it releases one group:
commit pending work, stop polling, delete its lease.

### 5.7 Checkpoints

Per group, every 60 s or every 12 entries: PUT `checkpoints/<g>/<seq>`, then
PUT `_last_checkpoint`, then delete checkpoints older than the newest two.
A checkpoint's bytes are a function of the log up to `seq` (D3), so a second
writer would write the same object.

### 5.8 Reconciler

Daily, it reads the source bucket's S3 Inventory, keeps each actor's newest
segment key, and publishes **Hints** to the SNS topic. Owners treat them as
doorbells and drop anything at or before `pos`.

### 5.9 Retention

An S3 lifecycle rule expires `log/` entries after R days (default 7). R must
exceed the materializer's worst lag and the checkpoint interval by a wide
margin. The materializer's compaction clusters SCD1 and SCD2 by `scope`
(Delta z-order or liquid clustering), so a query or reconciliation for one
actor reads about one file per `(class, table_name)` partition.

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
- Held segments past the gap timeout, resyncs, 412s on commits, lease
  changes, and each queue's oldest message age.

## 6. Optional celld change: retention hold

The standard way to rule out a resync entirely is a **retention hold**, as a
Postgres replication slot keeps WAL until its consumer confirms it: celld
would not delete an actor's epochs past a watermark that celld-cdc publishes.
That's a change to celld, so it's listed here, not assumed. The cost: celld
keeps data longer for an actor celld-cdc is stuck on.

## 7. Cost (rough)

| Request | When |
| --- | --- |
| SNS publish | one per doorbell (deliveries to SQS are free) |
| SQS | G receives and deletes per doorbell, about $0.40 per million each |
| GET | one per segment, plus page reads for the diff; G footer reads per bundle; one per group per materializer poll |
| PUT | one log entry per group per batch window; a checkpoint per group per minute; lease and heartbeat renewals every 10 s |
| LIST | source bucket: gaps the guessed GET can't fill, epoch changes, cold actors. Our bucket: `workers/` every 30 s, and checkpoints only when a hint is missing. |
| S3 Inventory | daily, priced per million objects listed |

Requests grow with segments (GETs, G × SQS) and with groups × time (PUTs),
not with actors × wake-ups as in v1. There is no cluster to run.

## 8. Failures

| Failure | What happens | Why it is safe |
| --- | --- | --- |
| **Doorbell late, duplicated, or reordered** | The owner holds, applies, or drops each segment by the rule in §5.2. | Positions come from keys (I2), not arrival order (I6, I8); applying only `pos + 1` keeps one contiguous history (D4, I1). |
| **Doorbell lost, a later one arrives** | The later segment is held; after the gap timeout the owner GETs the guessed key, then LISTs. | S3 shows every object that exists (I3), so the missing one is found. |
| **Doorbell lost for an actor's newest segment** | Nothing reveals it until the reconciler's hint, up to a day later. | The inventory lists every object (I9); a hint at or before `pos` is dropped (D4). |
| **SNS fails to deliver to a queue** | SNS retries (I7). If it never delivers, it's a lost doorbell (the two rows above). | Same as a lost doorbell. |
| **A predecessor isn't in S3 yet** (multi-node fleet: on peer disks, not uploaded) | The segment is held; its own doorbell or the gap probe finds it once uploaded. | Holding never skips a txid (D4). |
| **A bundle spans many groups** | Every group's queue has its own copy; each owner takes its actors and deletes its copy. | Copies are independent (I7, I8); each owner deletes only for its own group (D5). |
| **A group has no owner** | Its messages wait in its queue until a worker acquires it. | Nothing deletes them (D5); retention is 14 days (I8). |
| **Owner dies before committing** | Its lease expires; another worker acquires the group and recovers. Held messages reappear and are processed again. | The uncommitted batch was never created (I3); recovery restores exactly the committed positions (§5.5, D3); redoing gives the same events (I10). |
| **Owner dies after committing, before deleting messages** | The messages reappear; the new owner drops their segments as duplicates and deletes them. | `pos` after recovery includes the entry (§5.5, D3, D4). |
| **Two writers on one group** (a paused owner wakes past its lease, or clock skew makes a lease look free) | Both try to create entry `n`; exactly one succeeds. The loser GETs the winner's entry, folds it, and stops if it no longer holds the lease. | Only one create of `n` succeeds (I3, D2), and each writer built its entry from the log at `n − 1`, so either entry is a valid continuation (D3). Leases don't matter for this (D7). |
| **Commit outcome unknown** (timeout, 5xx) | Retry the same create; on 412, compare entry `n`'s nonce with ours. | The create is idempotent per key (I3); the nonce identifies the writer. |
| **Concurrent create in flight** (409) | Retry. | I3. |
| **Checkpoint written twice, or by a stale writer** | Same bytes either way. | A checkpoint is a function of the log up to its `seq` (D3). |
| **`_last_checkpoint` stale, backwards, or missing** | Recovery walks forward further, or LISTs `checkpoints/<g>/`. | The hint is only a starting point; the walk always ends at the true head (D2, I3). |
| **A worker configured with a different G** | Impossible: G comes only from `config.json`. | D1. |
| **Materializer crashes mid-merge** | The Delta commit either happened, with its offsets, or didn't. The next run resumes from the offsets in the table. | Data and offsets commit atomically (I12); merges are idempotent (I11). |
| **Two materializers** (a stale holder still running) | Delta's optimistic concurrency makes one commit win; the other re-reads the offsets and skips entries already applied. | I12, I11. The lease is only for efficiency (D7). |
| **Materializer behind log retention** | Entries it hasn't read have expired. | **Must not happen**: the lag alert (§5.11) fires long before R. Recovery is a re-snapshot of the affected group's actors (§5.10). |
| **celld drains a bundle before we read it** | Its txids appear in `cells/`, possibly as an L1; the planner applies that, coarser. | I4, I5. The before-state comes from archived segments in our log (§4.2). |
| **An archived segment an actor needs has expired** (the actor was idle past R) | The actor is re-snapshotted (§5.10). | The same argument as the row below. |
| **celld takeover, new epoch, or a fenced owner's late writes** | The epoch-change path applies the chain rule, and rejoins from the old state if the history moved. | I5, as in v1. |
| **Actor state no longer rebuildable** (celld's retention removed its segments while celld-cdc was far behind) | The actor is re-snapshotted (§5.10): its current rows as `r` events plus a marker. The tables reconcile on the marker; the change log has no delete events for the gap, as with a Debezium re-snapshot. | The snapshot is celld's current history (I5). The marker and its rows are in one entry (D6), so the actor's rows older than the marker's `seq` are exactly those the snapshot no longer has. A later event has a greater `seq` and is untouched, and replaying an older entry can't revive a row, since SCD1 only takes a newer `seq` (I11). |
| **A worker's cache is lost** | Cold actors are rebuilt from S3. | Caches only save time (I4, D8). |
| **Clock skew** | Leases expire early or late. | Leases are advisory (D7); conditional creates decide (I3); timers affect liveness only (D8). |
| **S3 throttling** (503 SlowDown) | Back off and retry. | Request rates are low (§7); a retried create is safe (I3). |

## 9. Open questions

- **delta-rs application transactions:** confirm delta-rs reads and writes
  Delta `txn` actions (I12). If not, store the materializer's offsets in a
  CAS object per group, written after each Delta commit. Merges are
  idempotent (I11), so a crash between the two only replays.
- **Inventory delay:** the reconciler finds a lost newest-segment doorbell
  within a day. If that's too slow, add a weekly or daily flat LIST, or run
  the inventory more often.
- **G:** 32 by default; it caps active workers and can't change later.
