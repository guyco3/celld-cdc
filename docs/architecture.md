# celld-cdc architecture

## Overview

`celld-cdc` turns the state of [celld](https://github.com/denoland/celld)
actors (self-hosted Durable Objects) into central, queryable change data:

- a **change log** of row-level events, as Parquet files on S3;
- **SCD Type 1** (latest state) and **SCD Type 2** (full history) tables, as
  Delta Lake tables on S3, readable by DuckDB, Spark, Trino, Databricks,
  Snowflake, ClickHouse, and Polars.

celld already writes every committed SQLite change of every actor to an
S3-compatible bucket in the LTX format. celld-cdc reads those files. It
needs no change to celld, no hook in application code, and no database of
its own: every worker is stateless, and all state lives in object storage.

```
 celld fleet ──PUT .ltx/.ltxb──► S3 "celld" ──notification──► SQS cdc-doorbell
                                                                    │
                       ┌────────────────────────────────────────────┘
                       ▼
        decoder (N, stateless) ──PUT changes/*.parquet──► S3 "celld-cdc"
          │  cursor + lease per actor: cursors/<scope>.json (If-Match)      │
          │  archived bundle segments: segments/<scope>/…                   │
          └─────────────────────────────────────────────────────────────────┤
                                                       notification         │
                                                            ▼               │
                                                     SQS cdc-changes        │
                                                            │               │
                         materializer (stateless) ◄─────────┘               │
                            └─MERGE─► tables/scd1, tables/scd2 (Delta) ─────┘
```

## What celld writes

### Keys

| Object | Key |
| --- | --- |
| Per-cell LTX file | `cells/{scope}/ltx/e{epoch}/{level:04x}/{min:016x}-{max:016x}.ltx` |
| Node-log bundle | `log/{node}/{generation}/bundle/e{log_epoch}-{seq:08}.ltxb` |
| Ownership record | `cells/{scope}/own.json` |

- **scope** is `{Class}:{64-hex id}`. The id is an HMAC of the object name,
  so the name cannot be recovered from the key. celld stores it inside the
  actor's database, in `_cf_METADATA.actor_name`, and celld-cdc reads it
  from there into every event.
- **epoch** grows with every activation of the actor. A non-paged epoch
  restarts txids at 1 with a full-database snapshot; a paged epoch continues
  the txids of the epoch below it.
- **level** 0 holds WAL captures; 1 holds compactions of L0 (merged pages,
  intermediate states gone); 9 holds full snapshots written at handoff.
- S3 notifications URL-encode keys: the scope's `:` arrives as `%3A`.

### LTX files are pages, not rows

An LTX file holds the SQLite pages a capture changed, LZ4-compressed, with a
header giving `min_txid`, `max_txid`, page size, the database size after the
capture (`commit`), and a timestamp. A row change has to be recovered by
comparing the database before and after the file.

One L0 file is one **capture**: everything committed to the WAL since the
last sync, which can be several SQLite transactions. Type 2 history
therefore has one version per captured upload, not necessarily per commit.
In a single-node fleet celld captures every commit separately.

### Two nodes or more: bundles

In celld's default `fleet` durability mode with two or more nodes, commits do
not reach `cells/` right away. Every ~2 s a node writes one bundle object
holding the L0 segments of every cell it changed (format `CLB1`: the raw L0
bytes, then a footer of `cell, epoch, txid, offset, len` rows). The txids
reach `cells/` later, when celld compacts to L1 or hands the cell off, and
the drained bundle is deleted. celld-cdc reads bundles directly so changes
arrive within seconds instead of at the next compaction.

### Storage APIs

| Durable Object API | SQLite table | Captured as |
| --- | --- | --- |
| `ctx.storage.sql` | the application's tables | one table each, keyed by PRIMARY KEY (or `_rowid`) |
| `ctx.storage.put/get` | `_cf_KV(scope, k, v)` | table `_cf_KV`, key `{"k": …}`, value decoded from V8 serialization |
| alarms, wake records, facets, metadata | `_cf_ALARM`, `_cf_WAKE`, `_cf_FACETS`, `_cf_METADATA` | not captured |

## Ordering without an ordered queue

S3 notifications are unordered, can be duplicated, and can be lost. S3
cannot target an SQS FIFO queue, and EventBridge's FIFO message group is a
constant, so a broker cannot provide per-actor order either.

celld-cdc does not need it. An actor has one writer at a time (celld fences
ownership with a conditional write, and the actor itself is single
threaded), so each actor's history is one line of `(epoch, txid)` positions,
and every key spells out its position. S3 LIST is strongly consistent and
returns keys sorted. So a notification is only a **doorbell**: "actor X has
something new". On any doorbell, the actor's lane:

1. takes the actor's lease;
2. lists the actor's segments: `cells/{scope}/` (all epochs and levels),
   live bundles, and celld-cdc's own archive of bundle segments;
3. builds the actor's chain with celld's rule (walk epochs down from the
   newest; an epoch that does not open with a snapshot must continue the one
   below it exactly at its cut);
4. applies, in txid order, every segment that continues from its cursor,
   choosing the finest segment available (an L0 or bundle row over an L1);
5. stops at the newest segment, or at a gap (a txid not visible yet), and
   asks to be woken again shortly.

Duplicates, reordering, and bursts only change when work happens, never
what it produces: fifty doorbells for one actor become one listing and one
ordered pass. A periodic **sweep** runs every actor's lane, which recovers
lost notifications.

## Statelessness

All of a worker's state is in the `celld-cdc` bucket:

| State | Where | Why it is safe |
| --- | --- | --- |
| Cursor: last applied `(epoch, txid)` | `cursors/{scope}.json` | written only with `If-Match` on its ETag |
| Lease: holder and expiry | the same object | taking or renewing it is the same conditional write |
| Archived bundle segments | `segments/{scope}/e{epoch}/{min}-{max}.ltx` | a verbatim copy, so state at any applied position stays rebuildable after celld deletes the bundle |
| Change files | `changes/class=…/dt=…/hh=…/{scope}-e{epoch}-{min}-{max}.parquet` | named and encoded deterministically |

A lane never downloads the actor's database. It opens the state at its
cursor **paged**: a page map over the fewest segments that reach it
(snapshot, L1s, then L0s), built from each file's trailing page index, behind
celld-ltx's paged SQLite VFS, which fetches a page with one ranged GET the
first time anything reads it. The next segment is layered on top in memory
to give the "after" state, and the two are diffed by the pages the segment
changed (below). The VFS's sparse file lives in a temp directory, and a
bounded in-memory cache keeps page indexes and frames across runs; nothing
local outlives the process, and losing it costs only time.

## Finding the changed rows

LTX files hold pages, so rows have to be recovered. A segment lists every
page it wrote, and the rows that changed live on those pages:

1. **Ownership.** For each captured table, walk its b-tree from its root
   (in `sqlite_schema`) on both sides, reading interior pages only. Every
   leaf sits at one depth, so one read per level tells interior pages from
   leaves. A changed page that is a leaf of the table's tree, before or
   after, holds changed rows.
2. **Candidates.** Read the rowids off the old and new version of each such
   leaf: a table leaf cell starts with two varints, payload length and
   rowid.
3. **Rows.** Have SQLite read just those rowids on both sides (it handles
   records, overflow chains, and added columns), and compare by key. A row
   that a page split moved appears on two pages under one key and nets out;
   a `REPLACE` that changes a row's rowid but keeps its primary key comes
   out as an update.

The cost follows the size of the change, not the database: the changed
leaves, their tables' interior pages (about 1% of a database, and cached),
and the rows on them. A whole-table comparison stands in where pages are
not a cheap guide: from an empty database (an actor's first snapshot),
after a schema change, when more than a quarter of the pages changed (a
VACUUM), and for a WITHOUT ROWID table whose tree the segment touched. Those
read ahead in megabyte runs instead of page by page.

The whole-table comparison over a fully rebuilt database (`--diff full`) is
kept as the oracle. Property tests run random workloads (overflowing rows,
page splits, deletes that free pages, `REPLACE`, tables with and without
keys, WITHOUT ROWID, `_cf_KV`, indexes, schema changes, VACUUM) through
both and require identical events, and require the events to fold back
into each table's real final rows.

**Lease and crash safety.** The cursor advances after the change file is
written. A worker that crashes between the two, or loses its lease, leaves
a change file that the next holder rewrites byte for byte, because both the
key and the content are functions of the segment. The lease is the same
pattern celld uses for cell ownership, so the only infrastructure is the
bucket.

## Change events

One schema for every table (Parquet, zstd):

| Column | Type | Meaning |
| --- | --- | --- |
| `event_id` | string | sha256 of `scope, epoch, txid, table, pk`; stable across replays |
| `class`, `scope`, `actor_name` | string | the Durable Object class, cell scope, and `idFromName` name |
| `table_name` | string | the SQLite table |
| `op` | string | `c` create, `u` update, `d` delete, `r` read from a snapshot (no earlier state) |
| `epoch`, `min_txid`, `txid` | int64 | the segment; the change is visible at `(epoch, txid)` |
| `seq` | string | `e{epoch:016x}-t{txid:016x}`, sortable: the actor's total order |
| `commit_ts_ms`, `commit_ts` | int64, timestamp | the LTX header timestamp |
| `source_key` | string | the `.ltx` or `.ltxb` object read |
| `pk`, `before`, `after` | JSON | key columns, row before, row after |

Row data is JSON because SQLite is dynamically typed: a column declared
TEXT can hold integers and blobs, which would break a typed schema. Integers
and reals are numbers, text is a string, a BLOB is `{"$b64": …}`. A `_cf_KV`
value is decoded from V8's serialization into plain JSON (`{"$date": ms}`,
`{"$map": …}`, and so on for types JSON lacks), or kept as `{"$v8": …}`.

## SCD tables

The materializer batches change-file notifications (a few seconds or a few
hundred files) and commits each batch to two Delta tables, partitioned by
`class` and `table_name`:

- **scd1** keeps, per `(scope, table_name, pk)`, the event with the greatest
  `seq`. A MERGE updates a row only when the incoming `seq` is newer. A
  delete is kept as a tombstone (`is_deleted = true`), so a replayed older
  insert can never bring the row back.
- **scd2** keeps every version, with `valid_from_seq`, `valid_to_seq`,
  their timestamps, and `is_current`. For the keys a batch touches, the
  stored versions and the incoming ones are unioned, deduplicated, sorted by
  `seq`, and their validity recomputed; the changed versions are merged in
  one commit.

Both are order-independent and idempotent, so change files can be applied
in any order, more than once. That is what makes the materializer safe with
an unordered queue, and lets a periodic rescan of recent partitions repair
any lost notification.

Delta commits use S3 conditional writes (`If-None-Match`) for their log, so
no DynamoDB lock is needed.

## Guarantees and limits

- **Exactly-once results** from at-least-once delivery: every write is
  idempotent and keyed by the segment.
- **Per-actor order** follows the actor's own commit order.
- **Granularity** is one version per captured segment. With a single node,
  that is one per commit. With bundles, one per capture. When only an L1
  remains (a bundle drained before celld-cdc read it), the changes inside
  it collapse into one version.
- **Epoch changes.** When an actor's history moves to a new chain (a new
  non-paged epoch, or a takeover that drops a fenced owner's unacknowledged
  writes), the lane diffs its last state against the new chain's first
  state, so the tables converge on the history celld kept.
- **Starting late.** An actor seen for the first time from a snapshot that
  is not its birth gets `op = r` rows, as in a Debezium initial snapshot.
- **Cost.** A segment costs its own bytes, a few page reads per changed
  leaf, and the interior pages of the tables it touched. In the test suite,
  a one-row update to a 22 MB actor reads 332 KB (most of it one 256 KB
  page-index read) in 18 ms, against 20.8 MB in 1.8 s for a full rebuild.
  Schema changes, VACUUMs, and first snapshots still cost a scan.

## Roadmap

- Batch a hot actor's backlog: diff a run of contiguous segments at once
  when a lane is behind, trading SCD2 granularity for throughput.
- Change-log compaction and retention.
- Kafka/Kinesis doorbell sources, Iceberg output.

## Failure cases, and why they are safe

Everything below rests on three facts:

1. **The actor's order is fixed.** An actor has one writer at a time and
   runs single threaded, so its txids form one sequence: 3-4 was built on
   top of 1-2. Every key spells out its `(epoch, txid)` range.
2. **The cursor only moves one contiguous segment at a time.** A lane
   applies the segment that starts at `cursor + 1`, or nothing. It never
   skips a txid, so a missing file means waiting, never guessing.
3. **Every write is deterministic.** Change-file keys and bytes, and event
   ids, are functions of `(scope, epoch, txid)`. Redoing work rewrites the
   same bytes.

Single threading fixes the order of the actor's *writes*. It does not fix
the order in which their uploads become visible in S3, nor the order of
their notifications, so celld-cdc relies on neither.

### Notifications

| Case | What happens | Why it is safe |
| --- | --- | --- |
| **Out of order, files present** (doorbell 3-4 arrives before doorbell 1-2; both objects are in S3) | The 3-4 doorbell runs the lane. It lists the actor, applies 1-2 then 3-4, and moves the cursor to 4. The 1-2 doorbell later finds nothing past 4 and is deleted. | The doorbell's key never chooses what to apply; the sorted, strongly consistent LIST does. |
| **Out of order, file missing** (3-4 visible, 1-2 not yet: an upload retry, or a txid still on its way from a bundle) | The lane sees nothing starting at 1, but newer txids exist: a gap. It leaves the cursor at 0 and makes the message visible again in 5 s. The retry finds 1-2 and applies both. | Applying 3-4 onto the wrong base would corrupt the rebuilt database. Rule 2 forbids it. |
| **Duplicate** | The second run lists, finds nothing past the cursor, and deletes the message. | Rule 2: nothing at or below the cursor is applied again. |
| **Lost** (S3 or LocalStack drops it) | The next doorbell for the actor, a gap retry, or the sweep (every 5 min) runs the lane, and the LIST finds the file. | A doorbell only says "look". Looking later finds the same files. |
| **Burst** (many doorbells for one actor in a batch) | One lane run per actor per batch. | The lane drains everything contiguous, whatever woke it. |
| **S3 test event** | Names no object; deleted. | Not a key celld-cdc reads. |

### Workers

| Case | What happens | Why it is safe |
| --- | --- | --- |
| **Two workers, one actor** | Both try to claim the cursor with a conditional PUT; one wins. The other gets `Busy` and retries its message in 2 s, in case the holder listed before its file landed. | `If-Match` on the cursor's ETag admits one writer. |
| **Crash after writing a change file, before advancing the cursor** | The lease expires (`CDC_LEASE_MS`). The next holder reapplies the segment and rewrites the same change file. | Rule 3: same key, same bytes, same event ids. The materializer dedupes on `event_id`. |
| **Crash mid-segment** | Nothing durable was written for the segment; the next holder starts it over. | The cursor advances only after the change file is written. |
| **Slow worker loses its lease** (paused past its TTL) | Another worker takes the lane. The slow worker's next cursor write fails its `If-Match`, so it stops. | Fencing by ETag, as celld fences cell owners. Anything it wrote before is identical to what the new holder writes. |
| **Local disk lost** | Nothing to recover: the scratch database is rebuilt from the bucket on the next run. | No local state outlives a run. |
| **Materializer crash, or a failed Delta commit** | Its messages are not deleted. They come back after the visibility timeout, and the batch is reapplied. | SCD1 keeps the greatest `seq` per key, and SCD2 dedupes by `event_id` and recomputes validity. Reapplying is a no-op. |
| **Change files applied out of order** | A newer change for a key is applied before an older one. | SCD1 updates only when the incoming `seq` is newer. SCD2 re-sorts every touched key's versions. Order does not change the result. |
| **Change-file notification lost** | The materializer's rescan (every 5 min, over the last 2 hours of `changes/`) reapplies it. | Idempotent, as above. |

### celld

| Case | What happens | Why it is safe |
| --- | --- | --- |
| **Multi-node fleet** (txids sit in bundles, not `cells/`) | The catalog reads live bundles' footers and takes each actor's rows from them. | Bundle rows are the same L0 bytes celld later folds into `cells/`. Duplicates across sources collapse to one segment per range. |
| **Bundle drained before celld-cdc reads it** | Its txids reach `cells/` later, as an L1 or a merged L0, and the lane applies that. Until then it is a gap. | Correct, at coarser granularity: the changes inside one L1 become one version. |
| **Bundle drained after celld-cdc applied it** | Rebuilding the cursor's state uses the archived copy in `segments/`. | celld-cdc archives every bundle segment before advancing past it. |
| **Takeover, paged** (new epoch continues the txids) | The chain links the epochs at the cut; the lane crosses into the new epoch. | celld's chain rule, the same one celld restores with. |
| **Takeover or reactivation, non-paged** (new epoch restarts at txid 1 with a snapshot) | The cursor's epoch is no longer on the chain. The lane rebuilds its old state, diffs it against the new chain's first state, and continues from there. | The tables converge on the history celld kept. Nothing is double-counted, because the diff is against the state celld-cdc last emitted. |
| **Fenced owner's late writes** (an old owner uploads into a superseded epoch) | The chain clips that epoch at the cut and ignores the extra txids. If celld-cdc had already applied them, the next run diffs back to the kept history: those rows come out as updates or deletes. | The chain decides what history is real; the lane follows it, never the other way round. |
| **Cursor's state no longer rebuildable** (celld's retention deleted the epochs the cursor is in, while celld-cdc was far behind) | The lane logs `cursor state is gone; resyncing from empty` and diffs an empty database against the current one. | **Only partly safe; a known limit.** Every live row is re-emitted, so SCD1 has every current value and SCD2 gains a version per row with the same content. But a row deleted while the state was unrebuildable is never emitted as a delete, so it stays live in SCD1. Keep `CELLD_LTX_RETENTION_SECS` well above the longest celld-cdc outage; a planned fix is to diff against SCD1 itself instead of an empty database. |
| **First sight of an actor that started earlier** (only a later snapshot remains) | Rows from that snapshot are emitted as `op = r`. | As in Debezium's initial snapshot: present now, with no earlier state to diff against. |
