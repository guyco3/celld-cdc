# celld-cdc v2 architecture

**Status:** proposed. Replaces v1's per-actor cursor and lease objects in S3,
the `cdc-changes` queue, and the materializer rescan. Decoding (paged
databases, page diff, V8 values) and the SCD merges are unchanged.

## 1. Invariants

Everything below is argued from these. **I** are facts about the systems we
build on; **D** are properties this design maintains.

| # | Invariant | Source |
| --- | --- | --- |
| I1 | **An actor has one writer at a time and runs single threaded**, so its history is one sequence of positions `(epoch, txid)`. | celld ownership records (`own.json`, conditional writes) |
| I2 | **Every segment names its position.** A cell key names `scope, epoch, level, min, max`; a bundle footer row names `cell, epoch, txid, offset, len`. | celld key and bundle formats |
| I3 | **S3 is strongly consistent**: a GET, LIST, or 404 after a PUT reflects it. LIST returns keys sorted. | S3 (since 2020) |
| I4 | **Segments are immutable.** celld never rewrites a key with other content. A bundle is deleted only after its rows are drained to `cells/`. | celld |
| I5 | **celld's chain rule** decides which epochs are history: a non-paged epoch opens with a snapshot; a paged epoch continues the one below it exactly at its cut; txids past a cut in a superseded epoch are not history. | celld restore |
| I6 | **S3 notifications** can be late, duplicated, reordered, or (rarely) lost. Each names one object key. | S3 |
| I7 | **SNS delivers every published message to every subscribed SQS queue**, at least once, retrying failed deliveries. | SNS |
| I8 | **SQS standard queues** are at-least-once and unordered. A received message is hidden for its visibility timeout (≤ 12 h) and removed only by a delete with its receipt handle. Retention ≤ 14 days. Each queue's copy of a message is independent. | SQS |
| I9 | **etcd transactions are linearizable and atomic across keys.** A key attached to a lease is deleted when the lease expires or is revoked; a compare on that key then fails. | etcd |
| I10 | **etcd tolerates a minority of failed members.** Losing quorum stops writes; it loses no committed write. | etcd (Raft, WAL on disk, members across zones) |
| I11 | **Decoding is deterministic**: the same segment over the same before-state gives the same events, and `event_id = H(scope, epoch, txid, table, pk)`. | celld-cdc |
| I12 | **SCD merges are idempotent and order-independent**: SCD1 keeps the greatest `seq` per key, SCD2 recomputes validity for touched keys. | celld-cdc (v1, unchanged) |
| D1 | **Each group has at most one owner**, the holder of `groups/<g>/owner`, and only that owner reads queue `q-<g>`. | I9 |
| D2 | **A row group is part of the change log iff a log entry in etcd names it.** Nothing else reads batch files. | design |
| D3 | **An actor's position only moves forward, one contiguous segment at a time.** | design, I1, I2 |
| D4 | **An owner deletes a message from its queue only after every segment in it for its group is committed or is a duplicate.** | design, I8 |
| D5 | **A snapshot marker for an actor at `seq` S is committed in the same row group as that actor's complete set of rows at S**, each a `r` event carrying `seq` S. | design (§5.10) |
| D6 | **Wall-clock time is used only for liveness** (lease TTLs, batch and gap timers), never for correctness. | design |

## 2. Services

| Service | Count | State | Role |
| --- | --- | --- | --- |
| **celld fleet** | existing | its bucket | writes `.ltx` and `.ltxb` (unchanged) |
| **S3 `celld`** | 1 | celld's data | source, read only; notifies SNS |
| **SNS `cdc-doorbell`** | 1 topic | none | fans every doorbell out to every group queue |
| **SQS `cdc-q-<g>`** | G queues (default 32) | in-flight doorbells | one per group; every queue gets every message |
| **etcd cluster** | 3 or 5 members, one per zone, **its own deployment** | group ownership, shard state, commit log tail, consumer offsets, roles | the metadata store |
| **workers** | up to G active, **separate deployment** | none durable | one binary; each owns groups, decodes their actors; some hold elected roles |
| **S3 `celld-cdc`** | 1 | batches, archive, checkpoints, manifests, Delta tables | output and durable per-actor state |

Elected roles (etcd election), each held by one worker:

| Role | Job |
| --- | --- |
| **materializer** | reads committed row groups, merges them into the SCD1/SCD2 Delta tables |
| **sweeper** | hourly flat LIST of the source bucket; publishes hints for anything a doorbell missed |
| **janitor** | moves old log entries into S3 manifests, deletes old checkpoints, orphans, and expired data |

## 3. Shards, groups, positions

- `shard(scope) = xxh3_64(scope, seed 0) mod N`, default **N = 1024**.
  The shard is the unit of state: its checkpoint and its commit log.
- `group(shard) = shard mod G`, default **G = 32**. The group is the unit of
  ownership: one owner, one queue. Workers beyond G stand by.
- N and G are fixed and stored in etcd at `config`. A worker with other
  values refuses to start, and every commit compares `config`'s revision.
- `pos(actor) = (epoch, txid, seq)`: the end of the last segment applied, and
  the shard log entry that committed it. An owner keeps `pos` for every actor
  in its groups in memory (about 100 B each; 10M actors over 1024 shards is
  about 1 MB per shard).

## 4. Data

### 4.1 etcd keys (prefix `/cdc/v2/`)

| Key | Value | Lease | Written by |
| --- | --- | --- | --- |
| `config` | `{shards: N, groups: G}` | — | operator, once |
| `workers/<id>` | `{started}` | worker's | the worker |
| `groups/<g>/owner` | `<worker id>` | owner's | acquire, release |
| `shards/<s>/state` | `{next_seq, checkpoint: {key, seq}}` | — | commit, checkpoint |
| `log/<s>/<seq:020>` | `{file, row_group, events, actors, archive}` | — | commit; deleted by janitor |
| `manifests/<s>/<to_seq:020>` | `{key, from_seq}` | — | janitor |
| `consumers/<group>/<s>` | `{seq}` | — | the consumer group's elected holder |
| `roles/<role>` | election | holder's | etcd election |

A shard's change log is its `log/<s>/` entries plus its manifests. **`seq`
is the shard's offset**; order is total per shard, so per actor.

### 4.2 S3 `celld-cdc` keys

| Key | Content |
| --- | --- |
| `batches/<worker>/<lease>/<n:016x>.parquet` | one batch: v1 change-event schema, **one row group per shard**. Footer metadata per row group: `shard`, and `positions`: the new `pos` of each actor it advanced. Rows are `c`/`u`/`d` events, `r` events (rows read from a snapshot), and **snapshot markers** (`op = snapshot`, no table or key): "the `r` rows at this `seq` are the actor's complete state". |
| `archive/<worker>/<lease>/<n:016x>.ltxb` | the bundle segments that batch applied, in celld's bundle format, so a state stays rebuildable after celld drains the bundle (I4) |
| `checkpoints/<s>/<seq:020>.json.zst` | the shard's full `pos` map, plus each actor's archived segments still needed, as of `seq` |
| `manifests/<s>/<from>-<to>.parquet` | log entries moved out of etcd |
| `tables/scd1/`, `tables/scd2/` | Delta tables (unchanged) |

### 4.3 Messages

All messages travel S3/sweeper → SNS → every `cdc-q-<g>` → that group's owner.
There is no worker-to-worker traffic.

| Message | Published by | Content |
| --- | --- | --- |
| **Doorbell** | S3 notification (source bucket: `cells/**.ltx`, `log/**.ltxb`) | S3 event JSON, `Records[].s3.object.key` (URL-encoded). A `.ltx` names one actor; a `.ltxb` names a bundle holding many. |
| **Hint** | sweeper | `{keys: [newest segment key per actor]}`, up to 256 KB each. Treated as doorbells. |
| **Commit watch** | etcd | a PUT on `log/<s>/…`: wakes consumers; they read by key range (§5.4). |

## 5. Flows

### 5.1 Receiving doorbells

The owner of group g polls `cdc-q-<g>`. For each message:

1. Decode the key. For a bundle, GET its footer (a ranged read, cached).
2. Keep the segments whose actors are in group g; ignore the rest.
3. Nothing for g: delete the message now. Otherwise, hold it (extending its
   visibility) until every kept segment is committed or a duplicate, then
   delete it (D4).

### 5.2 Owner decode loop

Every **batch window** (default 5 s, or sooner at 64 MiB of events), the
owner processes every actor with pending segments:

```
for each segment K of actor A, in (epoch, min) order:
  K ends at or before pos(A)                      → duplicate: drop
  K.epoch == pos.epoch and K.min == pos.txid + 1  → apply: GET K, diff, pos(A) = K.end
  K.epoch == pos.epoch and K.min >  pos.txid + 1  → hold: a predecessor is missing
  K.epoch >  pos.epoch                            → epoch change: LIST e{K.epoch}/,
                                                    apply the chain rule (I5)
```

- **Apply** is a GET (a ranged GET for a bundle row), then the page diff over
  the actor's paged state (v1). No LIST.
- **Hold** waits up to the **gap timeout** (default 30 s). Then the owner
  GETs the guessed key `…/{pos+1}-{pos+1}.ltx`, and on a 404 does one LIST of
  the epoch's level-0 directory starting after `pos`.
- An actor not in the owner's cache rebuilds its state at `pos` from a plan
  (one LIST of `cells/<scope>/`, plus its archived segments). Cold path only.
- If the state at `pos` can no longer be rebuilt, the actor resyncs (§5.10).

### 5.3 Commit

1. PUT the batch file and, if bundle segments were applied, its archive
   object. Retry until both succeed.
2. One etcd transaction for every shard in the batch:

   ```
   If   config.mod_revision == R_config
        and, for each shard s: groups/<group(s)>/owner == me (lease L)
                           and shards/<s>/state.mod_revision == R_s
   Then for each shard s: put state.next_seq += 1
                          put log/<s>/<seq> = {file, row_group, …}
   ```

3. **Success:** update `pos` in memory; delete every held message whose
   segments are now all committed or duplicates (D4). **Failure:** re-read
   the state; if a group was lost, drop its shards (their row groups are never
   referenced, D2) and retry the rest. **Ambiguous** (timeout): re-read
   `log/<s>/`; an entry naming this file means it committed.

### 5.4 Consumers (the materializer)

1. Read `consumers/materializer/<s>` for every shard; read log entries after
   it by key range, from manifests first if needed. A watch on `log/` only
   wakes the reader.
2. Read the named row groups (ranged GETs) and merge them into the Delta
   tables (one Delta commit for many shards). A **snapshot marker** for actor
   A at `seq` S adds a reconciliation to the same commit, the standard
   full-load MERGE scoped to one actor:
   - SCD1: rows with `scope = A`, `seq < S`, and `is_deleted = false` get
     `is_deleted = true` and `seq = S`. The marker's own `r` rows already
     carry `seq` S, so exactly the rows missing from the snapshot match.
   - SCD2: the current versions of those rows are closed at S, and a deleted
     version opens at S.
3. Advance `consumers/materializer/<s>`, comparing the holder's election
   lease.

### 5.5 Group acquire and recovery

1. **Acquire:** `If groups/<g>/owner does not exist Then put it with my lease`.
2. **Recover** each shard of g: read `state`, load its checkpoint, then apply
   the `positions` of each log entry after the checkpoint's seq (a ranged GET
   of each file's footer). The result is the exact committed `pos` map.
3. Start polling `cdc-q-<g>`. Messages in flight under the previous owner
   reappear when their visibility expires and are handled as usual.

### 5.6 Rebalancing

Each worker computes `target = ceil(G / live workers)` from `workers/`.
Below target, it acquires unowned groups. Above target by more than one, it
releases one group at a time: commit pending work, stop polling, delete
`groups/<g>/owner`. A worker that dies just lets its lease expire.

### 5.7 Checkpoints

Per shard, every 60 s or every 12 commits: PUT `checkpoints/<s>/<seq>`, then
`If group owner == me and state.mod_revision == R Then put state.checkpoint`.

### 5.8 Sweeper

Hourly, it does one flat LIST of `cells/` and `log/` (1,000 keys per
request), keeps each actor's newest segment key, and publishes **Hints** to
the SNS topic. Owners treat them as doorbells and drop anything at or before
`pos`.

### 5.9 Janitor

- **Log compaction:** for entries at or below the shard's checkpoint seq and
  older than 1 h, PUT a manifest, then `put manifests/<s>/<to>` and delete
  the range `log/<s>/<from..to>` in one transaction.
- **Checkpoints:** keep the newest two per shard.
- **Orphans:** delete batch and archive objects older than 1 h that no log
  entry or manifest names.
- **Retention:** delete manifests and batch files past the change-log
  retention.
- **Clustering:** compact the SCD1 and SCD2 tables clustered by `scope`
  (Delta z-order or liquid clustering), so a query or reconciliation for one
  actor reads about one file per `(class, table_name)` partition.

### 5.10 Resync (re-snapshot)

When an actor's state at `pos` can't be rebuilt (celld's retention removed
the segments it was built from while celld-cdc was far behind), its owner
re-snapshots it, the way a Debezium connector does when its log position is
gone:

1. builds the first state of the actor's current chain (I5);
2. emits every row of every captured table as an `r` event, and then one
   **snapshot marker**, all at the chain's position (`seq` S), in the same
   row group (D5);
3. commits as usual and continues from there.

Deletes that happened in the gap are not in the change log. The tables get
them from the marker's reconciliation (§5.4): rows of that actor older than
S are exactly the rows the snapshot no longer contains. Consumers of the
change log treat a marker the same way, as Debezium consumers treat the end
of a snapshot.

### 5.11 Monitoring

- **Lag**, per shard: the age (by `commit_ts`) of the oldest segment pending
  or held. **Alert well before celld's retention** (`CELLD_LTX_RETENTION_SECS`):
  for example, at one hour against a retention of days. A resync should
  never happen in normal operation; this alert is what keeps it that way.
- **Held segments** past the gap timeout, resyncs, commit failures, etcd
  leader changes and commit latency, and each queue's oldest message age.

## 6. Optional celld change: retention hold

The standard way to rule out a resync entirely is a **retention hold**, as a
Postgres replication slot keeps WAL until its consumer confirms it: celld
would not delete an actor's epochs past a watermark that celld-cdc publishes
in its bucket. That's a change to celld, so it's listed here, not assumed.
The cost: celld keeps data longer for an actor celld-cdc is stuck on.

## 7. Cost (rough)

| Request | When |
| --- | --- |
| SNS publish | one per doorbell (deliveries to SQS are free) |
| SQS | G receives and deletes per doorbell, about $0.40 per million each |
| GET | one per segment, plus page reads for the diff; G footer reads per bundle |
| PUT | one batch file (plus one archive object) per worker per batch window; checkpoints about once a minute per shard |
| LIST | gap timeouts, epoch changes, cold actors, and the hourly sweep (about $0.50 per 100M source objects) |
| etcd | about one transaction per worker per batch window; a few thousand keys in all |

Requests grow with segments (cheap GETs, G × SQS) and with workers × time
(PUTs), not with actors × wake-ups as in v1.

## 8. Failures

| Failure | What happens | Why it is safe |
| --- | --- | --- |
| **Doorbell late, duplicated, or reordered** | The owner holds, applies, or drops each segment by the rule in §5.2. | Positions come from keys (I2), not arrival order (I6, I8); applying only `pos + 1` keeps one contiguous history (D3, I1). |
| **Doorbell lost, a later one arrives** | The later segment is held; after the gap timeout the owner probes with a GET, then LISTs. | S3 shows every object that exists (I3), so the missing one is found. |
| **Doorbell lost for an actor's newest segment** | Nothing reveals the gap until the sweeper's hint, at most an hour later. | The flat LIST sees every object (I3); a hint at or before `pos` is dropped (D3). |
| **SNS fails to deliver to a queue** | SNS retries (I7). If it never delivers, it's a lost doorbell (the two rows above). | Same as a lost doorbell. |
| **A predecessor isn't in S3 yet** (multi-node fleet: on peer disks, not uploaded) | The segment is held; its own doorbell or the gap probe finds it once uploaded. | Holding never skips a txid (D3). |
| **A bundle spans many groups** | Every group's queue has its own copy; each owner takes its actors and deletes its copy. | Copies are independent (I7, I8); each owner deletes only for its own group (D4). |
| **A group has no owner** | Its messages wait in its queue until a worker acquires it. | Nothing deletes them (D1, D4); retention is 14 days (I8). |
| **Owner dies before committing** | Its lease expires; its owner keys go (I9); another worker acquires the group and recovers its shards. Held messages reappear and are processed again. | Uncommitted rows are referenced by no log entry (D2); recovery restores exactly the committed positions (§5.5); redoing gives the same events (I11). |
| **Owner dies after committing, before deleting messages** | The messages reappear; the new owner drops their segments as duplicates and deletes them. | `pos` after recovery includes the commit (§5.5, D3). |
| **Owner paused past its lease** (GC pause, VM stall) | Another worker owns the group. The paused one wakes, writes a file, and its commit fails. | The owner compare fails once the lease is gone (I9, D1); its file is an orphan (D2); it deletes messages only after a successful commit (D4). |
| **Two workers try to own a group** | Only one create succeeds. | Create-if-absent is atomic (I9, D1). |
| **Batch spans a group that was lost** | The commit fails; the owner retries without that group's shards. | Those row groups are never referenced (D2). |
| **Commit outcome unknown** (timeout) | The owner re-reads `log/<s>/` for its file. | Linearizable reads (I9) settle it; at worst it redoes uncommitted work (I11). |
| **S3 PUT of a batch fails** | No commit is attempted; retried. | The commit follows the PUT (§5.3). |
| **Checkpoint PUT succeeds, its pointer update fails** | An orphan checkpoint; the janitor deletes it. | Recovery uses only the checkpoint `state` names (I9). |
| **Workers with a different N or G** | They refuse to start; a racing commit fails. | Commits compare `config`'s revision (§5.3). |
| **etcd loses a minority** | Nothing visible. | I10. |
| **etcd loses quorum** | Commits stop. Owners stop receiving when their buffers fill; doorbells wait in their queues, and any whose visibility runs out are received again later as duplicates. Progress resumes with quorum. | No committed write is lost (I10); nothing is deleted without a commit (D4); duplicates are dropped (D3). Messages are kept 14 days (I8). |
| **Materializer crashes between its Delta commit and its offset** | The next holder re-reads from the old offset and merges again. | Merges are idempotent (I12). |
| **Two materializers** (an old leader still running) | Both merge; Delta retries its conflicts; the stale one's offset write fails. | I12; offsets compare the election lease (I9). |
| **Consumer far behind etcd's history** | It reads by key range and from manifests, not from the watch. | The watch is only a wake-up (§5.4); entries are deleted only after they're in a manifest (§5.9). |
| **celld drains a bundle before we read it** | Its txids appear in `cells/`, possibly as an L1; the planner applies that, coarser. | I4, I5. The before-state comes from our archive (§4.2). |
| **celld takeover, new epoch, or a fenced owner's late writes** | The epoch-change path applies the chain rule, and rejoins from the old state if the history moved. | I5, as in v1. |
| **Actor state no longer rebuildable** (celld's retention removed its segments while celld-cdc was far behind) | The actor is re-snapshotted (§5.10): its current rows as `r` events plus a marker. The tables reconcile on the marker; the change log has no delete events for the gap, as with a Debezium re-snapshot. The lag alert (§5.11) exists so this doesn't happen. | The snapshot is celld's current history (I5). The marker and its rows arrive together (D5), so the actor's rows older than the marker's `seq` are exactly those the snapshot no longer has. A later event has a greater `seq` and is untouched, and replaying an older batch can't revive a row, since SCD1 only takes newer `seq` (I12). |
| **A worker's cache is lost** | Cold actors are rebuilt from S3. | Caches only save time (I4, D6). |
| **Clock skew** | Lease and timer durations stretch or shrink. | Leases expire on etcd's clock (I9); local timers affect liveness only (D6). |
