# celld-cdc

Change data capture for [celld](https://github.com/denoland/celld) actors
(self-hosted Durable Objects): every row your actors write, in one place,
as a Parquet change log and as **SCD Type 1** (latest state) and
**SCD Type 2** (history) Delta Lake tables.

```
 Durable Objects on celld ─► S3 (.ltx/.ltxb) ─► SQS ─► decoder ─► changes/*.parquet
                                                                     │
                                       Delta scd1 + scd2 ◄─ materializer ◄─ SQS
```

- **No application changes.** celld already writes every committed SQLite
  change to your bucket; celld-cdc reads it.
- **Stateless workers.** Cursors, leases, and outputs live in object
  storage. Run as many decoders as you like; kill any of them at any time.
- **Exactly-once results from at-least-once delivery.** Notifications can
  arrive late, twice, or not at all; output is the same.
- **Both storage APIs.** `ctx.storage.sql` tables are captured by primary
  key; `ctx.storage.put/get` values are decoded from V8 serialization into
  JSON.
- **Open formats.** Parquet change log, Delta tables readable by DuckDB,
  Spark, Trino, Databricks, Snowflake, ClickHouse, Polars.

Read [docs/architecture.md](docs/architecture.md) for how it works and what
it guarantees.

## Quickstart: run the end-to-end test

The end-to-end test runs the whole pipeline on your machine:
[`examples/counter`](examples/counter) actors on a two-node celld fleet,
LocalStack for S3 and SQS, two decoders, a materializer, and a sweep. It
sends traffic, injects failures, and checks after each step that the SCD
tables match what the app itself reports.

### 1. Prerequisites

- **Docker** (Docker Desktop on macOS), running. No Rust toolchain is
  needed on the host; everything compiles in containers.
- **About 15 GB of free disk** for the first build (celld, its V8 build,
  and the Delta Lake stack). Check with `df -h`.
- **A celld checkout next to this repository**, or `CELLD_SRC` pointing at
  one. celld images are built from source:

  ```
  some-dir/
  ├── celld/        git clone https://github.com/denoland/celld (tag v0.6.3)
  └── celld-cdc/    this repository
  ```

- `curl` and `python3` on the host (the test uses both). `duckdb` is
  optional, for the SQL queries in step 4.

### 2. Build the images (once)

```sh
cd celld-cdc
scripts/build-images.sh
```

The first run takes a while (celld compiles from source); later runs reuse
the build caches. When it finishes, three images exist:

```sh
docker image ls | grep celld-cdc
# celld-cdc/celld-cdc    dev      ...   the celld-cdc workers
# celld-cdc/celld        v0.6.3   ...   celld + esbuild (nodes and `celld deploy`)
# celld-cdc/celld-build  v0.6.3   ...   celld compiled from ../celld
```

### 3. Run the test

```sh
scripts/e2e.sh
echo "exit code: $?"
```

It takes a few minutes. It starts the stack with
`docker compose -f deploy/docker-compose.yml up -d` (tearing down any
previous run first), deploys the counter app, and then runs four phases:

| Phase | What happens | What it proves |
| --- | --- | --- |
| 1 | KV increments and SQL inserts on both celld nodes, plus a `reset` that deletes carol's rows | inserts, updates, and deletes reach both tables, through per-cell files and multi-node bundles |
| 2 | the doorbell queue is purged while traffic runs | lost notifications are recovered by the sweep |
| 3 | `decoder-1` is killed while traffic runs | a dead worker's leases expire and the other decoder carries on |
| 4 | `celld-a` is killed; its actors move to `celld-b` in a new epoch | history continues across a takeover with no gap |

After each phase the script reads every actor's state from the app
(`GET /state`) and polls the tables until they match, for up to four
minutes (five for the takeover phase). **A passing run looks like this** (row counts are from a real run;
yours can differ slightly):

```
== start the stack
== phase 1: traffic on both nodes
ok: 4 actors, 28 scd1 rows, 107 scd2 versions
== phase 2: purge the doorbell queue mid-traffic
ok: 4 actors, 34 scd1 rows, 137 scd2 versions
== phase 3: kill decoder-1 mid-traffic
ok: 4 actors, 40 scd1 rows, 167 scd2 versions
== phase 4: kill celld-a; its cells move to celld-b
ok: 4 actors, 46 scd1 rows, 197 scd2 versions
== change log summary
   alice   _cf_KV c  1
   alice   _cf_KV u  18
     bob   _cf_KV c  2
     bob   _cf_KV u  55
     bob   clicks c  19
   carol   _cf_KV c  3
   carol   _cf_KV d  2
   carol   _cf_KV u  19
   carol   clicks c  1
   carol   clicks d  1
    dave   _cf_KV c  2
    dave   _cf_KV u  55
    dave   clicks c  19
== all phases passed
exit code: 0
```

What to look for:

- **`ok:` after every phase, `== all phases passed` at the end, and exit
  code 0.** Each `ok:` line means all of these held:
  - every actor's KV counter `n` in **scd1** equals the app's value (or is
    a tombstone with `is_deleted = true` after a reset);
  - every actor's live `clicks` rows in scd1 are exactly the app's rows;
  - in **scd2**, every key has exactly one `is_current` version, the last
    one; each version's `valid_to_seq` is the next version's
    `valid_from_seq`; and `n` only grows between resets;
  - scd1 and scd2 cover the same keys.
- **The change log summary** counts events by actor, table, and op (`c`
  create, `u` update, `d` delete). Carol's `d` rows are her reset; the
  `clicks` creates are the SQL inserts; everything else is the KV counter.
- **A failure** prints `did not converge:` followed by the mismatches, for
  example `alice: scd1 n=12, app says 14`, then the last 40 log lines of
  the decoders, materializer, and sweep. The script then exits non-zero.

The stack stays up after the test, so you can look around.

### 4. Look at the results yourself

Make some changes, then read the tables back (wait a few seconds for the
pipeline):

```sh
curl 'localhost:8080/?name=alice'                       # n += 1        (KV, node a)
curl 'localhost:8082/?name=alice'                       # n += 1        (KV, node b)
curl -XPOST 'localhost:8080/click?name=alice&delta=5'   # inserts a row (SQL)
curl 'localhost:8080/state?name=alice'                  # what the app holds
curl -XPOST 'localhost:8080/reset?name=alice'           # deletes both

C="docker compose -f deploy/docker-compose.yml run --rm --no-deps -T materializer"
$C dump scd1      # latest state: one JSON line per row
$C dump scd2      # every version of every row
$C dump changes   # the raw change log
```

A scd1 line for alice's counter looks like this; `row.v` is the counter:

```json
{"scope":"Counter:1182fb…","class":"Counter","actor_name":"alice","table_name":"_cf_KV",
 "pk":"{\"k\":\"n\"}","row":"{\"k\":\"n\",\"v\":19}","is_deleted":false,"op":"u",
 "epoch":2,"txid":3,"seq":"e0000000000000002-t0000000000000003", …}
```

Alice's counter history, oldest first. In the test run, the epoch in `seq`
moves from 1 to 2 where `celld-a` was killed:

```sh
$C dump scd2 | python3 -c '
import json, sys
rows = [r for r in map(json.loads, sys.stdin)
        if r["actor_name"] == "alice" and r["table_name"] == "_cf_KV" and json.loads(r["pk"]) == {"k": "n"}]
for r in sorted(rows, key=lambda r: r["valid_from_seq"]):
    print(r["valid_from_seq"], json.loads(r["row"])["v"], "<- current" if r["is_current"] else "")
'
# …
# e0000000000000001-t0000000000000010 16
# e0000000000000002-t0000000000000001 17
# e0000000000000002-t0000000000000002 18
# e0000000000000002-t0000000000000003 19 <- current
```

The same tables from DuckDB (`duckdb`, then paste):

```sql
INSTALL delta; LOAD delta; INSTALL httpfs; LOAD httpfs;
CREATE SECRET (TYPE s3, KEY_ID 'test', SECRET 'test', REGION 'us-east-1',
               ENDPOINT 'localhost:4566', URL_STYLE 'path', USE_SSL false);

-- SCD1: current counter values; these match `curl localhost:8080/state?name=…`
SELECT actor_name, json_extract(row, '$.v') AS n
FROM delta_scan('s3://celld-cdc/tables/scd1')
WHERE table_name = '_cf_KV' AND json_extract_string(pk, '$.k') = 'n' AND NOT is_deleted
ORDER BY actor_name;

-- SCD2: alice's last three versions
SELECT valid_from_seq, valid_to_seq, json_extract(row, '$.v') AS n, is_current
FROM delta_scan('s3://celld-cdc/tables/scd2')
WHERE actor_name = 'alice' AND table_name = '_cf_KV' AND json_extract_string(pk, '$.k') = 'n'
ORDER BY valid_from_seq DESC LIMIT 3;

-- live SQL rows per actor
SELECT actor_name, count(*) AS clicks
FROM delta_scan('s3://celld-cdc/tables/scd1')
WHERE table_name = 'clicks' AND NOT is_deleted
GROUP BY ALL ORDER BY 1;

-- the raw change log
SELECT op, count(*) FROM read_parquet('s3://celld-cdc/changes/**/*.parquet') GROUP BY ALL ORDER BY 1;
```

Worker logs show the pipeline at work (`lane caught up`, `applied change
files`):

```sh
docker compose -f deploy/docker-compose.yml logs -f decoder-1 decoder-2 materializer
```

### 5. Clean up

```sh
docker compose -f deploy/docker-compose.yml down -v   # stop and delete all state
scripts/e2e.sh --down                                 # or: run the test, then tear down
```

### Troubleshooting

- **`port is already allocated`**: something else is on 4566, 8080, or
  8082. Stop it, or change the host ports in `deploy/docker-compose.yml`.
- **`no space left on device`**, or Docker hangs: the first build needs
  about 15 GB. Free space, run `docker builder prune`, and restart Docker.
- **`celld-deploy` exits with `bucket unavailable or inaccessible`**: rare
  on a cold start; run `scripts/e2e.sh` again.
- **A phase times out**: run
  `docker compose -f deploy/docker-compose.yml logs decoder-1 materializer`
  and look for `lane failed` or `apply failed`.

## Running it against your fleet

1. Create a bucket for celld-cdc (or use a prefix of another) and two SQS
   standard queues, `cdc-doorbell` and `cdc-changes`.
2. Add S3 event notifications (`s3:ObjectCreated:*`):
   - celld's bucket, prefix `cells/` suffix `.ltx`, to `cdc-doorbell`;
   - celld's bucket, prefix `log/` suffix `.ltxb`, to `cdc-doorbell`;
   - celld-cdc's bucket, prefix `changes/` suffix `.parquet`, to `cdc-changes`.

   [`deploy/localstack/ready.d/10-cdc.sh`](deploy/localstack/ready.d/10-cdc.sh)
   does exactly this with the AWS CLI.
3. Run the workers, with read access to celld's bucket and read-write to
   celld-cdc's:

   ```sh
   export CELLD_BUCKET=s3://my-cells CDC_BUCKET=s3://my-cdc
   export CDC_DOORBELL_QUEUE_URL=... CDC_CHANGES_QUEUE_URL=...
   celld-cdc decode        # as many as you like
   celld-cdc materialize   # one per deployment
   celld-cdc sweep         # one; catches lost notifications every 5 min
   ```

   `S3_ENDPOINT` (and `AWS_ENDPOINT_URL` for SQS) select an S3-compatible
   store. `CDC_DIFF=full` replaces the page-level diff with whole-table
   comparison (slow on large actors; a fallback, and the test oracle).
   `celld-cdc --help` lists every option.

`celld-cdc events 'Counter:<hex>'` prints one actor's full change history
from the bucket as JSON lines without touching any state: handy to see what
celld-cdc would capture before you deploy it.

## Development

The Rust workspace builds in Docker with celld's toolchain, so the host
needs no compiler:

```sh
scripts/cargo.sh test
scripts/cargo.sh clippy --all-targets -- -D warnings
```

| Crate | |
| --- | --- |
| `celld-cdc-core` | keys, cursor/lease store, segment chain and planner, paged databases over the bucket, the page-level row diff and its full-table oracle, V8 values, the lane |
| `celld-cdc-parquet` | the change-log schema and Parquet codec |
| `celld-cdc-materialize` | SCD1/SCD2 Delta tables |
| `celld-cdc` | the worker binary |

`celld-cdc-core` reads LTX with celld's own `celld-ltx` crate, pinned to a
celld release tag.

## License

Apache-2.0
