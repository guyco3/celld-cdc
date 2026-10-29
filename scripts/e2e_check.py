#!/usr/bin/env python3
"""Check the SCD tables against what the counter app reports.

usage: e2e_check.py EXPECTED.json SCD1.jsonl SCD2.jsonl

EXPECTED.json maps an actor name to its `GET /state` response:
{"alice": {"n": 10, "clicks": [{"id": 1, "ts": ..., "delta": 1, "note": null}]}}

Exit 0 when every check passes, 1 with a report otherwise. The e2e script
polls this until it passes, since the pipeline is asynchronous.
"""
import collections
import json
import sys


def load_jsonl(path):
    with open(path) as f:
        return [json.loads(line) for line in f if line.strip()]


def main():
    expected = json.load(open(sys.argv[1]))
    scd1 = load_jsonl(sys.argv[2])
    scd2 = load_jsonl(sys.argv[3])
    errors = []

    by_actor = collections.defaultdict(list)
    for r in scd1:
        by_actor[r["actor_name"]].append(r)

    for name, state in expected.items():
        rows = by_actor.get(name, [])
        kv = {json.loads(r["pk"])["k"]: r for r in rows if r["table_name"] == "_cf_KV"}
        n = kv.get("n")
        if state["n"] == 0:
            if n is not None and not n["is_deleted"]:
                errors.append(f"{name}: n should be deleted, scd1 has {n['row']}")
        elif n is None or n["is_deleted"]:
            errors.append(f"{name}: scd1 has no live n, app says {state['n']}")
        elif json.loads(n["row"])["v"] != state["n"]:
            errors.append(f"{name}: scd1 n={json.loads(n['row'])['v']}, app says {state['n']}")

        live = {
            json.loads(r["pk"])["id"]: json.loads(r["row"])
            for r in rows
            if r["table_name"] == "clicks" and not r["is_deleted"]
        }
        want = {c["id"]: c for c in state["clicks"]}
        if live != want:
            errors.append(f"{name}: scd1 clicks {sorted(live)} != app clicks {sorted(want)}")

    versions = collections.defaultdict(list)
    for r in scd2:
        versions[(r["scope"], r["table_name"], r["pk"])].append(r)
    for key, vs in versions.items():
        vs.sort(key=lambda r: r["valid_from_seq"])
        current = [v for v in vs if v["is_current"]]
        if len(current) != 1 or current[0] is not vs[-1]:
            errors.append(f"{key}: {len(current)} current versions, want exactly the last")
        for a, b in zip(vs, vs[1:]):
            if a["valid_to_seq"] != b["valid_from_seq"]:
                errors.append(f"{key}: version {a['valid_from_seq']} ends at {a['valid_to_seq']}, next starts {b['valid_from_seq']}")
        if vs[-1]["valid_to_seq"] is not None:
            errors.append(f"{key}: the last version is closed")
        if key[1] == "_cf_KV" and json.loads(key[2]) == {"k": "n"}:
            ns = [json.loads(v["row"])["v"] for v in vs if not v["is_deleted"]]
            # Between resets, n only grows.
            runs, run = [], []
            for v in vs:
                if v["is_deleted"]:
                    runs.append(run)
                    run = []
                else:
                    run.append(json.loads(v["row"])["v"])
            runs.append(run)
            for run in runs:
                if run != sorted(set(run)):
                    errors.append(f"{key}: n history {ns} is not increasing")

    scd1_keys = {(r["scope"], r["table_name"], r["pk"]) for r in scd1}
    if scd1_keys != set(versions):
        errors.append(f"scd1 has {len(scd1_keys)} keys, scd2 has {len(versions)}")

    if errors:
        print("\n".join(errors))
        sys.exit(1)
    n_versions = sum(len(v) for v in versions.values())
    print(f"ok: {len(expected)} actors, {len(scd1)} scd1 rows, {n_versions} scd2 versions")


if __name__ == "__main__":
    main()
