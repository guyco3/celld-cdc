//! Change-log object keys.
//!
//! `{prefix}changes/class={class}/dt=YYYY-MM-DD/hh=HH/{scope}-e{epoch}-{min:016x}-{max:016x}.parquet`
//!
//! `dt`/`hh` come from the segment's commit timestamp (UTC), so a replay
//! writes the same key. `epoch` is decimal; the txid range is fixed-width
//! hex. The scope keeps its `:`; [`parse_change_log_key`] splits the file
//! name from the right, so a scope may contain `-` too.

use anyhow::{bail, Context, Result};

/// The parts of a change-log key.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChangeLogKey {
    /// Everything before `changes/`, as passed to [`change_log_key`].
    pub prefix: String,
    pub class: String,
    /// `YYYY-MM-DD`.
    pub dt: String,
    /// `HH`.
    pub hh: String,
    pub scope: String,
    pub epoch: u64,
    pub min_txid: u64,
    pub max_txid: u64,
}

/// The UTC `(YYYY-MM-DD, HH)` of a Unix-millisecond timestamp.
pub fn dt_hh(commit_ts_ms: i64) -> (String, String) {
    let secs = commit_ts_ms.div_euclid(1000);
    let days = secs.div_euclid(86_400);
    let hour = secs.rem_euclid(86_400) / 3600;
    // Howard Hinnant's civil_from_days.
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = yoe + era * 400 + i64::from(m <= 2);
    (format!("{y:04}-{m:02}-{d:02}"), format!("{hour:02}"))
}

/// The key of the change-log file for one segment of one actor.
pub fn change_log_key(
    prefix: &str,
    class: &str,
    commit_ts_ms: i64,
    scope: &str,
    epoch: u64,
    min_txid: u64,
    max_txid: u64,
) -> String {
    let (dt, hh) = dt_hh(commit_ts_ms);
    format!(
        "{prefix}changes/class={class}/dt={dt}/hh={hh}/{scope}-e{epoch}-{min_txid:016x}-{max_txid:016x}.parquet"
    )
}

/// Parses a key made by [`change_log_key`].
pub fn parse_change_log_key(key: &str) -> Result<ChangeLogKey> {
    let (prefix, rest) = key.rsplit_once("changes/class=").context("no changes/class=")?;
    let mut parts = rest.splitn(4, '/');
    let class = parts.next().context("no class")?;
    let dt = parts.next().and_then(|p| p.strip_prefix("dt=")).context("no dt=")?;
    let hh = parts.next().and_then(|p| p.strip_prefix("hh=")).context("no hh=")?;
    let file = parts.next().context("no file name")?;
    if file.contains('/') {
        bail!("unexpected directory in {key}");
    }
    let stem = file.strip_suffix(".parquet").context("not .parquet")?;
    let mut it = stem.rsplitn(4, '-');
    let max = it.next().context("no max txid")?;
    let min = it.next().context("no min txid")?;
    let epoch = it.next().and_then(|e| e.strip_prefix('e')).context("no epoch")?;
    let scope = it.next().filter(|s| !s.is_empty()).context("no scope")?;
    let hex = |s: &str, what: &str| -> Result<u64> {
        if s.len() != 16 {
            bail!("{what} {s:?} is not 16 hex digits");
        }
        u64::from_str_radix(s, 16).with_context(|| format!("bad {what} {s:?}"))
    };
    Ok(ChangeLogKey {
        prefix: prefix.to_string(),
        class: class.to_string(),
        dt: dt.to_string(),
        hh: hh.to_string(),
        scope: scope.to_string(),
        epoch: epoch.parse().with_context(|| format!("bad epoch {epoch:?}"))?,
        min_txid: hex(min, "min txid")?,
        max_txid: hex(max, "max txid")?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dates_are_utc() {
        assert_eq!(dt_hh(0), ("1970-01-01".into(), "00".into()));
        // 2024-02-29T23:59:59.999Z
        assert_eq!(dt_hh(1_709_251_199_999), ("2024-02-29".into(), "23".into()));
        assert_eq!(dt_hh(1_709_251_200_000), ("2024-03-01".into(), "00".into()));
        assert_eq!(dt_hh(-1), ("1969-12-31".into(), "23".into()));
    }

    #[test]
    fn key_round_trips() {
        let scope = format!("Counter:{}", "ab".repeat(32));
        let key = change_log_key("celld-cdc/", "Counter", 1_700_000_000_000, &scope, 3, 0x10, 0x2f);
        assert_eq!(
            key,
            format!(
                "celld-cdc/changes/class=Counter/dt=2023-11-14/hh=22/{scope}-e3-0000000000000010-000000000000002f.parquet"
            )
        );
        let k = parse_change_log_key(&key).unwrap();
        assert_eq!(
            k,
            ChangeLogKey {
                prefix: "celld-cdc/".into(),
                class: "Counter".into(),
                dt: "2023-11-14".into(),
                hh: "22".into(),
                scope: scope.clone(),
                epoch: 3,
                min_txid: 0x10,
                max_txid: 0x2f,
            }
        );
        // A scope with dashes and an empty prefix.
        let key = change_log_key("", "A", 0, "A:x-y", u64::MAX, 0, u64::MAX);
        let k = parse_change_log_key(&key).unwrap();
        assert_eq!((k.prefix.as_str(), k.scope.as_str(), k.epoch, k.max_txid), ("", "A:x-y", u64::MAX, u64::MAX));
    }

    #[test]
    fn rejects_garbage() {
        assert!(parse_change_log_key("changes/class=A/dt=x/hh=y/s-e1-1-2.parquet").is_err());
        assert!(parse_change_log_key("foo.parquet").is_err());
    }
}
