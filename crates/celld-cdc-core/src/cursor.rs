//! The per-actor cursor: how far the decoder has processed an actor, and
//! which worker holds the actor's lane.
//!
//! One JSON object per scope, `{cdc}cursors/{scope}.json`, changed only by a
//! conditional write on its ETag. That makes the lane single-writer the
//! same way celld makes a cell single-owner (`own.json`): a worker that lost
//! its lease fails its next write and stops, and anything it wrote before is
//! deterministic, so the next holder rewrites the same bytes.

use crate::bucket::{Bucket, CasOutcome};
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Cursor {
    pub scope: String,
    /// The last processed position, `None` before the first segment.
    pub pos: Option<Position>,
    pub lease: Option<Lease>,
    /// Segments processed so far, for operators.
    #[serde(default)]
    pub segments: u64,
    #[serde(default)]
    pub updated_ms: i64,
}

/// A point in an actor's history: the state after `txid` in `epoch`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Position {
    pub epoch: u64,
    pub txid: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Lease {
    pub holder: String,
    pub expires_ms: i64,
}

/// A cursor this worker holds the lease on, with the version to write over.
#[derive(Debug)]
pub struct Held {
    pub cursor: Cursor,
    e_tag: Option<String>,
}

pub enum Claim {
    Held(Held),
    /// Another worker's lease is live; it will see whatever woke us.
    Busy { holder: String },
}

pub struct CursorStore {
    bucket: Bucket,
    holder: String,
    ttl_ms: i64,
}

pub fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or_default()
}

impl CursorStore {
    pub fn new(bucket: Bucket, holder: impl Into<String>, ttl_ms: i64) -> Self {
        Self {
            bucket,
            holder: holder.into(),
            ttl_ms,
        }
    }

    pub fn key(scope: &str) -> String {
        format!("cursors/{scope}.json")
    }

    pub async fn load(&self, scope: &str) -> Result<Option<Cursor>> {
        let Some(v) = self.bucket.get(&Self::key(scope)).await? else {
            return Ok(None);
        };
        Ok(Some(serde_json::from_slice(&v.bytes).context("parse cursor")?))
    }

    /// Take the lane for `scope`, creating its cursor on first sight.
    pub async fn claim(&self, scope: &str) -> Result<Claim> {
        let key = Self::key(scope);
        let (mut cursor, e_tag) = match self.bucket.get(&key).await? {
            Some(v) => (
                serde_json::from_slice::<Cursor>(&v.bytes).context("parse cursor")?,
                v.e_tag,
            ),
            None => (
                Cursor {
                    scope: scope.to_string(),
                    pos: None,
                    lease: None,
                    segments: 0,
                    updated_ms: 0,
                },
                None,
            ),
        };
        let now = now_ms();
        if let Some(lease) = &cursor.lease {
            if lease.holder != self.holder && lease.expires_ms > now {
                return Ok(Claim::Busy {
                    holder: lease.holder.clone(),
                });
            }
        }
        cursor.lease = Some(Lease {
            holder: self.holder.clone(),
            expires_ms: now + self.ttl_ms,
        });
        let mut held = Held { cursor, e_tag };
        if self.write(&mut held).await? {
            Ok(Claim::Held(held))
        } else {
            let holder = self
                .load(scope)
                .await?
                .and_then(|c| c.lease)
                .map(|l| l.holder)
                .unwrap_or_default();
            Ok(Claim::Busy { holder })
        }
    }

    /// Move the cursor to `pos` and renew the lease. `false` means the lease
    /// was lost: stop working on this lane.
    pub async fn advance(&self, held: &mut Held, pos: Position) -> Result<bool> {
        held.cursor.pos = Some(pos);
        held.cursor.segments += 1;
        held.cursor.lease = Some(Lease {
            holder: self.holder.clone(),
            expires_ms: now_ms() + self.ttl_ms,
        });
        self.write(held).await
    }

    /// Give the lane up so the next doorbell need not wait for the TTL.
    pub async fn release(&self, mut held: Held) -> Result<()> {
        held.cursor.lease = None;
        self.write(&mut held).await?;
        Ok(())
    }

    async fn write(&self, held: &mut Held) -> Result<bool> {
        held.cursor.updated_ms = now_ms();
        let body = serde_json::to_vec(&held.cursor)?;
        let key = Self::key(&held.cursor.scope);
        match self
            .bucket
            .put_if(&key, body.into(), held.e_tag.as_deref())
            .await?
        {
            CasOutcome::Written { e_tag } => {
                held.e_tag = e_tag;
                Ok(true)
            }
            CasOutcome::Conflict => Ok(false),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn one_holder_at_a_time() {
        let bucket = Bucket::memory();
        let a = CursorStore::new(bucket.clone(), "a", 30_000);
        let b = CursorStore::new(bucket.clone(), "b", 30_000);
        let Claim::Held(mut held) = a.claim("C:1").await.unwrap() else {
            panic!("a should claim a new cursor");
        };
        assert!(matches!(b.claim("C:1").await.unwrap(), Claim::Busy { holder } if holder == "a"));
        let pos = Position { epoch: 1, txid: 4 };
        assert!(a.advance(&mut held, pos).await.unwrap());
        a.release(held).await.unwrap();
        let Claim::Held(held) = b.claim("C:1").await.unwrap() else {
            panic!("b should claim a released cursor");
        };
        assert_eq!(held.cursor.pos, Some(pos));
    }

    #[tokio::test]
    async fn an_expired_lease_is_taken_and_the_old_holder_is_fenced() {
        let bucket = Bucket::memory();
        let a = CursorStore::new(bucket.clone(), "a", -1);
        let b = CursorStore::new(bucket.clone(), "b", 30_000);
        let Claim::Held(mut stale) = a.claim("C:1").await.unwrap() else {
            panic!("a should claim");
        };
        assert!(matches!(b.claim("C:1").await.unwrap(), Claim::Held(_)));
        let pos = Position { epoch: 1, txid: 1 };
        assert!(!a.advance(&mut stale, pos).await.unwrap());
    }

    #[tokio::test]
    async fn concurrent_first_claims_have_one_winner() {
        let bucket = Bucket::memory();
        let a = CursorStore::new(bucket.clone(), "a", 30_000);
        let b = CursorStore::new(bucket.clone(), "b", 30_000);
        let (ra, rb) = tokio::join!(a.claim("C:2"), b.claim("C:2"));
        let held = [ra.unwrap(), rb.unwrap()]
            .iter()
            .filter(|c| matches!(c, Claim::Held(_)))
            .count();
        assert_eq!(held, 1);
    }
}
