//! One actor's lane: take its lease, apply every contiguous segment after
//! its cursor, and emit each segment's row changes.
//!
//! A doorbell names an actor, never a segment. Whatever woke the lane, it
//! lists the actor's segments and walks them in txid order, so duplicate,
//! late, reordered, or lost notifications change only when work happens,
//! never what it produces.

use crate::catalog::Catalog;
use crate::cursor::{Claim, CursorStore, Held, Position};
use crate::diff::{self, Kind, RowChange};
use crate::event::{class_of, event_id, seq, ChangeEvent, Op};
use crate::image::DbImage;
use crate::paged::PagedDb;
use crate::pagediff::page_diff;
use crate::segment::{Chain, Segment, SNAPSHOT_LEVEL};
use anyhow::{Context, Result};
use async_trait::async_trait;
use bytes::Bytes;
use std::sync::Arc;
use tracing::{info, warn};

/// Where a lane's events go. A write must be idempotent: a lane that loses
/// its lease after writing is redone by the next holder, with identical
/// events for the same segment.
#[async_trait]
pub trait EventSink: Send + Sync {
    async fn write(&self, step: &Step, events: &[ChangeEvent]) -> Result<()>;
}

/// One applied step: a segment, or a resync jump onto a new chain.
#[derive(Clone, Debug)]
pub struct Step {
    pub scope: String,
    pub epoch: u64,
    pub min_txid: u64,
    pub max_txid: u64,
    pub commit_ts_ms: i64,
    pub source_key: String,
}

#[derive(Debug, PartialEq, Eq)]
pub enum Outcome {
    /// Applied `applied` steps and reached the newest segment.
    CaughtUp { pos: Option<Position>, applied: u64 },
    /// Stopped before a missing txid; try again shortly.
    Gap {
        pos: Option<Position>,
        applied: u64,
        want: u64,
    },
    /// Another worker holds the lane.
    Busy,
    /// The lease was taken over mid-run; the new holder continues.
    LeaseLost { applied: u64 },
}

/// How a lane finds row changes.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum DiffMode {
    /// Page the database in from the bucket and diff only the rows on the
    /// pages each segment changed ([`crate::pagediff`]).
    #[default]
    Paged,
    /// Download the whole database and compare every row of every table.
    /// Slow on large actors; the oracle the paged diff is tested against.
    Full,
}

pub struct Decoder {
    pub catalog: Arc<Catalog>,
    pub cursors: CursorStore,
    pub sink: Arc<dyn EventSink>,
    pub mode: DiffMode,
}

impl Decoder {
    pub async fn run_lane(&self, scope: &str) -> Result<Outcome> {
        let mut held = match self.cursors.claim(scope).await? {
            Claim::Held(h) => h,
            Claim::Busy { .. } => return Ok(Outcome::Busy),
        };
        let result = self.drain(scope, &mut held).await;
        match &result {
            Ok(Outcome::LeaseLost { .. }) => {}
            _ => {
                if let Err(e) = self.cursors.release(held).await {
                    warn!(scope, error = %e, "release lane");
                }
            }
        }
        result
    }

    async fn drain(&self, scope: &str, held: &mut Held) -> Result<Outcome> {
        let segments = self.catalog.segments(scope).await?;
        let chain = Chain::build(&segments);
        let mut pos = held.cursor.pos;
        let mut applied = 0;
        if chain.is_empty() {
            return Ok(Outcome::CaughtUp { pos, applied });
        }
        let mut state: Option<State> = None;

        let newest_epoch = chain.spans.last().map_or(0, |s| s.epoch);
        if let Some(p) = pos.filter(|p| p.epoch > newest_epoch) {
            // Our position's epoch is not listed yet (a bundle drained but
            // not folded in); wait rather than mistake it for a fork.
            return Ok(Outcome::Gap { pos, applied, want: p.txid + 1 });
        }
        if let Some(p) = pos.filter(|p| !chain.contains(*p)) {
            // The actor's history moved off our position: a new non-paged
            // epoch, or a cut that drops a fenced owner's writes we saw.
            // Diff our old state against the first state of the new chain.
            let fork = chain
                .spans
                .windows(2)
                .find(|w| w[0].epoch == p.epoch)
                .map_or(1, |w| w[1].lo);
            let Some(target) = chain.rejoin_at(fork) else {
                return Ok(Outcome::Gap { pos, applied, want: fork });
            };
            let old: Vec<Segment> = segments.iter().filter(|s| s.epoch <= p.epoch).cloned().collect();
            let before = match Chain::build(&old).plan_to(p.txid) {
                Some(plan) => self.state(plan).await?,
                None => {
                    warn!(scope, ?p, "cursor state is gone; resyncing from empty");
                    self.state(Vec::new()).await?
                }
            };
            let plan = chain.plan_to(target.txid).context("rejoin plan")?;
            let last = plan.last().context("empty rejoin plan")?.clone();
            let header = self.header(&last).await?;
            let after = self.state(plan.clone()).await?;
            let step = Step {
                scope: scope.to_string(),
                epoch: target.epoch,
                min_txid: plan.first().map_or(1, |s| s.min),
                max_txid: target.txid,
                commit_ts_ms: header.timestamp,
                source_key: last.source_key().to_string(),
            };
            info!(scope, from = ?p, to = ?target, "lane rejoined a new chain");
            let after = self.emit(&step, before, after, None, false).await?;
            if !self.cursors.advance(held, target).await? {
                return Ok(Outcome::LeaseLost { applied });
            }
            pos = Some(target);
            state = Some(after);
            applied += 1;
        }

        loop {
            let t = pos.map_or(0, |p| p.txid);
            let Some(seg) = chain.next_after(t).cloned() else {
                break;
            };
            let before = match state.take() {
                Some(s) => s,
                None => match chain.plan_to(t) {
                    Some(plan) => self.state(plan).await?,
                    None => {
                        warn!(scope, t, "cursor state is not rebuildable; resyncing from empty");
                        self.state(Vec::new()).await?
                    }
                },
            };
            let bytes = match self.catalog.fetch(&seg).await {
                Ok(b) => b,
                // A bundle drained between the listing and the read; its
                // segment reaches `cells/` and the next run finds it there.
                Err(e) => {
                    warn!(scope, key = seg.source_key(), error = %e, "segment fetch failed");
                    return Ok(Outcome::Gap { pos, applied, want: t + 1 });
                }
            };
            let decoded = celld_ltx::ltx::decode_file(&bytes).context("decode segment")?;
            let after = self.apply(&before, &seg, &bytes).await?;
            let step = Step {
                scope: scope.to_string(),
                epoch: seg.epoch,
                min_txid: seg.min,
                max_txid: seg.max,
                commit_ts_ms: decoded.header.timestamp,
                source_key: seg.source_key().to_string(),
            };
            // With no cursor, a snapshot we start from is not the actor's
            // birth unless it is the first epoch's own first txid.
            let snapshot_read =
                pos.is_none() && seg.is_snapshot() && (seg.level == SNAPSHOT_LEVEL || seg.epoch > 1);
            let after = self.emit(&step, before, after, Some(decoded.pgnos), snapshot_read).await?;
            self.catalog.archive(scope, &seg, &bytes).await?;
            if !self.cursors.advance(held, seg.end()).await? {
                return Ok(Outcome::LeaseLost { applied });
            }
            pos = Some(seg.end());
            state = Some(after);
            applied += 1;
        }

        let t = pos.map_or(0, |p| p.txid);
        if chain.max_txid() > t {
            return Ok(Outcome::Gap { pos, applied, want: t + 1 });
        }
        Ok(Outcome::CaughtUp { pos, applied })
    }

    /// The database after `plan`.
    async fn state(&self, plan: Vec<Segment>) -> Result<State> {
        Ok(match self.mode {
            DiffMode::Paged => State::Paged {
                db: PagedDb::open(self.catalog.clone(), &plan, None).await?,
                plan,
            },
            DiffMode::Full => {
                let mut image = DbImage::empty()?;
                for seg in &plan {
                    image.apply(&self.catalog.fetch(seg).await?)?;
                }
                State::Image(image)
            }
        })
    }

    /// The database after `before` and then `seg`, leaving `before` as is.
    async fn apply(&self, before: &State, seg: &Segment, bytes: &Bytes) -> Result<State> {
        Ok(match before {
            State::Paged { plan, .. } => {
                let db = PagedDb::open(self.catalog.clone(), plan, Some((seg.clone(), bytes.clone()))).await?;
                let mut plan = plan.clone();
                plan.push(seg.clone());
                State::Paged { plan, db }
            }
            State::Image(image) => {
                let mut after = image.fork()?;
                after.apply(bytes)?;
                State::Image(after)
            }
        })
    }

    async fn header(&self, seg: &Segment) -> Result<celld_ltx::ltx::Header> {
        let bytes = self.catalog.fetch_range(seg, 0, celld_ltx::ltx::HEADER_SIZE as u64).await?;
        celld_ltx::ltx::Header::parse(&bytes).context("parse ltx header")
    }

    /// Diff `before` against `after`, write the events, and hand `after`
    /// back as the next step's `before`. `changed` is the segment's pages;
    /// `None` compares whole tables.
    async fn emit(
        &self,
        step: &Step,
        before: State,
        after: State,
        changed: Option<Vec<u32>>,
        snapshot_read: bool,
    ) -> Result<State> {
        let scope = step.scope.clone();
        // SQLite runs on a blocking thread: a paged page fault blocks.
        let (after, changes, name) = tokio::task::spawn_blocking(move || -> Result<_> {
            let (changes, name) = match (&before, &after) {
                (State::Paged { db: b, .. }, State::Paged { db: a, .. }) => {
                    let changes = match &changed {
                        Some(pages) => page_diff(b, a, pages)?,
                        None => {
                            b.scan(true);
                            a.scan(true);
                            diff::full_diff(b.conn(), a.conn())?
                        }
                    };
                    let name = diff::actor_name(a.conn(), &scope)?.or(diff::actor_name(b.conn(), &scope)?);
                    (changes, name)
                }
                (State::Image(b), State::Image(a)) => {
                    let (b, a) = (diff::open(b.path())?, diff::open(a.path())?);
                    let name = diff::actor_name(&a, &scope)?.or(diff::actor_name(&b, &scope)?);
                    (diff::full_diff(&b, &a)?, name)
                }
                _ => anyhow::bail!("diff of mixed states"),
            };
            drop(before);
            Ok((after, changes, name))
        })
        .await??;
        let events: Vec<ChangeEvent> = changes
            .into_iter()
            .map(|c| to_event(step, name.clone(), c, snapshot_read))
            .collect();
        self.sink.write(step, &events).await?;
        Ok(after)
    }
}

/// A database state a lane diffs against.
enum State {
    Paged { plan: Vec<Segment>, db: PagedDb },
    Image(DbImage),
}

fn to_event(step: &Step, actor_name: Option<String>, c: RowChange, snapshot_read: bool) -> ChangeEvent {
    let op = match c.kind {
        Kind::Insert if snapshot_read => Op::Read,
        Kind::Insert => Op::Create,
        Kind::Update => Op::Update,
        Kind::Delete => Op::Delete,
    };
    ChangeEvent {
        event_id: event_id(&step.scope, step.epoch, step.max_txid, &c.table, &c.pk),
        class: class_of(&step.scope).to_string(),
        scope: step.scope.clone(),
        actor_name,
        table: c.table,
        op,
        epoch: step.epoch,
        min_txid: step.min_txid,
        txid: step.max_txid,
        seq: seq(step.epoch, step.max_txid),
        commit_ts_ms: step.commit_ts_ms,
        source_key: step.source_key.clone(),
        pk: c.pk,
        before: c.before,
        after: c.after,
    }
}
