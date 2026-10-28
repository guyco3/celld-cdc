//! An actor's segments and the order to apply them in.
//!
//! A segment is one LTX file's worth of pages covering `min..=max` txids of
//! one epoch. Segments come from three places: celld's per-cell objects
//! (every level), live node-log bundles (single-txid L0s that have not
//! reached `cells/` yet), and celld-cdc's own archive of bundle segments it
//! has applied, kept because a drained bundle is deleted and its txids may
//! then exist in `cells/` only inside a merged L1.
//!
//! The actor's single writer makes this a line, not a graph: within a
//! chain, the txids form one sequence, and S3 LIST order plus the txids in
//! every key reconstruct it no matter what order notifications arrive in.
//! The chain rule is celld's (`celld_ltx::client::epochs::EpochChain`):
//! walk epochs down from the newest; an epoch that does not open with a
//! snapshot continues the one below it, which must end exactly at the cut.

use crate::cursor::Position;
use std::collections::{BTreeMap, HashMap, VecDeque};

pub const SNAPSHOT_LEVEL: u32 = 9;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum Source {
    /// A per-cell object in the celld bucket.
    Cell { key: String },
    /// One row of a node-log bundle in the celld bucket.
    Bundle { key: String, offset: u64, len: u64 },
    /// celld-cdc's archived copy of a bundle row, in the cdc bucket.
    Archive { key: String },
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Segment {
    pub epoch: u64,
    pub level: u32,
    pub min: u64,
    pub max: u64,
    /// The LTX file's size in bytes.
    pub size: u64,
    pub source: Source,
}

impl Segment {
    pub fn is_snapshot(&self) -> bool {
        self.min == 1
    }

    /// Where this segment leaves the actor.
    pub fn end(&self) -> Position {
        Position {
            epoch: self.epoch,
            txid: self.max,
        }
    }

    /// The bucket key a change file or log line can name.
    pub fn source_key(&self) -> &str {
        match &self.source {
            Source::Cell { key } | Source::Bundle { key, .. } | Source::Archive { key } => key,
        }
    }

    /// Rank of the source when two segments cover the same range: celld's
    /// own object, then our archive, then a bundle that may vanish.
    fn source_rank(&self) -> u8 {
        match self.source {
            Source::Cell { .. } => 0,
            Source::Archive { .. } => 1,
            Source::Bundle { .. } => 2,
        }
    }
}

/// One epoch of a chain and the txids it serves.
#[derive(Clone, Debug)]
pub struct Span {
    pub epoch: u64,
    /// The first txid this epoch serves.
    pub lo: u64,
    pub segments: Vec<Segment>,
}

/// An actor's current history: spans oldest first, each clipped below the
/// span after it.
#[derive(Clone, Debug, Default)]
pub struct Chain {
    pub spans: Vec<Span>,
}

impl Chain {
    /// Build from every known segment, with celld's chain rule. Duplicate
    /// ranges across sources collapse to the best-ranked source.
    pub fn build(segments: &[Segment]) -> Self {
        let mut by_epoch: BTreeMap<u64, Vec<Segment>> = BTreeMap::new();
        let mut best: HashMap<(u64, u32, u64, u64), &Segment> = HashMap::new();
        for s in segments {
            let k = (s.epoch, s.level, s.min, s.max);
            match best.get(&k) {
                Some(b) if b.source_rank() <= s.source_rank() => {}
                _ => {
                    best.insert(k, s);
                }
            }
        }
        for s in best.into_values() {
            by_epoch.entry(s.epoch).or_default().push(s.clone());
        }
        let mut spans: Vec<Span> = Vec::new();
        for (epoch, mut segs) in by_epoch.into_iter().rev() {
            if let Some(above) = spans.last() {
                segs.retain(|s| s.max < above.lo);
                let ends = segs.iter().map(|s| s.max).max();
                if ends != Some(above.lo - 1) {
                    continue;
                }
            }
            let Some(lo) = segs.iter().map(|s| s.min).min() else {
                continue;
            };
            segs.sort_by_key(|s| (s.min, s.max, s.level));
            spans.push(Span {
                epoch,
                lo,
                segments: segs,
            });
            if lo == 1 {
                break;
            }
        }
        spans.reverse();
        Chain { spans }
    }

    pub fn is_empty(&self) -> bool {
        self.spans.is_empty()
    }

    fn all(&self) -> impl Iterator<Item = &Segment> {
        self.spans.iter().flat_map(|s| s.segments.iter())
    }

    /// The chain's newest txid.
    pub fn max_txid(&self) -> u64 {
        self.all().map(|s| s.max).max().unwrap_or(0)
    }

    /// The span that serves `txid`.
    pub fn span_of(&self, txid: u64) -> Option<&Span> {
        self.spans.iter().rev().find(|s| s.lo <= txid)
    }

    /// Whether `pos` is a state on this chain, so processing can continue
    /// from it. A position in an epoch the chain dropped, or past a cut
    /// (a fenced owner's late writes), is not.
    pub fn contains(&self, pos: Position) -> bool {
        if pos.txid == 0 {
            return true;
        }
        match self.span_of(pos.txid) {
            Some(span) if span.epoch == pos.epoch => true,
            // The last txid of a span the next span continues from.
            _ => self
                .spans
                .windows(2)
                .any(|w| w[0].epoch == pos.epoch && w[1].lo == pos.txid + 1),
        }
    }

    /// The next segment to apply after `txid`: the finest one that starts
    /// right after it, so history keeps every captured version. `None` is
    /// either caught up or a gap; see [`Chain::max_txid`].
    pub fn next_after(&self, txid: u64) -> Option<&Segment> {
        self.all()
            .filter(|s| s.min == txid + 1)
            .min_by_key(|s| (s.max, s.source_rank()))
    }

    /// The fewest segments that rebuild the exact state after `txid`, in
    /// apply order, or `None` if no contiguous path from empty reaches it.
    pub fn plan_to(&self, txid: u64) -> Option<Vec<Segment>> {
        if txid == 0 {
            return Some(Vec::new());
        }
        let mut from: HashMap<u64, Vec<&Segment>> = HashMap::new();
        for s in self.all().filter(|s| s.max <= txid) {
            from.entry(s.min - 1).or_default().push(s);
        }
        // Breadth-first from the empty database; the first visit of a
        // position is a fewest-segments path to it.
        let mut prev: HashMap<u64, &Segment> = HashMap::new();
        let mut queue = VecDeque::from([0u64]);
        while let Some(at) = queue.pop_front() {
            if at == txid {
                break;
            }
            for s in from.get(&at).into_iter().flatten() {
                if s.max != 0 && !prev.contains_key(&s.max) {
                    prev.insert(s.max, s);
                    queue.push_back(s.max);
                }
            }
        }
        let mut path = Vec::new();
        let mut at = txid;
        while at != 0 {
            let s = *prev.get(&at)?;
            path.push(s.clone());
            at = s.min - 1;
        }
        path.reverse();
        Some(path)
    }

    /// The first state on this chain a lane that left it (see
    /// [`Chain::contains`]) can rejoin at: the smallest reachable position
    /// at or past `after`.
    pub fn rejoin_at(&self, after: u64) -> Option<Position> {
        let mut ends: Vec<&Segment> = self.all().filter(|s| s.max >= after.max(1)).collect();
        ends.sort_by_key(|s| s.max);
        ends.into_iter().find_map(|s| {
            self.plan_to(s.max).map(|_| Position {
                epoch: s.epoch,
                txid: s.max,
            })
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn seg(epoch: u64, level: u32, min: u64, max: u64) -> Segment {
        Segment {
            epoch,
            level,
            min,
            max,
            size: 0,
            source: Source::Cell {
                key: format!("e{epoch}/{level}/{min}-{max}"),
            },
        }
    }

    #[test]
    fn steps_through_l0s_in_txid_order() {
        let chain = Chain::build(&[seg(1, 0, 3, 3), seg(1, 0, 1, 1), seg(1, 0, 2, 2)]);
        assert_eq!(chain.next_after(0).unwrap().max, 1);
        assert_eq!(chain.next_after(1).unwrap().max, 2);
        assert!(chain.next_after(3).is_none());
        assert_eq!(chain.max_txid(), 3);
    }

    #[test]
    fn prefers_the_finest_next_segment() {
        let chain = Chain::build(&[seg(1, 0, 1, 1), seg(1, 1, 2, 9), seg(1, 0, 2, 2)]);
        assert_eq!(chain.next_after(1).unwrap().level, 0);
    }

    #[test]
    fn plans_the_fewest_segments_and_avoids_dead_ends() {
        let chain = Chain::build(&[
            seg(1, 9, 1, 5),
            seg(1, 0, 6, 10),
            seg(1, 0, 6, 8),
            seg(1, 0, 9, 12),
        ]);
        let plan = chain.plan_to(12).unwrap();
        let ranges: Vec<_> = plan.iter().map(|s| (s.min, s.max)).collect();
        assert_eq!(ranges, vec![(1, 5), (6, 8), (9, 12)]);
        assert!(chain.plan_to(11).is_none());
    }

    #[test]
    fn a_snapshot_epoch_ends_the_chain_below_it() {
        let chain = Chain::build(&[seg(1, 0, 1, 1), seg(1, 0, 2, 2), seg(2, 0, 1, 1)]);
        assert_eq!(chain.spans.len(), 1);
        assert_eq!(chain.spans[0].epoch, 2);
        assert!(!chain.contains(Position { epoch: 1, txid: 2 }));
        assert_eq!(
            chain.rejoin_at(1),
            Some(Position { epoch: 2, txid: 1 })
        );
    }

    #[test]
    fn a_paged_epoch_continues_from_the_cut_and_drops_fenced_writes() {
        // Epoch 2 continues at txid 4; epoch 1's txid 4 is a fenced owner's.
        let chain = Chain::build(&[
            seg(1, 0, 1, 1),
            seg(1, 0, 2, 3),
            seg(1, 0, 4, 4),
            seg(2, 0, 4, 4),
            seg(2, 0, 5, 5),
        ]);
        assert_eq!(chain.spans.len(), 2);
        assert!(chain.contains(Position { epoch: 1, txid: 3 }));
        assert!(!chain.contains(Position { epoch: 1, txid: 4 }));
        assert_eq!(chain.next_after(3).unwrap().epoch, 2);
        assert_eq!(chain.plan_to(5).unwrap().len(), 4);
    }

    #[test]
    fn duplicate_ranges_keep_celld_objects_over_bundles() {
        let mut b = seg(1, 0, 2, 2);
        b.source = Source::Bundle {
            key: "log/b".into(),
            offset: 0,
            len: 1,
        };
        let chain = Chain::build(&[seg(1, 0, 1, 1), b, seg(1, 0, 2, 2)]);
        assert!(matches!(chain.next_after(1).unwrap().source, Source::Cell { .. }));
    }
}
