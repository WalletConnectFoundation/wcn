mod merge;

pub use merge::*;
use {
    num_traits::Bounded,
    serde::{Deserialize, Serialize},
    smallvec::SmallVec,
    std::{
        fmt::Debug,
        ops::{Range, RangeFrom, RangeTo},
    },
};

/// A (half-open) range bounded inclusively below and exclusively above i.e.
/// `[start..end)`.
///
/// If `start >= end`, the range is considered wrapping and is equivalent to
/// covering union of two ranges: `[start..MAX_VALUE]` and `[0..end)`.
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct KeyRange<Idx>
where
    Idx: Bounded,
{
    pub(super) start: Idx,
    pub(super) end: Idx,
}

impl<Idx: Bounded> KeyRange<Idx> {
    /// Creates a new range.
    pub(in crate::cluster) fn new(start: Idx, end: Idx) -> Self {
        Self { start, end }
    }
}

impl<Idx> KeyRange<Idx>
where
    Idx: PartialOrd<Idx> + Clone + Debug + Bounded + Ord,
{
    /// Returns `true` if the range is wrapping, which is equivalent to union of
    /// the following two ranges:  `[start..MAX_VALUE]` and `[0..end)`..
    pub fn is_wrapping(&self) -> bool {
        self.is_inverted() && !self.ends_at_origin()
    }

    /// Returns `true` if the range is inverted, i.e. `start >= end`.
    pub fn is_inverted(&self) -> bool {
        self.start >= self.end
    }

    /// Returns `true` if the range ends at the origin.
    ///
    /// This is useful for distinguishing a special case of non-wrapping range
    /// that has inverted positions, `end < start`, but is still
    /// non-wrapping.
    pub fn ends_at_origin(&self) -> bool {
        self.end == Idx::min_value()
    }

    /// Returns `true` if the range covers the whole ring.
    pub fn covers_whole_ring(&self) -> bool {
        self.start == self.end
    }

    /// Returns `true` if `item` is contained in the range.
    pub fn contains(&self, item: &Idx) -> bool {
        if self.is_inverted() {
            self.range_from().contains(&item) || self.range_to().contains(&item)
        } else {
            self.range_from().contains(&item) && self.range_to().contains(&item)
        }
    }

    /// Returns `true` if the range overlaps with `other`.
    pub fn is_overlapping(&self, other: &Self) -> bool {
        self.contains(&other.start) || other.contains(&self.start)
    }

    /// Returns `true` if one range is a continuation of the other.
    ///
    /// That's intervals do not intersect, but can be merged i.e. for a given
    /// intervals [a, b) and [b, c) the union is [a, c).
    pub fn is_continuous(&self, other: &Self) -> bool {
        // Return immediately if any of the ranges describes the whole ring.
        if self.covers_whole_ring() || other.covers_whole_ring() {
            return false;
        }
        self.end == other.start || other.end == self.start
    }

    /// Returns a new range that is the union of `self` and `other` if they can
    /// be merged into a single interval. For ranges that can't be merged,
    /// returns `None`.
    pub fn merged(&self, other: &Self) -> Option<Self> {
        // Return immediately if any of the ranges describe the whole ring.
        // Returned range is always in the form of `[0..0)`.
        if self.covers_whole_ring() || other.covers_whole_ring() {
            return Some(Self::new(Idx::min_value(), Idx::min_value()));
        }
        if self.is_overlapping(other) || self.is_continuous(other) {
            let start: Idx;
            let end: Idx;
            let both_inverted = self.is_inverted() && other.is_inverted();
            let both_non_inverted = !(self.is_inverted() || other.is_inverted());
            if both_inverted || both_non_inverted {
                start = self.start.clone().min(other.start.clone());
                end = self.end.clone().max(other.end.clone());
            } else {
                // Assign inverted range to `a` and non-inverted to `b`.
                let (a, b) = if self.is_inverted() {
                    (self, other)
                } else {
                    (other, self)
                };

                // See if `b` is touching from the left or right.
                if a.start <= b.end {
                    // Touching from the left.
                    start = a.start.clone().min(b.start.clone());
                    end = a.end.clone();
                } else {
                    // Touching from the right.
                    start = a.start.clone();
                    end = a.end.clone().max(b.end.clone());
                }
            }

            if start == end {
                // The merged range is the whole ring.
                Some(Self::new(Idx::min_value(), Idx::min_value()))
            } else {
                Some(Self::new(start, end))
            }
        } else {
            None
        }
    }

    /// Extends both `start` and `end` of the range to match `other`.
    pub fn extend(&mut self, other: &Self) {
        self.extend_start(other);
        self.extend_end(other);
    }

    /// Extends the range's `start` to match `other.start` if `other.start <
    /// self.start`.
    pub fn extend_start(&mut self, other: &Self) {
        if other.start < self.start {
            if self.is_wrapping() && other.start < self.end {
                self.start = self.end.clone();
            } else {
                self.start = other.start.clone();
            }
        }
    }

    /// Extends the range's `end` to match `other.end` if `other.end >
    /// self.end`.
    pub fn extend_end(&mut self, other: &Self) {
        if other.end > self.end {
            if self.is_wrapping() && other.end > self.start {
                self.end = self.start.clone();
            } else {
                self.end = other.end.clone();
            }
        }
    }

    /// Returns a vector of ranges (which are non-wrapping and with possibly
    /// unbounded ends) that cover the same keyspace as the original range.
    /// Those ranges are safe to use in storage, or any other contexts that are
    /// unaware of the ring structure of the keyspace.
    ///
    /// Our current type is essentially a half-open interval, i.e.
    /// `[start..end)`, with the whole ring expressed as `[k..k)` (whenever
    /// `start == end` we consider the range covering the whole ring).
    ///
    /// Whenever we are working with storage, we need to express the range as
    /// some interval in `[0..MAX_VALUE]` (interval is in closed on both sides).
    /// To make sure that we can include the `MAX_VALUE`, we express bounds
    /// as `Option<Idx>`, where `None` means unbounded.
    ///
    /// Two special cases are:
    /// 1. `[m..n)` where `m >= n && n == 0` (range ends at origin) is expressed
    ///    as `m..None`.
    /// 2. `[m..n)` where `m >= n && n != 0` (range passes through origin) is
    ///    expressed as union of `m..None` and `0..n`.
    ///
    /// Note, that when `m == n`, we're covering the whole ring.
    ///
    /// In all other cases, we can use the same type to express the range. In
    /// storage, ranges are also assumed to be within half-open intervals
    /// `[start..end)` but with an additional constraint that `start < end`, and
    /// an additional feature of `end` being optional (unbounded, if necessary).
    pub fn into_std_ranges(self) -> SmallVec<[Range<Option<Idx>>; 2]> {
        let mut result = SmallVec::new();

        if self.is_inverted() {
            if self.ends_at_origin() {
                result.push(Some(self.start)..None);
            } else {
                result.push(Some(self.start)..None);
                result.push(Some(Idx::min_value())..Some(self.end));
            }
        } else {
            result.push(Some(self.start)..Some(self.end));
        }

        result
    }

    fn range_from(&self) -> RangeFrom<&Idx> {
        &self.start..
    }

    fn range_to(&self) -> RangeTo<&Idx> {
        ..&self.end
    }
}

impl<Idx: Bounded> From<Range<Idx>> for KeyRange<Idx> {
    fn from(value: Range<Idx>) -> Self {
        Self {
            start: value.start,
            end: value.end,
        }
    }
}

impl KeyRange<u64> {
    pub fn size(&self) -> u64 {
        if self.is_inverted() {
            u64::MAX - (self.start - self.end)
        } else {
            self.end - self.start
        }
    }
}

#[cfg(test)]
mod tests {
    use {super::*, crate::cluster::keyspace::KeyPosition};

    #[test]
    fn basic() {
        let range = KeyRange::new(10, 5);

        assert!(range.is_wrapping());
        assert!(range.contains(&0));
        assert!(!range.contains(&5));
        assert!(!range.contains(&7));
        assert!(!range.contains(&9));
        assert!(range.contains(&0));
        assert!(range.contains(&4));
        assert!(range.contains(&10));
        assert!(range.contains(&u64::MAX));

        let range = KeyRange::new(5, 10);

        assert!(!range.is_wrapping());
        assert!(!range.contains(&0));
        assert!(range.contains(&5));
        assert!(range.contains(&7));
        assert!(range.contains(&9));
        assert!(!range.contains(&0));
        assert!(!range.contains(&4));
        assert!(!range.contains(&10));
        assert!(!range.contains(&u64::MAX));
    }

    #[test]
    fn overlap() {
        {
            // =====
            //    =====
            let r1 = KeyRange::new(5, 10);
            let r2 = KeyRange::new(8, 13);

            assert!(r1.is_overlapping(&r2));
            assert!(r2.is_overlapping(&r1));
        }

        {
            // =====
            //      =====
            let r1 = KeyRange::new(5, 10);
            let r2 = KeyRange::new(10, 15);

            assert!(!r1.is_overlapping(&r2));
            assert!(!r2.is_overlapping(&r1));
        }

        {
            //     =====
            // ====     ====
            let r1 = KeyRange::new(5, 10);
            let r2 = KeyRange::new(10, 5);

            assert!(!r1.is_overlapping(&r2));
            assert!(!r2.is_overlapping(&r1));
        }

        {
            //     =====
            // ======   ====
            let r1 = KeyRange::new(5, 10);
            let r2 = KeyRange::new(10, 7);

            assert!(r1.is_overlapping(&r2));
            assert!(r2.is_overlapping(&r1));
        }

        {
            //       =====
            // ======   ====
            let r1 = KeyRange::new(5, 10);
            let r2 = KeyRange::new(7, 5);

            assert!(r1.is_overlapping(&r2));
            assert!(r2.is_overlapping(&r1));
        }

        {
            //       =====
            // =============
            let r1 = KeyRange::new(5, 10);
            let r2 = KeyRange::new(5, 5);

            assert!(r1.is_overlapping(&r2));
            assert!(r2.is_overlapping(&r1));
        }

        {
            // =====     ====
            // =======  =====
            let r1 = KeyRange::new(10, 5);
            let r2 = KeyRange::new(9, 6);

            assert!(r1.is_overlapping(&r2));
            assert!(r2.is_overlapping(&r1));
        }
    }

    #[test]
    fn extension() {
        {
            // =====
            //    =====
            let mut r1 = KeyRange::new(5, 10);
            let r2 = KeyRange::new(8, 13);

            r1.extend_end(&r2);

            assert_eq!(r1, KeyRange::new(5, 13));
        }

        {
            // =====
            //    =====
            let r1 = KeyRange::new(5, 10);
            let mut r2 = KeyRange::new(8, 13);

            r2.extend_end(&r1);

            assert_eq!(r2, KeyRange::new(8, 13));
        }

        {
            // =====
            //    =====
            let mut r1 = KeyRange::new(5, 10);
            let r2 = KeyRange::new(8, 13);

            r1.extend_start(&r2);

            assert_eq!(r1, KeyRange::new(5, 10));
        }

        {
            // =====
            //    =====
            let r1 = KeyRange::new(5, 10);
            let mut r2 = KeyRange::new(8, 13);

            r2.extend_start(&r1);

            assert_eq!(r2, KeyRange::new(5, 13));
        }

        {
            //     =====
            // ======   ====
            let mut r1 = KeyRange::new(5, 10);
            let r2 = KeyRange::new(10, 7);

            r1.extend_end(&r2);

            assert_eq!(r1, KeyRange::new(5, 10));
        }

        {
            //     =====
            // ======   ====
            let r1 = KeyRange::new(5, 10);
            let mut r2 = KeyRange::new(10, 7);

            r2.extend_end(&r1);

            assert_eq!(r2, KeyRange::new(10, 10));
        }

        {
            //     =====
            // ======   ====
            let mut r1 = KeyRange::new(5, 10);
            let r2 = KeyRange::new(10, 7);

            r1.extend_start(&r2);

            assert_eq!(r1, KeyRange::new(5, 10));
        }

        {
            //     =====
            // ======   ====
            let r1 = KeyRange::new(5, 10);
            let mut r2 = KeyRange::new(10, 7);

            r2.extend_start(&r1);

            assert_eq!(r2, KeyRange::new(7, 7));
        }

        {
            //       =====
            // =============
            let mut r1 = KeyRange::new(5, 10);
            let r2 = KeyRange::new(5, 5);

            r1.extend_start(&r2);

            assert_eq!(r1, KeyRange::new(5, 10));
        }

        {
            //       =====
            // =============
            let mut r1 = KeyRange::new(5, 10);
            let r2 = KeyRange::new(5, 5);

            r1.extend_end(&r2);

            assert_eq!(r1, KeyRange::new(5, 10));
        }
    }

    #[test]
    fn size() {
        // Wrapping ranges.
        assert_eq!(KeyRange::new(0, 0).size(), u64::MAX);
        assert_eq!(KeyRange::new(10, 10).size(), u64::MAX);
        assert_eq!(KeyRange::new(10, 9).size(), u64::MAX - 1);

        // Regular ranges.
        assert_eq!(KeyRange::new(5, 10).size(), 5);
    }

    #[test]
    fn merged() {
        // All test cases are structured around the origin 0 i.e. one of the ranges
        // crosses the origin, or other, or their intersection includes the origin. This
        // allows to capture all the possible wrapping cases.

        {
            // =====
            //               0
            //        =====
            let r1 = KeyRange::new(5, 10);
            let r2 = KeyRange::new(50, 100);
            assert_eq!(r1.merged(&r2), None);
            assert_eq!(r2.merged(&r1), None);
        }

        {
            // =====
            //            0
            //      =====
            let r1 = KeyRange::new(5, 10);
            let r2 = KeyRange::new(10, 100);
            let expected = KeyRange::new(5, 100);

            assert_eq!(r1.merged(&r2), Some(expected));
            assert_eq!(r2.merged(&r1), Some(expected));
        }

        {
            // =====
            //    0
            //        =====
            let r1 = KeyRange::new(u64::MAX - 100, 10);
            let r2 = KeyRange::new(50, 100);
            assert_eq!(r1.merged(&r2), None);
            assert_eq!(r2.merged(&r1), None);
        }

        {
            // =====
            //    0
            //      =====
            let r1 = KeyRange::new(u64::MAX - 100, 10);
            let r2 = KeyRange::new(10, 100);
            let expected = KeyRange::new(u64::MAX - 100, 100);

            assert_eq!(r1.merged(&r2), Some(expected));
            assert_eq!(r2.merged(&r1), Some(expected));
        }

        {
            //      =====
            //         0
            // =====
            let r1 = KeyRange::new(u64::MAX - 100, 10);
            let r2 = KeyRange::new(u64::MAX - 200, u64::MAX - 100);
            let expected = KeyRange::new(u64::MAX - 200, 10);

            assert_eq!(r1.merged(&r2), Some(expected));
            assert_eq!(r2.merged(&r1), Some(expected));
        }

        {
            // =====
            //           0
            //    =====
            let r1 = KeyRange::new(5, 100);
            let r2 = KeyRange::new(80, 120);
            let expected = KeyRange::new(5, 120);

            assert_eq!(r1.merged(&r2), Some(expected));
            assert_eq!(r2.merged(&r1), Some(expected));
        }

        {
            // =========
            //           0
            //   =====
            let r1 = KeyRange::new(5, 100);
            let r2 = KeyRange::new(25, 80);
            let expected = KeyRange::new(5, 100);

            assert_eq!(r1.merged(&r2), Some(expected));
            assert_eq!(r2.merged(&r1), Some(expected));
        }

        {
            //  =====
            //         0
            //  =====
            let r1 = KeyRange::new(5, 100);
            let r2 = KeyRange::new(5, 100);
            let expected = KeyRange::new(5, 100);

            assert_eq!(r1.merged(&r2), Some(expected));
            assert_eq!(r2.merged(&r1), Some(expected));
        }

        {
            // =====
            //  0
            //    =====
            let r1 = KeyRange::new(u64::MAX - 100, 10);
            let r2 = KeyRange::new(5, 50);
            let expected = KeyRange::new(u64::MAX - 100, 50);

            assert_eq!(r1.merged(&r2), Some(expected));
            assert_eq!(r2.merged(&r1), Some(expected));
        }

        {
            // ==========
            //  0
            //    =====
            let r1 = KeyRange::new(u64::MAX - 100, 100);
            let r2 = KeyRange::new(5, 50);
            let expected = KeyRange::new(u64::MAX - 100, 100);

            assert_eq!(r1.merged(&r2), Some(expected));
            assert_eq!(r2.merged(&r1), Some(expected));
        }

        {
            //     =====
            //        0
            // =====
            let r1 = KeyRange::new(u64::MAX - 100, 10);
            let r2 = KeyRange::new(u64::MAX - 150, u64::MAX - 50);
            let expected = KeyRange::new(u64::MAX - 150, 10);

            assert_eq!(r1.merged(&r2), Some(expected));
            assert_eq!(r2.merged(&r1), Some(expected));
        }

        {
            // =========
            //        0
            //  =====
            let r1 = KeyRange::new(u64::MAX - 200, 10);
            let r2 = KeyRange::new(u64::MAX - 150, u64::MAX - 50);
            let expected = KeyRange::new(u64::MAX - 200, 10);

            assert_eq!(r1.merged(&r2), Some(expected));
            assert_eq!(r2.merged(&r1), Some(expected));
        }

        {
            //   =====
            //    0
            // =========
            let r1 = KeyRange::new(u64::MAX - 100, 10);
            let r2 = KeyRange::new(u64::MAX - 150, 50);
            let expected = KeyRange::new(u64::MAX - 150, 50);

            assert_eq!(r1.merged(&r2), Some(expected));
            assert_eq!(r2.merged(&r1), Some(expected));
        }

        {
            //   =====
            //    0
            // =====
            let r1 = KeyRange::new(u64::MAX - 100, 10);
            let r2 = KeyRange::new(u64::MAX - 150, 5);
            let expected = KeyRange::new(u64::MAX - 150, 10);

            assert_eq!(r1.merged(&r2), Some(expected));
            assert_eq!(r2.merged(&r1), Some(expected));
        }

        {
            //  =====
            //    0
            //  =====
            let r1 = KeyRange::new(u64::MAX - 200, 10);
            let r2 = KeyRange::new(u64::MAX - 200, 10);
            let expected = KeyRange::new(u64::MAX - 200, 10);

            assert_eq!(r1.merged(&r2), Some(expected));
            assert_eq!(r2.merged(&r1), Some(expected));
        }

        {
            // ====[=======
            //     0
            //       ====
            let r1 = KeyRange::new(0u64, 0);
            let r2 = KeyRange::new(100, 200);
            let expected = KeyRange::new(0, 0);

            assert_eq!(r1.merged(&r2), Some(expected));
            assert_eq!(r2.merged(&r1), Some(expected));
        }

        {
            //     [====
            //     0
            //       =====
            let r1 = KeyRange::new(0u64, 100);
            let r2 = KeyRange::new(100, 200);
            let expected = KeyRange::new(0, 200);

            assert_eq!(r1.merged(&r2), Some(expected));
            assert_eq!(r2.merged(&r1), Some(expected));
        }

        {
            //     ====)
            //         0
            // =====
            let r1 = KeyRange::new(u64::MAX - 200, 0);
            let r2 = KeyRange::new(u64::MAX - 1000, u64::MAX - 200);
            let expected = KeyRange::new(u64::MAX - 1000, 0);

            assert_eq!(r1.merged(&r2), Some(expected));
            assert_eq!(r2.merged(&r1), Some(expected));
        }

        {
            // ====)
            //     0
            //     [=====
            let r1 = KeyRange::new(u64::MAX - 200, 0);
            let r2 = KeyRange::new(0, 200);
            let expected = KeyRange::new(u64::MAX - 200, 200);

            assert_eq!(r1.merged(&r2), Some(expected));
            assert_eq!(r2.merged(&r1), Some(expected));
        }

        {
            // ====[=========
            //     0
            // ======)  [====
            let r1 = KeyRange::new(0u64, 0);
            let r2 = KeyRange::new(1000, 100);
            let expected = KeyRange::new(0, 0);

            assert_eq!(r1.merged(&r2), Some(expected));
            assert_eq!(r2.merged(&r1), Some(expected));
        }

        {
            // ====[=======
            //  0  k
            //       ====
            let r1 = KeyRange::new(50u64, 50);
            let r2 = KeyRange::new(100, 200);
            let expected = KeyRange::new(0, 0);

            assert_eq!(r1.merged(&r2), Some(expected));
            assert_eq!(r2.merged(&r1), Some(expected));
        }
    }

    #[test]
    fn into_ranges() {
        struct TestCase {
            name: &'static str,
            range: KeyRange<KeyPosition>,
            expected: Vec<Range<Option<KeyPosition>>>,
        }

        let tests = vec![
            TestCase {
                name: "whole ring, starts at zero",
                range: KeyRange::new(0, 0),
                expected: vec![Some(0)..None],
            },
            TestCase {
                name: "whole ring",
                range: KeyRange::new(10, 10),
                expected: vec![Some(10)..None, Some(0)..Some(10)],
            },
            TestCase {
                name: "wrapping range",
                range: KeyRange::new(10, 5),
                expected: vec![Some(10)..None, Some(0)..Some(5)],
            },
            TestCase {
                name: "ends at origin",
                range: KeyRange::new(10, 0),
                expected: vec![Some(10)..None],
            },
            TestCase {
                name: "starts at origin, non-wrapping",
                range: KeyRange::new(0, 50),
                expected: vec![Some(0)..Some(50)],
            },
            TestCase {
                name: "starts at non-origin position, non-wrapping",
                range: KeyRange::new(KeyPosition::MAX / 2, KeyPosition::MAX),
                expected: vec![Some(KeyPosition::MAX / 2)..Some(KeyPosition::MAX)],
            },
        ];
        for test in tests {
            assert_eq!(
                test.range.into_std_ranges().into_vec(),
                test.expected,
                "case: {}",
                test.name
            );
        }
    }
}
