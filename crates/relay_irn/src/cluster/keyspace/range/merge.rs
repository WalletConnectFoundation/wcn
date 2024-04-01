use {super::KeyRange, num_traits::Bounded, std::fmt::Debug};

pub struct MergedRanges<K: Bounded, I: IntoIterator<Item = KeyRange<K>>> {
    values: I,
    last: Option<KeyRange<K>>,
}

/// Sorts the provided ranges and returns an iterator that yields merged
/// (non-overlapping) ranges.
pub fn merge_ranges<K, I>(
    ranges: I,
) -> MergedRanges<K, <Vec<KeyRange<K>> as IntoIterator>::IntoIter>
where
    K: Bounded + PartialOrd + Ord + Clone,
    I: Into<Vec<KeyRange<K>>>,
{
    let mut ranges = ranges.into();
    ranges.sort_by(|a, b| a.start.cmp(&b.start));

    merge_ranges_sorted(ranges)
}

/// Returns an iterator that yields merged (non-overlapping) ranges without any
/// pre-processing.
///
/// Whenever we move through ring positions there's an intrinsic order to the
/// produced ranges, and for certain cases it is better to merge without
/// any additional transformations to that intrinsic order.
pub fn merge_presorted_ranges<K, I>(
    ranges: I,
) -> MergedRanges<K, <Vec<KeyRange<K>> as IntoIterator>::IntoIter>
where
    K: Bounded + PartialOrd + Ord + Clone,
    I: Into<Vec<KeyRange<K>>>,
{
    merge_ranges_sorted(ranges.into())
}

/// Returns an iterator that yields merged (non-overlapping) ranges from the
/// input. The input iterator must yield ranges sorted on by `start` ascending.
pub fn merge_ranges_sorted<K, T>(ranges: T) -> MergedRanges<K, <T as IntoIterator>::IntoIter>
where
    K: Bounded + PartialOrd + Ord + Clone,
    T: IntoIterator<Item = KeyRange<K>>,
{
    let mut iterator = ranges.into_iter();
    let last = iterator.next();

    MergedRanges {
        values: iterator,
        last,
    }
}

impl<K, I> Iterator for MergedRanges<K, I>
where
    K: Bounded + PartialOrd + Ord + Clone + Debug,
    I: Iterator<Item = KeyRange<K>>,
{
    type Item = KeyRange<K>;

    fn next(&mut self) -> Option<KeyRange<K>> {
        if let Some(mut last) = self.last.clone() {
            for next in &mut self.values {
                // If we can merge `last` and `next`, we do so and keep going.
                if let Some(merged) = last.merged(&next) {
                    last = merged;
                } else {
                    self.last = Some(next);
                    return Some(last);
                }
            }

            self.last = None;
            return Some(last);
        }

        None
    }
}

#[cfg(test)]
#[allow(clippy::reversed_empty_ranges)]
mod tests {
    use {super::*, std::ops::Range};

    fn r(range: Range<u64>) -> KeyRange<u64> {
        range.into()
    }

    #[test]
    fn merge_regular_ranges() {
        let ranges = [r(3..6), r(8..10), r(2..5), r(1..4)];
        let merged = merge_ranges(ranges).collect::<Vec<_>>();

        assert_eq!(merged, vec![r(1..6), r(8..10)]);

        let ranges = [r(3..6), r(8..10), r(2..5), r(1..4), r(0..20)];
        let merged = merge_ranges(ranges).collect::<Vec<_>>();

        assert_eq!(merged, vec![r(0..20)]);
    }

    #[test]
    fn merge_wrapping_ranges() {
        let ranges = [r(25..30), r(10..5), r(11..4)];
        let merged = merge_ranges(ranges).collect::<Vec<_>>();

        assert_eq!(merged, vec![r(10..5)]);
    }

    #[test]
    fn merge_wrapping_ranges_with_regular_inside_ranges() {
        let ranges = [r(0..7), r(10..5)];
        let merged = merge_ranges(ranges).collect::<Vec<_>>();

        assert_eq!(merged, vec![r(10..7)]);
    }
}
