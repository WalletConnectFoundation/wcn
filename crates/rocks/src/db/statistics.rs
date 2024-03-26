use std::{collections::HashMap, str::FromStr};

/// RocksDB statistic types.
///
/// See: https://github.com/facebook/rocksdb/wiki/Statistics#stats-types
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Statistic {
    /// The ticker type is represented by 64-bit unsigned integer. The value
    /// never decreases or resets. Ticker stats are used to measure counters
    /// (e.g. "rocksdb.block.cache.hit"), cumulative bytes (e.g.
    /// "rocksdb.bytes.written") or time (e.g. "rocksdb.l0.slowdown.micros").
    Ticker(u64),

    /// The histogram type measures distribution of a stat across all
    /// operations. Most of the histograms are for distribution of duration of a
    /// DB operation. Taking "rocksdb.db.get.micros" as an example, we measure
    /// time spent on each Get() operation and calculate the distribution for
    /// all of them.
    Histogram(Histogram),
}

/// RocksDB histogram statistic.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Histogram {
    /// 50 percentile.
    pub p50: f64,

    /// 95 percentile.
    pub p95: f64,

    /// 99 percentile.
    pub p99: f64,

    /// 100 percentile.
    pub p100: f64,

    /// Total count of measurements.
    pub count: u64,

    /// Total sum of measurements.
    pub sum: u64,
}

impl Statistic {
    pub(super) fn parse_multiple(s: &str) -> Result<HashMap<String, Self>, ParseError> {
        s.lines()
            .map(|l| Self::parse(l).ok_or(ParseError))
            .collect()
    }

    fn parse(s: &str) -> Option<(String, Self)> {
        fn split_next_stat(s: &str) -> Option<(&str, &str)> {
            let (_name, s) = s.split_once(' ')?;
            let (_colon, s) = s.split_once(' ')?;
            Some(s.split_once(' ').unwrap_or((s, "")))
        }

        fn parse_next_stat<T: FromStr>(s: &mut &str) -> Option<T> {
            let (stat, rest) = split_next_stat(s)?;
            *s = rest;
            T::from_str(stat).ok()
        }

        let (name, s) = s.split_once(' ')?;
        let (first, mut s) = split_next_stat(s)?;

        let stat = if first.contains('.') {
            let s = &mut s;
            Self::Histogram(Histogram {
                p50: first.parse().ok()?,
                p95: parse_next_stat(s)?,
                p99: parse_next_stat(s)?,
                p100: parse_next_stat(s)?,
                count: parse_next_stat(s)?,
                sum: parse_next_stat(s)?,
            })
        } else {
            Self::Ticker(first.parse().ok()?)
        };

        Some((name.to_string(), stat))
    }
}

#[derive(Clone, Copy, Debug, thiserror::Error)]
#[error("Failed to parse string representation of RocksDB statistics")]
pub struct ParseError;

#[test]
fn test_statistic_parse() {
    #[rustfmt::skip]
    let s = "\
    rocksdb.block.cache.miss COUNT : 42\n\
    rocksdb.async.prefetch.abort.micros P50 : 1.000000 P95 : 2.000000 P99 : 3.000000 P100 : 4.000000 COUNT : 5 SUM : 6";

    let stats = Statistic::parse_multiple(s).expect("parse_multiple");
    assert_eq!(stats.len(), 2);

    let expected = Some(&Statistic::Ticker(42));
    assert_eq!(stats.get("rocksdb.block.cache.miss"), expected);

    let expected = Some(&Statistic::Histogram(Histogram {
        p50: 1.0,
        p95: 2.0,
        p99: 3.0,
        p100: 4.0,
        count: 5,
        sum: 6,
    }));
    assert_eq!(stats.get("rocksdb.async.prefetch.abort.micros"), expected);
}
