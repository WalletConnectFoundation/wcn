use {
    super::{storage, ReplicationResults},
    std::collections::HashMap,
};

pub fn reconcile_map_page(
    results: ReplicationResults<storage::MapPage>,
    required_replicas: usize,
) -> Option<storage::MapPage> {
    let ok_count = results.iter().filter(|res| res.is_ok()).count();
    if ok_count < required_replicas {
        return None;
    }

    let has_next = results
        .iter()
        .filter(|res| res.as_ref().is_ok_and(|page| page.has_next))
        .count()
        >= required_replicas;

    let size = results
        .iter()
        .map(|res| {
            res.as_ref()
                .map(|page| page.records.len())
                .unwrap_or_default()
        })
        .max()
        .unwrap_or_default();

    let mut counters = HashMap::with_capacity(size);
    for record in results
        .into_iter()
        .filter_map(|res| res.inner.ok().map(|page| page.records))
        .flatten()
    {
        *counters.entry(record).or_insert(0) += 1;
    }

    let mut records: Vec<_> = counters
        .into_iter()
        .filter_map(move |(record, count)| (count >= required_replicas).then_some(record))
        .collect();

    records.sort_unstable_by(|a, b| a.field.cmp(&b.field));

    Some(storage::MapPage { records, has_next })
}

pub fn reconcile_map_cardinality(
    results: ReplicationResults<u64>,
    required_replicas: usize,
) -> Option<u64> {
    let ok_count = results.iter().filter(|res| res.is_ok()).count();
    if ok_count < required_replicas {
        return None;
    }

    let mut counters = HashMap::with_capacity(results.len());
    for res in results {
        if let Ok(value) = res.inner {
            *counters.entry(value).or_insert(0) += 1;
        }
    }

    // If there's no consensus on the collection cardinality, return the lowest
    // value that replicas agree on.
    counters
        .iter()
        .find_map(|(value, count)| (*count >= required_replicas).then_some(value))
        .or_else(|| counters.keys().min())
        .copied()
}
