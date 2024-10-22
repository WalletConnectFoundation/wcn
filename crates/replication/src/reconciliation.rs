use {
    super::{storage, ReplicationResults},
    std::collections::HashMap,
};

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Reconciliation error.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Error {
    /// Not enough successful responses to reconcile.
    InsufficientValues,
}

pub fn reconcile_map_page(
    results: ReplicationResults<storage::MapPage>,
    required_replicas: usize,
) -> Result<storage::MapPage> {
    let ok_count = results.iter().filter(|res| res.is_ok()).count();
    if ok_count < required_replicas {
        return Err(Error::InsufficientValues);
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

    Ok(storage::MapPage { records, has_next })
}

pub fn reconcile_map_cardinality(
    results: ReplicationResults<u64>,
    required_replicas: usize,
) -> Result<u64> {
    let ok_count = results.iter().filter(|res| res.is_ok()).count();
    if ok_count < required_replicas {
        return Err(Error::InsufficientValues);
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
        .ok_or(Error::InsufficientValues)
}

#[cfg(test)]
mod test {
    use {super::*, crate::consistency::ReplicationResult, smallvec::smallvec, tap::Pipe};

    #[test]
    fn reconcile_map_page() {
        let expiration = storage::EntryExpiration::from(std::time::Duration::from_secs(60));
        let version = storage::EntryVersion::new();

        let page = |values: &[u8], has_next| storage_api::MapPage {
            records: values
                .iter()
                .map(|&byte| storage::MapRecord {
                    field: vec![byte],
                    value: vec![byte],
                    expiration,
                    version,
                })
                .collect(),
            has_next,
        };

        let result = |res: storage_api::client::Result<(&[u8], bool)>| {
            res.map(|(values, has_next)| page(values, has_next))
                .pipe(|res| ReplicationResult::new_test(res))
        };

        let results = smallvec![
            result(Ok((&[0, 1, 2], true))),
            result(Ok((&[0, 1, 2], true))),
            result(Ok((&[0, 1, 2], true)))
        ];
        assert_eq!(
            super::reconcile_map_page(results, 2),
            Ok(page(&[0, 1, 2], true))
        );

        let results = smallvec![
            result(Ok((&[0, 1, 2], true))),
            result(Ok((&[0, 1, 2], true))),
            result(Ok((&[0, 1], true)))
        ];
        assert_eq!(
            super::reconcile_map_page(results, 2),
            Ok(page(&[0, 1, 2], true))
        );

        let results = smallvec![
            result(Ok((&[0, 1, 2], true))),
            result(Ok((&[0, 1], true))),
            result(Ok((&[0, 1], true)))
        ];
        assert_eq!(
            super::reconcile_map_page(results, 2),
            Ok(page(&[0, 1], true))
        );

        let results = smallvec![
            result(Ok((&[0, 1, 2], false))),
            result(Ok((&[0, 1], false))),
            result(Ok((&[0, 1], true)))
        ];
        assert_eq!(
            super::reconcile_map_page(results, 2),
            Ok(page(&[0, 1], false))
        );

        let results = smallvec![
            result(Err(storage_api::client::Error::Timeout)),
            result(Ok((&[0, 1, 2], true))),
            result(Ok((&[0, 1], true)))
        ];
        assert_eq!(
            super::reconcile_map_page(results, 2),
            Ok(page(&[0, 1], true))
        );

        let results = smallvec![
            result(Err(storage_api::client::Error::Timeout)),
            result(Ok((&[0, 1, 2], true))),
            result(Ok((&[0, 1], false)))
        ];
        assert_eq!(
            super::reconcile_map_page(results, 2),
            Ok(page(&[0, 1], false))
        );

        let results = smallvec![
            result(Err(storage_api::client::Error::Timeout)),
            result(Ok((&[0, 1, 4], true))),
            result(Ok((&[0, 1, 5], true)))
        ];
        assert_eq!(
            super::reconcile_map_page(results, 2),
            Ok(page(&[0, 1], true))
        );

        let results = smallvec![
            result(Err(storage_api::client::Error::Timeout)),
            result(Ok((&[0, 1, 4], true))),
            result(Err(storage_api::client::Error::Timeout))
        ];
        assert_eq!(
            super::reconcile_map_page(results, 2),
            Err(Error::InsufficientValues)
        );

        let results = smallvec![
            result(Ok((&[0], false))),
            result(Ok((&[1], true))),
            result(Ok((&[2], true))),
        ];
        assert_eq!(super::reconcile_map_page(results, 2), Ok(page(&[], true)));
    }

    #[test]
    fn reconcile_map_cardinality() {
        let result = ReplicationResult::<u64>::new_test;

        let results = smallvec![result(Ok(5)), result(Ok(5)), result(Ok(4))];
        assert_eq!(super::reconcile_map_cardinality(results, 2), Ok(5));

        let results = smallvec![result(Ok(5)), result(Ok(4)), result(Ok(4))];
        assert_eq!(super::reconcile_map_cardinality(results, 2), Ok(4));

        let results = smallvec![result(Ok(5)), result(Ok(4)), result(Ok(3))];
        assert_eq!(super::reconcile_map_cardinality(results, 2), Ok(3));

        let results = smallvec![
            result(Ok(5)),
            result(Err(storage_api::client::Error::Timeout)),
            result(Ok(3))
        ];
        assert_eq!(super::reconcile_map_cardinality(results, 2), Ok(3));

        let results = smallvec![
            result(Ok(5)),
            result(Err(storage_api::client::Error::Timeout)),
            result(Err(storage_api::client::Error::Timeout)),
        ];
        assert_eq!(
            super::reconcile_map_cardinality(results, 2),
            Err(Error::InsufficientValues)
        );
    }
}
