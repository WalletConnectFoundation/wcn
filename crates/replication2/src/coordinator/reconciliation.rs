use {
    super::{Response, MAJORITY_QUORUM_THRESHOLD},
    std::collections::HashMap,
    wcn_storage_api2::{operation, MapPage, Operation},
};

pub(super) fn reconcile(
    operation: &Operation<'_>,
    responses: &[Option<Response>],
) -> Option<operation::Output> {
    use operation::{Borrowed, Owned};

    match operation {
        Operation::Owned(owned) => match owned {
            Owned::HScan(_) => reconcile_map_page(responses).map(Into::into),
            Owned::HCard(_) => reconcile_map_cardinality(responses).map(Into::into),
            Owned::Get(_)
            | Owned::Set(_)
            | Owned::Del(_)
            | Owned::GetExp(_)
            | Owned::SetExp(_)
            | Owned::HGet(_)
            | Owned::HSet(_)
            | Owned::HDel(_)
            | Owned::HGetExp(_)
            | Owned::HSetExp(_) => None,
        },
        Operation::Borrowed(borrowed) => match borrowed {
            Borrowed::HScan(_) => reconcile_map_page(responses).map(Into::into),
            Borrowed::HCard(_) => reconcile_map_cardinality(responses).map(Into::into),
            Borrowed::Get(_)
            | Borrowed::Set(_)
            | Borrowed::Del(_)
            | Borrowed::GetExp(_)
            | Borrowed::SetExp(_)
            | Borrowed::HGet(_)
            | Borrowed::HSet(_)
            | Borrowed::HDel(_)
            | Borrowed::HGetExp(_)
            | Borrowed::HSetExp(_) => None,
        },
    }
}

// TODO: We need to figure out a unified way to handle read repair and
// reconciliation for maps.
//
/// Innefficent, but we currently have < 1 map reconciliation per second within
/// the entire network.
pub(super) fn reconcile_map_page(responses: &[Option<Response>]) -> Option<MapPage> {
    let iter = || {
        responses
            .iter()
            .filter_map(|opt| match opt.as_ref()?.as_ref().ok()? {
                operation::Output::MapPage(page) => Some(page),
                _ => None,
            })
    };

    if iter().count() < MAJORITY_QUORUM_THRESHOLD {
        return None;
    }

    let has_next = iter().filter(|page| page.has_next).count() >= MAJORITY_QUORUM_THRESHOLD;

    let size = iter()
        .map(|page| page.entries.len())
        .max()
        .unwrap_or_default();

    let counters = iter().fold(HashMap::with_capacity(size), |mut counters, page| {
        page.entries.iter().for_each(|entry| {
            *counters.entry(entry).or_insert(0) += 1;
        });

        counters
    });

    let mut entries: Vec<_> = counters
        .into_iter()
        .filter(|(_, count)| *count >= MAJORITY_QUORUM_THRESHOLD)
        .map(|(entry, _)| entry.clone())
        .collect();

    entries.sort_unstable_by(|a, b| a.field.cmp(&b.field));

    Some(MapPage { entries, has_next })
}

pub(super) fn reconcile_map_cardinality(responses: &[Option<Response>]) -> Option<u64> {
    let iter = || {
        responses
            .iter()
            .filter_map(|opt| match opt.as_ref()?.as_ref().ok()? {
                operation::Output::Cardinality(card) => Some(card),
                _ => None,
            })
    };

    let count = iter().count();
    if count < MAJORITY_QUORUM_THRESHOLD {
        return None;
    }

    let counters = iter().fold(HashMap::with_capacity(count), |mut counters, &card| {
        *counters.entry(card).or_insert(0) += 1;
        counters
    });

    // If there's no consensus on the collection cardinality, return the lowest
    // value that replicas agree on.
    counters
        .iter()
        .find_map(|(&value, count)| (*count >= MAJORITY_QUORUM_THRESHOLD).then_some(value))
        .or_else(|| counters.keys().min().copied())
}

#[cfg(test)]
mod test {
    use {
        super::*,
        std::array,
        wcn_storage_api2::{
            self as storage_api,
            Error,
            MapEntry,
            Record,
            RecordExpiration,
            RecordVersion,
        },
    };

    #[test]
    fn reconcile_map_page() {
        let expiration = RecordExpiration::from(std::time::Duration::from_secs(60));
        let version = RecordVersion::now();

        let page = |values: &[u8], has_next| MapPage {
            entries: values
                .iter()
                .map(|&byte| MapEntry {
                    field: vec![byte],
                    record: Record {
                        value: vec![byte],
                        expiration,
                        version,
                    },
                })
                .collect(),
            has_next,
        };

        let response = |values, has_next| Ok(operation::Output::MapPage(page(values, has_next)));
        let err_response = || Err(Error::new(storage_api::ErrorKind::Internal));

        let responses: &mut [_; 5] = &mut array::from_fn(|_| Some(response(&[0, 1, 2], true)));

        let assert_page = |responses: &_, values, has_next| {
            assert_eq!(
                super::reconcile_map_page(responses),
                Some(page(values, has_next))
            )
        };

        let assert_none = |responses: &_| assert_eq!(super::reconcile_map_page(responses), None);

        assert_page(responses, &[0, 1, 2], true);

        responses[0] = Some(response(&[0, 1], true));
        assert_page(responses, &[0, 1, 2], true);

        responses[1] = Some(response(&[0, 1], false));
        assert_page(responses, &[0, 1, 2], true);

        responses[2] = Some(response(&[0, 1], false));
        assert_page(responses, &[0, 1], true);

        responses[3] = Some(response(&[0, 1], false));
        assert_page(responses, &[0, 1], false);

        responses[4] = Some(err_response());
        assert_page(responses, &[0, 1], false);

        responses[0] = None;
        assert_page(responses, &[0, 1], false);

        responses[1] = Some(err_response());
        assert_none(responses);

        responses[0] = Some(response(&[0, 1, 2], true));
        assert_page(responses, &[0, 1], false);

        responses[2] = Some(err_response());
        assert_none(responses);
    }

    #[test]
    fn reconcile_map_cardinality() {
        let response = |value| Ok(operation::Output::Cardinality(value));
        let err_response = || Err(Error::new(storage_api::ErrorKind::Internal));

        let responses: &mut [_; 5] = &mut array::from_fn(|_| Some(response(42)));

        let assert_cardinality = |responses: &_, value| {
            assert_eq!(super::reconcile_map_cardinality(responses), Some(value));
        };

        assert_cardinality(responses, 42);

        responses[0] = Some(response(10));
        assert_cardinality(responses, 42);

        responses[1] = Some(response(10));
        assert_cardinality(responses, 42);

        responses[2] = Some(response(10));
        assert_cardinality(responses, 10);

        responses[3] = None;
        assert_cardinality(responses, 10);

        responses[4] = Some(err_response());
        assert_cardinality(responses, 10);
    }
}
