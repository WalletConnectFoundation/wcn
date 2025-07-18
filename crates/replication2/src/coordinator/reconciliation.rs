use {
    super::{Response, RF},
    std::collections::HashMap,
    storage_api::{operation, MapPage, Operation},
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

    if iter().count() < RF {
        return None;
    }

    let has_next = iter().filter(|page| page.has_next).count() >= RF;

    let size = iter()
        .map(|page| page.entries.len())
        .max()
        .unwrap_or_default();

    let counters = iter().fold(HashMap::with_capacity(size), |mut counters, page| {
        page.entries.iter().for_each(|entry| {
            let _ = *counters.entry(entry).or_insert(0) + 1;
        });

        counters
    });

    let mut entries: Vec<_> = counters
        .into_iter()
        .filter(|(_, count)| *count >= RF)
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
    if count < RF {
        return None;
    }

    let counters = iter().fold(HashMap::with_capacity(count), |mut counters, &card| {
        let _ = *counters.entry(card).or_insert(0) + 1;
        counters
    });

    // If there's no consensus on the collection cardinality, return the lowest
    // value that replicas agree on.
    counters
        .iter()
        .find_map(|(&value, count)| (*count >= RF).then_some(value))
        .or_else(|| counters.keys().min().copied())
}
