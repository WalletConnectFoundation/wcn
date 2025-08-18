use {
    super::{Config, Quorum, Response, ResponseBuffer, RF},
    futures_concurrency::future::Join as _,
    std::array,
    tap::TapFallible,
    wc::metrics,
    wcn_cluster::NodeOperator,
    wcn_storage_api::{operation, MapEntryBorrowed, Operation},
};

/// Max number of [`ReadRepair`] tagrets.
const MAX_TARGETS: usize = RF - super::MAJORITY_QUORUM_THRESHOLD;

// TODO: figure out how to repair `hscan`
pub(super) struct ReadRepair<'a, C: Config> {
    operation: &'a Operation<'a>,

    /// Successful response to be used as the baseline for repair.
    output: &'a operation::Output,

    /// Replicas to be repaired.
    targets: [Option<&'a NodeOperator<C::Node>>; MAX_TARGETS],
}

impl<'a, C: Config> ReadRepair<'a, C> {
    pub(super) fn new(
        operation: &'a Operation<'a>,
        responses: &'a ResponseBuffer,
        quorum: &'a Quorum<'a, C::Node>,
    ) -> Option<Self> {
        if !is_repairable(operation) {
            return None;
        }

        let quorum_response_idx = quorum.response_idx?;

        // Errors are not being repaired.
        let output = responses[quorum_response_idx as usize]
            .as_ref()
            .and_then(|resp| resp.as_ref().ok())?;

        let mut targets = quorum
            .replica_responses
            .iter()
            .copied()
            .enumerate()
            .filter_map(|(replica_idx, response_idx)| {
                let response_idx = response_idx.filter(|&idx| idx != quorum_response_idx)?;

                responses[response_idx as usize]
                    .as_ref()
                    .filter(|resp| resp.is_ok())?;

                Some(quorum.replica_set[replica_idx])
            });

        Some(Self {
            operation,
            output,
            targets: array::from_fn(|_| targets.next()),
        })
    }

    pub(super) fn check_response(
        &mut self,
        operator: &'a NodeOperator<C::Node>,
        response: Response,
    ) {
        match response {
            Ok(output) if &output != self.output => {}

            // Errors are not being repaired.
            _ => return,
        }

        let _ = self
            .targets
            .iter_mut()
            .find_map(|slot| slot.is_none().then(|| *slot = Some(operator)));
    }

    pub(super) async fn run(self) {
        let Some(operation) = &repair_operation(self.operation, self.output) else {
            return;
        };

        self.targets
            .map(|opt| async move {
                if let Some(operator) = opt {
                    let _ = super::execute::<C>(operator, operation).await.tap_err(|_| {
                        metrics::counter!("wcn_replication_coordinator_read_repair_errors")
                            .increment(1);
                    });
                }
            })
            .join()
            .await;
    }
}

pub(super) fn is_repairable(operation: &Operation<'_>) -> bool {
    use operation::{Borrowed, Owned};

    match operation {
        Operation::Owned(owned) => match owned {
            Owned::Get(_) | Owned::HGet(_) => true,
            Owned::Set(_)
            | Owned::Del(_)
            | Owned::GetExp(_)
            | Owned::SetExp(_)
            | Owned::HSet(_)
            | Owned::HDel(_)
            | Owned::HGetExp(_)
            | Owned::HSetExp(_)
            | Owned::HCard(_)
            | Owned::HScan(_) => false,
        },
        Operation::Borrowed(borrowed) => match borrowed {
            Borrowed::Get(_) | Borrowed::HGet(_) => true,
            Borrowed::Set(_)
            | Borrowed::Del(_)
            | Borrowed::GetExp(_)
            | Borrowed::SetExp(_)
            | Borrowed::HSet(_)
            | Borrowed::HDel(_)
            | Borrowed::HGetExp(_)
            | Borrowed::HSetExp(_)
            | Borrowed::HCard(_)
            | Borrowed::HScan(_) => false,
        },
    }
}

/// Given an [`Operation`] and it's successful [`operation::Output`] returns
/// another [`Operation`] meant to repair replicas that responded with a
/// wrong [`operation::Output`].
pub(super) fn repair_operation<'a>(
    operation: &'a Operation<'a>,
    output: &'a operation::Output,
) -> Option<Operation<'a>> {
    use operation::{Borrowed, Owned};

    // TODO: consider refactoring `Operation` (again) in a way so:
    // `enum Get = GetBorrowed | GetOwned`
    Some(Operation::Borrowed(match operation {
        Operation::Owned(owned) => match owned {
            Owned::Get(get) => match output {
                operation::Output::Record(Some(record)) => operation::SetBorrowed {
                    namespace: get.namespace,
                    key: &get.key,
                    record: record.borrow(),
                    keyspace_version: get.keyspace_version,
                }
                .into(),
                _ => return None,
            },
            Owned::HGet(hget) => match output {
                operation::Output::Record(Some(record)) => operation::HSetBorrowed {
                    namespace: hget.namespace,
                    key: &hget.key,
                    entry: MapEntryBorrowed {
                        field: &hget.field,
                        record: record.borrow(),
                    },
                    keyspace_version: hget.keyspace_version,
                }
                .into(),
                _ => return None,
            },
            Owned::Set(_)
            | Owned::Del(_)
            | Owned::GetExp(_)
            | Owned::SetExp(_)
            | Owned::HSet(_)
            | Owned::HDel(_)
            | Owned::HGetExp(_)
            | Owned::HSetExp(_)
            | Owned::HCard(_)
            | Owned::HScan(_) => return None,
        },
        Operation::Borrowed(owned) => match owned {
            Borrowed::Get(get) => match output {
                operation::Output::Record(Some(record)) => operation::SetBorrowed {
                    namespace: get.namespace,
                    key: get.key,
                    record: record.borrow(),
                    keyspace_version: get.keyspace_version,
                }
                .into(),
                _ => return None,
            },
            Borrowed::HGet(hget) => match output {
                operation::Output::Record(Some(record)) => operation::HSetBorrowed {
                    namespace: hget.namespace,
                    key: hget.key,
                    entry: MapEntryBorrowed {
                        field: hget.field,
                        record: record.borrow(),
                    },
                    keyspace_version: hget.keyspace_version,
                }
                .into(),
                _ => return None,
            },
            Borrowed::Set(_)
            | Borrowed::Del(_)
            | Borrowed::GetExp(_)
            | Borrowed::SetExp(_)
            | Borrowed::HSet(_)
            | Borrowed::HDel(_)
            | Borrowed::HGetExp(_)
            | Borrowed::HSetExp(_)
            | Borrowed::HCard(_)
            | Borrowed::HScan(_) => return None,
        },
    }))
}
