use {
    super::*,
    tokio::{
        sync::{
            mpsc,
            mpsc::{UnboundedReceiver, UnboundedSender},
        },
        time::{sleep, timeout},
    },
};

async fn async_value(
    id: usize,
    value: u32,
    delay: Duration,
    tx: UnboundedSender<usize>,
) -> IdValue<usize, u32> {
    tokio::time::sleep(delay).await;
    tx.send(id).unwrap(); // mark future as completed

    IdValue::new(id, value)
}

async fn collect_k_values(k: usize, mut rx: UnboundedReceiver<usize>) -> Vec<usize> {
    let mut results = Vec::new();
    for _ in 0..k {
        if let Some(value) = rx.recv().await {
            results.push(value);
        }
    }
    results
}

struct MatchingValuesTestCase {
    // Number of matching values to wait for.
    k: usize,
    // Async function with (id, value, delay) params.
    futures: Vec<(usize, u32, Duration)>,
    // How much time to wait for the matching values, before giving up.
    ttl: Duration,
    // How much time to wait, for the remaining futures to complete.
    delay: Option<Duration>,
    // Which futures are expected to be returned. This vector contains ids of futures.
    returned_futures: Vec<usize>,
    // Expected result.
    expected: Result<MatchResult<usize, u32>, ()>,
}

async fn assert_matching_values_test_case(test_case: MatchingValuesTestCase) {
    let (tx, rx) = mpsc::unbounded_channel();
    let futures = FuturesUnordered::new();
    for (id, value, delay) in test_case.futures {
        futures.push(async_value(id, value, delay, tx.clone()));
    }
    let result = match_values(test_case.k, futures, test_case.ttl)
        .await
        .map_err(drop);
    assert_eq!(result, test_case.expected);

    // Allow the remaining futures to complete.
    if let Some(delay) = test_case.delay {
        sleep(delay).await;
    }

    // Check which futures were returned.
    let returned_futures = timeout(
        Duration::from_secs(1),
        collect_k_values(test_case.returned_futures.len(), rx),
    )
    .await
    .unwrap();
    assert_eq!(returned_futures, test_case.returned_futures);
}

#[tokio::test]
async fn matching_value_exists_but_timeout() {
    // Common value exists, but timeout is reached before it is found.
    let test_case = MatchingValuesTestCase {
        k: 2,
        futures: vec![
            (1, 1, Duration::from_millis(200)),
            (2, 1, Duration::from_millis(300)),
            (3, 1, Duration::from_millis(400)),
        ],
        ttl: Duration::from_millis(250),
        delay: None,
        returned_futures: vec![1],
        expected: Err(()),
    };

    assert_matching_values_test_case(test_case).await;
}

#[tokio::test]
async fn no_matching_value_timeout() {
    // No common value exists, timeout is reached just before the last future is
    // checked.
    assert_matching_values_test_case(MatchingValuesTestCase {
        k: 2,
        futures: vec![
            (1, 1, Duration::from_millis(200)),
            (2, 2, Duration::from_millis(300)),
            (3, 3, Duration::from_millis(400)),
        ],
        ttl: Duration::from_millis(350),
        delay: None,
        returned_futures: vec![1, 2],
        expected: Err(()),
    })
    .await;
}

#[tokio::test]
async fn no_matching_value() {
    // No common value exists, operation completes without timeout, but with empty
    // value.
    assert_matching_values_test_case(MatchingValuesTestCase {
        k: 2,
        futures: vec![
            (1, 1, Duration::from_millis(200)),
            (2, 2, Duration::from_millis(300)),
            (3, 3, Duration::from_millis(400)),
        ],
        ttl: Duration::from_millis(500),
        delay: None,
        returned_futures: vec![1, 2, 3],
        expected: Ok(MatchResult::Inconsistent(vec![
            IdValue::new(1, 1),
            IdValue::new(2, 2),
            IdValue::new(3, 3),
        ])),
    })
    .await;
}

#[tokio::test]
async fn matching_value() {
    // Common value exists, operation completes without timeout.
    // All futures are returned.
    assert_matching_values_test_case(MatchingValuesTestCase {
        k: 2,
        futures: vec![
            (1, 1, Duration::from_millis(200)),
            (2, 2, Duration::from_millis(300)),
            (3, 1, Duration::from_millis(400)),
        ],
        ttl: Duration::from_millis(500),
        delay: None,
        returned_futures: vec![1, 2, 3],
        expected: Ok(MatchResult::Ok(1, vec![
            IdValue::new(1, 1),
            IdValue::new(2, 2),
            IdValue::new(3, 1),
        ])),
    })
    .await;
}

#[tokio::test]
async fn matching_value_and_remaining_futures1() {
    // Common value exists, operation completes without timeout.
    // Only first two futures are returned, and result is ready without waiting for
    // the third future.
    assert_matching_values_test_case(MatchingValuesTestCase {
        k: 2,
        futures: vec![
            (1, 1, Duration::from_millis(200)),
            (2, 1, Duration::from_millis(300)),
            (3, 2, Duration::from_millis(400)),
        ],
        ttl: Duration::from_millis(350),
        delay: None, // don't wait for the third future to complete
        returned_futures: vec![1, 2],
        expected: Ok(MatchResult::Ok(1, vec![
            IdValue::new(1, 1),
            IdValue::new(2, 1),
        ])),
    })
    .await;
}

#[tokio::test]
async fn matching_value_and_remaining_futures2() {
    // Now make sure that we eventually receive from the third future (although
    // result is, as expected, already ready).
    assert_matching_values_test_case(MatchingValuesTestCase {
        k: 2,
        futures: vec![
            (1, 1, Duration::from_millis(200)),
            (2, 1, Duration::from_millis(300)),
            (3, 2, Duration::from_millis(400)),
        ],
        ttl: Duration::from_millis(500),
        delay: Some(Duration::from_millis(200)), // wait for the third future to complete
        returned_futures: vec![1, 2, 3],
        expected: Ok(MatchResult::Ok(1, vec![
            IdValue::new(1, 1),
            IdValue::new(2, 1),
        ])),
    })
    .await;
}
