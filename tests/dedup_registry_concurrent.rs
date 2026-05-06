use futures::future::join_all;
use std::sync::Arc;
use syspref_ingest::dedup::create_registry;
use syspref_ingest::shared::DedupKey;
use tokio::sync::Barrier;

#[tokio::test]
async fn dedup_registry_concurrent_access() {
    let registry = create_registry();
    let key = DedupKey::new("h".into(), "p".into());
    const N: usize = 256;
    let barrier = Arc::new(Barrier::new(N));

    let mut handles = Vec::with_capacity(N);
    for _ in 0..N {
        let reg = registry.clone();
        let key = key.clone();
        let barrier = barrier.clone();
        handles.push(tokio::spawn(async move {
            barrier.wait().await;
            let mut r = reg.lock().await;
            r.get_or_create(key, "p".into())
        }));
    }

    let results = join_all(handles)
        .await
        .into_iter()
        .map(|r| r.unwrap())
        .collect::<Vec<_>>();

    let first_id = results[0].0;
    assert!(
        results.iter().all(|(id, _, _)| *id == first_id),
        "all tasks must observe the same canonical job_id"
    );
    assert_eq!(
        results.iter().filter(|(_, dedup, _)| !*dedup).count(),
        1,
        "exactly one task must have created the canonical job"
    );

    let r = registry.lock().await;
    assert_eq!(r.dedup.len(), 1, "dedup map must contain exactly one entry");
    assert_eq!(r.jobs.len(), 1, "jobs map must contain exactly one job");
    assert_eq!(
        r.dedup.get(&key).expect("dedup entry").job_id,
        first_id,
        "dedup entry must point at the canonical job_id"
    );
    assert!(
        r.jobs.contains_key(&first_id),
        "jobs map must contain the canonical job_id"
    );
}
