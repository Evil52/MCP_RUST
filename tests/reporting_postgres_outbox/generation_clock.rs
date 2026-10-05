use super::*;

#[tokio::test]
async fn scheduler_persists_each_daily_identity_once_without_delivery() {
    let Ok(url) = std::env::var("REPORT_OUTBOX_TEST_WORKER_URL") else {
        return;
    };
    let _guard = DB_TEST_LOCK.lock().await;
    let repository = PostgresOutboxRepository::connect(&Config::from_str(&url).unwrap())
        .await
        .unwrap();
    let recipient = format!("scheduler_{}", std::process::id());
    let policy = disabled_policy(recipient);

    let morning = utc(3, 0);
    let first = repository.plan_due(morning, &policy).await.unwrap();
    assert!(matches!(
        first.as_slice(),
        [(_, CreateOutcome::Inserted(_))]
    ));
    assert!(
        repository
            .plan_due(morning, &policy)
            .await
            .unwrap()
            .is_empty()
    );

    let evening = repository.plan_due(utc(12, 0), &policy).await.unwrap();
    assert!(matches!(
        evening.as_slice(),
        [(_, CreateOutcome::Inserted(_))]
    ));
    let covered = repository
        .covered_keys(utc(12, 30), &policy.audiences[0].id, 1)
        .await
        .unwrap();
    assert_eq!(covered.len(), 2);

    assert_eq!(
        repository.generation_candidate(0, utc(3, 1)).await,
        Err(PostgresOutboxError::InvalidDelivery)
    );

    let recipient = format!("generation_{}", std::process::id());
    let delivery = due_deliveries(utc(3, 0), &recipient, 1, &BTreeSet::new())
        .unwrap()
        .remove(0);
    let batch_id = match repository.create_planned(delivery).await.unwrap() {
        CreateOutcome::Inserted(id) => id,
        CreateOutcome::Existing(_) => unreachable!(),
    };
    let early_candidates = repository
        .pending_generation_ids(utc(3, 29), 16)
        .await
        .unwrap();
    assert!(
        !early_candidates.contains(&batch_id),
        "automatic generation must wait for the collection window to close"
    );

    let ready_candidates = repository
        .pending_generation_ids(utc(3, 30), 16)
        .await
        .unwrap();

    assert!(
        ready_candidates.contains(&batch_id),
        "the report must become generatable when the collection window closes"
    );

    for limit in [0, 17] {
        assert_eq!(
            repository.pending_generation_ids(utc(3, 1), limit).await,
            Err(PostgresOutboxError::InvalidDelivery)
        );
    }
    assert_eq!(
        repository.generation_candidate(batch_id, utc(2, 59)).await,
        Err(PostgresOutboxError::Conflict)
    );
    assert_eq!(
        repository.generation_candidate(batch_id, utc(3, 29)).await,
        Err(PostgresOutboxError::Conflict),
        "direct generation must not bypass the source collection window"
    );
    let planned = repository
        .generation_candidate(batch_id, utc(3, 30))
        .await
        .unwrap();
    assert_eq!(planned.status, GenerationStatus::Planned);
    assert_eq!(planned.batch_id, batch_id);
    assert_eq!(planned.key.recipient_id, recipient);
    assert_eq!(planned.key.kind, mcp_ozon::reporting::ReportKind::Morning);
    assert!(planned.generated_at <= utc(3, 30));

    // Snapshots may be observed after the job was created. Pinning must occur
    // after those inputs have been loaded, and survive recovery unchanged.
    let observed_after_creation = Utc::now();
    repository.start_generation(batch_id).await.unwrap();
    let pinned = repository
        .generation_candidate(batch_id, utc(3, 31))
        .await
        .unwrap()
        .generated_at;
    assert!(pinned >= observed_after_creation);
    assert!(pinned > planned.generated_at);
    assert_eq!(
        repository.start_generation(batch_id).await,
        Err(PostgresOutboxError::Conflict)
    );
    assert_eq!(
        repository
            .generation_candidate(batch_id, utc(3, 31))
            .await
            .unwrap()
            .generated_at,
        pinned
    );
    assert_eq!(
        repository
            .generation_candidate(batch_id, utc(3, 31))
            .await
            .unwrap()
            .status,
        GenerationStatus::Generating
    );
    repository
        .mark_ready(
            batch_id,
            &artifact_for_kind("2099/08/16", &recipient, "morning"),
        )
        .await
        .unwrap();
    assert_eq!(
        repository
            .generation_candidate(batch_id, utc(3, 32))
            .await
            .unwrap()
            .status,
        GenerationStatus::Ready
    );
    assert_eq!(
        repository
            .generation_candidate(batch_id, utc(3, 32))
            .await
            .unwrap()
            .generated_at,
        pinned
    );
    assert!(
        !repository
            .pending_generation_ids(utc(3, 32), 16)
            .await
            .unwrap()
            .contains(&batch_id)
    );

    let recovered_recipient = format!("recovered_{}", std::process::id());
    let recovered = due_deliveries(utc(13, 30), &recovered_recipient, 1, &BTreeSet::new())
        .unwrap()
        .remove(0);
    let recovered_id = match repository.create_planned(recovered).await.unwrap() {
        CreateOutcome::Inserted(id) => id,
        CreateOutcome::Existing(_) => unreachable!(),
    };
    let recovered_candidate = repository
        .generation_candidate(recovered_id, utc(13, 31))
        .await
        .unwrap();
    assert_eq!(recovered_candidate.batch_id, recovered_id);
    assert_eq!(recovered_candidate.key.recipient_id, recovered_recipient);
    assert_eq!(
        recovered_candidate.key.kind,
        mcp_ozon::reporting::ReportKind::Morning
    );
    assert_eq!(recovered_candidate.status, GenerationStatus::Planned);
    assert!(
        repository
            .pending_generation_ids(utc(13, 31), 16)
            .await
            .unwrap()
            .contains(&recovered_id)
    );
    assert_eq!(
        repository.generation_candidate(batch_id, utc(9, 1)).await,
        Err(PostgresOutboxError::Conflict)
    );
    drop(repository);
    verify_report_worker_runtime(&url).await;
}

#[tokio::test]
async fn pinned_generation_clock_is_immutable_and_legacy_artifacts_keep_creation_clock() {
    let Ok(url) = std::env::var("REPORT_OUTBOX_TEST_WORKER_URL") else {
        return;
    };
    let _guard = DB_TEST_LOCK.lock().await;
    let repository = PostgresOutboxRepository::connect(&Config::from_str(&url).unwrap())
        .await
        .unwrap();
    let admin_url = std::env::var("POSITION_REPOSITORY_TEST_ADMIN_URL").unwrap();
    let (mut admin, connection) = Config::from_str(&admin_url)
        .unwrap()
        .connect(tokio_postgres::NoTls)
        .await
        .unwrap();
    let driver = tokio::spawn(async move {
        connection.await.unwrap();
    });
    for legacy in [false, true] {
        let recipient = format!("generation_clock_{}_{legacy}", std::process::id());
        let delivery = due_deliveries(utc(3, 0), &recipient, 1, &BTreeSet::new())
            .unwrap()
            .remove(0);
        let CreateOutcome::Inserted(batch_id) = repository.create_planned(delivery).await.unwrap()
        else {
            panic!("fresh identity")
        };
        let created_at = repository
            .generation_candidate(batch_id, utc(3, 30))
            .await
            .unwrap()
            .generated_at;
        if legacy {
            // Simulate a generating batch from before migration 050, in one
            // transaction so other clients never see a disabled trigger.
            let transaction = admin.transaction().await.unwrap();
            transaction.batch_execute("ALTER TABLE daily_reporting.delivery_batches DISABLE TRIGGER delivery_batch_generation_clock").await.unwrap();
            transaction.execute("UPDATE daily_reporting.delivery_batches SET status='generating', updated_at=greatest(clock_timestamp(), updated_at + interval '1 microsecond') WHERE id=$1", &[&batch_id]).await.unwrap();
            transaction.batch_execute("ALTER TABLE daily_reporting.delivery_batches ENABLE TRIGGER delivery_batch_generation_clock").await.unwrap();
            transaction.commit().await.unwrap();
        } else {
            repository.start_generation(batch_id).await.unwrap();
        }
        let pinned = repository
            .generation_candidate(batch_id, utc(3, 30))
            .await
            .unwrap()
            .generated_at;
        if legacy {
            assert_eq!(pinned, created_at);
        } else {
            assert!(pinned > created_at);
        }
        let error = admin
            .execute(
                "UPDATE daily_reporting.delivery_batches SET generation_started_at=$2 WHERE id=$1",
                &[&batch_id, &(pinned + Duration::seconds(1))],
            )
            .await
            .unwrap_err();
        assert_eq!(
            error.as_db_error().unwrap().message(),
            "generation clock is immutable"
        );
        let artifact = artifact_for_kind("2099/08/16", &recipient, "morning");
        repository.mark_ready(batch_id, &artifact).await.unwrap();
        assert_eq!(
            repository
                .generation_candidate(batch_id, utc(3, 31))
                .await
                .unwrap()
                .generated_at,
            pinned
        );
    }
    drop(admin);
    driver.await.unwrap();
}
