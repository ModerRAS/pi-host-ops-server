//! Reaper: a long-running job lives while its lease is renewed; no fixed age cap.
use chrono::{Duration, Utc};
use pi_host_ops_server::db::{Db, SubmitOutcome};
use pi_host_ops_server::models::JobStatus;
use sqlx::Row;
use uuid::Uuid;

async fn setup() -> (Db, std::path::PathBuf) {
    let dir = std::env::temp_dir().join(format!("pi-host-ops-reaper-{}", Uuid::new_v4()));
    std::fs::create_dir_all(&dir).unwrap();
    let db = Db::connect(dir.join("t.db").to_str().unwrap()).await.unwrap();
    (db, dir)
}

async fn insert_running(db: &Db, claimed_ago: Duration, lease_in: Duration) -> Uuid {
    let id = Uuid::new_v4();
    let claimed = (Utc::now() - claimed_ago).to_rfc3339();
    let lease = (Utc::now() + lease_in).to_rfc3339();
    sqlx::query(
        "INSERT INTO jobs (id, job_type, status, payload, created_at, updated_at, delivery_mode, claimed_at, lease_expires_at)
         VALUES (?, 'act', 'running', '{}', ?, ?, 'steer', ?, ?)",
    )
    .bind(id.to_string())
    .bind(&claimed)
    .bind(&claimed)
    .bind(&claimed)
    .bind(&lease)
    .execute(db.pool())
    .await
    .unwrap();
    id
}

/// The old reaper killed any job whose `claimed_at` was >10 min old. A job that
/// has been running for hours with a live lease must survive.
#[tokio::test]
async fn old_running_job_with_live_lease_survives() {
    let (db, dir) = setup().await;
    let id = insert_running(&db, Duration::hours(6), Duration::minutes(1)).await;

    let reaped = db.mark_lease_expired().await.unwrap();

    assert!(reaped.is_empty(), "healthy long job must not be reaped");
    let job = db.get_job(id).await.unwrap().expect("job exists");
    assert_eq!(job.status, JobStatus::Running);

    drop(db);
    let _ = std::fs::remove_dir_all(&dir);
}

/// A worker that stops renewing is still recovered — by lease expiry only.
#[tokio::test]
async fn dead_worker_lease_expiry_times_out_job() {
    let (db, dir) = setup().await;
    let id = insert_running(&db, Duration::hours(6), Duration::seconds(-1)).await;

    let reaped = db.mark_lease_expired().await.unwrap();

    assert_eq!(reaped.len(), 1, "expired lease must be reaped");
    let row = sqlx::query("SELECT status, error FROM jobs WHERE id = ?")
        .bind(id.to_string())
        .fetch_one(db.pool())
        .await
        .unwrap();
    let status: String = row.try_get("status").unwrap();
    let error: Option<String> = row.try_get("error").unwrap();
    assert_eq!(status, "timed_out");
    assert_eq!(error.as_deref(), Some("lease expired"));

    drop(db);
    let _ = std::fs::remove_dir_all(&dir);
}

async fn insert_running_agent(db: &Db, agent_id: &str) -> Uuid {
    let id = Uuid::new_v4();
    let now = Utc::now().to_rfc3339();
    let lease = (Utc::now() + Duration::minutes(1)).to_rfc3339();
    sqlx::query(
        "INSERT INTO jobs (id, job_type, status, payload, created_at, updated_at, delivery_mode, claimed_at, agent_id, lease_expires_at)
         VALUES (?, 'act', 'running', '{}', ?, ?, 'steer', ?, ?, ?)",
    )
    .bind(id.to_string())
    .bind(&now)
    .bind(&now)
    .bind(&now)
    .bind(agent_id)
    .bind(&lease)
    .execute(db.pool())
    .await
    .unwrap();
    id
}

/// The worker reports completion itself. Idle success is `succeeded`.
/// A dead Pi child is `failed` with the unreachable error the worker sent.
/// Neither path is rewritten to `timed_out` (that status is lease expiry only).
#[tokio::test]
async fn worker_result_is_succeeded_or_failed_never_timed_out() {
    let (db, dir) = setup().await;

    let ok_id = insert_running_agent(&db, "agent-1").await;
    match db
        .submit_result(ok_id, "agent-1", true, None, None)
        .await
        .unwrap()
    {
        SubmitOutcome::Ok(job) => {
            assert_eq!(job.status, JobStatus::Succeeded);
            assert_ne!(job.status, JobStatus::TimedOut);
        }
        other => panic!("expected succeeded submit, {other:?}"),
    }

    let bad_id = insert_running_agent(&db, "agent-1").await;
    let err = "pi unreachable: pi stdout closed (pi child gone)";
    match db
        .submit_result(bad_id, "agent-1", false, None, Some(err))
        .await
        .unwrap()
    {
        SubmitOutcome::Ok(job) => {
            assert_eq!(job.status, JobStatus::Failed);
            assert_ne!(job.status, JobStatus::TimedOut);
            assert_eq!(job.error.as_deref(), Some(err));
        }
        other => panic!("expected failed submit, {other:?}"),
    }

    drop(db);
    let _ = std::fs::remove_dir_all(&dir);
}
