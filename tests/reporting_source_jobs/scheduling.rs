use tokio_postgres::Client;

pub async fn verify(admin: &Client) {
    // The administrator controls only disposable fixtures and simulated time.
    // Every claim/defer still exercises the installed production SQL function.
    admin
        .batch_execute(include_str!("scheduling.sql"))
        .await
        .expect("source deadlines must preserve eligibility and finish short-lived pages");
}
