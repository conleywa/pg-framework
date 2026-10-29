use pg_framework_core::KeyId;

pub(crate) async fn notify_service(
    transaction: &tokio_postgres::Transaction<'_>,
    channel: &str,
    service_hash: KeyId,
) -> Result<(), tokio_postgres::Error> {
    transaction
        .query_one(
            "SELECT pg_notify($1, $2)",
            &[&channel, &service_hash.to_hex()],
        )
        .await?;
    Ok(())
}
