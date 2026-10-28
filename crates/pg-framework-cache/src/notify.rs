use pg_framework_core::KeyId;

pub(crate) async fn notify_key(
    transaction: &tokio_postgres::Transaction<'_>,
    channel: &str,
    key: KeyId,
) -> Result<(), tokio_postgres::Error> {
    transaction
        .query_one("SELECT pg_notify($1, $2)", &[&channel, &key.to_hex()])
        .await?;
    Ok(())
}

pub(crate) async fn notify_keys(
    transaction: &tokio_postgres::Transaction<'_>,
    channel: &str,
    keys: &[Vec<u8>],
) -> Result<(), tokio_postgres::Error> {
    if keys.is_empty() {
        return Ok(());
    }
    transaction
        .query(
            "SELECT pg_notify($1, encode(key_hash, 'hex'))
             FROM unnest($2::bytea[]) AS input(key_hash)",
            &[&channel, &keys],
        )
        .await?;
    Ok(())
}

pub(crate) async fn notify_namespace(
    transaction: &tokio_postgres::Transaction<'_>,
    channel: &str,
    namespace_id: KeyId,
    clearing: bool,
) -> Result<(), tokio_postgres::Error> {
    let state = if clearing { '1' } else { '0' };
    let payload = format!("{state}{}", namespace_id.to_hex());
    transaction
        .query_one("SELECT pg_notify($1, $2)", &[&channel, &payload])
        .await?;
    Ok(())
}
