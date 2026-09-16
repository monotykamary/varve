use std::collections::BTreeMap;
use std::env;
use std::time::{SystemTime, UNIX_EPOCH};

use varve_client::{Client, RequestId, Row, TableConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let url = env::var("VARVE_URL").unwrap_or_else(|_| "ws://127.0.0.1:8080/v1/ws".into());
    let token = env::var("VARVE_TOKEN").unwrap_or_default();
    let table = env::args()
        .nth(1)
        .unwrap_or_else(|| "example_metrics".into());
    let request_id = RequestId::new(
        env::args()
            .nth(2)
            .unwrap_or_else(|| "rust-example-1".into()),
    )?;

    let client = Client::connect(&url, &token).await?;
    if let Err(error) = client.create_table(&table, TableConfig::default()).await {
        eprintln!("create table returned {error}; continuing in case it already exists");
    }
    let timestamp_us = SystemTime::now()
        .duration_since(UNIX_EPOCH)?
        .as_micros()
        .try_into()?;
    let receipt = client
        .insert(
            &table,
            Row {
                timestamp_us,
                tenant: "example".into(),
                series: "temperature".into(),
                value: 21.5,
                tags: BTreeMap::from([("unit".into(), "celsius".into())]),
            },
            request_id,
        )
        .await?;
    println!(
        "committed sequence {} (duplicate={})",
        receipt.sequence, receipt.duplicate
    );
    println!("{}", client.query(&format!("SELECT * FROM {table}")).await?);
    client.close().await?;
    Ok(())
}
