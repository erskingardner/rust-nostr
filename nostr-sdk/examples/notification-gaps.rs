use nostr_sdk::prelude::*;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let relay_url = std::env::args().nth(1).expect("pass a relay URL");
    let client = Client::new();
    client.add_relay(relay_url).and_connect().await?;

    // Subscribe to notifications before asking for events.
    let mut notifications = client.notifications_with_gaps();
    client
        .subscribe(Filter::new().kind(Kind::TextNote).since(Timestamp::now()))
        .await?;

    let mut coverage_invalid = false;
    while let Some(update) = notifications.next().await {
        match update {
            NotificationUpdate::Lagged { skipped } => {
                coverage_invalid = true;
                eprintln!("This receiver missed {skipped} notifications; reacquisition is needed");
            }
            NotificationUpdate::Notification(ClientNotification::Event { event, .. }) => {
                // Events after a gap are still useful, but they do not repair it.
                println!("{} (coverage invalid: {coverage_invalid})", event.id);
            }
            NotificationUpdate::Notification(ClientNotification::Shutdown) => break,
            NotificationUpdate::Notification(_) => {}
        }
    }

    Ok(())
}
