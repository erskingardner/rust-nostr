use std::borrow::Cow;
use std::cmp;
use std::collections::{HashMap, HashSet};
use std::future::IntoFuture;

use async_utility::{task, time};
use negentropy::{Id, Negentropy, NegentropyStorageVector};
use nostr::event::EventId;
use nostr::filter::Filter;
use nostr::message::{ClientMessage, RelayMessage, SubscriptionId};
use nostr::types::Timestamp;
use tokio::sync::broadcast;
use universal_time::Instant;

use crate::error::Error;
use crate::future::BoxedFuture;
use crate::relay::constants::{
    NEGENTROPY_BATCH_SIZE_DOWN, NEGENTROPY_FRAME_SIZE_LIMIT, NEGENTROPY_HIGH_WATER_UP,
    NEGENTROPY_LOW_WATER_UP,
};
use crate::relay::{Relay, RelayNotification, SyncOptions};

/// Relay negentropy reconciliation summary
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SyncSummary {
    /// Events that were stored locally (missing on relay)
    pub local: HashSet<EventId>,
    /// Events that were stored on relay (missing locally)
    pub remote: HashSet<EventId>,
    /// Events that are **successfully** sent to relays during reconciliation
    pub sent: HashSet<EventId>,
    /// Event that are **successfully** received from relay during reconciliation
    pub received: HashSet<EventId>,
    /// Send failures
    pub send_failures: HashMap<EventId, String>,
    // /// Receive failures
    // pub receive: HashMap<EventId, Vec<String>>,
}

/// Reconciliation progress and its terminal result for one relay.
///
/// Progress may be useful after failure, but only `error == None` means the
/// selected filter/window completed. Received events are not evidence of
/// downstream durable admission.
#[derive(Debug)]
pub struct RelaySyncOutcome {
    /// Progress observed before completion or failure.
    pub summary: SyncSummary,
    /// Failure that interrupted reconciliation, if any.
    pub error: Option<Error>,
}

/// Sync events with relay
///
/// <https://github.com/nostr-protocol/nips/blob/master/77.md>
/// Use [`SyncEvents::with_outcomes`] to retain partial progress when this relay
/// fails. Completion only concerns the selected filter/window.
#[must_use = "Does nothing unless you await!"]
pub struct SyncEvents<'relay> {
    relay: &'relay Relay,
    filter: Filter,
    items: Option<Vec<(EventId, Timestamp)>>,
    opts: SyncOptions,
}

impl<'relay> SyncEvents<'relay> {
    #[inline]
    pub(crate) fn new(relay: &'relay Relay, filter: Filter) -> Self {
        Self {
            relay,
            filter,
            items: None,
            opts: SyncOptions::new(),
        }
    }

    /// Set sync items
    ///
    /// When items are provided, negentropy items are NOT fetched from the database.
    #[inline]
    pub fn items<I>(mut self, items: I) -> Self
    where
        I: IntoIterator<Item = (EventId, Timestamp)>,
    {
        self.items = Some(items.into_iter().collect());
        self
    }

    /// Set sync options
    #[inline]
    pub fn opts(mut self, opts: SyncOptions) -> Self {
        self.opts = opts;
        self
    }

    /// Reconcile while retaining partial progress if the operation fails.
    ///
    /// Preflight errors still return `Err` before reconciliation begins.
    pub async fn with_outcomes(self) -> Result<RelaySyncOutcome, Error> {
        self.relay.inner.ensure_operational()?;
        if !self.relay.inner.capabilities.can_read() {
            return Err(Error::read_disabled());
        }

        let items: Vec<(EventId, Timestamp)> = match self.items {
            Some(items) => items,
            None => {
                let database = self.relay.inner.state.database();
                database.negentropy_items(self.filter.clone()).await?
            }
        };

        let mut summary = SyncSummary::default();
        let error = sync(self.relay, &self.filter, items, &self.opts, &mut summary)
            .await
            .err();
        Ok(RelaySyncOutcome { summary, error })
    }
}

#[inline]
async fn send_neg_msg(relay: &Relay, id: &SubscriptionId, message: &str) -> Result<(), Error> {
    relay
        .send_msg(ClientMessage::NegMsg {
            subscription_id: Cow::Borrowed(id),
            message: Cow::Borrowed(message),
        })
        .await
}

#[inline]
async fn send_neg_close(relay: &Relay, id: &SubscriptionId) -> Result<(), Error> {
    relay
        .send_msg(ClientMessage::NegClose {
            subscription_id: Cow::Borrowed(id),
        })
        .await
}

#[inline]
fn neg_id_to_event_id(id: Id) -> EventId {
    EventId::from_byte_array(id.to_bytes())
}

// A dropped sync future must not keep its request-owned subscription registered.
// Network CLOSE is best effort because the relay may already be disconnected.
struct SyncCleanup {
    relay: Relay,
    neg_id: SubscriptionId,
    down_id: SubscriptionId,
    armed: bool,
}

impl SyncCleanup {
    fn new(relay: &Relay, neg_id: SubscriptionId, down_id: SubscriptionId) -> Self {
        Self {
            relay: relay.clone(),
            neg_id,
            down_id,
            armed: true,
        }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for SyncCleanup {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }

        let relay = self.relay.clone();
        let neg_id = self.neg_id.clone();
        let down_id = self.down_id.clone();
        task::spawn(async move {
            relay.inner.remove_subscription(&down_id).await;
            let _ = relay
                .send_msg(ClientMessage::Close(Cow::Borrowed(&down_id)))
                .await;
            let _ = send_neg_close(&relay, &neg_id).await;
        });
    }
}

#[inline(never)]
async fn handle_neg_msg<I>(
    relay: &Relay,
    subscription_id: &SubscriptionId,
    msg: Option<Vec<u8>>,
    curr_have_ids: I,
    curr_need_ids: I,
    opts: &SyncOptions,
    output: &mut SyncSummary,
    have_ids: &mut Vec<EventId>,
    need_ids: &mut Vec<EventId>,
    sync_done: &mut bool,
) -> Result<(), Error>
where
    I: Iterator<Item = EventId>,
{
    let mut counter: u64 = 0;

    // If event ID wasn't already seen, add to the HAVE IDs
    // Add to HAVE IDs only if `do_up` is true
    for id in curr_have_ids.into_iter() {
        if output.local.insert(id) && opts.do_up() {
            have_ids.push(id);
            counter += 1;
        }
    }

    // If event ID wasn't already seen, add to the NEED IDs
    // Add to NEED IDs only if `do_down` is true
    for id in curr_need_ids.into_iter() {
        if output.remote.insert(id) && opts.do_down() {
            need_ids.push(id);
            counter += 1;
        }
    }

    if let Some(progress) = &opts.progress {
        progress.send_modify(|state| {
            state.total += counter;
        });
    }

    match msg {
        Some(query) => {
            let message: String = faster_hex::hex_string(&query);
            send_neg_msg(relay, subscription_id, &message).await
        }
        None => {
            // Mark sync as done
            *sync_done = true;

            // Send NEG-CLOSE message
            send_neg_close(relay, subscription_id).await
        }
    }
}

#[inline(never)]
async fn upload_neg_events(
    relay: &Relay,
    have_ids: &mut Vec<EventId>,
    in_flight_up: &mut HashSet<EventId>,
    opts: &SyncOptions,
) -> Result<(), Error> {
    // Check if it should skip the upload
    if !opts.do_up() || have_ids.is_empty() || in_flight_up.len() > NEGENTROPY_LOW_WATER_UP {
        return Ok(());
    }

    let mut num_sent = 0;

    while !have_ids.is_empty() && in_flight_up.len() < NEGENTROPY_HIGH_WATER_UP {
        if let Some(id) = have_ids.pop() {
            match relay.inner.state.database().event_by_id(&id).await {
                Ok(Some(event)) => {
                    in_flight_up.insert(id);
                    relay.send_msg(ClientMessage::event(event)).await?;
                    num_sent += 1;
                }
                Ok(None) => {
                    // Event not found
                }
                Err(e) => tracing::error!(
                    url = %relay.url(),
                    error = %e,
                    "Can't upload event."
                ),
            }
        }
    }

    // Update progress
    if let Some(progress) = &opts.progress {
        progress.send_modify(|state| {
            state.current += num_sent;
        });
    }

    if num_sent > 0 {
        tracing::info!(
            "Negentropy UP for '{}': {} events ({} remaining)",
            relay.url(),
            num_sent,
            have_ids.len()
        );
    }

    Ok(())
}

#[inline(never)]
async fn req_neg_events(
    relay: &Relay,
    need_ids: &mut Vec<EventId>,
    in_flight_down: &mut bool,
    down_sub_id: &SubscriptionId,
    opts: &SyncOptions,
) -> Result<(), Error> {
    // Check if it should skip the download
    if !opts.do_down() || need_ids.is_empty() || *in_flight_down {
        return Ok(());
    }

    let capacity: usize = cmp::min(need_ids.len(), NEGENTROPY_BATCH_SIZE_DOWN);
    let mut ids: Vec<EventId> = Vec::with_capacity(capacity);

    while !need_ids.is_empty() && ids.len() < NEGENTROPY_BATCH_SIZE_DOWN {
        if let Some(id) = need_ids.pop() {
            ids.push(id);
        }
    }

    tracing::info!(
        "Negentropy DOWN for '{}': {} events ({} remaining)",
        relay.url(),
        ids.len(),
        need_ids.len()
    );

    // Update progress
    if let Some(progress) = &opts.progress {
        progress.send_modify(|state| {
            state.current += ids.len() as u64;
        });
    }

    let filter = Filter::new().ids(ids);
    let msg: ClientMessage = ClientMessage::Req {
        subscription_id: Cow::Borrowed(down_sub_id),
        filters: vec![Cow::Borrowed(&filter)],
    };

    // Register an auto-closing subscription
    relay
        .inner
        .add_auto_closing_subscription(down_sub_id.clone(), vec![filter.clone()])
        .await?;

    // Send msg
    if let Err(e) = relay.send_msg(msg).await {
        // Remove previously added subscription
        relay.inner.remove_subscription(down_sub_id).await;

        // Propagate error
        return Err(e);
    }

    *in_flight_down = true;

    Ok(())
}

/// Returns `true` if the events was in the `in_flight_up` collection.
#[inline(never)]
fn handle_neg_ok(
    relay: &Relay,
    in_flight_up: &mut HashSet<EventId>,
    event_id: EventId,
    status: bool,
    message: Cow<'_, str>,
    output: &mut SyncSummary,
) -> bool {
    if in_flight_up.remove(&event_id) {
        if status {
            output.sent.insert(event_id);
        } else {
            tracing::error!(
                url = %relay.url(),
                id = %event_id,
                msg = %message,
                "Can't upload event."
            );

            output.send_failures.insert(event_id, message.to_string());
        }

        true
    } else {
        false
    }
}

/// New negentropy protocol
#[inline(never)]
pub(super) async fn sync(
    relay: &Relay,
    filter: &Filter,
    items: Vec<(EventId, Timestamp)>,
    opts: &SyncOptions,
    output: &mut SyncSummary,
) -> Result<(), Error> {
    // Prepare the negentropy client
    let storage: NegentropyStorageVector = prepare_negentropy_storage(items)?;
    let mut negentropy: Negentropy<NegentropyStorageVector> =
        Negentropy::borrowed(&storage, NEGENTROPY_FRAME_SIZE_LIMIT)?;

    // Initiate reconciliation
    let initial_message: Vec<u8> = negentropy.initiate()?;

    // Subscribe
    let mut notifications = relay.inner.internal_notification_sender.subscribe();
    let mut temp_notifications = relay.inner.internal_notification_sender.subscribe();

    // Send the initial negentropy message
    let sub_id: SubscriptionId = SubscriptionId::generate();
    let down_sub_id: SubscriptionId = SubscriptionId::generate();
    let open_msg: ClientMessage = ClientMessage::NegOpen {
        subscription_id: Cow::Borrowed(&sub_id),
        filter: Cow::Borrowed(filter),
        initial_message: Cow::Owned(faster_hex::hex_string(&initial_message)),
    };
    relay.send_msg(open_msg).await?;
    let mut cleanup = SyncCleanup::new(relay, sub_id.clone(), down_sub_id.clone());

    // Check if negentropy is supported
    check_negentropy_support(&sub_id, opts, &mut temp_notifications).await?;

    let mut in_flight_up: HashSet<EventId> = HashSet::new();
    let mut in_flight_down: bool = false;
    let mut sync_done: bool = false;
    let mut have_ids: Vec<EventId> = Vec::new();
    let mut need_ids: Vec<EventId> = Vec::new();
    let mut last_relevant_msg: Instant = Instant::now();

    // Start reconciliation
    loop {
        let notification = time::timeout(Some(opts.idle_timeout), notifications.recv())
            .await
            .ok_or(Error::timeout())??;

        if last_relevant_msg.elapsed() > opts.idle_timeout {
            return Err(Error::timeout());
        }

        match notification {
            RelayNotification::Message { message } => {
                let is_relevant: bool = match *message {
                    RelayMessage::NegMsg {
                        subscription_id,
                        message,
                    } => {
                        #[allow(clippy::collapsible_match)]
                        if subscription_id.as_ref() == &sub_id {
                            let mut curr_have_ids: Vec<Id> = Vec::new();
                            let mut curr_need_ids: Vec<Id> = Vec::new();

                            match message.len().checked_div(2) {
                                Some(size) => {
                                    // Parse message
                                    let mut query: Vec<u8> = vec![0; size];
                                    faster_hex::hex_decode(message.as_bytes(), &mut query)?;

                                    // Reconcile
                                    let msg: Option<Vec<u8>> = negentropy.reconcile_with_ids(
                                        &query,
                                        &mut curr_have_ids,
                                        &mut curr_need_ids,
                                    )?;

                                    // Handle the message
                                    handle_neg_msg(
                                        relay,
                                        &subscription_id,
                                        msg,
                                        curr_have_ids.into_iter().map(neg_id_to_event_id),
                                        curr_need_ids.into_iter().map(neg_id_to_event_id),
                                        opts,
                                        output,
                                        &mut have_ids,
                                        &mut need_ids,
                                        &mut sync_done,
                                    )
                                    .await?;
                                }
                                None => {
                                    tracing::warn!("Can't divide negentropy message.")
                                }
                            }

                            // Relevant to this sync
                            true
                        } else {
                            // Not relevant to this sync
                            false
                        }
                    }
                    RelayMessage::NegErr {
                        subscription_id,
                        message,
                    } => {
                        #[allow(clippy::collapsible_match)]
                        if subscription_id.as_ref() == &sub_id {
                            return Err(Error::relay_msg(message.into_owned()));
                        } else {
                            // Not relevant to this sync
                            false
                        }
                    }
                    RelayMessage::Ok {
                        event_id,
                        status,
                        message,
                    } => handle_neg_ok(relay, &mut in_flight_up, event_id, status, message, output),
                    RelayMessage::Event {
                        subscription_id,
                        event,
                    } => {
                        #[allow(clippy::collapsible_match)]
                        if subscription_id.as_ref() == &down_sub_id {
                            output.received.insert(event.id);

                            // Relevant to this sync
                            true
                        } else {
                            // Not relevant to this sync
                            false
                        }
                    }
                    RelayMessage::EndOfStoredEvents(subscription_id) => {
                        #[allow(clippy::collapsible_match)]
                        if subscription_id.as_ref() == &down_sub_id {
                            in_flight_down = false;

                            // Remove subscription
                            relay.inner.remove_subscription(&down_sub_id).await;

                            // Close subscription
                            relay
                                .send_msg(ClientMessage::Close(Cow::Borrowed(&down_sub_id)))
                                .await?;

                            // Relevant to this sync
                            true
                        } else {
                            // Not relevant to this sync
                            false
                        }
                    }
                    RelayMessage::Closed {
                        subscription_id, ..
                    } => {
                        #[allow(clippy::collapsible_match)]
                        if subscription_id.as_ref() == &down_sub_id {
                            in_flight_down = false;

                            // NOTE: the subscription is removed in the `InnerRelay::handle_relay_message` method,
                            // so there is no need to try to remove it also here.

                            // Relevant to this sync
                            true
                        } else {
                            // Not relevant to this sync
                            false
                        }
                    }
                    _ => false,
                };

                // Send events
                upload_neg_events(relay, &mut have_ids, &mut in_flight_up, opts).await?;

                // Get events
                req_neg_events(
                    relay,
                    &mut need_ids,
                    &mut in_flight_down,
                    &down_sub_id,
                    opts,
                )
                .await?;

                // NOTE: update this after the uploading and requesting of the events, as it may require some time.
                if is_relevant {
                    last_relevant_msg = Instant::now();
                }
            }
            RelayNotification::RelayStatus { status } if status.is_disconnected() => {
                return Err(Error::not_connected());
            }
            _ => (),
        };

        if sync_done
            && have_ids.is_empty()
            && need_ids.is_empty()
            && in_flight_up.is_empty()
            && !in_flight_down
        {
            break;
        }
    }

    tracing::info!(url = %relay.url(), "Negentropy reconciliation terminated.");

    cleanup.disarm();

    Ok(())
}

fn prepare_negentropy_storage(
    items: Vec<(EventId, Timestamp)>,
) -> Result<NegentropyStorageVector, Error> {
    // Compose negentropy storage
    let mut storage = NegentropyStorageVector::with_capacity(items.len());

    // Add items
    for (id, timestamp) in items.into_iter() {
        let id: Id = Id::from_byte_array(id.to_bytes());
        storage.insert(timestamp.as_secs(), id)?;
    }

    // Seal
    storage.seal()?;

    // Build negentropy client
    Ok(storage)
}

/// Check if negentropy is supported
#[inline(never)]
async fn check_negentropy_support(
    sub_id: &SubscriptionId,
    opts: &SyncOptions,
    temp_notifications: &mut broadcast::Receiver<RelayNotification>,
) -> Result<(), Error> {
    time::timeout(Some(opts.initial_timeout), async {
        loop {
            let notification = temp_notifications.recv().await?;

            if let RelayNotification::Message { message } = notification {
                match *message {
                    RelayMessage::NegMsg {
                        subscription_id, ..
                    } if subscription_id.as_ref() == sub_id => {
                        break;
                    }
                    RelayMessage::NegErr {
                        subscription_id,
                        message,
                    } if subscription_id.as_ref() == sub_id => {
                        return Err(Error::relay_msg(message.into_owned()));
                    }
                    RelayMessage::Notice(message) => {
                        if message == "ERROR: negentropy error: negentropy query missing elements" {
                            // The relay expects the deprecated five-element NEG-OPEN format.
                            return Err(negentropy::Error::UnsupportedProtocolVersion.into());
                        } else if message.contains("bad msg")
                            && (message.contains("unknown cmd")
                                || message.contains("negentropy")
                                || message.contains("NEG-"))
                        {
                            return Err(Error::negentropy_not_supported());
                        } else if message.contains("bad msg: invalid message")
                            && message.contains("NEG-OPEN")
                        {
                            return Err(Error::unknown_negentropy_error());
                        }
                    }
                    _ => (),
                }
            }
        }

        Ok(())
    })
    .await
    .ok_or_else(Error::timeout)?
}

impl<'relay> IntoFuture for SyncEvents<'relay> {
    type Output = Result<SyncSummary, Error>;
    type IntoFuture = BoxedFuture<'relay, Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move {
            let outcome = self.with_outcomes().await?;
            match outcome.error {
                Some(error) => Err(error),
                None => Ok(outcome.summary),
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};
    use std::sync::Arc;
    use std::time::Duration;

    use nostr_memory::prelude::*;
    use tokio::sync::broadcast;

    use super::*;
    use crate::error::ErrorKind;
    use crate::local_relay::*;
    use crate::relay::{SyncDirection, SyncOptions};

    #[tokio::test]
    async fn cancelled_sync_cleans_only_its_subscription() {
        let relay = Relay::new("wss://relay.example.com".parse().unwrap());
        let down_id = SubscriptionId::new("sync-download");
        let unrelated_id = SubscriptionId::new("unrelated-live");
        relay
            .inner
            .add_auto_closing_subscription(down_id.clone(), vec![Filter::new()])
            .await
            .unwrap();
        relay
            .inner
            .add_long_lived_subscription(unrelated_id.clone(), vec![Filter::new()])
            .await
            .unwrap();

        let cleanup = SyncCleanup::new(&relay, SubscriptionId::new("sync-neg"), down_id.clone());
        drop(cleanup);

        tokio::time::timeout(Duration::from_secs(1), async {
            while relay.inner.has_subscription(&down_id).await {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(relay.inner.has_subscription(&unrelated_id).await);
    }

    #[tokio::test]
    async fn test_check_negentropy_support_times_out() {
        let (_tx, mut rx) = broadcast::channel(1);
        let sub_id = SubscriptionId::generate();
        let opts = SyncOptions::default().initial_timeout(Duration::from_millis(10));

        let error = check_negentropy_support(&sub_id, &opts, &mut rx)
            .await
            .unwrap_err();

        assert_eq!(error.kind(), ErrorKind::Timeout);
    }

    #[tokio::test]
    async fn test_check_negentropy_support_fails_when_notifications_close() {
        let (tx, mut rx) = broadcast::channel(1);
        drop(tx);

        let sub_id = SubscriptionId::generate();
        let opts = SyncOptions::default().initial_timeout(Duration::from_secs(1));

        let error = check_negentropy_support(&sub_id, &opts, &mut rx)
            .await
            .unwrap_err();

        assert_eq!(error.kind(), ErrorKind::Other);
    }

    #[tokio::test]
    async fn test_negentropy_sync() {
        // Mock relay
        let mock = MockRelay::run().await.unwrap();
        let url = mock.url().await;

        // Database
        let database = Arc::new(MemoryDatabase::unbounded());

        // Build events to store in the local database
        let local_events = [
            EventBuilder::new(Kind::TextNote, "Local 1")
                .finalize(&Keys::generate())
                .unwrap(),
            EventBuilder::new(Kind::TextNote, "Local 2")
                .finalize(&Keys::generate())
                .unwrap(),
            EventBuilder::new(Kind::Custom(123), "Local 123")
                .finalize(&Keys::generate())
                .unwrap(),
        ];

        // Save an event to the local database
        for event in local_events.iter() {
            database.save_event(event).await.unwrap();
        }
        assert_eq!(database.count(Filter::new()).await.unwrap(), 3);

        // Relay
        let relay = Relay::builder(url).database(database.clone()).build();

        // Connect
        relay
            .try_connect()
            .timeout(Duration::from_secs(2))
            .await
            .unwrap();

        // Build events to send to the relay
        let relays_events = [
            // Event in common with the local database
            local_events[0].clone(),
            EventBuilder::new(Kind::TextNote, "Test 2")
                .finalize(&Keys::generate())
                .unwrap(),
            EventBuilder::new(Kind::TextNote, "Test 3")
                .finalize(&Keys::generate())
                .unwrap(),
            EventBuilder::new(Kind::Custom(123), "Test 4")
                .finalize(&Keys::generate())
                .unwrap(),
        ];

        // Send events to the relays
        for event in relays_events.iter() {
            relay.send_event(event).await.unwrap();
        }

        // Sync
        let filter = Filter::new().kind(Kind::TextNote);
        let opts = SyncOptions::default().direction(SyncDirection::Both);
        let output = relay.sync(filter).opts(opts).await.unwrap();

        assert_eq!(
            output,
            SyncSummary {
                local: HashSet::from([local_events[1].id]),
                remote: HashSet::from([relays_events[1].id, relays_events[2].id]),
                sent: HashSet::from([local_events[1].id]),
                received: HashSet::from([relays_events[1].id, relays_events[2].id]),
                send_failures: HashMap::new(),
            }
        );
    }
}
