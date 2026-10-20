use std::collections::HashMap;

use async_utility::task;
use futures::future::{AbortHandle, join_all};
use nostr::types::RelayUrl;
use tokio::sync::{oneshot, watch};

use super::req_target::ReqTarget;
use super::util::build_targets;
use crate::client::Client;
use crate::error::Error;
use crate::relay::{AcquisitionLimits, RelayAcquisition, acquire_relay};

/// A bounded result for every selected relay. No cross-relay deduplication
/// state is allocated; callers can deduplicate the bounded batches themselves.
#[derive(Debug)]
pub struct AcquisitionReport {
    /// Relay-scoped partial results and terminal outcomes.
    pub relays: HashMap<RelayUrl, RelayAcquisition>,
}

/// Cloneable cancellation signal for an acquisition whose `finish` future
/// may already be waiting in another task.
#[derive(Clone)]
pub struct AcquisitionCanceller(watch::Sender<bool>);

impl AcquisitionCanceller {
    /// Request cancellation without discarding the partial report.
    pub fn cancel(&self) {
        self.0.send_replace(true);
    }
}

/// Background acquisition that continues draining its one-item activity
/// queues while the caller is not polling. Call `cancel` or retain a
/// [`AcquisitionCanceller`] to obtain a partial result with `Cancelled`
/// outcomes, or drop the handle to discard the result
/// and release request-owned state. A terminal report remains in a one-slot
/// result channel until `finish` is called or this handle is dropped.
#[must_use = "Dropping this handle cancels acquisition"]
pub struct AcquisitionHandle {
    cancel: watch::Sender<bool>,
    abort: AbortHandle,
    result: Option<oneshot::Receiver<AcquisitionReport>>,
}

impl AcquisitionHandle {
    /// Obtain a cancellation signal to retain while `finish` is awaited.
    pub fn canceller(&self) -> AcquisitionCanceller {
        AcquisitionCanceller(self.cancel.clone())
    }

    /// Request cancellation. `finish` still returns partial per-relay data.
    pub fn cancel(&self) {
        self.cancel.send_replace(true);
    }

    /// Wait for every relay to report its terminal state.
    pub async fn finish(mut self) -> Result<AcquisitionReport, Error> {
        let result = self.result.take().expect("acquisition result exists");
        result.await.map_err(Error::from)
    }
}

impl Drop for AcquisitionHandle {
    fn drop(&mut self) {
        self.cancel();
        self.abort.abort();
    }
}

impl Client {
    /// Start a bounded, cancellable acquisition from selected relays.
    ///
    /// Every relay has independent item and serialized-event-byte budgets.
    /// At most `max_relays` relay requests run at once, so the total retained
    /// distinct event JSON estimate is at most `max_relays * max_event_bytes`.
    /// Shared connection buffers, parsed events, and temporary serialization
    /// are additional allocations outside this request budget. Completion
    /// only satisfies the chosen request exit policy and says nothing about
    /// durable admission or complete historical coverage. Acquisition events
    /// are not saved to the client's shared event database or forwarded as
    /// ordinary live Event notifications; the returned report owns them.
    pub async fn acquire_events<'url, F>(
        &self,
        target: F,
        limits: AcquisitionLimits,
    ) -> Result<AcquisitionHandle, Error>
    where
        F: Into<ReqTarget<'url>>,
    {
        limits.validate()?;
        let targets = build_targets(self, target.into()).await?;
        if targets.is_empty() {
            return Err(Error::invalid_msg("acquisition has no target relays"));
        }
        if targets.len() > limits.max_relays {
            return Err(Error::limit_exceeded("too many acquisition relays"));
        }

        let mut requests = Vec::with_capacity(targets.len());
        for (url, filters) in targets {
            let relay = self.pool().relay(&url).await;
            requests.push((url, filters, relay));
        }
        let (cancel_tx, cancel_rx) = watch::channel(false);
        let (result_tx, result_rx) = oneshot::channel();
        let work = async move {
            let futures = requests.into_iter().map(|(url, filters, relay)| {
                let cancelled = cancel_rx.clone();
                async move {
                    let outcome = match relay {
                        Some(relay) => acquire_relay(relay, filters, limits, cancelled).await,
                        None => RelayAcquisition::missing_relay(),
                    };
                    (url, outcome)
                }
            });
            let relays = join_all(futures).await.into_iter().collect();
            let _ = result_tx.send(AcquisitionReport { relays });
        };
        let abort = task::abortable(work);
        Ok(AcquisitionHandle {
            cancel: cancel_tx,
            abort,
            result: Some(result_rx),
        })
    }
}

#[cfg(test)]
mod tests {
    use std::borrow::Cow;
    use std::collections::BTreeSet;
    use std::time::{Duration, Instant};

    use async_wsocket::native::{self, Message as WsMessage};
    use futures::{SinkExt, Stream, StreamExt};
    use nostr::event::{Event, EventBuilder, EventId, FinalizeEvent, Kind, Tag};
    use nostr::filter::{Filter, MatchEventOptions};
    use nostr::key::Keys;
    use nostr::message::{ClientMessage, RelayMessage, SubscriptionId};
    use nostr::types::Timestamp;
    use tokio::net::TcpListener;
    use tokio::sync::mpsc;
    use tokio::task::JoinHandle;
    use tokio::time::{sleep, timeout};

    use super::*;
    use crate::local_relay::MockRelay;
    use crate::relay::{
        AcquisitionEnd, Relay, RelayLimits, RelayMessageLimits, RelayNotification, ReqExitPolicy,
    };
    use crate::test_utils::setup_client;

    #[tokio::test]
    async fn byte_rejected_acquisition_does_not_preempt_later_live_subscription() {
        let relay = MockRelay::run().await.unwrap();
        let event = EventBuilder::new(Kind::TextNote, "x".repeat(4_096))
            .finalize(&Keys::generate())
            .unwrap();
        relay.add_event(event.clone()).await.unwrap();
        let url = relay.url().await;
        let client = setup_client(url.clone()).await;
        let connection = client.pool().relay(&url).await.unwrap();
        let mut notifications = client.notifications();
        let report = client
            .acquire_events(
                ReqTarget::single(&url, [Filter::new().id(event.id)]),
                AcquisitionLimits::new(1, 4, event.as_json().len() - 1, Duration::from_secs(2)),
            )
            .await
            .unwrap()
            .finish()
            .await
            .unwrap();
        let result = &report.relays[&url];
        assert!(matches!(result.end, AcquisitionEnd::ByteBudgetExceeded));
        assert_eq!(result.received_items, 1);
        assert!(result.events.is_empty());
        assert_eq!(
            client.database().check_id(&event.id).await.unwrap(),
            nostr_database::DatabaseEventStatus::NotExistent
        );
        // Raw Message::Event remains observable for request accounting, but
        // the first-seen delivery notification must belong to the live REQ.
        while let Ok(Some(notification)) =
            timeout(Duration::from_millis(10), notifications.next()).await
        {
            assert!(!matches!(
                notification,
                crate::client::ClientNotification::Event { .. }
            ));
        }
        let live_id = connection
            .subscribe(Filter::new().id(event.id))
            .await
            .unwrap();
        let received = timeout(Duration::from_secs(2), async {
            loop {
                if let Some(crate::client::ClientNotification::Event {
                    subscription_id,
                    event: received,
                    ..
                }) = notifications.next().await
                {
                    break (subscription_id, received.id);
                }
            }
        })
        .await
        .expect("live REQ receives the event after acquisition rejected its byte budget");
        assert_eq!(received, (live_id, event.id));
    }

    #[tokio::test]
    async fn two_relay_acquisition_keeps_shared_cache_free_until_live_delivery() {
        let left = MockRelay::run().await.unwrap();
        let right = MockRelay::run().await.unwrap();
        let event = EventBuilder::new(Kind::TextNote, "cross-relay copy")
            .finalize(&Keys::generate())
            .unwrap();
        left.add_event(event.clone()).await.unwrap();
        right.add_event(event.clone()).await.unwrap();
        let left_url = left.url().await;
        let right_url = right.url().await;
        let client = setup_client(left_url.clone()).await;
        client.add_relay(&right_url).await.unwrap();
        client
            .try_connect_relay(right_url.clone(), Duration::from_secs(3))
            .await
            .unwrap();
        let mut notifications = client.notifications();
        let result = client
            .acquire_events(
                ReqTarget::manual(vec![
                    (left_url.clone(), vec![Filter::new().id(event.id)]),
                    (right_url.clone(), vec![Filter::new().id(event.id)]),
                ]),
                limits(4),
            )
            .await
            .unwrap()
            .finish()
            .await
            .unwrap();
        for url in [&left_url, &right_url] {
            let endpoint = &result.relays[url];
            assert!(matches!(endpoint.end, AcquisitionEnd::Completed));
            assert_eq!(endpoint.events.len(), 1);
        }
        assert_eq!(
            client.database().check_id(&event.id).await.unwrap(),
            nostr_database::DatabaseEventStatus::NotExistent
        );
        while let Ok(Some(notification)) =
            timeout(Duration::from_millis(10), notifications.next()).await
        {
            assert!(!matches!(
                notification,
                crate::client::ClientNotification::Event { .. }
            ));
        }
        let live_id = client
            .pool()
            .relay(&left_url)
            .await
            .unwrap()
            .subscribe(Filter::new().id(event.id))
            .await
            .unwrap();
        let delivered = timeout(Duration::from_secs(2), async {
            loop {
                if let Some(crate::client::ClientNotification::Event {
                    subscription_id,
                    event: received,
                    ..
                }) = notifications.next().await
                {
                    break (subscription_id, received.id);
                }
            }
        })
        .await
        .unwrap();
        assert_eq!(delivered, (live_id, event.id));
    }

    fn limits(items: usize) -> AcquisitionLimits {
        AcquisitionLimits::new(2, items, 100_000, Duration::from_secs(2))
    }

    #[derive(Clone, Copy)]
    enum FilteredEvent {
        Expired,
        Invalid,
    }

    fn filtered_event(keys: &Keys, at: u64, rejection: FilteredEvent) -> Event {
        let builder: EventBuilder = EventBuilder::new(Kind::TextNote, format!("raw-{at}"))
            .custom_created_at(Timestamp::from(at));
        match rejection {
            FilteredEvent::Expired => builder
                .tags([Tag::expiration(Timestamp::from(1))])
                .finalize(keys)
                .unwrap(),
            FilteredEvent::Invalid => {
                let mut event: Event = builder.finalize(keys).unwrap();
                event.sig = EventBuilder::new(Kind::TextNote, "signature for another event")
                    .finalize(keys)
                    .unwrap()
                    .sig;
                event
            }
        }
    }

    // Unlike MockRelay's database, this socket fixture deliberately serves
    // expired events and invalid signatures without admitting them first.
    async fn raw_history_relay(
        mut events: Vec<Event>,
    ) -> (RelayUrl, JoinHandle<()>, mpsc::Receiver<SubscriptionId>) {
        events.sort_by_key(|event| std::cmp::Reverse(event.created_at));
        let listener: TcpListener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url: RelayUrl =
            RelayUrl::parse(&format!("ws://{}", listener.local_addr().unwrap())).unwrap();
        let (closed_tx, closed_rx) = mpsc::channel(4);
        let server: JoinHandle<()> = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut socket = native::accept_async(stream).await.unwrap();
            while let Some(Ok(frame)) = socket.next().await {
                let WsMessage::Text(text) = frame else {
                    continue;
                };
                match ClientMessage::from_json(text.as_str()) {
                    Ok(ClientMessage::Req {
                        subscription_id,
                        filters,
                    }) => {
                        for filter in filters {
                            for event in events
                                .iter()
                                .filter(|event| filter.match_event(event, MatchEventOptions::new()))
                                .take(filter.limit.unwrap_or(usize::MAX))
                            {
                                let reply: RelayMessage = RelayMessage::Event {
                                    subscription_id: Cow::Borrowed(subscription_id.as_ref()),
                                    event: Cow::Borrowed(event),
                                };
                                if socket
                                    .send(WsMessage::Text(reply.as_json().into()))
                                    .await
                                    .is_err()
                                {
                                    return;
                                }
                            }
                        }
                        let eose: RelayMessage = RelayMessage::eose(subscription_id.into_owned());
                        if socket
                            .send(WsMessage::Text(eose.as_json().into()))
                            .await
                            .is_err()
                        {
                            return;
                        }
                    }
                    Ok(ClientMessage::Close(id)) => {
                        // A queued reply after CLOSE must not affect the next
                        // request's scalar evidence or enter the shared cache.
                        if let Some(event) = events.last() {
                            let late: RelayMessage = RelayMessage::Event {
                                subscription_id: Cow::Borrowed(id.as_ref()),
                                event: Cow::Borrowed(event),
                            };
                            if socket
                                .send(WsMessage::Text(late.as_json().into()))
                                .await
                                .is_err()
                            {
                                return;
                            }
                        }
                        let _ = closed_tx.try_send(id.into_owned());
                    }
                    _ => {}
                }
            }
        });
        (url, server, closed_rx)
    }

    #[tokio::test]
    async fn raw_full_page_finds_older() {
        for rejection in [FilteredEvent::Expired, FilteredEvent::Invalid] {
            let keys: Keys = Keys::generate();
            let older: Event = EventBuilder::new(Kind::TextNote, "older valid commit")
                .custom_created_at(Timestamp::from(1))
                .finalize(&keys)
                .unwrap();
            let mut events: Vec<Event> = (257..513)
                .map(|at| filtered_event(&keys, at, rejection))
                .collect();
            let filtered_id: EventId = events[0].id;
            events.push(older.clone());
            let (url, server, mut closed) = raw_history_relay(events).await;
            let client: Client = setup_client(url.clone()).await;
            let report: AcquisitionReport = client
                .acquire_events(
                    ReqTarget::single(&url, [Filter::new().kind(Kind::TextNote).limit(256)]),
                    limits(256),
                )
                .await
                .unwrap()
                .finish()
                .await
                .unwrap();
            let page: &RelayAcquisition = &report.relays[&url];
            assert!(matches!(page.end, AcquisitionEnd::Completed));
            assert_eq!(page.raw_received_items, 256);
            assert_eq!(page.oldest_raw_created_at, Some(Timestamp::from(257)));
            assert_eq!(page.received_items, 0);
            assert_eq!(page.received_event_bytes, 0);
            assert!(page.events.is_empty());
            timeout(Duration::from_secs(2), closed.recv())
                .await
                .unwrap()
                .unwrap();

            let report: AcquisitionReport = client
                .acquire_events(
                    ReqTarget::single(
                        &url,
                        [Filter::new()
                            .kind(Kind::TextNote)
                            .until(Timestamp::from(256))
                            .limit(256)],
                    ),
                    limits(256),
                )
                .await
                .unwrap()
                .finish()
                .await
                .unwrap();
            let page: &RelayAcquisition = &report.relays[&url];
            assert!(matches!(page.end, AcquisitionEnd::Completed));
            assert_eq!(page.raw_received_items, 1);
            assert_eq!(page.oldest_raw_created_at, Some(older.created_at));
            assert_eq!(page.received_items, 1);
            assert_eq!(page.events.first().map(|event| event.id), Some(older.id));
            for id in [filtered_id, older.id] {
                assert_eq!(
                    client.database().check_id(&id).await.unwrap(),
                    nostr_database::DatabaseEventStatus::NotExistent
                );
            }
            server.abort();
        }
    }

    #[tokio::test]
    async fn raw_parse_loss_is_scoped() {
        enum Expected {
            Partial,
            Completed,
        }
        for (input, expected) in [
            ("[".to_owned(), Expected::Partial),
            (
                format!(r#"["NOTICE","{}"]"#, "x".repeat(1_024)),
                Expected::Partial,
            ),
            (
                format!(r#"["OK","{}",true]"#, "00".repeat(32)),
                Expected::Completed,
            ),
            (r#"["COUNT","request",{}]"#.to_owned(), Expected::Completed),
            (r#"["UNKNOWN",{}]"#.to_owned(), Expected::Completed),
        ] {
            let listener: TcpListener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let url: RelayUrl =
                RelayUrl::parse(&format!("ws://{}", listener.local_addr().unwrap())).unwrap();
            let server: JoinHandle<()> = tokio::spawn(async move {
                let (stream, _) = listener.accept().await.unwrap();
                let mut socket = native::accept_async(stream).await.unwrap();
                let mut requests: Vec<SubscriptionId> = Vec::with_capacity(2);
                let mut input: Option<String> = Some(input);
                while let Some(Ok(frame)) = socket.next().await {
                    let WsMessage::Text(text) = frame else {
                        continue;
                    };
                    let Ok(ClientMessage::Req {
                        subscription_id, ..
                    }) = ClientMessage::from_json(text.as_str())
                    else {
                        continue;
                    };
                    let id: SubscriptionId = subscription_id.into_owned();
                    if requests.len() < 2 {
                        requests.push(id);
                        if requests.len() == 2 {
                            socket
                                .send(WsMessage::Text(input.take().unwrap().into()))
                                .await
                                .unwrap();
                            for request in &requests {
                                socket
                                    .send(WsMessage::Text(
                                        RelayMessage::EndOfStoredEvents(Cow::Borrowed(request))
                                            .as_json()
                                            .into(),
                                    ))
                                    .await
                                    .unwrap();
                            }
                        }
                    } else {
                        socket
                            .send(WsMessage::Text(RelayMessage::eose(id).as_json().into()))
                            .await
                            .unwrap();
                    }
                }
            });
            let client: Client = Client::default();
            client
                .add_relay(&url)
                .limits(RelayLimits {
                    messages: RelayMessageLimits {
                        max_size: Some(512),
                    },
                    ..RelayLimits::default()
                })
                .await
                .unwrap();
            client
                .try_connect_relay(url.clone(), Duration::from_secs(2))
                .await
                .unwrap();
            let first: AcquisitionHandle = client
                .acquire_events(
                    ReqTarget::single(&url, [Filter::new().kind(Kind::Metadata)]),
                    limits(4),
                )
                .await
                .unwrap();
            let second: AcquisitionHandle = client
                .acquire_events(
                    ReqTarget::single(&url, [Filter::new().kind(Kind::TextNote)]),
                    limits(4),
                )
                .await
                .unwrap();
            let (first, second) = tokio::join!(first.finish(), second.finish());
            for report in [first.unwrap(), second.unwrap()] {
                match expected {
                    Expected::Partial => assert!(matches!(
                        report.relays[&url].end,
                        AcquisitionEnd::InvalidMessage
                    )),
                    Expected::Completed => {
                        assert!(matches!(report.relays[&url].end, AcquisitionEnd::Completed))
                    }
                }
                assert_eq!(report.relays[&url].raw_received_items, 0);
            }
            let report: AcquisitionReport = client
                .acquire_events(
                    ReqTarget::single(&url, [Filter::new().kind(Kind::Metadata)]),
                    limits(4),
                )
                .await
                .unwrap()
                .finish()
                .await
                .unwrap();
            assert!(
                matches!(report.relays[&url].end, AcquisitionEnd::Completed),
                "a later request must not inherit a closed request's input loss"
            );
            client.shutdown().await;
            server.abort();
        }
    }

    #[tokio::test]
    async fn raw_eose_keeps_page() {
        let keys = Keys::generate();
        let expired = filtered_event(&keys, 20, FilteredEvent::Expired);
        let live = EventBuilder::new(Kind::TextNote, "queued live input")
            .custom_created_at(Timestamp::from(1))
            .finalize(&keys)
            .unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = RelayUrl::parse(&format!("ws://{}", listener.local_addr().unwrap())).unwrap();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut socket = native::accept_async(stream).await.unwrap();
            while let Some(Ok(frame)) = socket.next().await {
                let WsMessage::Text(text) = frame else {
                    continue;
                };
                let Ok(ClientMessage::Req {
                    subscription_id, ..
                }) = ClientMessage::from_json(text.as_str())
                else {
                    continue;
                };
                for _ in 0..256 {
                    socket
                        .feed(WsMessage::Text(
                            RelayMessage::Event {
                                subscription_id: Cow::Borrowed(subscription_id.as_ref()),
                                event: Cow::Borrowed(&expired),
                            }
                            .as_json()
                            .into(),
                        ))
                        .await
                        .unwrap();
                }
                socket
                    .feed(WsMessage::Text(
                        RelayMessage::EndOfStoredEvents(Cow::Borrowed(subscription_id.as_ref()))
                            .as_json()
                            .into(),
                    ))
                    .await
                    .unwrap();
                socket
                    .feed(WsMessage::Text(
                        RelayMessage::Event {
                            subscription_id: Cow::Borrowed(subscription_id.as_ref()),
                            event: Cow::Borrowed(&live),
                        }
                        .as_json()
                        .into(),
                    ))
                    .await
                    .unwrap();
                socket.feed(WsMessage::Text("[".into())).await.unwrap();
                socket.flush().await.unwrap();
                while let Some(Ok(frame)) = socket.next().await {
                    let WsMessage::Text(text) = frame else {
                        continue;
                    };
                    if matches!(
                        ClientMessage::from_json(text.as_str()),
                        Ok(ClientMessage::Close(_))
                    ) {
                        return;
                    }
                }
                return;
            }
        });
        let client = setup_client(url.clone()).await;
        let report = client
            .acquire_events(
                ReqTarget::single(&url, [Filter::new().kind(Kind::TextNote).limit(256)]),
                limits(256),
            )
            .await
            .unwrap()
            .finish()
            .await
            .unwrap();
        let page = &report.relays[&url];
        assert!(
            matches!(page.end, AcquisitionEnd::Completed),
            "{:?}",
            page.end
        );
        assert_eq!(page.raw_received_items, 256);
        assert_eq!(page.oldest_raw_created_at, Some(Timestamp::from(20)));
        assert_eq!(page.received_items, 0);
        assert!(page.events.is_empty());
        timeout(Duration::from_secs(1), server)
            .await
            .unwrap()
            .unwrap();
        client.shutdown().await;
    }

    #[tokio::test]
    async fn raw_parse_loss_is_incomplete() {
        let valid: Event = EventBuilder::new(Kind::Metadata, "unaffected request")
            .finalize(&Keys::generate())
            .unwrap();
        let expected: EventId = valid.id;
        let live: Event = EventBuilder::new(Kind::Custom(4_000), "live before parse loss")
            .finalize(&Keys::generate())
            .unwrap();
        let live_expected: EventId = live.id;
        let update: Event = EventBuilder::new(Kind::Custom(4_000), "live after parse loss")
            .finalize(&Keys::generate())
            .unwrap();
        let update_expected: EventId = update.id;
        let listener: TcpListener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url: RelayUrl =
            RelayUrl::parse(&format!("ws://{}", listener.local_addr().unwrap())).unwrap();
        let server: JoinHandle<()> = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut socket = native::accept_async(stream).await.unwrap();
            let mut live_subscription: Option<SubscriptionId> = None;
            while let Some(Ok(frame)) = socket.next().await {
                let WsMessage::Text(text) = frame else {
                    continue;
                };
                match ClientMessage::from_json(text.as_str()) {
                    Ok(ClientMessage::Req {
                        subscription_id,
                        filters,
                    }) => {
                        let event: Option<&Event> = [&valid, &live].into_iter().find(|event| {
                            filters
                                .iter()
                                .any(|filter| filter.match_event(event, MatchEventOptions::new()))
                        });
                        if event.is_some_and(|event| event.id == live.id) {
                            live_subscription = Some(subscription_id.as_ref().clone());
                        }
                        let reply: String = match event {
                            Some(event) => RelayMessage::Event {
                                subscription_id: Cow::Borrowed(subscription_id.as_ref()),
                                event: Cow::Borrowed(event),
                            }
                            .as_json(),
                            None => format!(
                                r#"["EVENT","{}",{{"created_at":"invalid"}}]"#,
                                subscription_id
                            ),
                        };
                        socket.send(WsMessage::Text(reply.into())).await.unwrap();
                        socket
                            .send(WsMessage::Text(
                                RelayMessage::eose(subscription_id.into_owned())
                                    .as_json()
                                    .into(),
                            ))
                            .await
                            .unwrap();
                        if event.is_none() {
                            if let Some(id) = &live_subscription {
                                let delivery: RelayMessage = RelayMessage::Event {
                                    subscription_id: Cow::Borrowed(id),
                                    event: Cow::Borrowed(&update),
                                };
                                socket
                                    .send(WsMessage::Text(delivery.as_json().into()))
                                    .await
                                    .unwrap();
                            }
                        }
                    }
                    Ok(ClientMessage::Close(id)) => {
                        let late: String = format!(r#"["EVENT","{}",{{}}]"#, id);
                        if socket.send(WsMessage::Text(late.into())).await.is_err() {
                            return;
                        }
                    }
                    _ => {}
                }
            }
        });
        let client: Client = setup_client(url.clone()).await;
        let connection: Relay = client.pool().relay(&url).await.unwrap();
        let mut notifications = connection.notifications();
        let live_id: SubscriptionId = connection
            .subscribe(Filter::new().kind(Kind::Custom(4_000)))
            .await
            .unwrap();
        let pending: AcquisitionHandle = client
            .acquire_events(
                ReqTarget::single(&url, [Filter::new().id(expected)]),
                limits(4).policy(ReqExitPolicy::WaitDurationAfterEOSE(Duration::from_secs(5))),
            )
            .await
            .unwrap();
        wait_for_acquisition_eose(&connection, &mut notifications).await;
        let report: AcquisitionReport = client
            .acquire_events(
                ReqTarget::single(&url, [Filter::new().kind(Kind::TextNote)]),
                limits(4),
            )
            .await
            .unwrap()
            .finish()
            .await
            .unwrap();
        let page: &RelayAcquisition = &report.relays[&url];
        assert!(matches!(page.end, AcquisitionEnd::InvalidMessage));
        assert_eq!(page.raw_received_items, 0);
        assert_eq!(page.oldest_raw_created_at, None);
        assert_eq!(page.received_items, 0);
        assert!(page.events.is_empty());
        timeout(Duration::from_secs(2), async {
            loop {
                if let Some(RelayNotification::Event {
                    subscription_id,
                    event,
                    ..
                }) = notifications.next().await
                {
                    if subscription_id == live_id && event.id == update_expected {
                        break;
                    }
                }
            }
        })
        .await
        .unwrap();
        assert!(connection.inner.subscription(&live_id).await.is_some());
        for id in [live_expected, update_expected] {
            assert_eq!(
                client.database().check_id(&id).await.unwrap(),
                nostr_database::DatabaseEventStatus::Saved
            );
        }
        pending.cancel();
        let report: AcquisitionReport = pending.finish().await.unwrap();
        let page: &RelayAcquisition = &report.relays[&url];
        assert!(matches!(page.end, AcquisitionEnd::Cancelled));
        assert_eq!(page.raw_received_items, 1);
        assert_eq!(page.events.first().map(|event| event.id), Some(expected));
        let report: AcquisitionReport = client
            .acquire_events(
                ReqTarget::single(&url, [Filter::new().id(expected)]),
                limits(4),
            )
            .await
            .unwrap()
            .finish()
            .await
            .unwrap();
        assert!(matches!(report.relays[&url].end, AcquisitionEnd::Completed));
        assert_eq!(report.relays[&url].raw_received_items, 1);
        assert_eq!(
            client.database().check_id(&expected).await.unwrap(),
            nostr_database::DatabaseEventStatus::NotExistent
        );
        server.abort();
    }

    #[tokio::test]
    async fn raw_mixed_page_keeps_ties() {
        let keys: Keys = Keys::generate();
        let newest: Event = EventBuilder::new(Kind::TextNote, "newest")
            .custom_created_at(Timestamp::from(500))
            .finalize(&keys)
            .unwrap();
        let older: Event = EventBuilder::new(Kind::TextNote, "older")
            .custom_created_at(Timestamp::from(100))
            .finalize(&keys)
            .unwrap();
        let invalid: Event = filtered_event(&keys, 400, FilteredEvent::Invalid);
        let (url, server, _) = raw_history_relay(vec![
            newest.clone(),
            older.clone(),
            filtered_event(&keys, 400, FilteredEvent::Expired),
            invalid.clone(),
            invalid,
        ])
        .await;
        let client: Client = setup_client(url.clone()).await;
        let report: AcquisitionReport = client
            .acquire_events(
                ReqTarget::single(&url, [Filter::new().kind(Kind::TextNote).limit(4)]),
                limits(4),
            )
            .await
            .unwrap()
            .finish()
            .await
            .unwrap();
        let page: &RelayAcquisition = &report.relays[&url];
        assert!(matches!(page.end, AcquisitionEnd::Completed));
        assert_eq!(page.raw_received_items, 4);
        assert_eq!(page.oldest_raw_created_at, Some(Timestamp::from(400)));
        assert_eq!(page.received_items, 1);
        assert_eq!(page.events.first().map(|event| event.id), Some(newest.id));

        // Inclusive until preserves ties. Raw metadata does not certify that
        // every event sharing a saturated boundary timestamp was returned.
        let report: AcquisitionReport = client
            .acquire_events(
                ReqTarget::single(
                    &url,
                    [Filter::new()
                        .kind(Kind::TextNote)
                        .until(page.oldest_raw_created_at.unwrap())
                        .limit(4)],
                ),
                limits(4),
            )
            .await
            .unwrap()
            .finish()
            .await
            .unwrap();
        let page: &RelayAcquisition = &report.relays[&url];
        assert!(matches!(page.end, AcquisitionEnd::Completed));
        assert_eq!(page.raw_received_items, 4);
        assert_eq!(page.oldest_raw_created_at, Some(older.created_at));
        assert_eq!(page.received_items, 1);
        assert_eq!(page.events.first().map(|event| event.id), Some(older.id));
        server.abort();
    }

    #[tokio::test]
    async fn raw_limits_and_cancel_cleanup() {
        let keys: Keys = Keys::generate();
        let mut events: Vec<Event> = (100..120)
            .map(|at| filtered_event(&keys, at, FilteredEvent::Expired))
            .collect();
        let older: Event = EventBuilder::new(Kind::TextNote, "late valid")
            .custom_created_at(Timestamp::from(1))
            .finalize(&keys)
            .unwrap();
        events.push(older.clone());
        let (url, server, mut closed) = raw_history_relay(events).await;
        let client: Client = setup_client(url.clone()).await;
        let connection: Relay = client.pool().relay(&url).await.unwrap();
        let report: AcquisitionReport = client
            .acquire_events(
                ReqTarget::single(&url, [Filter::new().kind(Kind::TextNote)]),
                limits(4),
            )
            .await
            .unwrap()
            .finish()
            .await
            .unwrap();
        let page: &RelayAcquisition = &report.relays[&url];
        assert!(matches!(page.end, AcquisitionEnd::ItemBudgetExceeded));
        assert_eq!(page.raw_received_items, 5);
        assert_eq!(page.oldest_raw_created_at, Some(Timestamp::from(115)));
        assert_eq!(page.received_items, 0);
        assert!(page.events.is_empty());
        timeout(Duration::from_secs(2), closed.recv())
            .await
            .unwrap()
            .unwrap();

        let mut notifications = connection.notifications();
        let handle: AcquisitionHandle = client
            .acquire_events(
                ReqTarget::single(&url, [Filter::new().kind(Kind::TextNote).limit(2)]),
                limits(4).policy(ReqExitPolicy::WaitDurationAfterEOSE(Duration::from_secs(5))),
            )
            .await
            .unwrap();
        wait_for_acquisition_eose(&connection, &mut notifications).await;
        handle.cancel();
        let report: AcquisitionReport = handle.finish().await.unwrap();
        let page: &RelayAcquisition = &report.relays[&url];
        assert!(matches!(page.end, AcquisitionEnd::Cancelled));
        assert_eq!(page.raw_received_items, 2);
        assert_eq!(page.oldest_raw_created_at, Some(Timestamp::from(118)));
        assert_eq!(page.received_items, 0);
        timeout(Duration::from_secs(2), closed.recv())
            .await
            .unwrap()
            .unwrap();

        let report: AcquisitionReport = client
            .acquire_events(
                ReqTarget::single(&url, [Filter::new().id(older.id)]),
                limits(4),
            )
            .await
            .unwrap()
            .finish()
            .await
            .unwrap();
        let page: &RelayAcquisition = &report.relays[&url];
        assert!(matches!(page.end, AcquisitionEnd::Completed));
        assert_eq!(page.raw_received_items, 1);
        assert_eq!(page.received_items, 1);
        assert_eq!(page.oldest_raw_created_at, Some(older.created_at));
        assert_eq!(
            client.database().check_id(&older.id).await.unwrap(),
            nostr_database::DatabaseEventStatus::NotExistent
        );
        timeout(Duration::from_secs(2), async {
            while connection.inner.active_subscription_count().await != 0 {
                sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
        server.abort();
    }

    #[tokio::test]
    async fn raw_requests_keep_evidence() {
        let keys: Keys = Keys::generate();
        let expired: Event = filtered_event(&keys, 200, FilteredEvent::Expired);
        let valid: Event = EventBuilder::new(Kind::TextNote, "other acquisition")
            .custom_created_at(Timestamp::from(100))
            .finalize(&keys)
            .unwrap();
        let live: Event = EventBuilder::new(Kind::Metadata, "live subscription")
            .custom_created_at(Timestamp::from(50))
            .finalize(&keys)
            .unwrap();
        let (url, server, _) =
            raw_history_relay(vec![expired.clone(), valid.clone(), live.clone()]).await;
        let client: Client = setup_client(url.clone()).await;
        let connection: Relay = client.pool().relay(&url).await.unwrap();
        let mut notifications = connection.notifications();
        let live_id: SubscriptionId = connection
            .subscribe(Filter::new().id(live.id))
            .await
            .unwrap();
        let pending: AcquisitionHandle = client
            .acquire_events(
                ReqTarget::single(&url, [Filter::new().id(expired.id)]),
                limits(4).policy(ReqExitPolicy::WaitDurationAfterEOSE(Duration::from_secs(5))),
            )
            .await
            .unwrap();
        wait_for_acquisition_eose(&connection, &mut notifications).await;

        let report: AcquisitionReport = client
            .acquire_events(
                ReqTarget::single(&url, [Filter::new().id(valid.id)]),
                limits(4),
            )
            .await
            .unwrap()
            .finish()
            .await
            .unwrap();
        let page: &RelayAcquisition = &report.relays[&url];
        assert!(matches!(page.end, AcquisitionEnd::Completed));
        assert_eq!(page.raw_received_items, 1);
        assert_eq!(page.oldest_raw_created_at, Some(valid.created_at));
        pending.cancel();
        let report: AcquisitionReport = pending.finish().await.unwrap();
        let page: &RelayAcquisition = &report.relays[&url];
        assert!(matches!(page.end, AcquisitionEnd::Cancelled));
        assert_eq!(page.raw_received_items, 1);
        assert_eq!(page.oldest_raw_created_at, Some(expired.created_at));
        assert_eq!(page.received_items, 0);
        assert!(connection.inner.subscription(&live_id).await.is_some());
        assert_eq!(
            client.database().check_id(&valid.id).await.unwrap(),
            nostr_database::DatabaseEventStatus::NotExistent
        );
        assert_eq!(
            client.database().check_id(&live.id).await.unwrap(),
            nostr_database::DatabaseEventStatus::Saved
        );
        server.abort();
    }

    #[tokio::test]
    async fn raw_filtered_overflow_counts() {
        let keys: Keys = Keys::generate();
        let valid: Event = EventBuilder::new(Kind::TextNote, "valid overflow")
            .custom_created_at(Timestamp::from(1))
            .finalize(&keys)
            .unwrap();
        let mut events: Vec<Event> = (100..104)
            .map(|at| filtered_event(&keys, at, FilteredEvent::Expired))
            .collect();
        events.push(valid.clone());
        let (url, server, _) = raw_history_relay(events).await;
        let client: Client = setup_client(url.clone()).await;
        let report: AcquisitionReport = client
            .acquire_events(
                ReqTarget::single(&url, [Filter::new().kind(Kind::TextNote)]),
                limits(4),
            )
            .await
            .unwrap()
            .finish()
            .await
            .unwrap();
        let page: &RelayAcquisition = &report.relays[&url];
        assert!(matches!(page.end, AcquisitionEnd::ItemBudgetExceeded));
        assert_eq!(page.raw_received_items, 5);
        assert_eq!(page.oldest_raw_created_at, Some(valid.created_at));
        assert_eq!(page.received_items, 1);
        assert_eq!(page.received_event_bytes, valid.as_json().len());
        assert!(page.events.is_empty());
        assert_eq!(page.high_water_items, 0);
        server.abort();
    }

    async fn wait_for_acquisition_eose<S>(relay: &Relay, notifications: &mut S)
    where
        S: Stream<Item = RelayNotification> + Unpin,
    {
        let id = timeout(Duration::from_secs(1), async {
            loop {
                if let Some(id) = relay.inner.auto_closing_subscription_id().await {
                    break id;
                }
                sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("acquisition subscription was not registered");
        timeout(Duration::from_secs(1), async {
            loop {
                let update = notifications.next().await.expect("relay notifications closed");
                if matches!(update, RelayNotification::Message { message } if matches!(*message, RelayMessage::EndOfStoredEvents(ref received) if received.as_ref() == &id)) {
                    break;
                }
            }
        })
        .await
        .expect("acquisition did not receive EOSE");
    }

    async fn inventory(relay: &MockRelay, count: usize) -> Vec<Event> {
        let keys = Keys::generate();
        let mut events = Vec::new();
        for index in 0..count {
            let event = EventBuilder::new(Kind::TextNote, format!("item-{index}"))
                .finalize(&keys)
                .unwrap();
            relay.add_event(event.clone()).await.unwrap();
            events.push(event);
        }
        events
    }

    #[tokio::test]
    async fn finite_explicit_id_reacquisition_preserves_partial_and_reports_traffic() {
        let relay = MockRelay::run().await.unwrap();
        let events = inventory(&relay, 8).await;
        let url = relay.url().await;
        let client = setup_client(url.clone()).await;
        let connection = client.pool().relay(&url).await.unwrap();
        let before_text_bytes = connection.stats().bytes_received();
        let before_sent_text_bytes = connection.stats().bytes_sent();

        let first = client
            .acquire_events(
                ReqTarget::single(&url, [Filter::new().kind(Kind::TextNote)]),
                limits(3),
            )
            .await
            .unwrap()
            .finish()
            .await
            .unwrap();
        let first = &first.relays[&url];
        assert!(matches!(first.end, AcquisitionEnd::ItemBudgetExceeded));
        assert_eq!(first.events.len(), 3);
        assert_eq!(first.raw_received_items, 4);
        assert_eq!(first.received_items, 4);
        assert_eq!(first.high_water_items, 3);
        let first_high_water_bytes = first.high_water_event_bytes;

        let mut acquired: BTreeSet<EventId> = BTreeSet::new();
        let mut event_bytes = first.received_event_bytes;
        let mut received_items = first.received_items;
        let mut requests = 1;
        for pair in events.chunks(2) {
            let ids: Vec<EventId> = pair.iter().map(|event| event.id).collect();
            let report = client
                .acquire_events(ReqTarget::single(&url, [Filter::new().ids(ids)]), limits(3))
                .await
                .unwrap()
                .finish()
                .await
                .unwrap();
            let outcome = &report.relays[&url];
            assert!(matches!(outcome.end, AcquisitionEnd::Completed));
            event_bytes += outcome.received_event_bytes;
            received_items += outcome.received_items;
            requests += 1;
            acquired.extend(outcome.events.iter().map(|event| event.id));
        }
        assert_eq!(acquired.len(), 8);
        assert_eq!(requests, 5);
        assert!(received_items >= 12);
        timeout(Duration::from_secs(1), async {
            while connection.inner.active_subscription_count().await != 0 {
                sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
        sleep(Duration::from_millis(10)).await;
        let connection_text_bytes = connection.stats().bytes_received() - before_text_bytes;
        let sent_text_bytes = connection.stats().bytes_sent() - before_sent_text_bytes;
        eprintln!(
            "finite inventory: requests={requests} received_items={received_items} serialized_event_bytes={event_bytes} received_connection_text_bytes={connection_text_bytes} sent_connection_text_bytes={sent_text_bytes} total_connection_text_bytes={} first_high_water_items=3 first_high_water_event_bytes={first_high_water_bytes}",
            connection_text_bytes + sent_text_bytes,
        );
        assert!(connection_text_bytes >= event_bytes);
    }

    #[tokio::test]
    async fn failing_relay_does_not_hide_healthy_relay() {
        let relay = MockRelay::run().await.unwrap();
        inventory(&relay, 1).await;
        let healthy = relay.url().await;
        let missing = RelayUrl::parse("ws://127.0.0.1:1").unwrap();
        let client = setup_client(healthy.clone()).await;
        let target = ReqTarget::manual(vec![
            (healthy.clone(), vec![Filter::new().kind(Kind::TextNote)]),
            (missing.clone(), vec![Filter::new().kind(Kind::TextNote)]),
        ]);
        let report = client
            .acquire_events(target, limits(10))
            .await
            .unwrap()
            .finish()
            .await
            .unwrap();
        assert!(matches!(
            report.relays[&healthy].end,
            AcquisitionEnd::Completed
        ));
        assert_eq!(report.relays[&healthy].events.len(), 1);
        assert!(matches!(
            report.relays[&missing].end,
            AcquisitionEnd::Failed(_)
        ));
    }

    #[tokio::test]
    async fn stalled_caller_can_cancel_and_keeps_unrelated_subscription() {
        let relay = MockRelay::run().await.unwrap();
        inventory(&relay, 1).await;
        let url = relay.url().await;
        let client = setup_client(url.clone()).await;
        let connection = client.pool().relay(&url).await.unwrap();
        let unrelated = connection
            .subscribe(Filter::new().kind(Kind::Metadata))
            .await
            .unwrap();
        let slow = limits(10).policy(ReqExitPolicy::WaitDurationAfterEOSE(Duration::from_secs(5)));
        let mut notifications = connection.notifications();
        let handle = client
            .acquire_events(
                ReqTarget::single(&url, [Filter::new().kind(Kind::TextNote)]),
                slow,
            )
            .await
            .unwrap();
        wait_for_acquisition_eose(&connection, &mut notifications).await;
        let control_event = EventBuilder::new(Kind::Custom(1_234), "concurrent control")
            .finalize(&Keys::generate())
            .unwrap();
        relay.add_event(control_event).await.unwrap();
        let concurrent_control_started = Instant::now();
        let concurrent_control = client
            .acquire_events(
                ReqTarget::single(&url, [Filter::new().kind(Kind::Custom(1_234))]),
                limits(10),
            )
            .await
            .unwrap();
        let concurrent_control = timeout(Duration::from_secs(1), concurrent_control.finish())
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(
            concurrent_control.relays[&url].end,
            AcquisitionEnd::Completed
        ));
        assert_eq!(concurrent_control.relays[&url].events.len(), 1);
        eprintln!(
            "stalled-consumer concurrent control latency: {:?}",
            concurrent_control_started.elapsed()
        );
        let started = Instant::now();
        handle.cancel();
        let report = timeout(Duration::from_secs(1), handle.finish())
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(report.relays[&url].end, AcquisitionEnd::Cancelled));
        assert!(report.relays[&url].events.len() <= 1);
        assert!(started.elapsed() < Duration::from_secs(1));
        eprintln!(
            "stalled consumer cancellation latency: {:?}",
            started.elapsed()
        );
        assert!(connection.inner.subscription(&unrelated).await.is_some());
        let metadata = EventBuilder::new(Kind::Metadata, "control still works")
            .finalize(&Keys::generate())
            .unwrap();
        relay.add_event(metadata).await.unwrap();
        let control_started = Instant::now();
        let control = client
            .acquire_events(
                ReqTarget::single(&url, [Filter::new().kind(Kind::Metadata)]),
                limits(10),
            )
            .await
            .unwrap();
        let control = timeout(Duration::from_secs(1), control.finish())
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(
            control.relays[&url].end,
            AcquisitionEnd::Completed
        ));
        assert_eq!(control.relays[&url].events.len(), 1);
        eprintln!(
            "post-cancellation control acquisition latency: {:?}",
            control_started.elapsed()
        );
    }

    #[tokio::test]
    async fn stalled_caller_receives_timeout_and_disconnect_outcomes() {
        let relay = MockRelay::run().await.unwrap();
        inventory(&relay, 1).await;
        let url = relay.url().await;
        let client = setup_client(url.clone()).await;
        let slow = AcquisitionLimits::new(1, 10, 10_000, Duration::from_millis(100))
            .policy(ReqExitPolicy::WaitDurationAfterEOSE(Duration::from_secs(5)));
        let handle = client
            .acquire_events(
                ReqTarget::single(&url, [Filter::new().kind(Kind::TextNote)]),
                slow,
            )
            .await
            .unwrap();
        sleep(Duration::from_millis(200)).await;
        let report = handle.finish().await.unwrap();
        assert!(matches!(report.relays[&url].end, AcquisitionEnd::TimedOut));
        assert_eq!(report.relays[&url].events.len(), 1);

        let handle = client
            .acquire_events(
                ReqTarget::single(&url, [Filter::new().kind(Kind::TextNote)]),
                limits(10).policy(ReqExitPolicy::WaitDurationAfterEOSE(Duration::from_secs(5))),
            )
            .await
            .unwrap();
        let connection = client.pool().relay(&url).await.unwrap();
        let mut notifications = connection.notifications();
        wait_for_acquisition_eose(&connection, &mut notifications).await;
        connection.disconnect();
        let report = timeout(Duration::from_secs(1), handle.finish())
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(
            report.relays[&url].end,
            AcquisitionEnd::Disconnected
        ));
        assert!(report.relays[&url].events.len() <= 1);
    }

    #[tokio::test]
    async fn dropping_stalled_handle_releases_only_its_subscription() {
        let relay = MockRelay::run().await.unwrap();
        let url = relay.url().await;
        let client = setup_client(url.clone()).await;
        let connection = client.pool().relay(&url).await.unwrap();
        let unrelated = connection
            .subscribe(Filter::new().kind(Kind::Metadata))
            .await
            .unwrap();
        let baseline = connection.inner.active_subscription_count().await;
        let slow = limits(10).policy(ReqExitPolicy::WaitDurationAfterEOSE(Duration::from_secs(5)));
        let handle = client
            .acquire_events(
                ReqTarget::single(&url, [Filter::new().kind(Kind::TextNote)]),
                slow,
            )
            .await
            .unwrap();
        timeout(Duration::from_secs(1), async {
            while connection.inner.active_subscription_count().await <= baseline {
                sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
        let started = Instant::now();
        drop(handle);
        timeout(Duration::from_secs(1), async {
            while connection.inner.active_subscription_count().await != baseline {
                sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
        eprintln!(
            "stalled handle drop cleanup latency: {:?}",
            started.elapsed()
        );
        assert!(connection.inner.subscription(&unrelated).await.is_some());
    }

    #[tokio::test]
    async fn another_task_can_cancel_while_finish_waits_for_partial_report() {
        let relay = MockRelay::run().await.unwrap();
        inventory(&relay, 1).await;
        let url = relay.url().await;
        let client = setup_client(url.clone()).await;
        let slow = limits(10).policy(ReqExitPolicy::WaitDurationAfterEOSE(Duration::from_secs(5)));
        let handle = client
            .acquire_events(
                ReqTarget::single(&url, [Filter::new().kind(Kind::TextNote)]),
                slow,
            )
            .await
            .unwrap();
        let canceller = handle.canceller();
        let other_task = tokio::spawn(async move {
            canceller.cancel();
        });
        let report = timeout(Duration::from_secs(1), handle.finish())
            .await
            .unwrap()
            .unwrap();
        other_task.await.unwrap();
        assert!(matches!(report.relays[&url].end, AcquisitionEnd::Cancelled));
        assert!(report.relays[&url].events.len() <= 1);
    }

    #[tokio::test]
    async fn sustained_large_history_keeps_a_stalled_caller_bounded_and_live_delivery_running() {
        let relay = MockRelay::run().await.unwrap();
        let events = (0..96)
            .map(|index| {
                EventBuilder::new(Kind::TextNote, format!("{index}:{}", "x".repeat(8_192)))
                    .finalize(&Keys::generate())
                    .unwrap()
            })
            .collect::<Vec<_>>();
        for event in &events {
            relay.add_event(event.clone()).await.unwrap();
        }
        let url = relay.url().await;
        let client = setup_client(url.clone()).await;
        let connection = client.pool().relay(&url).await.unwrap();
        let live_id = connection
            .subscribe(Filter::new().kind(Kind::Metadata))
            .await
            .unwrap();
        let mut live = connection.notifications_with_gaps();
        let byte_budget = 64 * 1024;
        let handle = client
            .acquire_events(
                ReqTarget::single(&url, [Filter::new().kind(Kind::TextNote).limit(96)]),
                AcquisitionLimits::new(1, 96, byte_budget, Duration::from_secs(5)),
            )
            .await
            .unwrap();

        // The caller does not poll its handle during the history burst.
        sleep(Duration::from_millis(50)).await;
        let live_event = EventBuilder::new(Kind::Metadata, "live during acquisition")
            .finalize(&Keys::generate())
            .unwrap();
        let control_started = Instant::now();
        relay.add_event(live_event.clone()).await.unwrap();
        timeout(Duration::from_secs(2), async {
            loop {
                if let Some(crate::stream::NotificationUpdate::Notification(
                    RelayNotification::Event {
                        subscription_id,
                        event,
                    },
                )) = live.next().await
                {
                    if subscription_id == live_id && event.id == live_event.id {
                        break;
                    }
                }
            }
        })
        .await
        .unwrap();
        let control_latency = control_started.elapsed();
        let report = timeout(Duration::from_secs(2), handle.finish())
            .await
            .unwrap()
            .unwrap();
        let result = &report.relays[&url];
        assert!(matches!(result.end, AcquisitionEnd::ByteBudgetExceeded));
        assert!(result.events.len() < events.len());
        assert!(result.high_water_event_bytes <= byte_budget);
        assert!(result.high_water_items <= result.received_items);
        assert!(result.received_event_bytes > byte_budget);
        assert!(connection.inner.subscription(&live_id).await.is_some());
        eprintln!(
            "large history: received_items={} received_serialized_event_bytes={} retained_items={} retained_serialized_event_bytes={} live_delivery_latency={control_latency:?}",
            result.received_items,
            result.received_event_bytes,
            result.high_water_items,
            result.high_water_event_bytes,
        );
    }

    #[tokio::test]
    async fn duplicate_burst_exhausts_item_budget_without_growing_deduplication() {
        let relay = MockRelay::run().await.unwrap();
        let url = relay.url().await;
        let client = setup_client(url.clone()).await;
        let connection = client.pool().relay(&url).await.unwrap();
        let handle = client
            .acquire_events(
                ReqTarget::single(&url, [Filter::new().kind(Kind::TextNote)]),
                AcquisitionLimits::new(1, 16, 1_000_000, Duration::from_secs(5))
                    .policy(ReqExitPolicy::WaitDurationAfterEOSE(Duration::from_secs(5))),
            )
            .await
            .unwrap();
        let id = timeout(Duration::from_secs(1), async {
            loop {
                if let Some(id) = connection.inner.auto_closing_subscription_id().await {
                    break id;
                }
                sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
        let repeated = EventBuilder::new(Kind::TextNote, "duplicate burst")
            .finalize(&Keys::generate())
            .unwrap();
        let event_bytes = repeated.as_json().len();
        for _ in 0..32 {
            connection
                .inner
                .inject_notification(RelayNotification::Message {
                    message: Box::new(RelayMessage::Event {
                        subscription_id: Cow::Owned(id.clone()),
                        event: Cow::Owned(repeated.clone()),
                    }),
                });
        }
        let result = timeout(Duration::from_secs(2), handle.finish())
            .await
            .unwrap()
            .unwrap();
        let result = &result.relays[&url];
        assert!(matches!(result.end, AcquisitionEnd::ItemBudgetExceeded));
        assert_eq!(result.received_items, 17);
        assert_eq!(result.duplicates, 15);
        assert_eq!(result.high_water_items, 1);
        assert_eq!(result.high_water_event_bytes, event_bytes);
        assert_eq!(result.received_event_bytes, 17 * event_bytes);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn receive_loss_before_eose_cannot_complete_acquisition() {
        let relay = MockRelay::run().await.unwrap();
        let url = relay.url().await;
        let client = Client::new();
        client
            .add_relay(&url)
            .notification_channel_size(4)
            .await
            .unwrap();
        client
            .try_connect_relay(&url, Duration::from_secs(3))
            .await
            .unwrap();
        let connection = client.pool().relay(&url).await.unwrap();
        let handle = client
            .acquire_events(
                ReqTarget::single(&url, [Filter::new().kind(Kind::TextNote)]),
                AcquisitionLimits::new(1, 1_000, 1_000_000, Duration::from_secs(5))
                    .policy(ReqExitPolicy::WaitDurationAfterEOSE(Duration::from_secs(5))),
            )
            .await
            .unwrap();
        let id = timeout(Duration::from_secs(1), async {
            loop {
                if let Some(id) = connection.inner.auto_closing_subscription_id().await {
                    break id;
                }
                sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
        let repeated = EventBuilder::new(Kind::TextNote, "saturate")
            .finalize(&Keys::generate())
            .unwrap();
        // This current-thread burst cannot be drained until the final EOSE is queued.
        // Injection is at the parsed relay notification boundary, so no wire-byte
        // measurement is implied.
        for _ in 0..64 {
            connection
                .inner
                .inject_notification(RelayNotification::Message {
                    message: Box::new(RelayMessage::Event {
                        subscription_id: Cow::Owned(id.clone()),
                        event: Cow::Owned(repeated.clone()),
                    }),
                });
        }
        connection
            .inner
            .inject_notification(RelayNotification::Message {
                message: Box::new(RelayMessage::EndOfStoredEvents(Cow::Owned(id))),
            });
        let report = timeout(Duration::from_secs(2), handle.finish())
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(
            report.relays[&url].end,
            AcquisitionEnd::ReceiveLoss(_)
        ));
    }

    #[tokio::test]
    async fn cancellation_and_drop_during_live_traffic_release_only_their_requests() {
        let relay = MockRelay::run().await.unwrap();
        let url = relay.url().await;
        let client = setup_client(url.clone()).await;
        let connection = client.pool().relay(&url).await.unwrap();
        let live_id = connection
            .subscribe(Filter::new().kind(Kind::Metadata))
            .await
            .unwrap();
        let baseline = connection.inner.active_subscription_count().await;
        let mut notifications = connection.notifications_with_gaps();
        let limits = AcquisitionLimits::new(1, 1_000, 2_000_000, Duration::from_secs(5))
            .policy(ReqExitPolicy::WaitDurationAfterEOSE(Duration::from_secs(5)));
        let handle = client
            .acquire_events(
                ReqTarget::single(&url, [Filter::new().kind(Kind::TextNote)]),
                limits,
            )
            .await
            .unwrap();
        timeout(Duration::from_secs(1), async {
            while connection.inner.active_subscription_count().await <= baseline {
                sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
        let acquisition_id = connection
            .inner
            .auto_closing_subscription_id()
            .await
            .expect("acquisition request remains registered");
        let producer = tokio::spawn({
            let relay = relay.clone();
            async move {
                let keys = Keys::generate();
                for index in 0..128 {
                    let event = EventBuilder::new(
                        Kind::TextNote,
                        format!("traffic-{index}-{}", "x".repeat(512)),
                    )
                    .finalize(&keys)
                    .unwrap();
                    relay.add_event(event).await.unwrap();
                    sleep(Duration::from_millis(2)).await;
                }
            }
        });
        timeout(Duration::from_secs(1), async {
            loop {
                if let Some(crate::stream::NotificationUpdate::Notification(
                    RelayNotification::Message { message },
                )) = notifications.next().await
                {
                    if matches!(*message, RelayMessage::Event { ref subscription_id, ref event }
                        if subscription_id.as_ref() == &acquisition_id && event.kind == Kind::TextNote)
                    {
                        break;
                    }
                }
            }
        })
        .await
        .unwrap();
        let during_pressure = EventBuilder::new(Kind::Metadata, "live while history flows")
            .finalize(&Keys::generate())
            .unwrap();
        let control_started = Instant::now();
        relay.add_event(during_pressure.clone()).await.unwrap();
        timeout(Duration::from_secs(1), async {
            loop {
                if let Some(crate::stream::NotificationUpdate::Notification(
                    RelayNotification::Event {
                        subscription_id,
                        event,
                    },
                )) = notifications.next().await
                {
                    if subscription_id == live_id && event.id == during_pressure.id {
                        break;
                    }
                }
            }
        })
        .await
        .unwrap();
        let control_latency = control_started.elapsed();
        assert!(
            !producer.is_finished(),
            "traffic must continue during control delivery"
        );
        let started = Instant::now();
        handle.cancel();
        let report = timeout(Duration::from_secs(1), handle.finish())
            .await
            .unwrap()
            .unwrap();
        let cancel_latency = started.elapsed();
        let result = &report.relays[&url];
        assert!(matches!(result.end, AcquisitionEnd::Cancelled));
        assert!(result.received_items > 0);
        timeout(Duration::from_secs(1), async {
            while connection.inner.active_subscription_count().await != baseline {
                sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();

        let dropped = client
            .acquire_events(
                ReqTarget::single(&url, [Filter::new().kind(Kind::TextNote)]),
                limits,
            )
            .await
            .unwrap();
        timeout(Duration::from_secs(1), async {
            while connection.inner.active_subscription_count().await <= baseline {
                sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
        let drop_started = Instant::now();
        drop(dropped);
        timeout(Duration::from_secs(1), async {
            while connection.inner.active_subscription_count().await != baseline {
                sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
        let drop_latency = drop_started.elapsed();
        assert!(
            !producer.is_finished(),
            "traffic must still be active at cleanup"
        );
        let control = EventBuilder::new(Kind::Metadata, "still live")
            .finalize(&Keys::generate())
            .unwrap();
        relay.add_event(control.clone()).await.unwrap();
        timeout(Duration::from_secs(1), async {
            loop {
                if let Some(crate::stream::NotificationUpdate::Notification(
                    RelayNotification::Event {
                        subscription_id,
                        event,
                    },
                )) = notifications.next().await
                {
                    if subscription_id == live_id && event.id == control.id {
                        break;
                    }
                }
            }
        })
        .await
        .unwrap();
        producer.await.unwrap();
        eprintln!(
            "active traffic cleanup: control_latency={control_latency:?} cancel_latency={cancel_latency:?} drop_latency={drop_latency:?} partial_items={} live_delivery=true",
            result.received_items
        );
    }

    #[tokio::test]
    async fn cancellation_and_drop_during_registration_leave_no_request_subscription() {
        let relay = MockRelay::run().await.unwrap();
        let url = relay.url().await;
        let client = setup_client(url.clone()).await;
        let connection = client.pool().relay(&url).await.unwrap();
        let live_id = connection
            .subscribe(Filter::new().kind(Kind::Metadata))
            .await
            .unwrap();
        let baseline = connection.inner.active_subscription_count().await;

        let (entered, release) = connection.inner.gate_auto_closing_registration();
        let handle = client
            .acquire_events(
                ReqTarget::single(&url, [Filter::new().kind(Kind::TextNote)]),
                limits(10),
            )
            .await
            .unwrap();
        timeout(Duration::from_secs(1), entered)
            .await
            .unwrap()
            .unwrap();
        let started = Instant::now();
        handle.cancel();
        let report = timeout(Duration::from_secs(1), handle.finish())
            .await
            .unwrap()
            .unwrap();
        let cancel_latency = started.elapsed();
        assert!(matches!(report.relays[&url].end, AcquisitionEnd::Cancelled));
        let _ = release.send(());
        assert_eq!(connection.inner.active_subscription_count().await, baseline);

        let (entered, release) = connection.inner.gate_auto_closing_registration();
        let handle = client
            .acquire_events(
                ReqTarget::single(&url, [Filter::new().kind(Kind::TextNote)]),
                limits(10),
            )
            .await
            .unwrap();
        timeout(Duration::from_secs(1), entered)
            .await
            .unwrap()
            .unwrap();
        let started = Instant::now();
        drop(handle);
        let drop_latency = started.elapsed();
        let _ = release.send(());
        assert_eq!(connection.inner.active_subscription_count().await, baseline);

        let mut notifications = connection.notifications_with_gaps();
        let control = EventBuilder::new(Kind::Metadata, "setup cleanup live")
            .finalize(&Keys::generate())
            .unwrap();
        relay.add_event(control.clone()).await.unwrap();
        timeout(Duration::from_secs(1), async {
            loop {
                if let Some(crate::stream::NotificationUpdate::Notification(
                    RelayNotification::Event {
                        subscription_id,
                        event,
                    },
                )) = notifications.next().await
                {
                    if subscription_id == live_id && event.id == control.id {
                        break;
                    }
                }
            }
        })
        .await
        .unwrap();
        eprintln!(
            "registration cleanup: cancel_latency={cancel_latency:?} drop_latency={drop_latency:?} live_delivery=true"
        );
    }
}
