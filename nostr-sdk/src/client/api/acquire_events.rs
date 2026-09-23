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
    /// durable admission or complete historical coverage.
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

    use futures::StreamExt;
    use nostr::event::{Event, EventBuilder, EventId, FinalizeEvent, Kind};
    use nostr::filter::Filter;
    use nostr::key::Keys;
    use nostr::message::RelayMessage;
    use tokio::time::{sleep, timeout};

    use super::*;
    use crate::local_relay::MockRelay;
    use crate::relay::{AcquisitionEnd, RelayNotification, ReqExitPolicy};
    use crate::test_utils::setup_client;

    fn limits(items: usize) -> AcquisitionLimits {
        AcquisitionLimits::new(2, items, 100_000, Duration::from_secs(2))
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
        let handle = client
            .acquire_events(
                ReqTarget::single(&url, [Filter::new().kind(Kind::TextNote)]),
                slow,
            )
            .await
            .unwrap();
        sleep(Duration::from_millis(40)).await;
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
        assert_eq!(report.relays[&url].events.len(), 1);
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
        sleep(Duration::from_millis(40)).await;
        client.pool().relay(&url).await.unwrap().disconnect();
        let report = timeout(Duration::from_secs(1), handle.finish())
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(
            report.relays[&url].end,
            AcquisitionEnd::Disconnected
        ));
        assert_eq!(report.relays[&url].events.len(), 1);
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
            sleep(Duration::from_millis(40)).await;
            canceller.cancel();
        });
        let report = timeout(Duration::from_secs(1), handle.finish())
            .await
            .unwrap()
            .unwrap();
        other_task.await.unwrap();
        assert!(matches!(report.relays[&url].end, AcquisitionEnd::Cancelled));
        assert_eq!(report.relays[&url].events.len(), 1);
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
                    RelayNotification::Event { event, .. },
                )) = notifications.next().await
                {
                    if event.kind == Kind::TextNote {
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
