use std::borrow::Cow;
use std::collections::BTreeSet;
use std::time::Duration;

use async_utility::{task, time};
use nostr::event::Event;
use nostr::filter::Filter;
use nostr::message::{ClientMessage, SubscriptionId};
use tokio::sync::{mpsc, oneshot, watch};

use crate::error::Error;
use crate::relay::{
    Relay, ReqExitPolicy, SubscribeAutoCloseOptions, SubscriptionActivity,
    SubscriptionAutoClosedReason,
};

/// Per-relay resource limits for a bounded acquisition.
///
/// Each received EVENT, including a duplicate, consumes one item and its
/// serialized event JSON length in bytes. This bounds the retained event set,
/// its deduplication keys and the one-item request activity queue. The queue
/// producer and collector may each hold one additional event, which can be
/// larger than the budget. It does not bound Rust object overhead, temporary
/// serialization, shared connection queues, WebSocket frames, or parser allocations.
/// Every received event is serialized again to measure its JSON length, including
/// duplicates; callers should budget CPU as well as retained data for heavy traffic.
#[derive(Debug, Clone, Copy)]
pub struct AcquisitionLimits {
    /// Maximum received EVENT notifications per relay, including duplicates.
    pub max_items: usize,
    /// Maximum sum of serialized event JSON bytes received per relay.
    pub max_event_bytes: usize,
    /// Maximum simultaneously selected relays.
    pub max_relays: usize,
    /// Request deadline per relay, including subscription setup.
    pub timeout: Duration,
    /// Relay request exit policy. Completion is scoped to this policy.
    pub policy: ReqExitPolicy,
}

impl AcquisitionLimits {
    /// Construct limits with the EOSE exit policy.
    pub fn new(
        max_relays: usize,
        max_items: usize,
        max_event_bytes: usize,
        timeout: Duration,
    ) -> Self {
        Self {
            max_relays,
            max_items,
            max_event_bytes,
            timeout,
            policy: ReqExitPolicy::ExitOnEOSE,
        }
    }

    /// Set the relay request exit policy.
    pub fn policy(mut self, policy: ReqExitPolicy) -> Self {
        self.policy = policy;
        self
    }

    pub(crate) fn validate(&self) -> Result<(), Error> {
        if self.max_relays == 0
            || self.max_items == 0
            || self.max_event_bytes == 0
            || self.timeout.is_zero()
        {
            return Err(Error::invalid_msg("acquisition limits must be nonzero"));
        }
        Ok(())
    }
}

/// Why one relay's acquisition ended. Only `Completed` satisfies the chosen
/// request exit policy; it does not establish historical coverage or durable
/// downstream admission.
#[derive(Debug)]
pub enum AcquisitionEnd {
    /// Relay satisfied the selected exit policy.
    Completed,
    /// The configured relay event-count exit policy fired.
    ExitLimitReached,
    /// The next received EVENT exceeded `max_items`.
    ItemBudgetExceeded,
    /// The next received EVENT exceeded `max_event_bytes`.
    ByteBudgetExceeded,
    /// The caller cancelled the request.
    Cancelled,
    /// The request deadline or relay timeout elapsed.
    TimedOut,
    /// The relay disconnected.
    Disconnected,
    /// The shared connection notification receiver skipped notifications.
    ReceiveLoss(u64),
    /// The shared connection notification receiver closed unexpectedly.
    ReceiverClosed,
    /// NIP-42 authentication failed.
    AuthenticationFailed,
    /// The relay sent CLOSED or another subscription error.
    Rejected(String),
    /// Subscription setup failed.
    Failed(Error),
}

impl From<SubscriptionAutoClosedReason> for AcquisitionEnd {
    fn from(reason: SubscriptionAutoClosedReason) -> Self {
        match reason {
            SubscriptionAutoClosedReason::Completed => Self::Completed,
            SubscriptionAutoClosedReason::LimitReached => Self::ExitLimitReached,
            SubscriptionAutoClosedReason::TimedOut => Self::TimedOut,
            SubscriptionAutoClosedReason::Disconnected => Self::Disconnected,
            SubscriptionAutoClosedReason::Lagged(skipped) => Self::ReceiveLoss(skipped),
            SubscriptionAutoClosedReason::ReceiverClosed => Self::ReceiverClosed,
            SubscriptionAutoClosedReason::AuthenticationFailed => Self::AuthenticationFailed,
            SubscriptionAutoClosedReason::Closed(message) => Self::Rejected(message),
        }
    }
}

/// Bounded partial data and traffic counters for one relay.
#[derive(Debug)]
pub struct RelayAcquisition {
    /// Distinct events accepted within both budgets.
    pub events: BTreeSet<Event>,
    /// Terminal outcome for this relay.
    pub end: AcquisitionEnd,
    /// Received EVENT notifications, including duplicates and the over-budget item.
    pub received_items: usize,
    /// Sum of serialized event JSON lengths, including duplicates and the over-budget item.
    /// This is an estimate of event payload bytes, not WebSocket wire bytes.
    pub received_event_bytes: usize,
    /// Received EVENT notifications already represented in `events`.
    pub duplicates: usize,
    /// Greatest number of retained distinct events.
    pub high_water_items: usize,
    /// Greatest sum of serialized JSON lengths of retained distinct events.
    pub high_water_event_bytes: usize,
}

impl RelayAcquisition {
    fn new() -> Self {
        Self {
            events: BTreeSet::new(),
            end: AcquisitionEnd::ReceiverClosed,
            received_items: 0,
            received_event_bytes: 0,
            duplicates: 0,
            high_water_items: 0,
            high_water_event_bytes: 0,
        }
    }

    pub(crate) fn missing_relay() -> Self {
        let mut result = Self::new();
        result.end = AcquisitionEnd::Failed(Error::relay_not_found());
        result
    }

    fn accept_event(&mut self, event: Event, limits: AcquisitionLimits) -> bool {
        let event_bytes = event.as_json().len();
        self.received_items = match self.received_items.checked_add(1) {
            Some(items) => items,
            None => {
                self.end = AcquisitionEnd::ItemBudgetExceeded;
                return false;
            }
        };
        self.received_event_bytes = match self.received_event_bytes.checked_add(event_bytes) {
            Some(bytes) => bytes,
            None => {
                self.end = AcquisitionEnd::ByteBudgetExceeded;
                return false;
            }
        };
        if self.received_items > limits.max_items {
            self.end = AcquisitionEnd::ItemBudgetExceeded;
            return false;
        }
        if self.received_event_bytes > limits.max_event_bytes {
            self.end = AcquisitionEnd::ByteBudgetExceeded;
            return false;
        }
        if self.events.contains(&event) {
            self.duplicates += 1;
        } else {
            self.events.insert(event);
            self.high_water_items = self.events.len();
            self.high_water_event_bytes = self.high_water_event_bytes.saturating_add(event_bytes);
        }
        true
    }
}

struct CancelOnDrop(Option<oneshot::Sender<()>>);

impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        if let Some(cancel) = self.0.take() {
            let _ = cancel.send(());
        }
    }
}

// Cancelling setup after registration but before the auto-close handler is
// installed must not leave a subscription in the shared connection map.
struct SetupGuard {
    relay: Relay,
    id: Option<SubscriptionId>,
}

impl Drop for SetupGuard {
    fn drop(&mut self) {
        if let Some(id) = self.id.take() {
            let relay = self.relay.clone();
            task::spawn(async move {
                relay.inner.remove_subscription(&id).await;
                let _ = relay
                    .send_msg(ClientMessage::Close(Cow::Borrowed(&id)))
                    .await;
            });
        }
    }
}

pub(crate) async fn acquire_relay(
    relay: Relay,
    filters: Vec<Filter>,
    limits: AcquisitionLimits,
    mut cancelled: watch::Receiver<bool>,
) -> RelayAcquisition {
    let mut result = RelayAcquisition::new();
    let deadline = time::sleep(limits.timeout);
    tokio::pin!(deadline);

    if *cancelled.borrow() {
        result.end = AcquisitionEnd::Cancelled;
        return result;
    }

    let (activity_tx, mut activity_rx) = mpsc::channel(1);
    let (cancel_tx, cancel_rx) = oneshot::channel();
    let id = SubscriptionId::generate();
    if filters.is_empty() {
        result.end = AcquisitionEnd::Failed(Error::invalid_msg("filters cannot be empty"));
        return result;
    }
    let notifications = relay.inner.internal_notification_sender.subscribe();
    let register = relay
        .inner
        .add_auto_closing_subscription(id.clone(), filters.clone());
    tokio::pin!(register);
    let register_result = tokio::select! {
        biased;
        _ = cancelled.changed() => {
            result.end = AcquisitionEnd::Cancelled;
            return result;
        }
        _ = &mut deadline => {
            result.end = AcquisitionEnd::TimedOut;
            return result;
        }
        res = &mut register => res,
    };
    if let Err(error) = register_result {
        result.end = AcquisitionEnd::Failed(error);
        return result;
    }

    let mut setup_guard = SetupGuard {
        relay: relay.clone(),
        id: Some(id.clone()),
    };
    let message = ClientMessage::Req {
        subscription_id: Cow::Borrowed(&id),
        filters: filters.iter().map(Cow::Borrowed).collect(),
    };
    let send_result = tokio::select! {
        biased;
        _ = cancelled.changed() => {
            result.end = AcquisitionEnd::Cancelled;
            return result;
        }
        _ = &mut deadline => {
            result.end = AcquisitionEnd::TimedOut;
            return result;
        }
        res = relay.send_msg(message) => res,
    };
    if let Err(error) = send_result {
        result.end = AcquisitionEnd::Failed(error);
        return result;
    }
    let opts = SubscribeAutoCloseOptions::default().exit_policy(limits.policy);
    relay.inner.spawn_auto_closing_handler(
        id,
        filters,
        opts,
        notifications,
        Some(activity_tx),
        Some(cancel_rx),
    );
    setup_guard.id = None;
    let _cancel_on_drop = CancelOnDrop(Some(cancel_tx));

    loop {
        if *cancelled.borrow() {
            result.end = AcquisitionEnd::Cancelled;
            break;
        }
        tokio::select! {
            biased;
            _ = cancelled.changed() => {
                result.end = AcquisitionEnd::Cancelled;
                break;
            }
            _ = &mut deadline => {
                result.end = AcquisitionEnd::TimedOut;
                break;
            }
            activity = activity_rx.recv() => match activity {
                Some(SubscriptionActivity::ReceivedEvent(event)) => {
                    if !result.accept_event(event, limits) {
                        break;
                    }
                }
                Some(SubscriptionActivity::Closed(reason)) => {
                    result.end = reason.into();
                    break;
                }
                None => {
                    result.end = AcquisitionEnd::ReceiverClosed;
                    break;
                }
            }
        }
    }
    result
}

#[cfg(test)]
mod tests {
    use nostr::event::{EventBuilder, FinalizeEvent, Kind};
    use nostr::key::Keys;

    use super::*;

    fn event(content: &str) -> Event {
        EventBuilder::new(Kind::TextNote, content)
            .finalize(&Keys::generate())
            .unwrap()
    }

    #[test]
    fn large_event_exhausts_byte_budget_and_preserves_partial_result() {
        let small = event("first");
        let small_bytes = small.as_json().len();
        let large = event(&"x".repeat(4096));
        let large_bytes = large.as_json().len();
        let limits =
            AcquisitionLimits::new(1, 10, small_bytes + large_bytes - 1, Duration::from_secs(1));
        let mut result = RelayAcquisition::new();
        assert!(result.accept_event(small, limits));
        assert!(!result.accept_event(large, limits));
        assert!(matches!(result.end, AcquisitionEnd::ByteBudgetExceeded));
        assert_eq!(result.received_event_bytes, small_bytes + large_bytes);
        assert_eq!(result.high_water_items, 1);
        assert_eq!(result.high_water_event_bytes, small_bytes);
        assert_eq!(result.events.len(), 1);
    }

    #[test]
    fn many_small_events_exhaust_item_budget_with_partial_result() {
        let limits = AcquisitionLimits::new(1, 3, 100_000, Duration::from_secs(1));
        let mut result = RelayAcquisition::new();
        for index in 0..3 {
            assert!(result.accept_event(event(&index.to_string()), limits));
        }
        assert!(!result.accept_event(event("fourth"), limits));
        assert!(matches!(result.end, AcquisitionEnd::ItemBudgetExceeded));
        assert_eq!(result.events.len(), 3);
        assert_eq!(result.high_water_items, 3);
        assert_eq!(result.received_items, 4);
    }

    #[test]
    fn duplicate_traffic_consumes_budget_without_growing_dedup_state() {
        let repeated = event("same");
        let bytes = repeated.as_json().len();
        let limits = AcquisitionLimits::new(1, 1_024, 1_024 * bytes, Duration::from_secs(1));
        let mut result = RelayAcquisition::new();
        for _ in 0..1_024 {
            assert!(result.accept_event(repeated.clone(), limits));
        }
        assert!(!result.accept_event(repeated, limits));
        assert!(matches!(result.end, AcquisitionEnd::ItemBudgetExceeded));
        assert_eq!(result.events.len(), 1);
        assert_eq!(result.high_water_event_bytes, bytes);
        assert_eq!(result.duplicates, 1_023);
        assert_eq!(result.received_event_bytes, 1_025 * bytes);
    }

    #[test]
    fn relay_receive_loss_is_not_completion() {
        let end = AcquisitionEnd::from(SubscriptionAutoClosedReason::Lagged(7));
        assert!(matches!(end, AcquisitionEnd::ReceiveLoss(7)));
    }
}
