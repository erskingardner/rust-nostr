use std::borrow::Cow;
use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_utility::{task, time};
use nostr::event::Event;
use nostr::filter::Filter;
use nostr::message::{ClientMessage, SubscriptionId};
use nostr::types::Timestamp;
use tokio::sync::{Notify, mpsc, oneshot, watch};

use crate::error::Error;
use crate::relay::{
    Relay, ReqExitPolicy, SubscribeAutoCloseOptions, SubscriptionActivity,
    SubscriptionAutoClosedReason,
};

/// Per-relay resource limits for a bounded acquisition.
///
/// Each parsed raw EVENT, including filtered events and duplicates, consumes
/// one item. Validated EVENT notifications also consume their serialized event
/// JSON length in bytes. This bounds the retained event set,
/// its deduplication keys and the one-item request activity queue. The queue
/// producer and collector may each hold one additional event, which can be
/// larger than the budget. It does not bound Rust object overhead, temporary
/// serialization, shared connection queues, WebSocket frames, or parser allocations.
/// Every validated notification is serialized again to measure its JSON length,
/// including duplicates. Filtered raw events do not consume the byte budget;
/// connection message limits still apply before parsing.
#[derive(Debug, Clone, Copy)]
pub struct AcquisitionLimits {
    /// Maximum parsed raw EVENTs per relay, including filtered events and duplicates.
    pub max_items: usize,
    /// Maximum sum of validated notification event JSON bytes per relay.
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
    /// The notification receiver skipped this many messages.
    ReceiveLoss(u64),
    /// A rejected connection message may have contained this request's input.
    /// It is not an empty page or a skipped notification.
    InvalidMessage,
    /// The shared connection notification receiver closed unexpectedly.
    ReceiverClosed,
    /// NIP-42 authentication failed.
    AuthenticationFailed,
    /// The relay sent CLOSED or another subscription error.
    Rejected(String),
    /// The relay sent an unprefixed CLOSED before the exit policy was satisfied.
    RelayClosed(String),
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
            SubscriptionAutoClosedReason::RelayClosed(message) => Self::RelayClosed(message),
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
    /// Parsed EVENTs for this request before event filtering, including duplicates
    /// and the first over-item-budget event. Frozen when acquisition terminates.
    /// Malformed messages and frames rejected before EVENT parsing are excluded.
    pub raw_received_items: usize,
    /// Minimum raw EVENT timestamp counted in `raw_received_items`, even for
    /// expired, invalid, mismatched, rejected or deleted events. Untrusted paging
    /// evidence only: neither admission nor proof of historical coverage.
    pub oldest_raw_created_at: Option<Timestamp>,
    /// Validated EVENT notifications, including duplicates and the over-budget item.
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
            raw_received_items: 0,
            oldest_raw_created_at: None,
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

    fn accept_event(
        &mut self,
        event: Event,
        limits: AcquisitionLimits,
        budget: ItemBudget,
    ) -> bool {
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
        if budget == ItemBudget::Exceeded || self.received_items > limits.max_items {
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ItemBudget {
    Within,
    Exceeded,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RawPhase {
    Active,
    Exhausted,
    ExhaustedValid,
}

#[derive(Debug)]
struct RawEvidence {
    phase: RawPhase,
    closed: bool,
    items: usize,
    oldest: Option<Timestamp>,
    forwarded: usize,
    rejected: bool,
}

/// Scalar request-owned evidence: no raw EVENT queue or retained event copies.
#[derive(Debug)]
pub(crate) struct RawAcquisition {
    evidence: Mutex<RawEvidence>,
    max_items: usize,
    stop_on_eose: bool,
    terminal: Notify,
}

impl RawAcquisition {
    pub(crate) fn new(max_items: usize, policy: ReqExitPolicy) -> Self {
        Self {
            evidence: Mutex::new(RawEvidence {
                phase: RawPhase::Active,
                closed: false,
                items: 0,
                oldest: None,
                forwarded: 0,
                rejected: false,
            }),
            max_items,
            stop_on_eose: matches!(policy, ReqExitPolicy::ExitOnEOSE),
            terminal: Notify::new(),
        }
    }

    pub(crate) fn close(&self) {
        self.evidence.lock().unwrap().closed = true;
    }

    pub(crate) fn received_eose(&self) {
        if self.stop_on_eose {
            self.close();
        }
    }

    pub(crate) fn record(&self, created_at: Timestamp) -> bool {
        let mut evidence = self.evidence.lock().unwrap();
        if evidence.closed || evidence.phase != RawPhase::Active || evidence.rejected {
            return false;
        }
        let next: Option<usize> = evidence.items.checked_add(1);
        evidence.items = next.unwrap_or(usize::MAX);
        evidence.oldest = Some(
            evidence
                .oldest
                .map_or(created_at, |oldest| oldest.min(created_at)),
        );
        if next.is_none() || evidence.items > self.max_items {
            evidence.phase = RawPhase::Exhausted;
        }
        // Validate this first overflow frame so existing validated counters
        // still include it. The collector must not retain it.
        true
    }

    pub(crate) fn forward(&self) {
        let mut evidence = self.evidence.lock().unwrap();
        if evidence.closed {
            return;
        }
        match evidence.phase {
            RawPhase::Active => {
                evidence.forwarded = evidence.forwarded.saturating_add(1);
            }
            RawPhase::Exhausted => {
                evidence.forwarded = evidence.forwarded.saturating_add(1);
                evidence.phase = RawPhase::ExhaustedValid;
            }
            RawPhase::ExhaustedValid => {}
        }
    }

    pub(crate) fn processed(&self) {
        let evidence = self.evidence.lock().unwrap();
        if matches!(
            evidence.phase,
            RawPhase::Exhausted | RawPhase::ExhaustedValid
        ) {
            self.terminal.notify_one();
        }
    }

    pub(crate) fn reject_input(&self) {
        let mut evidence = self.evidence.lock().unwrap();
        if evidence.closed || evidence.rejected {
            return;
        }
        evidence.rejected = true;
        self.terminal.notify_one();
    }

    fn terminal_end(&self) -> AcquisitionEnd {
        if self.evidence.lock().unwrap().rejected {
            AcquisitionEnd::InvalidMessage
        } else {
            AcquisitionEnd::ItemBudgetExceeded
        }
    }

    fn item_budget(&self, received: usize) -> ItemBudget {
        let evidence = self.evidence.lock().unwrap();
        if evidence.phase == RawPhase::ExhaustedValid
            && received.saturating_add(1) >= evidence.forwarded
        {
            return ItemBudget::Exceeded;
        }
        ItemBudget::Within
    }

    fn drained(&self, received: usize) -> bool {
        received >= self.evidence.lock().unwrap().forwarded
    }

    fn finish(&self, result: &mut RelayAcquisition) {
        let mut evidence = self.evidence.lock().unwrap();
        result.raw_received_items = evidence.items;
        result.oldest_raw_created_at = evidence.oldest;
        if (evidence.rejected
            || matches!(
                evidence.phase,
                RawPhase::Exhausted | RawPhase::ExhaustedValid
            ))
            && matches!(
                result.end,
                AcquisitionEnd::Completed
                    | AcquisitionEnd::ExitLimitReached
                    | AcquisitionEnd::ReceiverClosed
            )
        {
            result.end = if evidence.rejected {
                AcquisitionEnd::InvalidMessage
            } else {
                AcquisitionEnd::ItemBudgetExceeded
            };
        }
        evidence.closed = true;
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
    cancelled: watch::Receiver<bool>,
) -> RelayAcquisition {
    let raw: Arc<RawAcquisition> = Arc::new(RawAcquisition::new(limits.max_items, limits.policy));
    let mut result: RelayAcquisition = acquire_inner(relay, filters, limits, cancelled, &raw).await;
    raw.finish(&mut result);
    result
}

async fn acquire_inner(
    relay: Relay,
    filters: Vec<Filter>,
    limits: AcquisitionLimits,
    mut cancelled: watch::Receiver<bool>,
    raw: &Arc<RawAcquisition>,
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
    let id = relay.inner.acquisition_subscription_id();
    if filters.is_empty() {
        result.end = AcquisitionEnd::Failed(Error::invalid_msg("filters cannot be empty"));
        return result;
    }
    let notifications = relay.inner.internal_notification_sender.subscribe();
    let register =
        relay
            .inner
            .add_acquisition_subscription(id.clone(), filters.clone(), raw.clone());
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
    let opts = SubscribeAutoCloseOptions::default()
        .exit_policy(limits.policy)
        .report_relay_closed(true);
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
    let mut raw_terminal: bool = false;

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
            _ = raw.terminal.notified(), if !raw_terminal => {
                raw_terminal = true;
                // Drain previously validated notifications through the existing
                // bounded queue before reporting raw overflow or parse loss.
                if raw.drained(result.received_items) {
                    result.end = raw.terminal_end();
                    break;
                }
            }
            activity = activity_rx.recv() => match activity {
                Some(SubscriptionActivity::ReceivedEvent(event)) => {
                    let budget: ItemBudget = raw.item_budget(result.received_items);
                    if !result.accept_event(event, limits, budget) {
                        break;
                    }
                    if raw_terminal && raw.drained(result.received_items) {
                        result.end = raw.terminal_end();
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
    fn raw_evidence_stops_at_limit() {
        let raw: RawAcquisition = RawAcquisition::new(2, ReqExitPolicy::ExitOnEOSE);
        assert!(raw.record(Timestamp::from(20)));
        raw.forward();
        assert!(raw.record(Timestamp::from(20)));
        assert!(raw.record(Timestamp::from(10)));
        raw.processed();
        assert!(!raw.record(Timestamp::from(1)));
        assert!(!raw.drained(0));
        assert!(raw.drained(1));
        let mut result: RelayAcquisition = RelayAcquisition::new();
        raw.finish(&mut result);
        assert_eq!(result.raw_received_items, 3);
        assert_eq!(result.oldest_raw_created_at, Some(Timestamp::from(10)));
        assert!(matches!(result.end, AcquisitionEnd::ItemBudgetExceeded));
        assert!(!raw.record(Timestamp::from(0)));
        assert_eq!(result.received_items, 0);
        assert_eq!(result.received_event_bytes, 0);
    }

    #[test]
    fn raw_parse_loss_survives_eose() {
        let raw: RawAcquisition = RawAcquisition::new(2, ReqExitPolicy::ExitOnEOSE);
        raw.reject_input();
        raw.received_eose();
        let mut result: RelayAcquisition = RelayAcquisition::new();
        result.end = AcquisitionEnd::Completed;
        raw.finish(&mut result);
        assert!(matches!(result.end, AcquisitionEnd::InvalidMessage));
        assert_eq!(result.raw_received_items, 0);
        assert_eq!(result.oldest_raw_created_at, None);
        assert!(!raw.record(Timestamp::from(1)));
    }

    #[test]
    fn raw_eose_ignores_late_input() {
        let raw = RawAcquisition::new(1, ReqExitPolicy::ExitOnEOSE);
        assert!(raw.record(Timestamp::from(20)));
        raw.forward();
        raw.received_eose();
        assert!(!raw.record(Timestamp::from(1)));
        raw.reject_input();
        let mut result = RelayAcquisition::new();
        result.end = AcquisitionEnd::Completed;
        raw.finish(&mut result);
        assert!(matches!(result.end, AcquisitionEnd::Completed));
        assert_eq!(result.raw_received_items, 1);
        assert_eq!(result.oldest_raw_created_at, Some(Timestamp::from(20)));
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
        assert!(result.accept_event(small, limits, ItemBudget::Within));
        assert!(!result.accept_event(large, limits, ItemBudget::Within));
        assert!(matches!(result.end, AcquisitionEnd::ByteBudgetExceeded));
        assert_eq!(result.received_event_bytes, small_bytes + large_bytes);
        assert_eq!(result.high_water_items, 1);
        assert_eq!(result.high_water_event_bytes, small_bytes);
        assert_eq!(result.events.len(), 1);
    }

    #[test]
    fn raw_overflow_keeps_valid_stats() {
        let raw: RawAcquisition = RawAcquisition::new(1, ReqExitPolicy::ExitOnEOSE);
        assert!(raw.record(Timestamp::from(20)));
        raw.forward();
        raw.processed();
        assert!(raw.record(Timestamp::from(10)));
        raw.forward();
        raw.processed();
        raw.close();
        assert_eq!(raw.item_budget(0), ItemBudget::Within);
        assert_eq!(raw.item_budget(1), ItemBudget::Exceeded);
        assert!(!raw.drained(1));
        let limits: AcquisitionLimits =
            AcquisitionLimits::new(1, 10, 100_000, Duration::from_secs(1));
        let mut result: RelayAcquisition = RelayAcquisition::new();
        assert!(result.accept_event(event("first"), limits, raw.item_budget(0)));
        assert!(!result.accept_event(event("overflow"), limits, raw.item_budget(1)));
        raw.finish(&mut result);
        assert_eq!(result.raw_received_items, 2);
        assert_eq!(result.received_items, 2);
        assert_eq!(result.events.len(), 1);
        assert!(matches!(result.end, AcquisitionEnd::ItemBudgetExceeded));
    }

    #[test]
    fn many_small_events_exhaust_item_budget_with_partial_result() {
        let limits = AcquisitionLimits::new(1, 3, 100_000, Duration::from_secs(1));
        let mut result = RelayAcquisition::new();
        for index in 0..3 {
            assert!(result.accept_event(event(&index.to_string()), limits, ItemBudget::Within));
        }
        assert!(!result.accept_event(event("fourth"), limits, ItemBudget::Within));
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
            assert!(result.accept_event(repeated.clone(), limits, ItemBudget::Within));
        }
        assert!(!result.accept_event(repeated, limits, ItemBudget::Within));
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

        let end = AcquisitionEnd::from(SubscriptionAutoClosedReason::RelayClosed(String::new()));
        assert!(matches!(end, AcquisitionEnd::RelayClosed(reason) if reason.is_empty()));
    }
}
