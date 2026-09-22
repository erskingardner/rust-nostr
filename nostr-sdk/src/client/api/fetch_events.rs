use std::collections::{BTreeSet, HashMap};
use std::future::IntoFuture;
use std::time::Duration;

use futures::StreamExt;
use nostr::event::Event;
use nostr::types::url::RelayUrl;

use super::req_target::ReqTarget;
use super::stream_events::StreamEvents;
use crate::client::{Client, Error};
use crate::future::BoxedFuture;
use crate::relay::{DEFAULT_FETCH_EVENTS_LIMIT, RelayStreamEvent, ReqExitPolicy};

/// The terminal state of one relay in a bounded aggregate fetch.
#[derive(Debug)]
pub enum FetchRelayOutcome {
    /// Collection stopped before this relay produced a terminal outcome.
    Incomplete,
    /// This relay satisfied the configured exit policy, including EOSE when requested.
    Completed,
    /// This relay reached the configured event count; more matches may exist.
    LimitReached,
    /// This relay failed to satisfy the configured exit policy.
    Failed(Error),
}

/// Events and per-relay outcomes from a bounded aggregate fetch.
///
/// Events are deduplicated across relays. A relay's `Completed` outcome only
/// describes its configured request policy; it does not establish global or
/// downstream durable completeness. If `truncated` is true, collection stopped
/// at the event buffer limit and outstanding requests were cancelled.
#[derive(Debug)]
pub struct FetchEventsOutcome {
    /// Unique events collected before the request ended or reached its buffer limit.
    pub events: BTreeSet<Event>,
    /// Outcome for each selected relay, including relays that produced no events.
    pub relays: HashMap<RelayUrl, FetchRelayOutcome>,
    /// Whether the unique event buffer limit stopped collection early.
    pub truncated: bool,
}

/// Fetch events
#[must_use = "Does nothing unless you await!"]
pub struct FetchEvents<'client, 'url> {
    // --------------------------------------------------
    // WHEN ADDING NEW OPTIONS HERE,
    // REMEMBER TO UPDATE THE "Configuration" SECTION in
    // Client::fetch_events DOC.
    // --------------------------------------------------
    client: &'client Client,
    target: ReqTarget<'url>,
    timeout: Option<Duration>,
    policy: ReqExitPolicy,
    max_events: usize,
}

impl<'client, 'url> FetchEvents<'client, 'url> {
    pub(crate) fn new(client: &'client Client, target: ReqTarget<'url>) -> Self {
        Self {
            client,
            target,
            timeout: None,
            policy: ReqExitPolicy::ExitOnEOSE,
            max_events: DEFAULT_FETCH_EVENTS_LIMIT,
        }
    }

    /// Set a timeout
    ///
    /// By default, no timeout is configured.
    #[inline]
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    /// Set request exit policy (default: [`ReqExitPolicy::ExitOnEOSE`]).
    #[inline]
    pub fn policy(mut self, policy: ReqExitPolicy) -> Self {
        self.policy = policy;
        self
    }

    /// Set the maximum number of unique events to buffer (default: 10,000).
    #[inline]
    pub fn max_events(mut self, max: usize) -> Self {
        self.max_events = max;
        self
    }

    /// Fetch bounded events with endpoint outcomes and retained partial data.
    ///
    /// A failed relay does not stop a healthy relay. The default bound is
    /// 10,000 unique events; use [`Self::max_events`] to change it. Reaching
    /// that bound on a new event returns the events already collected with
    /// `truncated = true` and cancels outstanding requests. Event count is not
    /// a byte bound; use streaming for large recovery workloads.
    pub async fn with_outcomes(self) -> Result<FetchEventsOutcome, Error> {
        let mut stream = self.client.stream_events(self.target).policy(self.policy);
        if let Some(timeout) = self.timeout {
            stream = stream.timeout(timeout);
        }

        let (urls, mut stream) = stream.into_outcome_stream_with_targets().await?;
        let mut result = FetchEventsOutcome {
            events: BTreeSet::new(),
            relays: urls
                .into_iter()
                .map(|url| (url, FetchRelayOutcome::Incomplete))
                .collect(),
            truncated: false,
        };

        while let Some((url, item)) = stream.next().await {
            match item {
                RelayStreamEvent::Event(event) => {
                    if result.events.len() >= self.max_events && !result.events.contains(&event) {
                        result.truncated = true;
                        break;
                    }
                    result.events.insert(event);
                }
                RelayStreamEvent::Error(error) => {
                    result.relays.insert(url, FetchRelayOutcome::Failed(error));
                }
                RelayStreamEvent::Completed => {
                    result.relays.insert(url, FetchRelayOutcome::Completed);
                }
                RelayStreamEvent::LimitReached => {
                    result.relays.insert(url, FetchRelayOutcome::LimitReached);
                }
            }
        }

        Ok(result)
    }
}

impl<'client, 'url> IntoFuture for FetchEvents<'client, 'url>
where
    'url: 'client,
{
    type Output = Result<BTreeSet<Event>, Error>;
    type IntoFuture = BoxedFuture<'client, Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move {
            // Stream events
            let mut stream: StreamEvents<'client, 'url> =
                self.client.stream_events(self.target).policy(self.policy);

            // Set timeout
            if let Some(timeout) = self.timeout {
                stream = stream.timeout(timeout);
            }

            // Execute stream
            let mut stream = stream.await?;

            // Lookup ID: EVENT_ORD_IMPL
            let mut events: BTreeSet<Event> = BTreeSet::new();

            // Collect events
            while let Some((url, result)) = stream.next().await {
                // NOTE: not propagate the error here! A single error by any of the relays would stop the entire fetching process.
                match result {
                    Ok(event) => {
                        if events.len() >= self.max_events && !events.contains(&event) {
                            // TODO: break the stream instead of returnin an error?
                            return Err(Error::limit_exceeded("too many fetched events"));
                        }

                        events.insert(event);
                    }
                    Err(e) => {
                        tracing::error!(url = %url, error = %e, "Failed to handle streamed event");
                    }
                }
            }

            Ok(events)
        })
    }
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::net::SocketAddr;
    use std::pin::Pin;
    use std::time::Duration;

    use nostr::event::{EventBuilder, FinalizeEvent, Kind};
    use nostr::filter::Filter;
    use nostr::key::Keys;
    use nostr::message::MachineReadablePrefix;

    use super::*;
    use crate::authenticator::SignerAuthenticator;
    use crate::error::ErrorKind;
    use crate::local_relay::{LocalRelay, QueryPolicy, QueryPolicyResult};
    use crate::test_utils::{
        setup_client, setup_client_with_authenticator, setup_nip42_read_local_relay,
    };

    #[derive(Debug)]
    struct RejectQuery;

    impl QueryPolicy for RejectQuery {
        fn admit_query<'a>(
            &'a self,
            _query: &'a mut Filter,
            _addr: &'a SocketAddr,
        ) -> Pin<Box<dyn Future<Output = QueryPolicyResult> + Send + 'a>> {
            Box::pin(async {
                QueryPolicyResult::reject(MachineReadablePrefix::Blocked, "query rejected")
            })
        }
    }

    #[tokio::test]
    async fn aggregate_fetch_retains_events_and_endpoint_failures() {
        let healthy = LocalRelay::builder().build();
        healthy.run().await.unwrap();
        let failing = LocalRelay::builder().query_policy(RejectQuery).build();
        failing.run().await.unwrap();

        let keys = Keys::generate();
        let expected = EventBuilder::new(Kind::TextNote, "healthy event")
            .finalize(&keys)
            .unwrap();
        healthy.add_event(expected.clone()).await.unwrap();

        let healthy_url = healthy.url().await;
        let failing_url = failing.url().await;
        let client = Client::new();
        client.add_relay(&healthy_url).and_connect().await.unwrap();
        client.add_relay(&failing_url).and_connect().await.unwrap();

        let outcome = client
            .fetch_events(Filter::new().author(keys.public_key()))
            .timeout(Duration::from_secs(2))
            .with_outcomes()
            .await
            .unwrap();

        assert!(!outcome.truncated);
        assert_eq!(outcome.events.len(), 1);
        assert_eq!(
            outcome.events.first().map(|event| event.id),
            Some(expected.id)
        );
        assert!(matches!(
            outcome.relays.get(&healthy_url),
            Some(FetchRelayOutcome::Completed)
        ));
        match outcome.relays.get(&failing_url) {
            Some(FetchRelayOutcome::Failed(error)) => {
                assert!(error.to_string().contains("query rejected"));
            }
            other => panic!("unexpected failing-relay outcome: {other:?}"),
        }
    }

    #[tokio::test]
    async fn aggregate_fetch_exposes_truncation_and_cancels_request() {
        let local = LocalRelay::new();
        local.run().await.unwrap();
        let keys = Keys::generate();
        for i in 0..5 {
            let event = EventBuilder::new(Kind::TextNote, i.to_string())
                .finalize(&keys)
                .unwrap();
            local.add_event(event).await.unwrap();
        }

        let url = local.url().await;
        let client = setup_client(url.clone()).await;
        let outcome = client
            .fetch_events(Filter::new().author(keys.public_key()))
            .max_events(3)
            .timeout(Duration::from_secs(2))
            .with_outcomes()
            .await
            .unwrap();

        assert!(outcome.truncated);
        assert_eq!(outcome.events.len(), 3);
        assert!(matches!(
            outcome.relays.get(&url),
            Some(FetchRelayOutcome::Incomplete)
        ));
    }

    #[tokio::test]
    async fn aggregate_fetch_empty_completion_is_not_failure() {
        let local = LocalRelay::new();
        local.run().await.unwrap();
        let url = local.url().await;
        let client = setup_client(url.clone()).await;

        let outcome = client
            .fetch_events(Filter::new().kind(Kind::Metadata))
            .timeout(Duration::from_secs(2))
            .with_outcomes()
            .await
            .unwrap();

        assert!(outcome.events.is_empty());
        assert!(!outcome.truncated);
        assert!(matches!(
            outcome.relays.get(&url),
            Some(FetchRelayOutcome::Completed)
        ));
    }

    #[tokio::test]
    async fn test_client_fetch_events_dont_resubscribes_after_auth_required_closed_without_authenticator()
     {
        let local = setup_nip42_read_local_relay().await;

        let keys = Keys::generate();
        let expected = EventBuilder::new(Kind::TextNote, "Test")
            .finalize(&keys)
            .unwrap();
        local.add_event(expected.clone()).await.unwrap();

        let client = setup_client(local.url().await).await;

        let filter = Filter::new().kind(Kind::TextNote).limit(1);

        let events = client
            .fetch_events(filter)
            .timeout(Duration::from_secs(5))
            .await
            .unwrap();

        assert_eq!(events.len(), 0);
    }

    #[tokio::test]
    async fn test_client_fetch_events_resubscribes_after_auth_required_closed() {
        let local = setup_nip42_read_local_relay().await;

        let keys = Keys::generate();
        let expected = EventBuilder::new(Kind::TextNote, "Test")
            .finalize(&keys)
            .unwrap();
        local.add_event(expected.clone()).await.unwrap();

        let authenticator = SignerAuthenticator::new(keys);
        let client = setup_client_with_authenticator(local.url().await, authenticator).await;

        let filter = Filter::new().kind(Kind::TextNote).limit(1);

        let events = client
            .fetch_events(filter)
            .timeout(Duration::from_secs(5))
            .await
            .unwrap();

        assert_eq!(events.len(), 1);
        assert_eq!(events.first().map(|event| event.id), Some(expected.id));
    }

    #[tokio::test]
    async fn test_client_fetch_events_enforces_buffer_limit() {
        let local = LocalRelay::new();
        local.run().await.unwrap();
        let keys = Keys::generate();

        for i in 0..5 {
            let event = EventBuilder::new(Kind::TextNote, i.to_string())
                .finalize(&keys)
                .unwrap();
            local.add_event(event).await.unwrap();
        }

        let client = setup_client(local.url().await).await;
        let err = client
            .fetch_events(Filter::new().kind(Kind::TextNote))
            .max_events(3)
            .timeout(Duration::from_secs(5))
            .await
            .unwrap_err();

        assert_eq!(err.kind(), ErrorKind::LimitExceeded);
    }
}
