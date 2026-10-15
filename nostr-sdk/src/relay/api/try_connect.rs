use std::future::IntoFuture;
use std::time::Duration;

use crate::error::Error;
use crate::future::BoxedFuture;
use crate::relay::Relay;

/// Try to connect relay
#[must_use = "Does nothing unless you await!"]
pub struct TryConnect<'relay> {
    relay: &'relay Relay,
    timeout: Duration,
}

impl<'relay> TryConnect<'relay> {
    #[inline]
    pub(crate) fn new(relay: &'relay Relay) -> Self {
        Self {
            relay,
            timeout: Duration::from_secs(15),
        }
    }

    /// Timeout (default: 15 sec)
    #[inline]
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }
}

impl<'relay> IntoFuture for TryConnect<'relay> {
    type Output = Result<(), Error>;
    type IntoFuture = BoxedFuture<'relay, Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(self.relay.inner.try_connect(self.timeout))
    }
}

#[cfg(test)]
mod tests {
    use nostr::types::RelayUrl;
    use tokio::task::JoinHandle;
    use tokio::time;

    use super::*;
    use crate::error::ErrorKind;
    use crate::local_relay::*;
    use crate::policy::AdmitStatus;
    use crate::relay::RelayStatus;
    use crate::test_utils::{
        ControlledConnectionPolicy, ControlledRelay, PolicyReply, TEST_TIMEOUT, fake_connection,
        start_try_connect, wait_until,
    };

    #[tokio::test]
    async fn test_try_connect() {
        // Mock relay
        let mock = MockRelay::run().await.unwrap();
        let url = mock.url().await;

        let relay: Relay = Relay::new(url);

        assert_eq!(relay.status(), RelayStatus::Initialized);

        relay
            .try_connect()
            .timeout(Duration::from_millis(500))
            .await
            .unwrap();

        assert_eq!(relay.status(), RelayStatus::Connected);

        time::sleep(Duration::from_millis(500)).await;

        assert!(relay.inner.is_running());
    }

    #[tokio::test]
    async fn test_try_connect_to_unreachable_relay() {
        let url = RelayUrl::parse("wss://127.0.0.1:666").unwrap();

        let relay: Relay = Relay::new(url);

        assert_eq!(relay.status(), RelayStatus::Initialized);

        let res = relay.try_connect().timeout(Duration::from_secs(2)).await;
        assert_eq!(res.unwrap_err().kind(), ErrorKind::Transport);

        assert_eq!(relay.status(), RelayStatus::Idle);

        // Connection failed; the persistent task waits for another request.
        assert!(relay.inner.is_running());
    }

    async fn terminal_operation_cancels_connection_policy(
        stop: fn(&Relay),
        expected_status: RelayStatus,
        expected_error: &str,
    ) {
        let (policy, mut checks) = ControlledConnectionPolicy::new();

        let ControlledRelay {
            relay,
            mut transport,
        } = ControlledRelay::with_policy(policy);

        let caller: JoinHandle<Result<(), Error>> = start_try_connect(&relay, TEST_TIMEOUT);
        let reply: PolicyReply = checks.next_check().await;

        assert_eq!(relay.status(), RelayStatus::Connecting);

        stop(&relay);

        let error: Error = time::timeout(TEST_TIMEOUT, caller)
            .await
            .expect("timed out waiting for the cancelled connection attempt")
            .expect("connection caller panicked")
            .unwrap_err();

        assert_eq!(error.kind(), ErrorKind::State);
        assert_eq!(error.to_string(), expected_error);
        assert_eq!(relay.status(), expected_status);
        assert_eq!(relay.stats().attempts(), 0);
        assert_eq!(relay.stats().success(), 0);

        wait_until("connection task to exit", || !relay.inner.is_running()).await;

        assert!(reply.is_closed(), "connection policy was not cancelled");
        transport.assert_no_pending_dials();
        checks.assert_no_pending_checks();
    }

    #[tokio::test]
    async fn try_connect_ban_cancels_pending_connection_policy() {
        terminal_operation_cancels_connection_policy(
            Relay::ban,
            RelayStatus::Banned,
            "relay banned",
        )
        .await;
    }

    #[tokio::test]
    async fn try_connect_shutdown_cancels_pending_connection_policy() {
        terminal_operation_cancels_connection_policy(
            Relay::shutdown,
            RelayStatus::Shutdown,
            "shutdown",
        )
        .await;
    }

    #[tokio::test]
    async fn concurrent_try_connect_shares_connection_policy_and_dial() {
        let (policy, mut checks) = ControlledConnectionPolicy::new();

        let ControlledRelay {
            relay,
            mut transport,
        } = ControlledRelay::with_policy(policy);

        let first: JoinHandle<Result<(), Error>> = start_try_connect(&relay, TEST_TIMEOUT);
        let reply: PolicyReply = checks.next_check().await;

        let mut second: BoxedFuture<'_, Result<(), Error>> =
            relay.try_connect().timeout(TEST_TIMEOUT).into_future();

        assert!(futures::poll!(second.as_mut()).is_pending());
        assert_eq!(relay.status(), RelayStatus::Connecting);
        checks.assert_no_pending_checks();
        transport.assert_no_pending_dials();

        reply
            .send(Ok(AdmitStatus::Success))
            .expect("connection policy was cancelled before approval");

        assert!(
            transport
                .next_dial()
                .await
                .send(Ok(fake_connection()))
                .is_ok(),
            "transport dial was cancelled before completion"
        );

        time::timeout(TEST_TIMEOUT, async {
            first
                .await
                .expect("first connection caller panicked")
                .unwrap();
            second.await.unwrap();
        })
        .await
        .expect("timed out waiting for concurrent connection callers");

        assert_eq!(relay.status(), RelayStatus::Connected);
        assert_eq!(relay.stats().attempts(), 1);
        assert_eq!(relay.stats().success(), 1);
        assert!(relay.inner.is_running());
        checks.assert_no_pending_checks();
        transport.assert_no_pending_dials();

        relay.shutdown();
        wait_until("connection task to exit", || !relay.inner.is_running()).await;
    }
}
