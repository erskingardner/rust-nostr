use std::future::IntoFuture;
use std::time::Duration;

use crate::error::Error;
use crate::future::BoxedFuture;
use crate::policy::AdmitStatus;
use crate::relay::{Relay, RelayStatus};
use crate::transport::websocket::{WebSocketSink, WebSocketStream};

/// Try to connect relay
#[must_use = "Does nothing unless you await!"]
pub struct TryConnect<'relay> {
    relay: &'relay Relay,
    timeout: Duration,
}

struct ReservedConnection<'relay> {
    relay: &'relay Relay,
    transferred: bool,
}

impl Drop for ReservedConnection<'_> {
    fn drop(&mut self) {
        if !self.transferred {
            self.relay.inner.finish_reserved_connection_task();
        }
    }
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
        Box::pin(async move {
            let status: RelayStatus = self.relay.status();

            if status.is_shutdown() {
                return Err(Error::shutdown());
            }

            if status.is_banned() {
                return Err(Error::banned());
            }

            // Check if relay can't connect
            if !status.can_connect() {
                return Ok(());
            }

            // Check connection policy
            if let AdmitStatus::Rejected { reason } =
                self.relay.inner.check_connection_policy().await?
            {
                // Set status to "terminated"
                self.relay.inner.set_status(RelayStatus::Terminated, false);

                // Return error
                return Err(Error::connection_rejected(reason));
            }

            // Reserve the relay before dialing. Otherwise a retiring task can
            // still own it after the handshake publishes Connected.
            if !self.relay.inner.reserve_try_connect()? {
                return Ok(());
            }
            let mut reservation = ReservedConnection {
                relay: self.relay,
                transferred: false,
            };

            // Try to connect
            // This will set the status to "terminated" if the connection fails
            let stream: (WebSocketSink, WebSocketStream) = self
                .relay
                .inner
                ._try_connect(self.timeout, RelayStatus::Terminated)
                .await?;

            self.relay.inner.spawn_reserved_connection_task(stream);
            reservation.transferred = true;

            Ok(())
        })
    }
}

#[cfg(test)]
mod tests {
    use async_utility::time;
    use nostr::types::RelayUrl;

    use super::*;
    use crate::error::ErrorKind;
    use crate::local_relay::*;

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

        assert_eq!(relay.status(), RelayStatus::Terminated);

        // Connection failed, the connection task is not running
        assert!(!relay.inner.is_running());
    }
}
