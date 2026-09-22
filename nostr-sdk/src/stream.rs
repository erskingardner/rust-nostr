use std::pin::Pin;
use std::task::{Context, Poll};

use futures::{Stream, StreamExt};
use tokio::sync::broadcast;
use tokio::sync::mpsc::Receiver;
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::wrappers::errors::BroadcastStreamRecvError;

/// An item from a notification stream that reports receiver-local loss.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NotificationUpdate<T> {
    /// A notification received by this stream.
    Notification(T),
    /// This stream missed `skipped` notifications because its buffer overflowed.
    ///
    /// The count is for this receiver only. It is not a count of unique events,
    /// nor does it identify which subscriptions or relays need reacquisition.
    /// Later notifications can still be received.
    Lagged {
        /// Number of notifications skipped by this receiver.
        skipped: u64,
    },
}

pub(crate) struct ReceiverStream<T> {
    inner: Receiver<T>,
}

impl<T> ReceiverStream<T> {
    #[inline]
    pub(crate) fn new(recv: Receiver<T>) -> Self {
        Self { inner: recv }
    }
}

impl<T> Stream for ReceiverStream<T> {
    type Item = T;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.inner.poll_recv(cx)
    }
}

pub(crate) struct NotificationStream<T> {
    inner: ReportingNotificationStream<T>,
}

impl<T> NotificationStream<T>
where
    T: Clone + Send + 'static,
{
    #[inline]
    pub(crate) fn new(inner: broadcast::Receiver<T>) -> Self {
        Self {
            inner: ReportingNotificationStream::new(inner),
        }
    }
}

impl<T> Stream for NotificationStream<T>
where
    T: Clone + Send + 'static,
{
    type Item = T;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            match self.inner.poll_next_unpin(cx) {
                Poll::Ready(Some(NotificationUpdate::Notification(notification))) => {
                    return Poll::Ready(Some(notification));
                }
                Poll::Ready(Some(NotificationUpdate::Lagged { .. })) => continue,
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

pub(crate) struct ReportingNotificationStream<T> {
    inner: BroadcastStream<T>,
}

impl<T> ReportingNotificationStream<T>
where
    T: Clone + Send + 'static,
{
    #[inline]
    pub(crate) fn new(inner: broadcast::Receiver<T>) -> Self {
        Self {
            inner: BroadcastStream::new(inner),
        }
    }
}

impl<T> Stream for ReportingNotificationStream<T>
where
    T: Clone + Send + 'static,
{
    type Item = NotificationUpdate<T>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.inner.poll_next_unpin(cx) {
            Poll::Ready(Some(Ok(notification))) => {
                Poll::Ready(Some(NotificationUpdate::Notification(notification)))
            }
            Poll::Ready(Some(Err(BroadcastStreamRecvError::Lagged(skipped)))) => {
                Poll::Ready(Some(NotificationUpdate::Lagged { skipped }))
            }
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

#[cfg(test)]
mod tests {
    use futures::StreamExt;
    use tokio::sync::broadcast;

    use super::{NotificationUpdate, ReportingNotificationStream};

    #[tokio::test]
    async fn reports_repeated_receiver_gaps_and_continues() {
        let (tx, rx) = broadcast::channel(2);
        let mut stream = ReportingNotificationStream::new(rx);

        for value in 0..8 {
            tx.send(value).unwrap();
        }
        assert_eq!(
            stream.next().await,
            Some(NotificationUpdate::Lagged { skipped: 6 })
        );
        assert_eq!(
            stream.next().await,
            Some(NotificationUpdate::Notification(6))
        );
        assert_eq!(
            stream.next().await,
            Some(NotificationUpdate::Notification(7))
        );

        for value in 8..12 {
            tx.send(value).unwrap();
        }
        assert_eq!(
            stream.next().await,
            Some(NotificationUpdate::Lagged { skipped: 2 })
        );
        assert_eq!(
            stream.next().await,
            Some(NotificationUpdate::Notification(10))
        );
        assert_eq!(
            stream.next().await,
            Some(NotificationUpdate::Notification(11))
        );

        drop(tx);
        assert_eq!(stream.next().await, None);
        assert_eq!(stream.next().await, None);
    }

    #[tokio::test]
    async fn dropping_a_receiver_does_not_interrupt_another() {
        let (tx, rx) = broadcast::channel(2);
        let mut retained = ReportingNotificationStream::new(rx);
        let dropped = ReportingNotificationStream::new(tx.subscribe());
        drop(dropped);

        tx.send(42).unwrap();
        assert_eq!(
            retained.next().await,
            Some(NotificationUpdate::Notification(42))
        );
    }
}
