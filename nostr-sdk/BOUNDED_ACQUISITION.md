# Bounded SDK acquisition (U4)

`Client::acquire_events` is a per-relay batch primitive. MDK chooses targets,
filters, budgets, and retries; the SDK owns each request's transport, resources,
and outcome. A batch is preferable here to a stream because it can keep draining
relay traffic while its caller is stalled, retain a bounded partial result, and
make the terminal outcome available later through `AcquisitionHandle::finish`.
It does not persist events or maintain recovery obligations.

```rust,no_run
use std::time::Duration;
use nostr_sdk::prelude::*;

# async fn example(client: &Client, relay: RelayUrl, ids: Vec<EventId>) -> Result<(), Box<dyn std::error::Error>> {
let limits = AcquisitionLimits::new(1, 256, 2 * 1024 * 1024, Duration::from_secs(15));
let handle = client.acquire_events(
    ReqTarget::single(&relay, [Filter::new().ids(ids)]),
    limits,
).await?;
let cancel = handle.canceller(); // Keep this in MDK's cancellation task.
// cancel.cancel() can run while finish is waiting and still return partial data.
let report = handle.finish().await?;
for (url, outcome) in report.relays {
    // Persist admitted events and durable retry state in MDK, based on outcome.end.
    println!("{url}: {:?}, {} events", outcome.end, outcome.events.len());
}
# Ok(()) }
```

## Resource contract

Limits are per relay, and the call rejects more than `max_relays` selected
targets. All selected relays run concurrently. `max_items` counts every received
EVENT notification, including duplicates and the first over-limit item.
`max_event_bytes` counts `Event::as_json().len()` for those same notifications.
This is a serialized event JSON estimate, excluding EVENT envelope, subscription
ID, WebSocket framing, TLS, and retransmission. A duplicate consumes both
budgets. The first over-limit event is counted but not retained; its outcome is
incomplete. Once exactly at a budget, the request waits for its normal terminal
message or the next event. It never silently drops a possible new event and
then reports completion.

Distinct retained events live in one `BTreeSet<Event>`, which is also the
deduplication state. It holds at most `max_items` events and at most
`max_event_bytes` summed serialized event JSON lengths per relay. There is no
request-long event-ID set and no cross-relay deduplication. The request-local
activity channel has one slot; its producer can hold one additional event
while waiting to send, and the collector can hold one while processing. These
in-flight events can exceed the byte budget individually. The terminal report
occupies one result channel slot until consumed or dropped. Thus the aggregate
retained event JSON estimate is at most `max_relays * max_event_bytes`; this is
**not** a process memory bound.
Rust `Event` structure and tree nodes add overhead, `as_json()` allocates a
temporary string, and an over-budget item can be larger than the budget.

Connection-level buffers are shared by all subscriptions. The default relay
notification broadcast has 2,048 slots; its occupants can include parsed
events, and slow handling can trigger `ReceiveLoss`. The default relay message
limit is 5 MiB of trimmed incoming JSON, checked after WebSocket receipt and
before parsing. It can be changed or disabled. WebSocket receive buffers and
transport/parser allocations precede these request limits. Concurrent calls
multiply request-local budgets; the SDK does not impose a client-wide aggregate
acquisition cap. MDK must bound its own concurrent calls and use suitable
connection limits for its memory target. Target/filter construction happens
before relay-count validation and is proportional to caller input or configured
gossip targets; these limits do not bound filter/ID input size.

## Lifecycle and outcome contract

The background task drains independently of `finish`. A stalled caller can
call `cancel()` and later `finish()` to observe bounded partial data and
`Cancelled`. A cloneable `AcquisitionCanceller` can cancel from another task
while `finish()` is waiting. A completed report is retained until `finish`
or handle drop.
Dropping the handle cancels and discards its report. After subscription setup,
cancel/drop signals only this request's auto-closing handler, which sends CLOSE
and removes its subscription. Setup cancellation has an asynchronous cleanup
guard that removes the generated subscription ID and attempts CLOSE. If the
connection is gone, CLOSE is best effort; local removal still proceeds.
Unrelated subscriptions on the connection remain registered.

Each relay reports `Completed`, `ExitLimitReached`, `ItemBudgetExceeded`,
`ByteBudgetExceeded`, `Cancelled`, `TimedOut`, `Disconnected`, `ReceiveLoss`,
`ReceiverClosed`, `AuthenticationFailed`, `Rejected`, or setup `Failed`.
Only `Completed` satisfies the chosen request exit policy. This says nothing
about durable MDK admission or full historical coverage. A configured relay
count exit policy is incomplete even if the relay stopped normally. There is
no cursor here: arbitrary relays need not resume a truncated query without
gaps. Explicit-ID batches can reacquire a known inventory, but do not discover
unknown missing history efficiently.

## Finite local experiment

The test `finite_explicit_id_reacquisition_preserves_partial_and_reports_traffic`
stores eight known events. One three-item-budget query returns three events
and `ItemBudgetExceeded` after four received EVENT notifications. Four
two-ID batches then acquire all eight IDs. A measured run used five requests,
12 EVENT notifications, 4,176 estimated serialized event bytes, 6,695
connection text bytes received, and 1,020 connection text bytes sent (7,715
total). These are local, deterministic inventory and connection-payload
measurements, not wire bytes or a general discovery result.
The counters include reacquisition overhead from the first truncated query.
