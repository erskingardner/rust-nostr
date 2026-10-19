# Bounded SDK acquisition (U4)

`Client::acquire_events` is a per-relay batch primitive. MDK chooses targets,
filters, budgets, and retries; the SDK owns each request's transport, resources,
and outcome. A batch is preferable here to a stream because it can keep draining
relay traffic while its caller is stalled, retain a bounded partial result, and
make the terminal outcome available later through `AcquisitionHandle::finish`.
It does not persist events or maintain recovery obligations.
Acquisition REQs use request-local subscription IDs. Valid events still reach
the bounded per-relay result, but the acquisition does not save a first-seen
event into the client's shared event database, update shared gossip routing, or
emit an ordinary live `Event` notification. A later copy on an active live
subscription retains its normal first-delivery behavior. Acquisition input
still passes signature, expiry, configured subscription/admission checks, and
the database's ID-level deletion check. It does not pass `save_event` rules for
coordinate deletion, replacement, vanish, or other storage rejection before
being returned. Callers must apply their own durable admission rules before
treating a report as state. The SDK recognizes only its own relay-instance
acquisition ID namespace after a request closes, so a late reply cannot enter
the shared cache; unrelated caller-issued raw REQ IDs keep their behavior.
Callers must not deliberately reuse an in-flight acquisition ID for a live
REQ: the Nostr wire ID alone cannot distinguish two owners of the same ID.

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
targets. All selected relays run concurrently. `max_items` counts parsed raw
EVENTs attributed to the registered request ID, including filtered events,
duplicates, and the first over-limit item. That first overflow frame still
passes through normal validation. If valid, it contributes to the validated
notification counters but is not retained, even if earlier raw events were
filtered. If rejected, it contributes only to raw evidence. Further frames for
the exhausted request are discarded before event validation. Previously
forwarded validated notifications drain through the existing bounded queue
before item-budget termination, unless cancellation, deadline or another
terminal failure interrupts them.

`max_event_bytes` counts `Event::as_json().len()` for validated EVENT
notifications, including duplicates and the first over-budget notification.
This is a serialized event JSON estimate, excluding EVENT envelope, subscription
ID, WebSocket framing, TLS, and retransmission. Rejected raw events do not
consume this byte budget. The raw item cap and deadline bound request-owned
processing of rejected events; connection message limits bound individual
incoming messages. Neither budget bounds all bytes or parsing work arriving on
the shared connection, including frames discarded during teardown. Raw evidence
does not serialize events or retain another event copy.

Once exactly at a budget, the request waits for its normal terminal message or
the next event. It never silently drops a possible new event and then reports
completion.

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

### Raw evidence and validated results

Each `RelayAcquisition` exposes two separate views of the request:

- `raw_received_items: usize` counts successfully parsed EVENTs while the
  request owns its subscription, before event size, tag, subscription/filter,
  expiry, signature, admission-policy or database ID-deletion filtering. It
  includes duplicates and the first over-item-budget frame, and then stops.
- `oldest_raw_created_at: Option<nostr::types::Timestamp>` is the minimum `created_at`
  of those counted raw EVENTs, or `None` when none were counted.
- `events`, `received_items`, `received_event_bytes`, `duplicates` and the
  high-water counters describe validated notifications and bounded retained
  events. Expired or signature-invalid events never enter this view. Existing
  all-valid traffic accounting includes the first over-budget notification.

Malformed messages and frames rejected before EVENT parsing are absent from raw
evidence. If the single-pass parser decoded an EVENT subscription ID before
failing, that exact active acquisition terminates with `InvalidMessage` rather
than treating a later EOSE as an empty completed page. The first attributable
parse loss is recorded; subsequent frames cannot restore completion. This
uses the already-decoded ID without another parse, queue or ID copy. When the
EVENT ID or message type cannot be decoded, or the connection rejects a message's
size before parsing, all active acquisitions on that connection become
`InvalidMessage`. A decoded non-EVENT message type cannot contain EVENT input;
its parse failure does not invalidate acquisitions. A later EOSE cannot turn
lost EVENT input into an empty page. Other connections and ordinary live
subscriptions remain registered.
The counters and timestamp are request-local scalar state. With `ExitOnEOSE`,
the connection reader freezes them at EOSE, before notifying the collector.
Other policies freeze them when their auto-closing handler stops; collector
termination also freezes them. Queued live input or a malformed frame after
termination cannot change the page's outcome. There is no raw-event metadata queue. Registration
attributes evidence to the exact request ID under the subscription registry
lock; simultaneous requests and ordinary live subscription traffic do not
share it. Closed generated IDs cannot update a completed report or populate the
shared event cache.

Raw evidence is untrusted paging input, not admission, a coverage certificate,
or a reusable cursor. A completed page can contain 256 raw expired or invalid
events and no validated events while older valid history remains available.
Its raw count and timestamp let a caller distinguish that case from an empty
raw page. Duplicate timestamps, dishonest timestamps and relay truncation still
require caller-owned bounded paging policy; subtracting one from a saturated
boundary timestamp can skip events tied at that timestamp.

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
An EVENT already in processing when teardown begins remains classified as
acquisition input. After local removal, late replies on that generated ID are
rejected without a retained tombstone or connection-wide teardown.

Each relay reports `Completed`, `ExitLimitReached`, `ItemBudgetExceeded`,
`ByteBudgetExceeded`, `Cancelled`, `TimedOut`, `Disconnected`, `ReceiveLoss`,
`InvalidMessage`, `ReceiverClosed`, `AuthenticationFailed`, `Rejected`, `RelayClosed`, or setup
`Failed`. An unprefixed relay `CLOSED` before the request exit policy is
satisfied is `RelayClosed`, including during the post-EOSE grace interval.
Only `Completed` satisfies the chosen request exit policy. This says nothing
about durable MDK admission or full historical coverage. A configured relay
count exit policy is incomplete even if the relay stopped normally. There is
no cursor here: arbitrary relays need not resume a truncated query without
gaps. Explicit-ID batches can reacquire a known inventory, but do not discover
unknown missing history efficiently.

## Finite local experiment

The test `finite_explicit_id_reacquisition_preserves_partial_and_reports_traffic`
stores eight known events. One three-item-budget query returns three events
and `ItemBudgetExceeded` after four raw EVENTs and four validated notifications. Four
two-ID batches then acquire all eight IDs. A measured run used five requests,
12 EVENT notifications, 4,176 estimated serialized event bytes, 6,695
connection text bytes received, and 1,020 connection text bytes sent (7,715
total). These are local, deterministic inventory and connection-payload
measurements, not wire bytes or a general discovery result.
The counters include reacquisition overhead from the first truncated query.

## Saturation qualification

The focused `acquire_events` tests include a 96-event loopback history with
8 KiB contents and a stalled caller. With a 64 KiB serialized-event budget,
the request ends as `ByteBudgetExceeded`, retains no more than seven events,
and reports the first over-budget EVENT in its received counters. A separate
live metadata subscription actually receives a new event during the test.
Another test injects 32 duplicate parsed relay notifications for one active
request: the 16-item budget stops at the seventeenth notification, with one
retained event and 15 counted duplicates. This injection measures request and
notification behavior, not WebSocket traffic.

A four-slot relay notification channel is synchronously filled with EVENTs
and a trailing EOSE. The request must return `ReceiveLoss`, never `Completed`.
Controlled registration gates and ongoing loopback traffic exercise cancel
and drop during setup and while EVENTs arrive. Both paths release only their
own subscriptions; the unrelated live subscription receives an event after
cleanup. Tests print cancellation and live-delivery elapsed times and the
request's retained-state high-water counters. These are local latency samples,
not service-level guarantees. Run them with
`cargo test -p nostr-sdk --lib acquire_events -- --nocapture`.

The loopback socket tests `raw_full_page_finds_older`,
`raw_mixed_page_keeps_ties`, `raw_limits_and_cancel_cleanup`,
`raw_requests_keep_evidence`, `raw_filtered_overflow_counts` and
`raw_parse_loss_is_incomplete` exercise raw
filtered-page evidence through the client API. The full-page case covers both
256 expired events and 256 invalid signatures above an older valid event.
The remaining cases cover mixed pages, duplicate timestamp boundaries,
over-limit accounting after filtering, cancellation, late replies after CLOSE,
overlapping acquisition/live subscription IDs, and attributable parser loss
followed by EOSE. These tests can be selected
with `cargo test -p nostr-sdk --lib client::api::acquire_events::tests::raw_ -- --nocapture`.
