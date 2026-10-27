# Ledjer (Swift)

A Swift port of the core of [planmdk/ledjer](https://github.com/planmdk/ledjer), a small
event sourcing library built around [Event Modeling](https://eventmodeling.org/), with an
on-device SQLite event store (GRDB).

| Product       | Contents                                                              | Dependencies |
|---------------|-----------------------------------------------------------------------|--------------|
| `Ledjer`      | events, queries, state views, persisted views, automations, `gwt`/`gt`, in-memory store | none (but SwiftPM still resolves GRDB, as the package declares it) |
| `LedjerGRDB`  | `GRDBEventStore`, `GRDBAccumulator`                                   | GRDB 7       |
| `LedjerStoreContract` | contract checks that any `EventStore` must pass                | none         |

Requires Swift 6.1, iOS 16 / macOS 13.

## Event Modeling → Ledjer

| Event Modeling   | Clojure                | Swift                                         |
|------------------|------------------------|-----------------------------------------------|
| event            | `defevent`             | `struct: Event`                               |
| state view       | `defview`              | `StateView`                                   |
| persisted view   | `defpersistedview`     | `PersistedStateView` → `PersistedViewHandle`  |
| automation       | `automation`           | `automation(on:triggers:onError:body:)`       |
| state change     | fn that appends        | any `async` function that appends             |
| GWT / GT         | `gwt` / `gt`           | `gwt(given:when:then:)` / `gt(...)`           |

```swift
struct Deposited: Event {
    static let eventType: EventType = "bank/deposited"
    static var tags: [EventTag<Self>] { [EventTag("account", \.account)] }
    var account: String
    var amount: Int
}

let store = try GRDBEventStore(dbPool)

// A state change: decide from events, append with a consistency condition.
let account = "a"
let balanceQuery = try Query.of(Deposited.self, Withdrawn.self, where: [.payload("account"): .bound(account)])
let seen = try await store.source(balanceQuery)
try await store.append(
    [Withdrawn(account: account, amount: 10)],
    condition: AppendCondition(query: balanceQuery, after: seen.last?.position)  // fails if others got in first
)

// A state view.
let balance = StateView(initial: 0, bindings: [
    .on(Deposited.self) { acc, e in acc += e.amount },
    .on(Withdrawn.self) { acc, e in acc -= e.amount },
])
let current = try await balance.run(on: store, where: [.payload("account"): .bound(account)])

// A persisted view: a read model in the same database, updated from a subscription.
let readModel = PersistedStateView<GRDBOperation>(bindings: [
    .on(Deposited.self) { e in [GRDBOperation { db in /* upsert row */ }] },
])
let accumulator = try await GRDBAccumulator.open(name: "balances", writer: dbPool)  // off the main thread
let view = try await readModel.start(on: store, accumulator: accumulator)
// ... and `await view.close()` when done (a dropped handle shuts the view down too)
```

Tests, in any framework:

```swift
try await gwt(given: [Deposited(account: "a", amount: 5)],
              when: { try await withdraw($0, "a", 10) },
              thenThrows: { _ in true })
```

`gwt`/`gt` compare the events an automation or state change appended with the expected ones by their
JSON. Keep `Set`s and dictionaries with non-`String` keys out of those events (use sorted arrays),
or equal events can compare unequal.

## How it differs from the Clojure library

- **Typed, not dynamic.** Events are `Codable` structs and their type is their schema, so there is no
  malli. Tags are declared with key paths (`EventTag("id", \.id)`). Queries are validated when
  constructed (`try Query.of(...)`), so stores never see an invalid one.
- **Tag values are typed**: `String`, `Int`, `Bool`, `UUID`, compared by type and value. The `"*"`
  wildcard string is `.any`.
- **No event versioning machinery.** `version` is recorded in metadata (and can be tagged), as in
  Clojure, but evolving an event means adding a new event type and binding it in the views.
- **`AppendCondition.after` is exclusive** (matching UmaDB and the DCB spec): the append fails if a
  matching event exists at a position *after* it. `nil` means no matching event may exist at all.
  The Clojure in-memory store treats `after` as inclusive and UmaDB treats `nil` as "the current head";
  this port makes all stores agree.
- **Positions are `Int`s starting at 1**, replacing "sequence" (`:start-sequence` → `startPosition`).
- **Reducers of persisted views are pure**: they return `[Operation]` instead of calling `-tx-append`
  on the accumulator, and `commit(_:through:)` receives the batch. A failed commit is retried with the
  same batch. `-reset` is not ported (nothing calls it); see `GRDBAccumulator.clearCheckpoint`.
- **Cancellation is not an error.** Stopping an automation, or cancelling a subscription's consumer, ends
  things cleanly: no `CancellationError` reaches `onError`, and a cancelled GRDB subscription finishes like
  an in-memory one.
- **Failures are not silent by default.** `onError` of persisted views and automations defaults to
  `ErrorReporter.default` (Clojure's failures end in the uncaught exception handler, which prints
  them): `os.Logger` on Apple platforms, where stderr doesn't reach the logs of a shipped app, standard
  error elsewhere. Pass your own handler, or `nil` to a persisted view's options.
- **A `CancellationError` thrown by an accumulator is a failure** like any other error. Only a
  cancellation of the view's own worker (a `close()` that gave up waiting) ends it quietly.
- **Automations** report a throwing body to `onError` and keep running. In Clojure a throwing body
  ends the go-loop and the exception goes to the uncaught exception handler. A failing event
  subscription, or one that ends, is reported and stops the automation (`isRunning`). A `.signal`
  stream that finishes is not reported: like a closed channel in Clojure, it just never fires again.
  Periodic and signal ticks coalesce while the body is busy (one more run, not one each), similar to
  Clojure's buffer-1 channel; each event of an event trigger is a run. `automation` returns a handle that
  stops the automation on `stop()` or when dropped.
  `stop()` doesn't wait for a body in flight, as in Clojure (where the body isn't interrupted at all;
  Swift also cancels it, cooperatively). A body that ignores cancellation can therefore still be running
  after `stop()` returns, so bodies should honour cancellation and be idempotent. "Runs never overlap"
  holds within one handle only: an automation started right after the stop of another can overlap its last run.
  A `Trigger` is a description that each automation starts itself, so it can be shared;
  `Trigger.signal` takes a stream factory instead of a core.async channel, and `Trigger.periodic(every:)`
  is a trigger of its own where Clojure has `automation/periodic`, a helper that makes such a channel.
- **Duplicate event triggers** run once per entry. Clojure collects the plain event types of its triggers
  into a set (`(into #{} (filter keyword? ...))`), so listing a type twice is the same as listing it
  once; here `[.event(A.self), .event(A.self)]` runs the body twice for each `A`. (Tagged triggers are
  one subscription each in both, so a tagged duplicate runs twice there too.)
- **Duplicate bindings** of a view are an error. Clojure collects the bindings that match an event into a
  map keyed by the binding's type (and tag bindings), so identical keys collapse and the last reducer
  wins (`into {}`); here two `.on(A.self)` match the same event, and the view throws "exactly one
  binding" when it meets one.
- **Persisted views** keep Clojure's `close()` semantics: stop has priority over queued events, so
  closing commits only what was already applied (the rest is left for the next run, which resumes
  after the checkpoint), waits for the worker at most `closeTimeout` (5 s), and then closes the accumulator.
  Concurrent closes share one shutdown. If the worker doesn't stop within the timeout (a stuck commit),
  `close()` cancels it and returns: it starts no further commit, and a commit already in flight is fenced
  (`PersistedAccumulator.commit(_:after:through:)` carries the checkpoint the view last saw, and
  `GRDBAccumulator` throws a non-retried `PersistedAccumulatorConflict` if the stored one differs), so a
  restarted view and a stale one can't both apply an event. (Clojure leaves such a worker running.)
  A retry that finds the accumulator exactly at its position (the first attempt landed, its
  acknowledgement didn't) counts as done. That can't tell the view's own lost commit from another
  committer's identical one, so two views with *different reducers* must never share a checkpoint name
  (they'd diverge silently, where a conflict would have been loud). A closed view whose stale commit
  is refused reports nothing: the new view is the one that carries on. A custom accumulator should implement the fenced commit if
  its commits can outlive a cancelled view: otherwise a stale commit that can't be called back may land
  twice. And if it does land after a new view has started, it is the new view that is refused, and must
  be restarted.
  A view whose handle is dropped shuts itself down. After
  `close()`, `awaitPosition` and `accumulator()` throw `PersistedViewError.closed`; a failure of the
  final commit doesn't throw from `close()`, see `handle.failure` and `options.onError`.
- **The GRDB store limits a query to 500 selectors** (see below); the in-memory store, like the Clojure
  one, has no such limit, and the store contract doesn't check it. A query that is fine in memory can
  throw `LedjerError.invalidQuery` against GRDB.
- **`awaitPosition` on a failed view throws its failure at once**, also for a `nil` position, when nothing
  the view applies is pending, or when the position is already committed. Clojure's `await-position`
  returns `nil` in those cases (it first looks for an uncommitted event up to the position) and throws
  the failure only while it waits for one. The same goes for a closed view (`PersistedViewError.closed`),
  which Clojure has no counterpart of.
- **A persisted view that fails to start closes its accumulator** before `start` throws. Clojure's
  `persisted-view*` doesn't: the caller is left with an accumulator (a connection, say) to close, and
  if it forgets, the accumulator leaks and its checkpoint stays held. In Swift the accumulator is the
  view's from the moment `start` is called, whatever happens.
- **Subscriptions** are `AsyncThrowingStream`s. Ending the iteration cancels them. The GRDB store reads
  the log as the consumer pulls (batches of 500), and a persisted view feeds its worker through a
  bounded queue, so a slow commit slows the reads down. The in-memory store pushes: it keeps every
  event in memory anyway.
- **View routing is Clojure's `has-bindings?`**: to pick the one binding for an event, a bound tag that
  the event doesn't have is ignored, and only a different value rules a binding out. The consequence is
  the same as in Clojure: an event without a tag, selected through an untagged binding of its type, also
  matches the type's tag-bound bindings, and the view throws "exactly one binding" for it. (Store queries,
  which decide what a view reads at all, require a bound tag to be present.)
- **`gwt`/`gt`** throw `ScenarioFailure` instead of calling `clojure.test/is`, so they work with
  XCTest and Swift Testing alike. With an empty `then` they assert that nothing was appended (in
  Clojure such a scenario passes trivially). `gt(view:)` doesn't report failures to `onError` by
  default, as in Clojure, since it throws them.
- **`gwt`/`gt` compare events as JSON bytes** (type and payload, keys sorted), where Clojure compares
  values with `=`. Equal events encode equally, except for collections whose encoding follows their
  iteration order: a `Set`, or a dictionary with non-`String` keys (encoded as an array), built
  differently can encode in another order, and the scenario then fails although the events are equal.
  The JSON doesn't say whether an array was a set, so comparing arrays as multisets would weaken the
  check for ordered ones. Use ordered collections (a sorted array) in events that scenarios compare.
- The types are named `StateView`, `EventTag`, … to stay clear of SwiftUI's `View` and Swift Testing's `Tag`.

## The GRDB store

- Tables are `ledjer_event`, `ledjer_event_tag` (indexed `(name, value, position)`) and `ledjer_checkpoint`,
  so they can share a database with the app's read models. Creating a `GRDBEventStore` or a
  `GRDBAccumulator` applies the schema migrations (in any order, idempotently). If your app has its own
  `DatabaseMigrator`, register them there too with `GRDBEventStore.registerMigrations(in:)`, which is
  required with `eraseDatabaseOnSchemaChange`, or GRDB would see the tables as unknown schema and erase them.
  `GRDBEventStore.open(_:)` and `GRDBAccumulator.open(name:writer:)` do this off the calling thread.
- Event types, tag names and values, and checkpoint names are stored and bound as their UTF-8 bytes (`BLOB`
  columns, not `TEXT`): GRDB binds a `String` as C text that ends at the first NUL character, so a tag
  value `"alice\0evil"` would have been stored, and matched, as `"alice"`. Strings compare exactly, NUL
  characters included, as in the Clojure library and the in-memory store. Query the tables by hand with
  `X'…'` or `CAST(… AS BLOB)` literals, or bound `Data`.
- The log is append-only: triggers abort `UPDATE` and `DELETE` on `ledjer_event` and `ledjer_event_tag`.
- An append and its condition check run in one write transaction.
- Queries are driven by the tag index: each selector is a range scan over `(name, value, position)` (or
  `(type, position)` without tags), so the cost follows the number of matching events, not the length of
  the log. With several tags in a selector, the scan is driven by the first one in alphabetical order
  of its path (e.g. `payload.customer` before `payload.orderId`) and the others are looked up per
  candidate, so the cost follows the number of events with *that* tag value, which can exceed the number of
  events matching all of them. The tests check the query plans of the SQL the store really generates.
- A query can have at most 500 selectors (one per event type of each query item, with the tag bindings
  that apply to it; identical ones count once). Each selector is one branch of a `UNION`, and SQLite's
  default limit for the terms of a compound `SELECT` is 500. Reads, subscriptions and the condition of
  an append throw `LedjerError.invalidQuery` beyond that, before running any SQL, so an append with
  such a condition writes nothing. Split a larger query, or use a coarser one and filter.
- `GRDBAccumulator` commits a batch of read-model writes and its checkpoint in one transaction.
  To rebuild a read model, empty its tables and call `GRDBAccumulator.clearCheckpoint(named:in:)` in a migration.
- Subscriptions use `ValueObservation`, which sees writes made through the same `DatabaseWriter`
  (not from other processes). Use a `DatabasePool`.
- Tag matching is by the tags an event had *when appended*. Adding a tag to an event type doesn't
  retro-tag old events.

## Backgrounding on iOS

You don't need to do anything special for a database in the app's own container. With a database in a
shared container (an app group, for a widget or extension) GRDB's `observesSuspensionNotifications`
makes writes fail with `SQLITE_ABORT` while the app is suspended, so that iOS doesn't kill it for
holding a lock on the file. A persisted view's commit (or, if its reads are suspended too, its
subscription: `PersistedViewError.subscribe`) then fails, and it fails for good after its retries:
there's no telling how long a suspension lasts, so retrying wouldn't be a fix. An automation's event
subscription fails the same way; it is reported to `onError` and the automation stops for good
(`isRunning == false`), and nothing restarts it.

Stop the views and automations when the app goes to the background and start new ones when it is
active again. That is cheap and exact: a new view resumes after the accumulator's checkpoint, and
events appended meanwhile are picked up. Use `beginBackgroundTask` around the `close()` if it needs
time to flush.

Closing a view waits for its worker, but `Automation.stop()` doesn't wait for a body in flight (as in
Clojure): a body that ignores cancellation can still be running when the new automation starts, so
bodies should honour cancellation or be idempotent.

Starting must wait for the previous stop to finish. Otherwise a quick background → active starts
the new view while the old one is still flushing, and the two commit against the same checkpoint: one of
them is fenced off with a `PersistedAccumulatorConflict`, and if it is the new one, it fails.

An actor is re-entrant (every `await` lets another call in), so the flag is set synchronously and the
work is done by tasks. `start()` and `stop()` capture the tasks they wait for when they are called, not
when their task first runs, so a task only ever waits for tasks created before it and none can wait for
itself or for each other (a `stop()` right after `start()` in the same turn would otherwise deadlock).
`running` is written only by the single start task in flight, after the previous stop has finished,
and read only by a stop task after that start has ended and the previous stop has finished, so they
never overlap.

```swift
@MainActor final class Views {
    private var wanted = false
    private var running: (balances: PersistedViewHandle<GRDBAccumulator>, reminders: Automation)?
    private var starting: Task<Void, Never>?
    private var stopping: Task<Void, Never>?

    func start() {
        guard !wanted else { return }
        wanted = true
        let previous = stopping  // captured now: by the time the task runs, `stopping` may be a later stop
        starting = Task {
            do { try await open(after: previous) } catch {
                if !Task.isCancelled { wanted = false; failed(error) }  // else a stop() cancelled us
            }
        }
    }

    func stop() {
        wanted = false
        let start = starting
        start?.cancel()
        let previous = stopping
        stopping = Task {
            await start?.value  // an unfinished start ends first, leaving what it opened in `running`
            await previous?.value
            guard let current = running else { return }
            running = nil
            current.reminders.stop()
            await current.balances.close()
        }
    }

    private func open(after previous: Task<Void, Never>?) async throws {
        await previous?.value  // the previous views are closed before new ones start
        try Task.checkCancellation()
        let accumulator = try await GRDBAccumulator.open(name: "balances", writer: pool)
        var options = PersistedViewOptions()
        options.onError = { [weak self] error in Task { @MainActor in self?.failed(error) } }
        let balances = try await readModel.start(on: store, accumulator: accumulator, options: options)
        do {
            try Task.checkCancellation()
            let reminders = try await automation(
                on: store, triggers: [.event(ReminderDue.self)],
                onError: { [weak self] error in Task { @MainActor in self?.failed(error) } }
            ) { store in /* ... */ }
            running = (balances, reminders)
        } catch {
            await balances.close()
            throw error
        }
    }

    /// A view or automation that failed anyway: restart them when the app is active.
    private func failed(_ error: any Error) { /* log it, then `stop()` and `start()` */ }
}

// In the App. The closure runs on the main actor, so the calls happen in the order of the phases.
.onChange(of: scenePhase) { phase in  // iOS 17+: { _, phase in
    switch phase {
    case .background: views.stop()
    case .active: views.start()
    default: break
    }
}
```

## Testing

`LedjerStoreContract` holds framework-agnostic contract checks that any `EventStore` must pass;
the in-memory and GRDB stores both run them. It is a product of its own, so that the tests of a
third-party store can too (it is never built for apps that don't import it):

```swift
import LedjerStoreContract
@Test func contract() async throws { try await StoreContract.all { MyStore() } }
```

`EventStore.lastPosition(matching:through:)` has a default
implementation that reads the events; stores should override it (the GRDB store does, with one index lookup).

No Swift toolchain on the machine? `./swift.sh test` runs `swift test` in a container
(`swift:6.1` + `libsqlite3-dev`, via podman).
