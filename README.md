# Ledjer (Swift)

A Swift port of the core of [planmdk/ledjer](https://github.com/planmdk/ledjer), a small
event sourcing library built around [Event Modeling](https://eventmodeling.org/), with an
on-device SQLite event store (GRDB).

| Product       | Contents                                                              | Dependencies |
|---------------|-----------------------------------------------------------------------|--------------|
| `Ledjer`      | events, queries, state views, persisted views, automations, `gwt`/`gt`, in-memory store | none |
| `LedjerGRDB`  | `GRDBEventStore`, `GRDBAccumulator`                                   | GRDB 7       |

Requires Swift 6.1, iOS 16 / macOS 13.

## Event Modeling → Ledjer

| Event Modeling   | Clojure                | Swift                                         |
|------------------|------------------------|-----------------------------------------------|
| event            | `defevent`             | `struct: Event`                               |
| state view       | `defview`              | `StateView`                                   |
| persisted view   | `defpersistedview`     | `PersistedStateView` → `PersistedViewHandle`  |
| automation       | `automation`           | `automation(on:triggers:body:)`               |
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
let view = try await readModel.start(on: store, accumulator: GRDBAccumulator(name: "balances", writer: dbPool))
```

Tests, in any framework:

```swift
try await gwt(given: [Deposited(account: "a", amount: 5)],
              when: { try await withdraw($0, "a", 10) },
              thenThrows: { _ in true })
```

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
- **Automations** report a throwing body to `onError` and keep running. In Clojure a throwing body
  silently ends the go-loop. `automation` returns a handle to `stop()`.
- **Subscriptions** are `AsyncThrowingStream`s. Ending the iteration cancels them. Buffers are
  unbounded, so a very large replay is held in memory until consumed.
- **`gwt`/`gt`** throw `ScenarioFailure` instead of calling `clojure.test/is`, so they work with
  XCTest and Swift Testing alike.
- The types are named `StateView`, `EventTag`, … to stay clear of SwiftUI's `View` and Swift Testing's `Tag`.

## The GRDB store

- Tables are `ledjer_event`, `ledjer_event_tag` (indexed `(name, value, position)`) and `ledjer_checkpoint`,
  so they can share a database with the app's read models. Run the schema through your own
  `DatabaseMigrator` with `GRDBEventStore.registerMigrations(in:)`, or let `init` do it.
- The log is append-only: triggers abort `UPDATE` and `DELETE` on `ledjer_event`.
- An append and its condition check run in one write transaction.
- `GRDBAccumulator` commits a batch of read-model writes and its checkpoint in one transaction.
  To rebuild a read model, empty its tables and call `GRDBAccumulator.clearCheckpoint(named:in:)` in a migration.
- Subscriptions use `ValueObservation`, which sees writes made through the same `DatabaseWriter`
  (not from other processes). Use a `DatabasePool`.
- Tag matching is by the tags an event had *when appended*. Adding a tag to an event type doesn't
  retro-tag old events.

## Testing

`LedjerStoreContract` holds framework-agnostic contract checks that any `EventStore` must pass;
the in-memory and GRDB stores both run them.

No Swift toolchain on the machine? `./swift.sh test` runs `swift test` in a container
(`swift:6.1` + `libsqlite3-dev`, via podman).
