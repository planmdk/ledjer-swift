import Foundation

// Given-When-Then and Given-Then scenarios, run against a fresh
// `InMemoryEventStore`. They throw `ScenarioFailure` when the expectation isn't
// met, so they work with any test framework: an uncaught throw fails the test.

public struct ScenarioFailure: Error, CustomStringConvertible {
    public let description: String
    public init(_ description: String) { self.description = description }
}

private func describe(_ events: [NewEvent]) -> String {
    "[" + events.map { "\($0.type) \(String(decoding: $0.payload, as: UTF8.self))" }.joined(separator: ", ") + "]"
}

private func describe(_ events: [StoredEvent]) -> String {
    "[" + events.map { "\($0.type) \(String(decoding: $0.payload, as: UTF8.self))" }.joined(separator: ", ") + "]"
}

private func eventTypes(of events: [any Event]) throws -> EventQuery {
    try EventQuery([EventQueryItem(unchecked: Set(events.map { EventTypeInfo(type(of: $0)) }), tagBindings: [:])])
}

private func sameTypeAndPayload(_ actual: [StoredEvent], _ expected: [NewEvent]) -> Bool {
    actual.count == expected.count
        && zip(actual, expected).allSatisfy { $0.type == $1.type && $0.payload == $1.payload }
}

/// Given `given` events, `change` appends `then`: a state change.
///
/// Only events of the types in `then` are compared, with their types and
/// payloads, in order. With an empty `then`, the change must append nothing at all.
public func gwt(
    given: [any Event] = [],
    when change: (any EventStore) async throws -> Void,
    then expected: [any Event]
) async throws {
    let store = InMemoryEventStore()
    let last = try await store.append(given)
    let start = (last ?? 0) + 1
    try await change(store)
    let actual =
        expected.isEmpty
        ? await store.allEvents(startPosition: start)
        : try await store.source(try eventTypes(of: expected), startPosition: start)
    let wanted = try expected.map { try NewEvent($0) }
    guard sameTypeAndPayload(actual, wanted) else {
        throw ScenarioFailure("Expected new events \(describe(wanted)), got \(describe(actual))")
    }
}

/// Given `given` events, `change` fails with an error satisfying `matching`.
///
/// By default any error passes, so a state change that fails for the wrong reason does too: pass a
/// specific predicate (`{ $0 is InsufficientFunds }`) to pin the failure down. (Clojure's regex form
/// only accepts an `ExceptionInfo` with a matching message.)
public func gwt(
    given: [any Event] = [],
    when change: (any EventStore) async throws -> Void,
    thenThrows matching: (any Error) -> Bool = { _ in true }
) async throws {
    let store = InMemoryEventStore()
    try await store.append(given)
    do {
        try await change(store)
    } catch {
        guard matching(error) else { throw ScenarioFailure("Threw an unexpected error: \(error)") }
        return
    }
    throw ScenarioFailure("Expected an error, but none was thrown")
}

/// Given `given` events, `view` results in `expected`.
public func gwt<Result: Equatable>(
    given: [any Event] = [],
    view: (any EventStore) async throws -> Result,
    then expected: Result
) async throws {
    let store = InMemoryEventStore()
    try await store.append(given)
    let actual = try await view(store)
    guard actual == expected else {
        throw ScenarioFailure("Expected \(expected), got \(actual)")
    }
}

/// Given `given` events, the state view results in `expected`.
public func gwt<Acc: Equatable & Sendable>(
    given: [any Event] = [],
    view: StateView<Acc>,
    where tagBindings: TagBindings = [:],
    then expected: Acc
) async throws {
    try await gwt(given: given, view: { try await view.run(on: $0, where: tagBindings) }, then: expected)
}

/// Given `given` events, the automation appends `then`, within `timeout`.
///
/// `automation` starts the automation on the given store. The `then` events
/// are compared with their types and payloads, in order, among the events of
/// the types in `then` that arrive after `given`. With an empty `then`, the
/// automation must append nothing at all, which is checked after `timeout`.
public func gt(
    automation start: (any EventStore) async throws -> Automation,
    given: [any Event] = [],
    then expected: [any Event],
    timeout: Duration = .seconds(1)
) async throws {
    let store = InMemoryEventStore()
    let running = try await start(store)
    defer { running.stop() }
    let last = try await store.append(given)
    // Subscribing from after `given` sees a reaction appended before this subscribes.
    let stream = try await store.subscribe(try eventTypes(of: expected), startPosition: (last ?? 0) + 1)
    let wanted = try expected.map { try NewEvent($0) }
    let count = wanted.count
    guard count > 0 else {
        try await Task.sleep(for: timeout)
        let unexpected = await store.allEvents(startPosition: (last ?? 0) + 1)
        guard unexpected.isEmpty else {
            throw ScenarioFailure("Expected no new events, got \(describe(unexpected))")
        }
        return
    }

    let actual: [StoredEvent] = try await withThrowingTaskGroup(of: [StoredEvent]?.self) { group in
        group.addTask {
            var received: [StoredEvent] = []
            for try await event in stream {
                received.append(event)
                if received.count == count { break }
            }
            return received
        }
        group.addTask {
            try await Task.sleep(for: timeout)
            return nil
        }
        defer { group.cancelAll() }
        guard let first = try await group.next(), let events = first else {
            throw ScenarioFailure("Timed out after \(timeout) waiting for \(count) events \(describe(wanted))")
        }
        return events
    }
    guard sameTypeAndPayload(actual, wanted) else {
        throw ScenarioFailure("Expected events \(describe(wanted)), got \(describe(actual))")
    }
}

/// Given `given` events, a persisted view results in an accumulator satisfying `then`.
///
/// `accumulator` must be fresh: create it per scenario. The view closes it
/// when done. `then` runs once the view has committed every given event it applies.
///
/// `options` changes the scenario's defaults, which are a `maxInterval` of 10 ms and no `onError`
/// (a failure is thrown by `awaitPosition`, not also reported), the other options being those of
/// `PersistedViewOptions()`. It merges over them, as Clojure's `opts` does: changing only `maxSize`
/// keeps the other two defaults.
public func gt<A: PersistedAccumulator>(
    view: PersistedStateView<A.Operation>,
    accumulator: A,
    options configure: (inout PersistedViewOptions) -> Void = { _ in },
    given: [any Event] = [],
    then predicate: (A) async throws -> Bool,
    timeout: Duration = .seconds(1)
) async throws {
    guard try await accumulator.sequenceNumber == nil else {
        throw ScenarioFailure("The accumulator must be fresh. Create it per scenario.")
    }
    var options = PersistedViewOptions()
    options.maxInterval = .milliseconds(10)
    options.onError = nil
    configure(&options)
    let store = InMemoryEventStore()
    let running = try await view.start(on: store, accumulator: accumulator, options: options)
    do {
        let position = try await store.append(given)
        try await running.awaitPosition(position, timeout: timeout)
        let satisfied = try await predicate(try running.accumulator())
        guard satisfied else {
            throw ScenarioFailure("The persisted view's accumulator does not satisfy `then`")
        }
    } catch {
        await running.close()
        throw error
    }
    await running.close()
}
