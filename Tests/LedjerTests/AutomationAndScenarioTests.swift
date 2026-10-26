import Foundation
import Ledjer
import LedjerStoreContract
import Testing

/// Appends a `Reacted` event as soon as a `Counted` event arrives.
private func reactImmediately(_ store: any EventStore) async throws -> Automation {
    try await automation(on: store, triggers: [.event(Counted.self)]) { store in
        try await store.append([Reacted(id: "a")])
    }
}

@Suite struct GtAutomationTests {
    @Test func theReactionOfAnAutomation() async throws {
        try await gt(
            automation: reactImmediately,
            given: [Counted(id: "a", num: 1)],
            then: [Reacted(id: "a")]
        )
    }

    @Test func aReactionAppendedBeforeGtSubscribesIsSeen() async throws {
        // The automation reacts to `given` right away; `then` is subscribed
        // for from after `given`, not from the current position.
        let slow: @Sendable (any EventStore) async throws -> Automation = { store in
            try await automation(on: store, triggers: [.event(Counted.self)]) { store in
                try await store.append([Reacted(id: "a")])
            }
        }
        try await gt(automation: slow, given: [Counted(id: "a", num: 1), Ignored(id: "x")], then: [Reacted(id: "a")], timeout: .milliseconds(500))
    }

    @Test func timesOutWhenTheAutomationDoesNotReact() async throws {
        await #expect(throws: ScenarioFailure.self) {
            try await gt(
                automation: { try await automation(on: $0, triggers: [.event(Counted.self)]) { _ in } },
                given: [Counted(id: "a", num: 1)],
                then: [Reacted(id: "a")],
                timeout: .milliseconds(100)
            )
        }
    }

    @Test func failsOnAWrongReaction() async throws {
        await #expect(throws: ScenarioFailure.self) {
            try await gt(
                automation: reactImmediately,
                given: [Counted(id: "a", num: 1)],
                then: [Reacted(id: "b")]
            )
        }
    }

    @Test func noGivenAndNoThen() async throws {
        try await gt(automation: reactImmediately, given: [], then: [])
    }
}

@Suite struct AutomationTests {
    @Test func triggersOnTaggedEventsOnly() async throws {
        let store = InMemoryEventStore()
        let runs = Counter()
        let running = try await automation(
            on: store,
            triggers: [try .event(Counted.self, where: [.payload("id"): "a"])]
        ) { _ in runs.increment() }
        try await store.append([Counted(id: "b", num: 1), Counted(id: "a", num: 1), Counted(id: "a", num: 2)])
        try await runs.wait(for: 2)
        try await Task.sleep(for: .milliseconds(100))
        #expect(runs.value == 2)
        running.stop()
    }

    @Test func periodicAndSignalTriggers() async throws {
        let store = InMemoryEventStore()
        let runs = Counter()
        let (signal, continuation) = AsyncStream<Void>.makeStream()
        let running = try await automation(
            on: store,
            triggers: [.periodic(every: .milliseconds(20)), .signal { signal }]
        ) { _ in runs.increment() }
        continuation.yield()
        try await runs.wait(for: 3)
        running.stop()
    }

    @Test func aTriggerCanBeSharedByAutomations() async throws {
        let store = InMemoryEventStore()
        let trigger = Trigger.periodic(every: .milliseconds(10))
        let a = Counter()
        let b = Counter()
        let first = try await automation(on: store, triggers: [trigger]) { _ in a.increment() }
        let second = try await automation(on: store, triggers: [trigger]) { _ in b.increment() }
        try await a.wait(for: 5)
        try await b.wait(for: 5)

        // Stopping one doesn't affect the other.
        first.stop()
        let before = b.value
        try await b.wait(for: before + 5)
        let frozen = a.value
        try await Task.sleep(for: .milliseconds(100))
        #expect(a.value <= frozen + 1)  // at most one run that was already underway
        second.stop()

        // And the trigger is reusable after both have stopped.
        let c = Counter()
        let third = try await automation(on: store, triggers: [trigger]) { _ in c.increment() }
        try await c.wait(for: 3)
        third.stop()
    }

    @Test func periodicTicksStartWithTheAutomation() async throws {
        let trigger = Trigger.periodic(every: .milliseconds(10))
        try await Task.sleep(for: .milliseconds(200))  // time passes while the trigger exists
        let runs = Counter()
        let running = try await automation(on: InMemoryEventStore(), triggers: [trigger]) { _ in runs.increment() }
        #expect(runs.value <= 1)  // no burst of ticks from before it started
        running.stop()
    }

    @Test func aStoppedAutomationDoesNotRun() async throws {
        let store = InMemoryEventStore()
        let runs = Counter()
        let running = try await automation(on: store, triggers: [.event(Counted.self)]) { _ in runs.increment() }
        running.stop()
        try await Task.sleep(for: .milliseconds(50))
        try await store.append([Counted(id: "a", num: 1)])
        try await Task.sleep(for: .milliseconds(100))
        #expect(runs.value == 0)
    }

    @Test func aDroppedAutomationStops() async throws {
        let store = InMemoryEventStore()
        let runs = Counter()
        func startAndDrop() async throws {
            _ = try await automation(on: store, triggers: [.event(Counted.self)]) { _ in runs.increment() }
        }
        try await startAndDrop()
        try await store.append([Counted(id: "a", num: 1)])
        try await Task.sleep(for: .milliseconds(150))
        #expect(runs.value == 0)
    }

    @Test func aFailingBodyIsReportedAndTheAutomationKeepsRunning() async throws {
        struct Boom: Error {}
        let store = InMemoryEventStore()
        let runs = Counter()
        let errors = Counter()
        let running = try await automation(
            on: store,
            triggers: [.event(Counted.self)],
            onError: { _ in errors.increment() }
        ) { _ in
            runs.increment()
            throw Boom()
        }
        try await store.append([Counted(id: "a", num: 1)])
        try await store.append([Counted(id: "a", num: 2)])
        try await runs.wait(for: 2)
        try await errors.wait(for: 2)
        running.stop()
    }

    @Test func bodiesDoNotOverlap() async throws {
        let store = InMemoryEventStore()
        let active = Counter()
        let overlaps = Counter()
        let done = Counter()
        let running = try await automation(on: store, triggers: [.event(Counted.self)]) { _ in
            if active.value > 0 { overlaps.increment() }
            active.increment()
            try await Task.sleep(for: .milliseconds(20))
            active.decrement()
            done.increment()
        }
        try await store.append((0..<4).map { Counted(id: "a", num: $0) })
        try await done.wait(for: 4)
        #expect(overlaps.value == 0)
        running.stop()
    }
}

@Suite struct GwtTests {
    private func placeOrder(_ store: any EventStore, id: String) async throws {
        let existing = try await store.source(Query.of(Counted.self, where: [.payload("id"): .bound(id)]))
        guard existing.isEmpty else { throw ScenarioFailure("already counted") }
        try await store.append([Counted(id: id, num: 1), Ignored(id: id)])
    }

    @Test func aStateChangeAppendsEvents() async throws {
        try await gwt(
            given: [Counted(id: "x", num: 5)],
            when: { try await placeOrder($0, id: "1") },
            then: [Counted(id: "1", num: 1)]
        )
    }

    @Test func onlyEventsOfTheThenTypesAreCompared() async throws {
        try await gwt(
            when: { try await placeOrder($0, id: "1") },
            then: [Ignored(id: "1")]
        )
    }

    @Test func aWrongEventFails() async throws {
        await #expect(throws: ScenarioFailure.self) {
            try await gwt(when: { try await placeOrder($0, id: "1") }, then: [Counted(id: "1", num: 2)])
        }
    }

    @Test func aStateChangeThatFails() async throws {
        try await gwt(
            given: [Counted(id: "1", num: 1)],
            when: { try await placeOrder($0, id: "1") },
            thenThrows: { "\($0)".contains("already") }
        )
        await #expect(throws: ScenarioFailure.self) {
            try await gwt(when: { try await placeOrder($0, id: "1") }, thenThrows: { _ in true })
        }
    }

    @Test func aStateView() async throws {
        let total = StateView(initial: 0, bindings: [
            .on(Counted.self) { acc, e in acc += e.num },
        ])
        try await gwt(given: [Counted(id: "1", num: 1), Counted(id: "2", num: 2)], view: total, then: 3)
        try await gwt(given: [Counted(id: "1", num: 1), Counted(id: "2", num: 2)], view: total, where: [.payload("id"): "2"], then: 2)
        await #expect(throws: ScenarioFailure.self) {
            try await gwt(given: [Counted(id: "1", num: 1)], view: total, then: 2)
        }
    }
}

final class Counter: @unchecked Sendable {
    private let lock = NSLock()
    private var _value = 0
    var value: Int { lock.withLock { _value } }
    func increment() { lock.withLock { _value += 1 } }
    func decrement() { lock.withLock { _value -= 1 } }

    func wait(for count: Int, timeout: Duration = .seconds(2)) async throws {
        let clock = ContinuousClock()
        let deadline = clock.now + timeout
        while value < count {
            if clock.now > deadline { throw ContractFailure(message: "expected \(count), got \(value)") }
            try await Task.sleep(for: .milliseconds(5))
        }
    }
}
