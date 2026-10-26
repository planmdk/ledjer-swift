import Foundation
import Ledjer
import LedjerStoreContract
import Testing

struct Counted: Event {
    static let eventType: EventType = "ledjer-test/counted"
    static var tags: [EventTag<Self>] { [EventTag("id", \.id)] }
    var id: String
    var num: Int
}

struct Ignored: Event {
    static let eventType: EventType = "ledjer-test/ignored"
    static var tags: [EventTag<Self>] { [EventTag("id", \.id)] }
    var id: String
}

struct Reacted: Event {
    static let eventType: EventType = "ledjer-test/reacted"
    static var tags: [EventTag<Self>] { [EventTag("id", \.id)] }
    var id: String
}

struct Add: Sendable, Equatable {
    var id: String
    var amount: Int
}

struct CommitFailed: Error {}

/// Totals per id. Operations are applied only on commit.
actor TotalsAccumulator: PersistedAccumulator {
    typealias Operation = Add

    private var totals: [String: Int] = [:]
    private var sequence: Position?
    private var failuresLeft: Int
    private(set) var commits: [(position: Position, operations: [Add])] = []
    private(set) var commitAttempts = 0
    private(set) var closed = false

    init(lastSequence: Position? = nil, failingCommits: Int = 0) {
        self.sequence = lastSequence
        self.failuresLeft = failingCommits
    }

    var sequenceNumber: Position? { sequence }

    func commit(_ operations: [Add], through position: Position) throws {
        commitAttempts += 1
        if failuresLeft > 0 {
            failuresLeft -= 1
            throw CommitFailed()
        }
        for op in operations { totals[op.id, default: 0] += op.amount }
        sequence = position
        commits.append((position, operations))
    }

    func close() { closed = true }

    func total(_ id: String) -> Int { totals[id, default: 0] }
}

let totalsView = PersistedStateView<Add>(bindings: [
    .on(Counted.self) { [Add(id: $0.id, amount: $0.num)] },
])

private func fastOptions(maxSize: Int = 64, maxInterval: Duration = .milliseconds(10)) -> PersistedViewOptions {
    var options = PersistedViewOptions()
    options.maxSize = maxSize
    options.maxInterval = maxInterval
    options.commitBackoff = .milliseconds(5)
    return options
}

@Suite struct PersistedStateViewTests {
    @Test func passesWhenTheAccumulatorSatisfiesThen() async throws {
        try await gt(
            view: totalsView,
            accumulator: TotalsAccumulator(),
            given: [Counted(id: "a", num: 1), Counted(id: "b", num: 5), Counted(id: "a", num: 2)],
            then: { acc in
                let (a, b) = (await acc.total("a"), await acc.total("b"))
                return a == 3 && b == 5
            }
        )
    }

    @Test func failsWhenTheAccumulatorDoesNotSatisfyThen() async throws {
        await #expect(throws: ScenarioFailure.self) {
            try await gt(
                view: totalsView,
                accumulator: TotalsAccumulator(),
                given: [Counted(id: "a", num: 1)],
                then: { await $0.total("a") == 2 }
            )
        }
    }

    @Test func waitsForTheLastAppliedEventWhenTheLastGivenIsNotApplied() async throws {
        try await gt(
            view: totalsView,
            accumulator: TotalsAccumulator(),
            given: [Counted(id: "a", num: 1), Ignored(id: "a")],
            then: { await $0.total("a") == 1 }
        )
    }

    @Test func noGivenEvents() async throws {
        try await gt(view: totalsView, accumulator: TotalsAccumulator(), given: [], then: { await $0.total("a") == 0 })
    }

    @Test func aUsedAccumulatorIsRejected() async throws {
        let used = TotalsAccumulator(lastSequence: 1)
        await #expect(throws: ScenarioFailure.self) {
            try await gt(view: totalsView, accumulator: used, given: [], then: { _ in true })
        }
    }

    @Test func aReducerFailureIsThrown() async throws {
        struct Boom: Error {}
        let failing = PersistedStateView<Add>(bindings: [
            .on(Counted.self) { _ in throw Boom() },
        ])
        do {
            try await gt(view: failing, accumulator: TotalsAccumulator(), given: [Counted(id: "a", num: 1)], then: { _ in true })
            Issue.record("expected a failure")
        } catch PersistedViewError.reduce(let type, let position, _) {
            #expect(type == Counted.eventType)
            #expect(position == 1)
        }
    }

    @Test func commitsEveryMaxSizeEventsAndFlushesTheRestOnClose() async throws {
        let store = InMemoryEventStore()
        let acc = TotalsAccumulator()
        let view = try await totalsView.start(on: store, accumulator: acc, options: fastOptions(maxSize: 3, maxInterval: .seconds(30)))
        try await store.append((1...7).map { Counted(id: "a", num: $0) })
        try await view.awaitPosition(6)
        #expect(await acc.commits.map(\.position) == [3, 6])
        #expect(await acc.total("a") == 21)  // 1 + ... + 6
        await view.close()
        #expect(await acc.commits.map(\.position) == [3, 6, 7])
        #expect(await acc.total("a") == 28)
        #expect(await acc.closed)
    }

    @Test func commitsAfterMaxIntervalAndDoesNotCommitAgainOnClose() async throws {
        let store = InMemoryEventStore()
        let acc = TotalsAccumulator()
        let view = try await totalsView.start(on: store, accumulator: acc, options: fastOptions(maxInterval: .milliseconds(30)))
        let position = try await store.append([Counted(id: "a", num: 1), Counted(id: "a", num: 2)])
        try await view.awaitPosition(position)
        #expect(await acc.commits.count == 1)
        await view.close()
        #expect(await acc.commits.count == 1)
    }

    @Test func retriesAFailedCommit() async throws {
        let store = InMemoryEventStore()
        let acc = TotalsAccumulator(failingCommits: 2)
        let view = try await totalsView.start(on: store, accumulator: acc, options: fastOptions())
        let position = try await store.append([Counted(id: "a", num: 4)])
        try await view.awaitPosition(position)
        #expect(await acc.commitAttempts == 3)
        #expect(await acc.total("a") == 4)
        await view.close()
    }

    @Test func aCommitFailingAfterAllRetriesFailsTheView() async throws {
        let store = InMemoryEventStore()
        let acc = TotalsAccumulator(failingCommits: 100)
        let reported = ErrorBox()
        var options = fastOptions()
        options.commitRetries = 2
        options.onError = { reported.set($0) }
        let view = try await totalsView.start(on: store, accumulator: acc, options: options)
        let position = try await store.append([Counted(id: "a", num: 4)])
        do {
            try await view.awaitPosition(position)
            Issue.record("expected a failure")
        } catch PersistedViewError.commit(let committedThrough, let attempts, _) {
            #expect(committedThrough == 1)
            #expect(attempts == 3)
        }
        await #expect(throws: PersistedViewError.self) { try await view.accumulator() }
        #expect(reported.value != nil)
        await view.close()
    }

    @Test func resumesAfterTheAccumulatorsLastSequenceNumber() async throws {
        let store = InMemoryEventStore()
        try await store.append([Counted(id: "a", num: 1), Counted(id: "a", num: 2), Counted(id: "a", num: 4)])
        let acc = TotalsAccumulator(lastSequence: 2)
        let view = try await totalsView.start(on: store, accumulator: acc, options: fastOptions())
        try await view.awaitPosition(3)
        #expect(await acc.total("a") == 4)  // only the third event
        await view.close()
    }

    @Test func awaitPositionTimesOut() async throws {
        let store = InMemoryEventStore()
        let acc = TotalsAccumulator()
        let view = try await totalsView.start(on: store, accumulator: acc, options: fastOptions(maxInterval: .seconds(30)))
        let position = try await store.append([Counted(id: "a", num: 1)])
        do {
            try await view.awaitPosition(position, timeout: .milliseconds(50))
            Issue.record("expected a timeout")
        } catch PersistedViewError.awaitTimedOut {
        }
        await view.close()
    }

    @Test func closingTwiceIsANoOp() async throws {
        let acc = TotalsAccumulator()
        let view = try await totalsView.start(on: InMemoryEventStore(), accumulator: acc)
        await view.close()
        await view.close()
        #expect(await acc.closed)
    }

    @Test func aViewReceivesEventsFromTaggedBindingsOnly() async throws {
        let view = PersistedStateView<Add>(bindings: [
            .on(Counted.self, where: [.payload("id"): "a"]) { [Add(id: $0.id, amount: $0.num)] },
        ])
        try await gt(
            view: view,
            accumulator: TotalsAccumulator(),
            given: [Counted(id: "a", num: 1), Counted(id: "b", num: 10)],
            then: { acc in
                let (a, b) = (await acc.total("a"), await acc.total("b"))
                return a == 1 && b == 0
            }
        )
    }
}

final class ErrorBox: @unchecked Sendable {
    private let lock = NSLock()
    private var _value: (any Error)?
    func set(_ error: any Error) { lock.withLock { _value = error } }
    var value: (any Error)? { lock.withLock { _value } }
}
