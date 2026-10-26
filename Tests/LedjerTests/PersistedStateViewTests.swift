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
        // Close commits what has been applied; an event still queued would be left for the next run.
        try await Task.sleep(for: .milliseconds(100))
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
        #expect(throws: PersistedViewError.self) { try view.accumulator() }
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

    @Test func aDroppedHandleShutsTheViewDown() async throws {
        let store = InMemoryEventStore()
        let acc = TotalsAccumulator()
        weak var weakHandle: PersistedViewHandle<TotalsAccumulator>?
        func startAndDrop() async throws {
            let view = try await totalsView.start(on: store, accumulator: acc, options: fastOptions())
            weakHandle = view
        }
        try await startAndDrop()
        try await eventually { await acc.closed }
        #expect(weakHandle == nil)

        // It no longer follows the log.
        try await store.append([Counted(id: "a", num: 1)])
        try await Task.sleep(for: .milliseconds(100))
        #expect(await acc.total("a") == 0)
    }

    @Test func aClosedViewSaysSo() async throws {
        let store = InMemoryEventStore()
        let view = try await totalsView.start(on: store, accumulator: TotalsAccumulator(), options: fastOptions())
        let position = try await store.append([Counted(id: "a", num: 1)])
        await view.close()

        let clock = ContinuousClock()
        let start = clock.now
        await #expect(throws: PersistedViewError.self) { try await view.awaitPosition(position, timeout: .seconds(5)) }
        #expect(clock.now - start < .seconds(1))  // not after the timeout
        do {
            _ = try view.accumulator()
            Issue.record("expected the closed error")
        } catch PersistedViewError.closed {
        }
    }

    @Test func closingReleasesAWaiter() async throws {
        let store = InMemoryEventStore()
        let acc = TotalsAccumulator()
        let view = try await totalsView.start(on: store, accumulator: acc, options: fastOptions(maxSize: 1000, maxInterval: .seconds(60)))
        let position = try await store.append([Counted(id: "a", num: 1)])
        async let waiting: Void = view.awaitPosition(position, timeout: .seconds(10))
        try await Task.sleep(for: .milliseconds(100))
        await view.close()
        do {
            try await waiting
            // The close flushed the applied event, so the wait may also have seen it commit.
        } catch PersistedViewError.closed {
        }
    }

    @Test func nonsensicalOptionsAreClamped() async throws {
        var options = fastOptions()
        options.maxSize = 0
        options.commitRetries = -3
        let acc = TotalsAccumulator()
        let store = InMemoryEventStore()
        let view = try await totalsView.start(on: store, accumulator: acc, options: options)
        let position = try await store.append([Counted(id: "a", num: 1), Counted(id: "a", num: 2)])
        try await view.awaitPosition(position)
        #expect(await acc.total("a") == 3)
        await view.close()
    }

    @Test func appendingNothingCompiles() async throws {
        let store = InMemoryEventStore()
        #expect(try await store.append([]) == nil)
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

/// Polls `condition` until it holds, or fails after `timeout`.
func eventually(timeout: Duration = .seconds(2), _ condition: () async -> Bool) async throws {
    let clock = ContinuousClock()
    let deadline = clock.now + timeout
    while await !condition() {
        if clock.now > deadline { throw ContractFailure(message: "condition not met within \(timeout)") }
        try await Task.sleep(for: .milliseconds(5))
    }
}

/// An accumulator that takes its time, to give the view a backlog and `close()` something to wait for.
actor SlowAccumulator: PersistedAccumulator {
    typealias Operation = Add
    private let commitDelay: Duration
    private let closeDelay: Duration
    private var gate: Bool
    private var sequence: Position?
    private var totals: [String: Int] = [:]
    private(set) var closed = false
    private(set) var commitCount = 0
    /// How many commits have been started, finished or not.
    private(set) var commitsEntered = 0

    /// With `gated`, commits wait until `openGate()`.
    init(commitDelay: Duration = .zero, closeDelay: Duration = .zero, gated: Bool = false, lastSequence: Position? = nil) {
        self.commitDelay = commitDelay
        self.closeDelay = closeDelay
        self.gate = !gated
        self.sequence = lastSequence
    }

    func openGate() { gate = true }
    var sequenceNumber: Position? { sequence }
    func total(_ id: String) -> Int { totals[id, default: 0] }

    func commit(_ operations: [Add], through position: Position) async throws {
        commitsEntered += 1
        while !gate { try await Task.sleep(for: .milliseconds(5)) }
        try await Task.sleep(for: commitDelay)
        for op in operations { totals[op.id, default: 0] += op.amount }
        sequence = position
        commitCount += 1
    }

    func close() async {
        try? await Task.sleep(for: closeDelay)
        closed = true
    }
}

@Suite struct PersistedViewCloseTests {
    @Test func closeDoesNotDrainTheBacklog() async throws {
        let store = InMemoryEventStore()
        let n = 3000
        try await store.append((1...n).map { Counted(id: "a", num: $0) })
        // 300 commits of 10 events at 20 ms each would take 6 s.
        let acc = SlowAccumulator(commitDelay: .milliseconds(20))
        let view = try await totalsView.start(on: store, accumulator: acc, options: fastOptions(maxSize: 10, maxInterval: .seconds(30)))
        try await Task.sleep(for: .milliseconds(100))

        let clock = ContinuousClock()
        let start = clock.now
        await view.close()
        #expect(clock.now - start < .seconds(2), "close took \(clock.now - start)")

        let committed = try #require(await acc.sequenceNumber)
        #expect(committed < n, "the backlog was left for the next run")
        #expect(await acc.closed)

        // What was committed is consistent, and a new run resumes exactly after it.
        #expect(await acc.total("a") == committed * (committed + 1) / 2)
        let resumed = try await totalsView.start(on: store, accumulator: acc, options: fastOptions(maxSize: 500))
        try await resumed.awaitPosition(n, timeout: .seconds(10))
        #expect(await acc.total("a") == n * (n + 1) / 2)
        await resumed.close()
    }

    @Test func closeCommitsWhatWasAppliedButNotYetCommitted() async throws {
        let store = InMemoryEventStore()
        let acc = SlowAccumulator()
        let view = try await totalsView.start(on: store, accumulator: acc, options: fastOptions(maxSize: 1000, maxInterval: .seconds(60)))
        try await store.append([Counted(id: "a", num: 1), Counted(id: "a", num: 2)])
        try await Task.sleep(for: .milliseconds(100))
        #expect(await acc.sequenceNumber == nil)  // applied, waiting for the batch to fill
        await view.close()
        #expect(await acc.sequenceNumber == 2)
        #expect(await acc.total("a") == 3)
    }

    @Test func closeStopsWaitingForAStuckWorkerAfterTheTimeout() async throws {
        let store = InMemoryEventStore()
        let acc = SlowAccumulator(gated: true)  // the first commit never finishes
        var options = fastOptions(maxSize: 1)
        options.closeTimeout = .milliseconds(200)
        let view = try await totalsView.start(on: store, accumulator: acc, options: options)
        try await store.append([Counted(id: "a", num: 1)])
        try await Task.sleep(for: .milliseconds(100))

        let clock = ContinuousClock()
        let start = clock.now
        await view.close()
        let took = clock.now - start
        #expect(took >= .milliseconds(150) && took < .seconds(2), "close took \(took)")
        #expect(await acc.closed)
        await acc.openGate()  // let the stuck commit finish
    }

    @Test func aRestartAfterACloseTimeoutAppliesEachEventOnce() async throws {
        let store = InMemoryEventStore()
        let acc = SlowAccumulator(gated: true)  // the first view's commit is stuck
        var options = fastOptions(maxSize: 1)
        options.closeTimeout = .milliseconds(100)
        let first = try await totalsView.start(on: store, accumulator: acc, options: options)
        try await store.append([Counted(id: "a", num: 1)])
        try await eventually { await acc.commitsEntered == 1 }
        await first.close()  // gives up waiting for the stuck commit

        let second = try await totalsView.start(on: store, accumulator: acc, options: fastOptions(maxSize: 1))
        await acc.openGate()
        try await second.awaitPosition(1)
        try await Task.sleep(for: .milliseconds(150))  // time for a stale worker to commit, if it could
        #expect(await acc.total("a") == 1, "the event was applied twice")
        #expect(await acc.commitCount == 1)
        await second.close()
    }

    @Test func concurrentClosesAllWaitForTheShutdown() async throws {
        let acc = SlowAccumulator(closeDelay: .milliseconds(150))
        let view = try await totalsView.start(on: InMemoryEventStore(), accumulator: acc, options: fastOptions())
        async let first: Void = view.close()
        async let second: Void = {
            try? await Task.sleep(for: .milliseconds(30))  // arrives while the first is shutting down
            await view.close()
            return
        }()
        await second
        #expect(await acc.closed, "the second close returned before the accumulator was closed")
        await first
    }

    @Test func aFailureOfTheFinalFlushIsRecorded() async throws {
        let store = InMemoryEventStore()
        let acc = TotalsAccumulator(failingCommits: 100)
        let reported = ErrorBox()
        var options = fastOptions(maxSize: 1000, maxInterval: .seconds(60))
        options.commitRetries = 1
        options.onError = { reported.set($0) }
        let view = try await totalsView.start(on: store, accumulator: acc, options: options)
        try await store.append([Counted(id: "a", num: 1)])
        try await Task.sleep(for: .milliseconds(100))
        #expect(view.failure == nil)

        await view.close()
        guard case PersistedViewError.commit(let position, _, _)? = view.failure else {
            Issue.record("expected the commit failure, got \(String(describing: view.failure))")
            return
        }
        #expect(position == 1)
        #expect(reported.value != nil)
    }
}
