import Dispatch
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

/// `totalsView`, counting the events its reducer has applied, so that tests can wait for "applied"
/// instead of sleeping and hoping.
private func observedView(_ applied: Counter) -> PersistedStateView<Add> {
    PersistedStateView<Add>(bindings: [
        .on(Counted.self) { event in
            applied.increment()
            return [Add(id: event.id, amount: event.num)]
        },
    ])
}

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
        let applied = Counter()
        let view = try await observedView(applied).start(on: store, accumulator: acc, options: fastOptions(maxSize: 3, maxInterval: .seconds(30)))
        try await store.append((1...7).map { Counted(id: "a", num: $0) })
        try await view.awaitPosition(6)
        #expect(await acc.commits.map(\.position) == [3, 6])
        #expect(await acc.total("a") == 21)  // 1 + ... + 6
        // Close commits what has been applied; an event still queued would be left for the next run.
        try await applied.wait(for: 7)
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
        do {
            try await view.awaitPosition(position, timeout: .seconds(5))
            Issue.record("expected the closed error")
        } catch PersistedViewError.closed {
        }
        #expect(clock.now - start < .seconds(1))  // not after the timeout
        do {
            _ = try view.accumulator()
            Issue.record("expected the closed error")
        } catch PersistedViewError.closed {
        }
    }

    @Test func closingReleasesAWaiter() async throws {
        let store = InMemoryEventStore()
        // The commit never finishes, so the event can't be committed: only the close can end the wait.
        let acc = SlowAccumulator(gated: true)
        var options = fastOptions(maxSize: 1)
        options.closeTimeout = .milliseconds(500)
        let view = try await totalsView.start(on: store, accumulator: acc, options: options)
        let position = try await store.append([Counted(id: "a", num: 1)])
        try await eventually { await acc.commitsEntered == 1 }

        let waiting = Task { try await view.awaitPosition(position, timeout: .seconds(30)) }
        try await Task.sleep(for: .milliseconds(50))  // let it start waiting; the assertion below doesn't depend on it
        let closing = Task { await view.close() }
        do {
            try await waiting.value
            Issue.record("the wait can't have seen the event committed")
        } catch PersistedViewError.closed {
        }
        await closing.value
        await acc.openGate()
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
    /// How many commits ended because the view cancelled its worker.
    private(set) var cancelledCommits = 0

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
        do {
            while !gate { try await Task.sleep(for: .milliseconds(5)) }
            try await Task.sleep(for: commitDelay)
        } catch {
            cancelledCommits += 1
            throw error
        }
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
        try await eventually { await acc.commitCount >= 1 }

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
        let applied = Counter()
        let view = try await observedView(applied).start(on: store, accumulator: acc, options: fastOptions(maxSize: 1000, maxInterval: .seconds(60)))
        try await store.append([Counted(id: "a", num: 1), Counted(id: "a", num: 2)])
        try await applied.wait(for: 2)
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
        try await eventually { await acc.commitsEntered == 1 }

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
        let applied = Counter()
        let view = try await observedView(applied).start(on: store, accumulator: acc, options: options)
        try await store.append([Counted(id: "a", num: 1)])
        try await applied.wait(for: 1)
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

/// A store whose subscriptions are scripted, over an in-memory store holding the events.
struct ScriptedSubscriptions: EventStore {
    enum Script: Sendable {
        /// The stream ends without an error.
        case endsCleanly
        /// The stream fails.
        case fails
        /// The stream has all of `events` ready at once, and `terminated` is raised when it is let go of.
        case floods(events: [StoredEvent], terminated: Counter)
        /// The stream waits for events, and when its consumer is cancelled it throws `CancellationError`
        /// instead of finishing, as a store that is reading when the cancellation lands might.
        case failsWhenCancelled
        /// `subscribe` itself throws.
        case subscribeThrows
    }

    struct Boom: Error {}
    let base = InMemoryEventStore()
    let script: Script

    func source(_ query: Query, startPosition: Position?) async throws -> [StoredEvent] {
        try await base.source(query, startPosition: startPosition)
    }

    func append(_ events: [NewEvent], condition: AppendCondition?) async throws -> Position? {
        try await base.append(events, condition: condition)
    }

    func subscribe(_ query: Query, startPosition: Position?) async throws -> AsyncThrowingStream<StoredEvent, any Error> {
        switch script {
        case .endsCleanly:
            return AsyncThrowingStream { $0.finish() }
        case .fails:
            return AsyncThrowingStream { $0.finish(throwing: Boom()) }
        case .subscribeThrows:
            throw Boom()
        case .failsWhenCancelled:
            return AsyncThrowingStream(unfolding: {
                try await Task.sleep(for: .seconds(30))
                return nil
            })
        case .floods(let events, let terminated):
            return AsyncThrowingStream { continuation in
                for event in events { continuation.yield(event) }
                continuation.onTermination = { _ in terminated.increment() }
            }
        }
    }
}

@Suite struct PersistedViewFailurePathTests {
    /// The accumulator is the view's once passed to `start`: a view that can't start closes it.
    @Test func aViewThatCannotStartClosesTheAccumulator() async throws {
        let acc = TotalsAccumulator()
        do {
            _ = try await totalsView.start(on: ScriptedSubscriptions(script: .subscribeThrows), accumulator: acc)
            Issue.record("expected start to throw")
        } catch PersistedViewError.subscribe(_, let underlying) {
            #expect(underlying is ScriptedSubscriptions.Boom)
        }
        #expect(await acc.closed)
    }

    @Test func aSubscriptionThatEndsFailsTheView() async throws {
        for script in [ScriptedSubscriptions.Script.endsCleanly, .fails] {
            let store = ScriptedSubscriptions(script: script)
            try await store.append([Counted(id: "a", num: 1)])
            let reported = ErrorBox()
            var options = fastOptions()
            options.onError = { reported.set($0) }
            let view = try await totalsView.start(on: store, accumulator: TotalsAccumulator(), options: options)
            do {
                try await view.awaitPosition(1)
                Issue.record("expected the subscription failure")
            } catch PersistedViewError.subscribe(let start, let underlying) {
                #expect(start == 0)
                switch script {
                case .endsCleanly: #expect(underlying == nil, "closed unexpectedly, without an error")
                default: #expect(underlying is ScriptedSubscriptions.Boom)
                }
            }
            #expect(reported.value != nil)
            await view.close()
        }
    }

    @Test func aWorkerThatFailsReleasesTheProducerBlockedOnAFullInbox() async throws {
        struct Boom: Error {}
        let events = try (1...100).map { n -> StoredEvent in
            let new = try NewEvent(Counted(id: "a", num: n))
            return StoredEvent(position: n, type: new.type, metadata: new.metadata, payload: new.payload)
        }
        let terminated = Counter()
        let store = ScriptedSubscriptions(script: .floods(events: events, terminated: terminated))
        try await store.append([Counted(id: "a", num: 1)])
        let failing = PersistedStateView<Add>(bindings: [.on(Counted.self) { _ in throw Boom() }])
        let view = try await failing.start(on: store, accumulator: TotalsAccumulator(), options: fastOptions(maxSize: 1))
        try await eventually { view.failure != nil }
        // The pump was blocked sending event 3 or so to a full inbox. If the failure didn't release
        // it, it would hold the stream forever.
        try await terminated.wait(for: 1)
        await view.close()
    }

    @Test func aDroppedHandleWithAStuckWorkerStillShutsDownAndNeverCommitsLater() async throws {
        let store = InMemoryEventStore()
        let acc = SlowAccumulator(gated: true)
        var options = fastOptions(maxSize: 1)
        options.closeTimeout = .milliseconds(100)
        weak var weakHandle: PersistedViewHandle<SlowAccumulator>?
        func startAndDrop() async throws {
            let view = try await totalsView.start(on: store, accumulator: acc, options: options)
            weakHandle = view
            try await store.append([Counted(id: "a", num: 1)])
            try await eventually { await acc.commitsEntered == 1 }
        }
        try await startAndDrop()
        try await eventually { await acc.closed }  // after the timeout
        #expect(weakHandle == nil)

        await acc.openGate()
        try await Task.sleep(for: .milliseconds(100))
        #expect(await acc.commitCount == 0, "the stuck commit went through after the view was shut down")
    }
}

/// An accumulator that throws a CancellationError of its own making, e.g. from a helper that
/// maps its timeouts to it. Nobody has cancelled the view.
actor CancelsOnItsOwn: PersistedAccumulator {
    typealias Operation = Add
    private var failuresLeft: Int
    private var sequence: Position?
    private(set) var attempts = 0
    private(set) var total = 0

    init(failures: Int) { failuresLeft = failures }

    var sequenceNumber: Position? { sequence }

    func commit(_ operations: [Add], through position: Position) async throws {
        attempts += 1
        if failuresLeft > 0 {
            failuresLeft -= 1
            throw CancellationError()
        }
        total += operations.reduce(0) { $0 + $1.amount }
        sequence = position
    }
}

@Suite struct PersistedViewOwnCancellationTests {
    @Test func aCancellationErrorOfTheAccumulatorsOwnIsAFailureNotASilentStop() async throws {
        let store = InMemoryEventStore()
        let acc = CancelsOnItsOwn(failures: 100)
        let reported = ErrorBox()
        var options = fastOptions()
        options.commitRetries = 1
        options.onError = { reported.set($0) }
        let view = try await totalsView.start(on: store, accumulator: acc, options: options)
        let position = try await store.append([Counted(id: "a", num: 1)])

        do {
            try await view.awaitPosition(position)
            Issue.record("expected the commit failure")
        } catch PersistedViewError.commit(let through, let attempts, let underlying) {
            #expect(through == 1)
            #expect(attempts == 2, "it was retried like any failed commit")
            #expect(underlying is CancellationError)
        }
        #expect(view.failure != nil)
        #expect(reported.value != nil, "onError was called")
        #expect(throws: PersistedViewError.self) { try view.accumulator() }
        await view.close()
    }

    @Test func aCancellationErrorOfTheAccumulatorsOwnIsRetried() async throws {
        let store = InMemoryEventStore()
        let acc = CancelsOnItsOwn(failures: 1)
        let view = try await totalsView.start(on: store, accumulator: acc, options: fastOptions())
        let position = try await store.append([Counted(id: "a", num: 5)])
        try await view.awaitPosition(position)
        #expect(await acc.attempts == 2)
        #expect(await acc.total == 5)

        // And the view keeps going afterwards.
        let more = try await store.append([Counted(id: "a", num: 1)])
        try await view.awaitPosition(more)
        #expect(await acc.total == 6)
        await view.close()
    }
}

struct LostAck: Error {}

/// An accumulator that fences its commits, like GRDBAccumulator: a commit that doesn't continue from its
/// own position is refused. Can apply a commit and then fail to say so, and can hold a commit
/// up in a way that cancellation can't reach, like a database write that has already begun.
actor FencedAccumulator: PersistedAccumulator {
    typealias Operation = Add
    private var sequence: Position?
    private var totals: [String: Int] = [:]
    private var loseAcknowledgement: Bool
    private var gated: Bool
    private var failNext = false
    private var waiting: [CheckedContinuation<Void, Never>] = []
    private(set) var attempts = 0
    private(set) var applied = 0

    init(loseAcknowledgementOnce: Bool = false, gated: Bool = false, lastSequence: Position? = nil) {
        self.loseAcknowledgement = loseAcknowledgementOnce
        self.gated = gated
        self.sequence = lastSequence
    }

    var sequenceNumber: Position? { sequence }
    func total(_ id: String) -> Int { totals[id, default: 0] }

    /// Makes the next commit fail before it applies anything, like a transient error.
    func failNextCommit() { failNext = true }

    /// Moves the accumulator as another committer would.
    func move(to position: Position) { sequence = position }

    /// Lets the longest-held-up commit go on, and no other.
    func releaseOldest() {
        if !waiting.isEmpty { waiting.removeFirst().resume() }
    }

    /// Lets the most recently held-up commit go on, and no other.
    func releaseNewest() {
        if !waiting.isEmpty { waiting.removeLast().resume() }
    }

    /// Lets the held-up commits go on.
    func openGate() {
        gated = false
        waiting.forEach { $0.resume() }
        waiting = []
    }

    func commit(_ operations: [Add], through position: Position) async throws {
        Issue.record("the view must use the fenced commit")
    }

    func commit(_ operations: [Add], after checkpoint: Position?, through position: Position) async throws {
        attempts += 1
        if gated {
            // Not cancellable: it waits for the gate, however cancelled the task is.
            await withCheckedContinuation { waiting.append($0) }
        }
        if failNext {
            failNext = false
            throw CommitFailed()
        }
        guard sequence == checkpoint else { throw PersistedAccumulatorConflict(expected: checkpoint, actual: sequence) }
        for op in operations { totals[op.id, default: 0] += op.amount }
        sequence = position
        applied += 1
        if loseAcknowledgement {
            loseAcknowledgement = false
            throw LostAck()
        }
    }
}

@Suite struct PersistedViewFencingTests {
    /// A retry that finds the accumulator somewhere else than where its own commit would have left it
    /// has not been acknowledged-and-lost: someone else committed, and carrying on would skip events.
    @Test func aRetryThatFindsTheAccumulatorElsewhereIsAConflict() async throws {
        let store = InMemoryEventStore()
        let acc = FencedAccumulator()
        var options = fastOptions(maxSize: 1)
        options.commitBackoff = .milliseconds(300)  // time to move the accumulator between the attempts
        await acc.failNextCommit()
        let view = try await totalsView.start(on: store, accumulator: acc, options: options)
        let position = try await store.append([Counted(id: "a", num: 1)])
        try await eventually { await acc.attempts == 1 }  // failed, now backing off
        await acc.move(to: 5)  // another committer got ahead

        do {
            try await view.awaitPosition(position)
            Issue.record("expected the commit to fail with a conflict")
        } catch PersistedViewError.commit(_, let attempts, let underlying) {
            #expect(underlying is PersistedAccumulatorConflict)
            #expect(attempts == 2)
        }
        #expect(await acc.applied == 0)
        #expect(view.failure != nil)
        await view.close()
    }

    @Test func aCommitThatLandedButWasNotAcknowledgedIsNotAConflictWithItself() async throws {
        let store = InMemoryEventStore()
        let acc = FencedAccumulator(loseAcknowledgementOnce: true)
        let view = try await totalsView.start(on: store, accumulator: acc, options: fastOptions())
        let position = try await store.append([Counted(id: "a", num: 4)])
        try await view.awaitPosition(position)
        #expect(await acc.attempts == 2, "the failed commit was retried")
        #expect(await acc.total("a") == 4, "and its operations were applied once")
        #expect(view.failure == nil)

        // The view carries on from where the accumulator is.
        let more = try await store.append([Counted(id: "a", num: 1)])
        try await view.awaitPosition(more)
        #expect(await acc.total("a") == 5)
        await view.close()
    }
}

@Suite struct PersistedViewCancellationTests {
    /// The worker that `close()` gives up on is stopped, not failed: nothing is reported for it.
    @Test func aCloseTimeoutIsNotAFailureOfTheView() async throws {
        let store = InMemoryEventStore()
        let acc = SlowAccumulator(gated: true)
        let reported = ErrorBox()
        var options = fastOptions(maxSize: 1)
        options.closeTimeout = .milliseconds(100)
        options.onError = { reported.set($0) }
        let view = try await totalsView.start(on: store, accumulator: acc, options: options)
        try await store.append([Counted(id: "a", num: 1)])
        try await eventually { await acc.commitsEntered == 1 }

        await view.close()
        try await eventually { await acc.cancelledCommits == 1 }
        try await Task.sleep(for: .milliseconds(100))  // for the worker to deal with it
        #expect(view.failure == nil)
        #expect(reported.value == nil)
    }

    @Test func cancellationDuringTheRetryBackoffStartsNoFurtherAttempt() async throws {
        let store = InMemoryEventStore()
        let acc = TotalsAccumulator(failingCommits: 100)
        let reported = ErrorBox()
        var options = fastOptions(maxSize: 1)
        options.commitBackoff = .seconds(30)
        options.closeTimeout = .milliseconds(100)
        options.onError = { reported.set($0) }
        let view = try await totalsView.start(on: store, accumulator: acc, options: options)
        try await store.append([Counted(id: "a", num: 1)])
        try await eventually { await acc.commitAttempts == 1 }  // failed, now backing off for 30 s

        let clock = ContinuousClock()
        let start = clock.now
        await view.close()
        #expect(clock.now - start < .seconds(5), "close waited for the backoff")
        try await Task.sleep(for: .milliseconds(100))
        #expect(await acc.commitAttempts == 1, "no attempt after the cancellation")
        #expect(view.failure == nil)
        #expect(reported.value == nil)
    }

    /// A cancelled worker stops applying events, even when the commit it was stuck in could not be cancelled.
    @Test func aCancelledWorkerAppliesNothingFurther() async throws {
        let store = InMemoryEventStore()
        let acc = FencedAccumulator(gated: true)  // the first commit can't be cancelled
        let applied = Counter()
        var options = fastOptions(maxSize: 1)
        options.closeTimeout = .milliseconds(100)
        let view = try await observedView(applied).start(on: store, accumulator: acc, options: options)
        try await store.append((1...5).map { Counted(id: "a", num: $0) })
        try await eventually { await acc.attempts == 1 }

        await view.close()  // gives up on the stuck commit, and cancels the worker
        await acc.openGate()  // the stale commit lands, and the worker is free again
        try await Task.sleep(for: .milliseconds(150))
        #expect(applied.value == 1, "the cancelled worker went on to apply \(applied.value) events")
        #expect(await acc.applied == 1)
    }

    /// A worker cancelled while it is between taking an event and committing it starts no commit.
    @Test func aWorkerCancelledJustBeforeACommitDoesNotStartIt() async throws {
        let store = InMemoryEventStore()
        let acc = SlowAccumulator()
        let inReducer = Counter()
        let release = DispatchSemaphore(value: 0)
        let blocking = PersistedStateView<Add>(bindings: [
            .on(Counted.self) { event in
                inReducer.increment()
                release.wait()  // the worker is stuck here, not in a commit
                return [Add(id: event.id, amount: event.num)]
            },
        ])
        var options = fastOptions(maxSize: 1)
        options.closeTimeout = .milliseconds(100)
        let view = try await blocking.start(on: store, accumulator: acc, options: options)
        try await store.append([Counted(id: "a", num: 1)])
        try await inReducer.wait(for: 1)

        await view.close()  // times out, and cancels the worker
        release.signal()
        try await Task.sleep(for: .milliseconds(150))
        #expect(await acc.commitsEntered == 0, "a commit was started after the worker was cancelled")
    }

    /// The stale view's commit can't be called back; the fence decides, and it is the *new* view that loses.
    @Test func aStaleUncancellableCommitFencesOffTheNewView() async throws {
        let store = InMemoryEventStore()
        let acc = FencedAccumulator(gated: true)
        var options = fastOptions(maxSize: 1)
        options.closeTimeout = .milliseconds(100)
        let stale = try await totalsView.start(on: store, accumulator: acc, options: options)
        let position = try await store.append([Counted(id: "a", num: 1)])
        try await eventually { await acc.attempts == 1 }
        await stale.close()

        // The new view reads the accumulator before the stale commit has landed, and its own commit waits too.
        let fresh = try await totalsView.start(on: store, accumulator: acc, options: fastOptions(maxSize: 1))
        try await eventually { await acc.attempts == 2 }
        await acc.releaseOldest()  // the stale commit goes first...
        try await eventually { await acc.applied == 1 }
        await acc.openGate()  // ...then the new view's

        do {
            try await fresh.awaitPosition(position)
            Issue.record("expected the new view to be fenced off")
        } catch PersistedViewError.commit(_, let attempts, let underlying) {
            #expect(underlying is PersistedAccumulatorConflict)
            #expect(attempts == 1, "a conflict is not retried")
        }
        #expect(await acc.total("a") == 1, "the event was applied once")
        #expect(await acc.applied == 1)
        #expect(stale.failure == nil, "and the stale view, closed already, has nothing to report")
        await fresh.close()
    }

    /// The other order: the new view wins, and the closed view's commit is then refused by the fence.
    /// That is nobody's failure: `close()` has returned, and the new view is the one that carries on.
    @Test func aClosedViewsCommitRefusedAfterTheNewViewWonReportsNothing() async throws {
        let store = InMemoryEventStore()
        let acc = FencedAccumulator(gated: true)
        let reported = ErrorBox()
        var options = fastOptions(maxSize: 1)
        options.closeTimeout = .milliseconds(100)
        options.onError = { reported.set($0) }
        let stale = try await totalsView.start(on: store, accumulator: acc, options: options)
        let position = try await store.append([Counted(id: "a", num: 1)])
        try await eventually { await acc.attempts == 1 }
        await stale.close()

        let fresh = try await totalsView.start(on: store, accumulator: acc, options: fastOptions(maxSize: 1))
        try await eventually { await acc.attempts == 2 }
        await acc.releaseNewest()  // the new view's commit goes first...
        try await fresh.awaitPosition(position)
        await acc.releaseOldest()  // ...and the stale one is refused
        try await Task.sleep(for: .milliseconds(150))  // for the stale worker to deal with it

        #expect(await acc.attempts == 2)
        #expect(await acc.applied == 1)
        #expect(stale.failure == nil, "a failure was recorded after close() returned: \(String(describing: stale.failure))")
        #expect(reported.value == nil, "the closed view reported \(String(describing: reported.value))")
        #expect(fresh.failure == nil)
        await fresh.close()
    }
}
