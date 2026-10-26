import Foundation
import GRDB
import Ledjer
import LedjerGRDB
import Testing

struct Deposited: Event {
    static let eventType: EventType = "bank/deposited"
    static var tags: [EventTag<Self>] { [EventTag("account", \.account)] }
    var account: String
    var amount: Int
}

struct Withdrawn: Event {
    static let eventType: EventType = "bank/withdrawn"
    static var tags: [EventTag<Self>] { [EventTag("account", \.account)] }
    var account: String
    var amount: Int
}

private func adjust(_ account: String, by amount: Int) -> GRDBOperation {
    GRDBOperation { db in
        try db.execute(
            sql: """
                INSERT INTO balance (account, amount) VALUES (?, ?)
                ON CONFLICT (account) DO UPDATE SET amount = amount + excluded.amount
                """,
            arguments: [account, amount]
        )
    }
}

private let balances = PersistedStateView<GRDBOperation>(bindings: [
    .on(Deposited.self) { [adjust($0.account, by: $0.amount)] },
    .on(Withdrawn.self) { [adjust($0.account, by: -$0.amount)] },
])

private func makeDatabase() throws -> DatabasePool {
    let pool = try DatabasePool(path: NSTemporaryDirectory() + "ledjer-\(UUID().uuidString).sqlite")
    try pool.write { try $0.execute(sql: "CREATE TABLE balance (account TEXT PRIMARY KEY, amount INTEGER NOT NULL)") }
    return pool
}

private func balance(_ pool: DatabasePool, _ account: String) throws -> Int {
    try pool.read { try Int.fetchOne($0, sql: "SELECT amount FROM balance WHERE account = ?", arguments: [account]) ?? 0 }
}

private func fast() -> PersistedViewOptions {
    var options = PersistedViewOptions()
    options.maxInterval = .milliseconds(10)
    return options
}

@Suite struct GRDBPersistedViewTests {
    @Test func keepsAReadModelInTheSameDatabaseUpToDate() async throws {
        let pool = try makeDatabase()
        let store = try GRDBEventStore(pool)
        let view = try await balances.start(on: store, accumulator: GRDBAccumulator(name: "balances", writer: pool), options: fast())

        let position = try await store.append([
            Deposited(account: "a", amount: 100), Withdrawn(account: "a", amount: 30), Deposited(account: "b", amount: 5),
        ])
        try await view.awaitPosition(position)
        #expect(try balance(pool, "a") == 70)
        #expect(try balance(pool, "b") == 5)

        let more = try await store.append([Deposited(account: "a", amount: 1)])
        try await view.awaitPosition(more)
        #expect(try balance(pool, "a") == 71)
        await view.close()
    }

    @Test func resumesFromTheCheckpointAfterARestart() async throws {
        let pool = try makeDatabase()
        let store = try GRDBEventStore(pool)
        let first = try await balances.start(on: store, accumulator: GRDBAccumulator(name: "balances", writer: pool), options: fast())
        let position = try await store.append([Deposited(account: "a", amount: 10)])
        try await first.awaitPosition(position)
        await first.close()

        // Appended while no view is running.
        let missed = try await store.append([Deposited(account: "a", amount: 5)])

        let second = try await balances.start(on: store, accumulator: GRDBAccumulator(name: "balances", writer: pool), options: fast())
        try await second.awaitPosition(missed)
        #expect(try balance(pool, "a") == 15)  // not 25: the first event isn't applied twice
        await second.close()
    }

    @Test func clearingTheCheckpointRebuildsTheReadModel() async throws {
        let pool = try makeDatabase()
        let store = try GRDBEventStore(pool)
        let acc = try GRDBAccumulator(name: "balances", writer: pool)
        let first = try await balances.start(on: store, accumulator: acc, options: fast())
        let position = try await store.append([Deposited(account: "a", amount: 10), Withdrawn(account: "a", amount: 3)])
        try await first.awaitPosition(position)
        await first.close()

        try await pool.write { db in
            try db.execute(sql: "DELETE FROM balance")
            try GRDBAccumulator.clearCheckpoint(named: "balances", in: db)
        }
        let second = try await balances.start(on: store, accumulator: acc, options: fast())
        try await second.awaitPosition(position)
        #expect(try balance(pool, "a") == 7)
        await second.close()
    }

    @Test func aFailingOperationRollsBackTheWholeBatch() async throws {
        let pool = try makeDatabase()
        let acc = try GRDBAccumulator(name: "balances", writer: pool)
        await #expect(throws: DatabaseError.self) {
            try await acc.commit(
                [adjust("a", by: 1), GRDBOperation { try $0.execute(sql: "INSERT INTO nonexistent VALUES (1)") }],
                through: 2
            )
        }
        #expect(try balance(pool, "a") == 0)
        #expect(try await acc.sequenceNumber == nil)
    }

    @Test func aStaleCommitIsFencedOff() async throws {
        let pool = try makeDatabase()
        let current = try GRDBAccumulator(name: "balances", writer: pool)
        let stale = try GRDBAccumulator(name: "balances", writer: pool)  // same view, e.g. from before a restart
        try await current.commit([adjust("a", by: 1)], after: nil, through: 1)

        do {
            try await stale.commit([adjust("a", by: 1)], after: nil, through: 1)
            Issue.record("expected a conflict")
        } catch let conflict as PersistedAccumulatorConflict {
            #expect(conflict.expected == nil)
            #expect(conflict.actual == 1)
        }
        #expect(try balance(pool, "a") == 1)  // not applied twice

        try await stale.commit([adjust("a", by: 5)], after: 1, through: 3)
        #expect(try balance(pool, "a") == 6)
        #expect(try await current.sequenceNumber == 3)
    }

    @Test func twoViewsOnOneCheckpointCannotBothApplyAnEvent() async throws {
        let pool = try makeDatabase()
        let store = try GRDBEventStore(pool)
        let first = try await balances.start(on: store, accumulator: GRDBAccumulator(name: "balances", writer: pool), options: fast())
        let second = try await balances.start(on: store, accumulator: GRDBAccumulator(name: "balances", writer: pool), options: fast())
        let position = try await store.append([Deposited(account: "a", amount: 10)])

        var failures: [any Error] = []
        for view in [first, second] {
            do { try await view.awaitPosition(position) } catch { failures.append(error) }
        }
        #expect(failures.count == 1, "one view wins the checkpoint, the other is fenced off")
        if case PersistedViewError.commit(_, let attempts, let underlying)? = failures.first {
            #expect(underlying is PersistedAccumulatorConflict)
            #expect(attempts == 1, "a conflict is not retried")
        } else {
            Issue.record("expected a commit failure, got \(failures)")
        }
        #expect(try balance(pool, "a") == 10)
        await first.close()
        await second.close()
    }

    @Test func worksWithGtOnAnInMemoryStore() async throws {
        let pool = try makeDatabase()
        try await gt(
            view: balances,
            accumulator: GRDBAccumulator(name: "balances", writer: pool),
            given: [Deposited(account: "a", amount: 2), Deposited(account: "a", amount: 3)],
            then: { _ in try balance(pool, "a") == 5 }
        )
    }
}

/// Counts the statements that read events, to see how much of the log a subscription reads.
private final class EventReads: @unchecked Sendable {
    private let lock = NSLock()
    private var _count = 0
    var count: Int { lock.withLock { _count } }
    func record(_ sql: String) {
        if sql.contains("SELECT e.position, e.type, e.metadata, e.payload") { lock.withLock { _count += 1 } }
    }
}

private func countingQueue(_ reads: EventReads) throws -> DatabaseQueue {
    var configuration = Configuration()
    configuration.prepareDatabase { db in db.trace { reads.record($0.expandedDescription) } }
    return try DatabaseQueue(configuration: configuration)
}

/// An accumulator whose commits wait until it is opened.
private actor GatedAccumulator: PersistedAccumulator {
    typealias Operation = Int
    private var open = false
    private var sequence: Position?
    private(set) var applied = 0
    func openGate() { open = true }
    var sequenceNumber: Position? { sequence }
    func commit(_ operations: [Int], through position: Position) async throws {
        while !open { try await Task.sleep(for: .milliseconds(5)) }
        applied += operations.count
        sequence = position
    }
}

@Suite struct GRDBBackpressureTests {
    @Test func aSubscriptionReadsTheLogOnlyAsFastAsItIsConsumed() async throws {
        let reads = EventReads()
        let queue = try countingQueue(reads)
        let store = try GRDBEventStore(queue)
        try await store.append((0..<3000).map { Deposited(account: "a", amount: $0) })

        let stream = try await store.subscribe(Query.of(Deposited.self), startPosition: 0)
        var iterator = stream.makeAsyncIterator()
        for _ in 0..<10 { _ = try await iterator.next() }
        #expect(reads.count == 1, "10 events asked for, one batch of 500 read: \(reads.count)")

        var seen = 10
        while seen < 3000, try await iterator.next() != nil { seen += 1 }
        #expect(seen == 3000)
        #expect(reads.count == 6)  // 3000 events in batches of 500
    }

    @Test func aSlowCommitHoldsBackTheReadsOfAPersistedView() async throws {
        let reads = EventReads()
        let queue = try countingQueue(reads)
        let store = try GRDBEventStore(queue)
        try await store.append((0..<5000).map { Deposited(account: "a", amount: $0) })

        let acc = GatedAccumulator()
        let view = PersistedStateView<Int>(bindings: [.on(Deposited.self) { [$0.amount] }])
        var options = PersistedViewOptions()
        options.maxSize = 10
        options.maxInterval = .milliseconds(10)
        let running = try await view.start(on: store, accumulator: acc, options: options)

        // The first commit is stuck, so the worker, the inbox and the pump fill up and stop reading.
        try await Task.sleep(for: .milliseconds(300))
        #expect(reads.count <= 2, "reads while the commit is stuck: \(reads.count) (10 batches without backpressure)")

        await acc.openGate()
        try await running.awaitPosition(5000, timeout: .seconds(20))
        #expect(await acc.applied == 5000)
        await running.close()
    }
}
