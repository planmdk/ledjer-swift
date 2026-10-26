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
