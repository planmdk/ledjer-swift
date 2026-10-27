// Deliberately no `@testable`: these tests only see what a client of the package sees,
// which is the surface the README's snippets use.
import Foundation
import GRDB
import Ledjer
import LedjerGRDB
import LedjerStoreContract
import Testing

// MARK: - Events

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

struct Reminded: Event {
    static let eventType: EventType = "bank/reminded"
    var account: String
}

// MARK: - A third-party store

/// An `EventStore` written against the public API only.
actor ThirdPartyStore: EventStore {
    private struct Subscriber {
        var query: Query
        var from: Position
        var continuation: AsyncThrowingStream<StoredEvent, any Error>.Continuation
    }

    private var log: [(event: StoredEvent, tags: [TagPath: Scalar])] = []
    private var subscribers: [UUID: Subscriber] = [:]

    private func select(_ query: Query, from: Position) -> [StoredEvent] {
        log.filter { $0.event.position >= from && query.matches(type: $0.event.type, tags: $0.tags) }.map(\.event)
    }

    func source(_ query: Query, startPosition: Position?) async throws -> [StoredEvent] {
        select(query, from: startPosition ?? 0)
    }

    func subscribe(_ query: Query, startPosition: Position?) async throws -> AsyncThrowingStream<StoredEvent, any Error> {
        let id = UUID()
        let from = startPosition ?? (log.count + 1)
        let (stream, continuation) = AsyncThrowingStream<StoredEvent, any Error>.makeStream()
        for event in select(query, from: from) { continuation.yield(event) }
        subscribers[id] = Subscriber(query: query, from: from, continuation: continuation)
        continuation.onTermination = { [weak self] _ in Task { await self?.unsubscribe(id) } }
        return stream
    }

    private func unsubscribe(_ id: UUID) { subscribers[id] = nil }

    @discardableResult
    func append(_ events: [NewEvent], condition: AppendCondition?) async throws -> Position? {
        if let condition {
            let after = condition.after ?? 0
            if log.contains(where: { $0.event.position > after && condition.query.matches(type: $0.event.type, tags: $0.tags) }) {
                throw LedjerError.appendConditionFailed
            }
        }
        var last: Position?
        for new in events {
            let stored = StoredEvent(position: log.count + 1, type: new.type, metadata: new.metadata, payload: new.payload)
            log.append((stored, new.tags))
            last = stored.position
            for subscriber in subscribers.values
            where subscriber.from <= stored.position && subscriber.query.matches(type: new.type, tags: new.tags) {
                subscriber.continuation.yield(stored)
            }
        }
        return last
    }
}

// MARK: - The README's examples

struct InsufficientFunds: Error {}

let balance = StateView(initial: 0, bindings: [
    .on(Deposited.self) { acc, e in acc += e.amount },
    .on(Withdrawn.self) { acc, e in acc -= e.amount },
])

/// A state change: decide from events, append with a consistency condition.
func withdraw(_ store: any EventStore, _ account: String, _ amount: Int) async throws {
    let query = try Query.of(Deposited.self, Withdrawn.self, where: [.payload("account"): .bound(account)])
    let seen = try await store.source(query)
    let current = try await balance.run(on: store, where: [.payload("account"): .bound(account)])
    guard current >= amount else { throw InsufficientFunds() }
    try await store.append(
        [Withdrawn(account: account, amount: amount)],
        condition: AppendCondition(query: query, after: seen.last?.position)
    )
}

private func temporaryPool() throws -> DatabasePool {
    let pool = try DatabasePool(path: NSTemporaryDirectory() + "ledjer-\(UUID().uuidString).sqlite")
    try pool.write { try $0.execute(sql: "CREATE TABLE balance (account TEXT PRIMARY KEY, amount INTEGER NOT NULL)") }
    return pool
}

private func fetchInt(_ pool: DatabasePool, _ sql: String) throws -> Int? {
    try pool.read { try Int.fetchOne($0, sql: sql) }
}

private let readModel = PersistedStateView<GRDBOperation>(bindings: [
    .on(Deposited.self) { e in
        [GRDBOperation { db in
            try db.execute(
                sql: """
                    INSERT INTO balance (account, amount) VALUES (?, ?)
                    ON CONFLICT (account) DO UPDATE SET amount = amount + excluded.amount
                    """,
                arguments: [e.account, e.amount])
        }]
    }
])

@Suite struct PublicAPITests {
    @Test func aThirdPartyStoreRunsTheContract() async throws {
        try await StoreContract.all { ThirdPartyStore() }
    }

    @Test func stateViewsAndScenarios() async throws {
        try await gwt(
            given: [Deposited(account: "a", amount: 10), Deposited(account: "b", amount: 99), Withdrawn(account: "a", amount: 3)],
            view: balance, where: [.payload("account"): .bound("a")], then: 7)

        try await gwt(
            given: [Deposited(account: "a", amount: 10)],
            when: { try await withdraw($0, "a", 4) },
            then: [Withdrawn(account: "a", amount: 4)])

        try await gwt(
            given: [Deposited(account: "a", amount: 5)],
            when: { try await withdraw($0, "a", 10) },
            thenThrows: { $0 is InsufficientFunds })

        await #expect(throws: ScenarioFailure.self) {
            try await gwt(
                given: [Deposited(account: "a", amount: 10)],
                when: { try await withdraw($0, "a", 4) },
                then: [Withdrawn(account: "a", amount: 5)])
        }
    }

    @Test func automationsAndTheirScenarios() async throws {
        try await gt(
            automation: { store in
                try await automation(on: store, triggers: [.event(Deposited.self)]) { store in
                    try await store.append([Reminded(account: "a")])
                }
            },
            given: [Deposited(account: "a", amount: 1)],
            then: [Reminded(account: "a")])
    }

    @Test func aCustomAccumulator() async throws {
        let view = PersistedStateView<Int>(bindings: [.on(Deposited.self) { [$0.amount] }])
        try await gt(
            view: view, accumulator: TotalAccumulator(),
            given: [Deposited(account: "a", amount: 5), Deposited(account: "b", amount: 7)],
            then: { await $0.total == 12 })
    }

    @Test func aPersistedViewInTheSameDatabase() async throws {
        let pool = try temporaryPool()
        let store = try await GRDBEventStore.open(pool)
        let accumulator = try await GRDBAccumulator.open(name: "balances", writer: pool)
        let view = try await readModel.start(on: store, accumulator: accumulator)

        let position = try await store.append([Deposited(account: "a", amount: 5), Deposited(account: "a", amount: 2)])
        try await view.awaitPosition(position)
        #expect(try fetchInt(pool, "SELECT amount FROM balance WHERE account = 'a'") == 7)
        #expect(try await view.accumulator().sequenceNumber == position)
        await view.close()
    }

    @Test func clearingACheckpointInAMigrationRebuildsTheReadModel() async throws {
        let pool = try temporaryPool()
        let store = try GRDBEventStore(pool)
        let first = try await readModel.start(on: store, accumulator: GRDBAccumulator(name: "balances", writer: pool))
        let position = try await store.append([Deposited(account: "a", amount: 5)])
        try await first.awaitPosition(position)
        await first.close()

        var migrator = DatabaseMigrator()
        GRDBEventStore.registerMigrations(in: &migrator)
        migrator.registerMigration("rebuild-balances") { db in
            try db.execute(sql: "DELETE FROM balance")
            try GRDBAccumulator.clearCheckpoint(named: "balances", in: db)
        }
        try migrator.migrate(pool)
        let accumulator = try GRDBAccumulator(name: "balances", writer: pool)
        #expect(try await accumulator.sequenceNumber == nil)
        #expect(try fetchInt(pool, "SELECT COUNT(*) FROM balance") == 0)

        let second = try await readModel.start(on: store, accumulator: accumulator)  // replays the log
        try await second.awaitPosition(position)
        #expect(try fetchInt(pool, "SELECT amount FROM balance WHERE account = 'a'") == 5)
        await second.close()
    }

    @Test func theGRDBStoreRunsTheContractThroughThePublicAPI() async throws {
        try await StoreContract.all { try GRDBEventStore(DatabaseQueue()) }
    }
}

/// An accumulator written against the public protocol only.
actor TotalAccumulator: PersistedAccumulator {
    typealias Operation = Int
    private(set) var total = 0
    private var position: Position?

    var sequenceNumber: Position? { position }

    func commit(_ operations: [Int], through position: Position) async throws {
        total += operations.reduce(0, +)
        self.position = position
    }
}
