import Foundation
import GRDB
import Ledjer
@testable import LedjerGRDB
import LedjerStoreContract
import Testing

private func temporaryDatabasePath() -> String {
    NSTemporaryDirectory() + "ledjer-\(UUID().uuidString).sqlite"
}

private let queueFactory: StoreFactory = { try GRDBEventStore(DatabaseQueue()) }
private let poolFactory: StoreFactory = { try GRDBEventStore(DatabasePool(path: temporaryDatabasePath())) }

@Suite struct GRDBEventStoreContractTests {
    @Test(arguments: [("queue", queueFactory), ("pool", poolFactory)] as [(String, StoreFactory)])
    func contract(_ name: String, _ make: StoreFactory) async throws {
        try await StoreContract.all(make)
    }
}

@Suite struct GRDBEventStoreTests {
    @Test func theLogIsAppendOnly() async throws {
        let queue = try DatabaseQueue()
        let store = try GRDBEventStore(queue)
        try await store.append([UUIDTagged(id: UUID(), num: 1)])
        #expect(throws: DatabaseError.self) {
            try queue.write { try $0.execute(sql: "UPDATE ledjer_event SET type = 'x'") }
        }
        #expect(throws: DatabaseError.self) {
            try queue.write { try $0.execute(sql: "DELETE FROM ledjer_event") }
        }
    }

    @Test func eventsSurviveReopeningTheDatabase() async throws {
        let path = temporaryDatabasePath()
        let id = UUID()
        do {
            let store = try GRDBEventStore(DatabasePool(path: path))
            try await store.append([UUIDTagged(id: id, num: 1)])
        }
        let store = try GRDBEventStore(DatabasePool(path: path))
        let position = try await store.append([UUIDTagged(id: UUID(), num: 2)])
        #expect(position == 2)
        let found = try await store.source(EventQuery.of(UUIDTagged.self, where: [.payload("id"): .bound(id)]))
        #expect(try found.map { try $0.decode(UUIDTagged.self).num } == [1])
    }

    @Test func aFailedAppendLeavesNothingBehind() async throws {
        let queue = try DatabaseQueue()
        let store = try GRDBEventStore(queue)
        let order = UUID()
        try await store.append([OrderPlaced(orderId: order)])
        let condition = AppendCondition(query: try EventQuery.of(OrderPlaced.self, where: [.payload("orderId"): .bound(order)]))
        await #expect(throws: LedjerError.self) {
            try await store.append([OrderPlaced(orderId: UUID()), OrderPlaced(orderId: UUID())], condition: condition)
        }
        let (events, tags) = try await queue.read { db in
            (try Int.fetchOne(db, sql: "SELECT COUNT(*) FROM ledjer_event")!, try Int.fetchOne(db, sql: "SELECT COUNT(*) FROM ledjer_event_tag")!)
        }
        #expect(events == 1)
        #expect(tags == 1)  // the one event's orderId; it has no customer
    }

    /// A query of `count` selectors: one item of the same type per order, so each is a branch of the union.
    private func query(selectors count: Int) -> EventQuery {
        try! EventQuery((0..<count).map { _ in try EventQueryItem([OrderPlaced.self], where: [.payload("orderId"): .bound(UUID())]) })
    }

    @Test func aQueryOfTheMaximumNumberOfSelectorsWorks() async throws {
        let store = try GRDBEventStore(DatabaseQueue())
        let order = UUID()
        let first = try await store.append([OrderPlaced(orderId: order)])!
        let items = (0..<(GRDBEventStore.maxSelectors - 1)).map { _ in
            try! EventQueryItem([OrderPlaced.self], where: [.payload("orderId"): .bound(UUID())])
        } + [try EventQueryItem([OrderPlaced.self], where: [.payload("orderId"): .bound(order)])]
        let query = try EventQuery(items)
        #expect(query.selectors.count == 500)

        #expect(try await store.source(query).map(\.position) == [first])
        #expect(try await store.lastPosition(matching: query, through: first) == first)
        await #expect(throws: LedjerError.self) {
            try await store.append([OrderPlaced(orderId: UUID())], condition: AppendCondition(query: query))
        }
        var events = try await store.subscribe(query, startPosition: 1).makeAsyncIterator()
        #expect(try await events.next()?.position == first)
    }

    /// The README's arithmetic for the bound-parameter limit: 2k+2 per branch for k bound tags, 2k+4 with an
    /// upper bound and a limit.
    @Test func aBranchBindsTwoParametersPerBoundTagPlusTwoToFour() throws {
        for (tags, k) in [([:], 0), ([.payload("orderId"): .bound(UUID())], 1),
                          ([.payload("orderId"): .bound(UUID()), .payload("customer"): "ann"], 2)] as [(TagBindings, Int)] {
            let query = try EventQuery.of(OrderPlaced.self, where: tags)
            let plain = try #require(GRDBEventStore.positionsSQL(for: query, after: 0, upTo: nil, limit: nil))
            #expect(plain.sql.filter { $0 == "?" }.count == 2 * k + 2, "k = \(k)")
            let bounded = try #require(GRDBEventStore.positionsSQL(for: query, after: 0, upTo: 10, limit: 5))
            #expect(bounded.sql.filter { $0 == "?" }.count == 2 * k + 4, "k = \(k)")
        }
    }

    @Test func anEventTypeWithTwoTagsOfOnePathIsRefusedBeforeAnythingIsWritten() async throws {
        struct Twice: Event {
            static let eventType: EventType = "grdb-test/twice"
            static var tags: [EventTag<Self>] { [EventTag("id", \.a), EventTag("id", \.b)] }
            var a: String
            var b: String
        }
        let queue = try DatabaseQueue()
        let store = try GRDBEventStore(queue)
        await #expect(throws: LedjerError.self) {
            try await store.append([OrderPlaced(orderId: UUID()), Twice(a: "a", b: "b")])
        }
        let count = try await queue.read { try Int.fetchOne($0, sql: "SELECT COUNT(*) FROM ledjer_event")! }
        #expect(count == 0)
    }

    @Test func moreSelectorsThanSQLiteCanCombineAreRefusedClearly() async throws {
        let queue = try DatabaseQueue()
        let store = try GRDBEventStore(queue)
        try await store.append([OrderPlaced(orderId: UUID())])
        let query = query(selectors: GRDBEventStore.maxSelectors + 1)
        #expect(query.selectors.count == 501)

        func refused(_ operation: () async throws -> Void) async -> Bool {
            do { try await operation() } catch LedjerError.invalidQuery(let why) {
                return why.contains("at most 500 query selectors")
            } catch { return false }
            return false
        }
        #expect(await refused { _ = try await store.source(query) })
        #expect(await refused { _ = try await store.lastPosition(matching: query, through: 1) })
        #expect(await refused { _ = try await store.subscribe(query) })
        #expect(await refused { try await store.append([OrderPlaced(orderId: UUID())], condition: AppendCondition(query: query)) })
        let count = try await queue.read { try Int.fetchOne($0, sql: "SELECT COUNT(*) FROM ledjer_event")! }
        #expect(count == 1)  // the refused append wrote nothing
    }

    /// The query plan of `sql(query)`, a statement the store really generates, after filling the log.
    private func plan(
        _ query: EventQuery,
        _ sql: (EventQuery) -> (sql: String, arguments: StatementArguments)?
    ) throws -> [String] {
        let queue = try DatabaseQueue()
        _ = try GRDBEventStore(queue)
        try queue.write { db in
            for i in 0..<2000 {
                try db.execute(sql: "INSERT INTO ledjer_event (type, metadata, payload) VALUES (?, '{}', '{}')",
                               arguments: [LedjerSchema.blob(i % 2 == 0 ? "contract/order-placed" : "contract/order-noted")])
                if i % 2 == 0 {
                    try db.execute(sql: "INSERT INTO ledjer_event_tag (position, name, value) VALUES (?, ?, ?)",
                                   arguments: [i + 1, LedjerSchema.blob("payload.orderId"), LedjerSchema.blob("uuid:\(UUID().uuidString)")])
                }
            }
            try db.execute(sql: "ANALYZE")
        }
        let statement = try #require(sql(query))
        return try queue.read { db in
            try Row.fetchAll(db, sql: "EXPLAIN QUERY PLAN \(statement.sql)", arguments: statement.arguments).map { $0["detail"] as String }
        }
    }

    /// Every statement the store runs to find events: the positions, the events, the last position.
    private func statements(after: Position = 0, upTo: Position? = nil, limit: Int? = nil)
        -> [(name: String, sql: (EventQuery) -> (sql: String, arguments: StatementArguments)?)]
    {
        [
            ("positions", { GRDBEventStore.positionsSQL(for: $0, after: after, upTo: upTo, limit: limit) }),
            ("events", { GRDBEventStore.eventsSQL(for: $0, after: after, upTo: upTo, limit: limit) }),
            ("last position", { GRDBEventStore.lastPositionSQL(for: $0, through: upTo ?? 1000) }),
        ]
    }

    private func neverScansTheLog(_ plan: [String]) -> Bool {
        !plan.contains { $0.contains("SCAN e") || $0.contains("SCAN ledjer_event ") || $0.contains("SCAN ledjer_event_tag") }
    }

    @Test func taggedSelectorsAreDrivenByTheTagIndex() throws {
        let query = try EventQuery.of(OrderPlaced.self, where: [.payload("orderId"): .bound(UUID())])
        for variant in [statements(), statements(after: 10, upTo: 1000, limit: 500)] {
            for (name, sql) in variant {
                let plan = try plan(query, sql)
                #expect(plan.contains { $0.contains("ledjer_event_tag_lookup") && $0.contains("name=?") && $0.contains("value=?") }, "\(name): \(plan)")
                // The log is neither scanned nor walked by type for a tagged selector.
                #expect(neverScansTheLog(plan) && !plan.contains { $0.contains("ledjer_event_type") }, "\(name): \(plan)")
            }
        }
    }

    @Test func aSecondSelectorDoesNotDefeatTheTagIndex() throws {
        let query = try EventQuery([
            try EventQueryItem([OrderPlaced.self], where: [.payload("orderId"): .bound(UUID())]),
            try EventQueryItem([OrderPlaced.self], where: [.payload("orderId"): .bound(UUID())]),
        ])
        for (name, sql) in statements(limit: 1) {
            let plan = try plan(query, sql)
            #expect(plan.filter { $0.contains("ledjer_event_tag_lookup") }.count >= 2, "\(name): \(plan)")
            #expect(neverScansTheLog(plan), "\(name): \(plan)")
        }
    }

    @Test func untaggedSelectorsUseTheTypeIndex() throws {
        let query = try EventQuery.of(OrderPlaced.self)
        for (name, sql) in statements(after: 5) {
            let plan = try plan(query, sql)
            #expect(plan.contains { $0.contains("ledjer_event_type") }, "\(name): \(plan)")
            #expect(neverScansTheLog(plan), "\(name): \(plan)")
        }
    }
}

@Suite struct GRDBAutomationTests {
    /// Stopping cancels an event subscription wherever it is, including in the middle of a database
    /// read, which used to be reported to `onError` as a failure (about 1 stop in 3).
    @Test func stoppingAnAutomationIsNotReportedAsAFailure() async throws {
        let store = try GRDBEventStore(DatabasePool(path: NSTemporaryDirectory() + "ledjer-\(UUID().uuidString).sqlite"))
        let reported = ErrorList()
        for round in 0..<40 {
            let running = try await automation(
                on: store,
                triggers: [.event(UUIDTagged.self)],
                onError: { reported.add($0) }
            ) { _ in }
            // A big append: the subscription's read of it is long enough to be cancelled in the middle.
            try await store.append((0..<400).map { UUIDTagged(id: UUID(), num: round * 1000 + $0) })
            if round % 2 == 0 { try await Task.sleep(for: .milliseconds(round % 5)) }
            running.stop()
        }
        try await Task.sleep(for: .milliseconds(200))
        #expect(reported.errors.isEmpty, "\(reported.errors)")
    }
}

final class ErrorList: @unchecked Sendable {
    private let lock = NSLock()
    private var _errors: [String] = []
    var errors: [String] { lock.withLock { _errors } }
    func add(_ error: any Error) { lock.withLock { _errors.append("\(error)") } }
}
