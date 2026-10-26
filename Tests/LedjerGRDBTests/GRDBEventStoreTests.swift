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
        let found = try await store.source(Query.of(UUIDTagged.self, where: [.payload("id"): .bound(id)]))
        #expect(try found.map { try $0.decode(UUIDTagged.self).num } == [1])
    }

    @Test func aFailedAppendLeavesNothingBehind() async throws {
        let queue = try DatabaseQueue()
        let store = try GRDBEventStore(queue)
        let order = UUID()
        try await store.append([OrderPlaced(orderId: order)])
        let condition = AppendCondition(query: try Query.of(OrderPlaced.self, where: [.payload("orderId"): .bound(order)]))
        await #expect(throws: LedjerError.self) {
            try await store.append([OrderPlaced(orderId: UUID()), OrderPlaced(orderId: UUID())], condition: condition)
        }
        let (events, tags) = try await queue.read { db in
            (try Int.fetchOne(db, sql: "SELECT COUNT(*) FROM ledjer_event")!, try Int.fetchOne(db, sql: "SELECT COUNT(*) FROM ledjer_event_tag")!)
        }
        #expect(events == 1)
        #expect(tags == 1)  // the one event's orderId; it has no customer
    }

    /// The query plan of the SQL the store really generates, after filling the log.
    private func plan(_ query: Query, after: Position = 0, upTo: Position? = nil, limit: Int? = nil) throws -> [String] {
        let queue = try DatabaseQueue()
        _ = try GRDBEventStore(queue)
        try queue.write { db in
            for i in 0..<2000 {
                try db.execute(sql: "INSERT INTO ledjer_event (type, metadata, payload) VALUES (?, '{}', '{}')",
                               arguments: [i % 2 == 0 ? "contract/order-placed" : "contract/order-noted"])
                if i % 2 == 0 {
                    try db.execute(sql: "INSERT INTO ledjer_event_tag (position, name, value) VALUES (?, 'payload.orderId', ?)",
                                   arguments: [i + 1, "uuid:\(UUID().uuidString)"])
                }
            }
            try db.execute(sql: "ANALYZE")
        }
        let (sql, arguments) = try #require(GRDBEventStore.positionsSQL(for: query, after: after, upTo: upTo, limit: limit))
        return try queue.read { db in
            try Row.fetchAll(db, sql: "EXPLAIN QUERY PLAN \(sql)", arguments: arguments).map { $0["detail"] as String }
        }
    }

    @Test func taggedSelectorsAreDrivenByTheTagIndex() throws {
        let query = try Query.of(OrderPlaced.self, where: [.payload("orderId"): .bound(UUID())])
        for plan in [try plan(query), try plan(query, after: 10, upTo: 1000, limit: 500)] {
            #expect(plan.contains { $0.contains("ledjer_event_tag_lookup") && $0.contains("name=?") && $0.contains("value=?") }, "\(plan)")
            // The log itself is never scanned or walked for a tagged selector.
            #expect(!plan.contains { $0.contains("SCAN e") || $0.contains("SCAN ledjer_event ") || $0.contains("ledjer_event_type") }, "\(plan)")
        }
    }

    @Test func aSecondSelectorDoesNotDefeatTheTagIndex() throws {
        let query = Query([
            try QueryItem([OrderPlaced.self], where: [.payload("orderId"): .bound(UUID())]),
            try QueryItem([OrderPlaced.self], where: [.payload("orderId"): .bound(UUID())]),
        ])
        let plan = try plan(query, limit: 1)
        #expect(plan.filter { $0.contains("ledjer_event_tag_lookup") }.count >= 2, "\(plan)")
        #expect(!plan.contains { $0.contains("SCAN e") || $0.contains("SCAN ledjer_event ") }, "\(plan)")
    }

    @Test func untaggedSelectorsUseTheTypeIndex() throws {
        let plan = try plan(try Query.of(OrderPlaced.self), after: 5)
        #expect(plan.contains { $0.contains("ledjer_event_type") }, "\(plan)")
        #expect(!plan.contains { $0.contains("SCAN") && !$0.contains("USING") }, "\(plan)")
    }
}
