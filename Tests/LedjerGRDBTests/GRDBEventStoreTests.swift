import Foundation
import GRDB
import Ledjer
import LedjerGRDB
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

    @Test func tagLookupsUseTheIndex() throws {
        let queue = try DatabaseQueue()
        _ = try GRDBEventStore(queue)
        let plan = try queue.read { db in
            try Row.fetchAll(db, sql: """
                EXPLAIN QUERY PLAN
                SELECT e.position FROM ledjer_event e
                WHERE e.type = ? AND EXISTS (SELECT 1 FROM ledjer_event_tag t WHERE t.position = e.position AND t.name = ? AND t.value = ?)
                """, arguments: ["x", "payload.id", "uuid:x"])
                .map { $0["detail"] as String }
        }
        #expect(plan.allSatisfy { !$0.hasPrefix("SCAN ledjer_event_tag") }, "\(plan)")
    }
}
