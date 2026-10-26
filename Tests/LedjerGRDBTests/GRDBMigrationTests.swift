import Foundation
import GRDB
import Ledjer
import LedjerGRDB
import LedjerStoreContract
import Testing

@Suite struct GRDBMigrationTests {
    private func path() -> String { NSTemporaryDirectory() + "ledjer-\(UUID().uuidString).sqlite" }

    @Test func anAccumulatorCreatedBeforeTheStoreOnAFreshDatabase() async throws {
        let pool = try DatabasePool(path: path())
        _ = try GRDBAccumulator(name: "x", writer: pool)
        let store = try GRDBEventStore(pool)  // used to fail: table ledjer_checkpoint already exists
        #expect(try await store.append([UUIDTagged(id: UUID(), num: 1)]) == 1)
        // and a restart is fine too
        _ = try GRDBEventStore(pool)
        _ = try GRDBAccumulator(name: "x", writer: pool)
    }

    @Test func theStoreCreatedBeforeTheAccumulator() async throws {
        let queue = try DatabaseQueue()
        _ = try GRDBEventStore(queue)
        let acc = try GRDBAccumulator(name: "x", writer: queue)
        #expect(try await acc.sequenceNumber == nil)
    }

    @Test func asyncOpenersWork() async throws {
        let pool = try DatabasePool(path: path())
        let acc = try await GRDBAccumulator.open(name: "x", writer: pool)
        let store = try await GRDBEventStore.open(pool)
        #expect(try await acc.sequenceNumber == nil)
        #expect(try await store.append([UUIDTagged(id: UUID(), num: 1)]) == 1)
    }

    @Test func anAppMigratorThatErasesOnSchemaChangeKeepsTheEventsOfItsOwnRegistration() async throws {
        let path = path()
        func migrator() -> DatabaseMigrator {
            var migrator = DatabaseMigrator()
            migrator.eraseDatabaseOnSchemaChange = true
            GRDBEventStore.registerMigrations(in: &migrator)
            migrator.registerMigration("app-1") { db in
                try db.execute(sql: "CREATE TABLE balance (account TEXT PRIMARY KEY, amount INTEGER NOT NULL)")
            }
            return migrator
        }
        do {
            let pool = try DatabasePool(path: path)
            try migrator().migrate(pool)
            let store = try GRDBEventStore(pool)
            try await store.append([UUIDTagged(id: UUID(), num: 1)])
            _ = try GRDBAccumulator(name: "x", writer: pool)
        }
        // A later launch with an app migration added.
        var later = migrator()
        later.registerMigration("app-2") { db in
            try db.execute(sql: "CREATE TABLE extra (id INTEGER PRIMARY KEY)")
        }
        let pool = try DatabasePool(path: path)
        try later.migrate(pool)
        let store = try GRDBEventStore(pool)
        let events = try await store.source(Query.of(UUIDTagged.self))
        #expect(events.count == 1)
    }

    @Test func tagsAreImmutableToo() async throws {
        let queue = try DatabaseQueue()
        let store = try GRDBEventStore(queue)
        try await store.append([UUIDTagged(id: UUID(), num: 1)])
        #expect(throws: DatabaseError.self) {
            try queue.write { try $0.execute(sql: "UPDATE ledjer_event_tag SET value = 'x'") }
        }
        #expect(throws: DatabaseError.self) {
            try queue.write { try $0.execute(sql: "DELETE FROM ledjer_event_tag") }
        }
    }
}
