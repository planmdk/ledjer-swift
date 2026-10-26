import Foundation
import GRDB
import Ledjer

/// One step of a persisted view's update of its read model: any writes to the database.
public struct GRDBOperation: Sendable {
    public let run: @Sendable (Database) throws -> Void
    public init(_ run: @escaping @Sendable (Database) throws -> Void) { self.run = run }
}

/// A persisted-view accumulator whose state lives in tables of a GRDB database.
///
/// Reducers emit `GRDBOperation`s, e.g. "upsert this row". A commit runs the
/// batch's operations and records the position of the last event in a
/// checkpoint row, in one transaction. After a crash the read model and its
/// checkpoint therefore agree, and the view resumes from the checkpoint.
///
/// ```swift
/// let acc = try GRDBAccumulator(name: "order-totals", writer: dbPool)
/// let view = try await orderTotals.start(on: store, accumulator: acc)
/// ```
///
/// To rebuild a read model after changing its reducers, drop or empty its
/// tables and call `clearCheckpoint(named:in:)` in a migration. The view then
/// replays the log from the start.
public final class GRDBAccumulator: PersistedAccumulator {
    public typealias Operation = GRDBOperation

    public let name: String
    private let writer: any DatabaseWriter

    /// `name` identifies the view's checkpoint. Use a different name per view.
    public init(name: String, writer: any DatabaseWriter) throws {
        self.name = name
        self.writer = writer
        // Also created by GRDBEventStore's migration, for databases where events live elsewhere.
        try writer.write { db in
            try db.execute(sql: """
                CREATE TABLE IF NOT EXISTS ledjer_checkpoint (
                    name     TEXT PRIMARY KEY NOT NULL,
                    position INTEGER NOT NULL
                )
                """)
        }
    }

    public var sequenceNumber: Position? {
        get async throws {
            let name = self.name
            return try await writer.read { db in
                try Int.fetchOne(db, sql: "SELECT position FROM ledjer_checkpoint WHERE name = ?", arguments: [name])
            }
        }
    }

    public func commit(_ operations: [GRDBOperation], through position: Position) async throws {
        let name = self.name
        try await writer.write { db in
            for operation in operations { try operation.run(db) }
            try db.execute(
                sql: """
                    INSERT INTO ledjer_checkpoint (name, position) VALUES (?, ?)
                    ON CONFLICT (name) DO UPDATE SET position = excluded.position
                    """,
                arguments: [name, position]
            )
        }
    }

    /// Forgets how far the view has got, so a new view with this name starts from the beginning.
    public static func clearCheckpoint(named name: String, in db: Database) throws {
        try db.execute(sql: "DELETE FROM ledjer_checkpoint WHERE name = ?", arguments: [name])
    }
}
