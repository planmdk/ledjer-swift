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
    ///
    /// Applies the ledjer migrations if they haven't been, so it can be
    /// created before or after the `GRDBEventStore`. See
    /// `GRDBEventStore.registerMigrations(in:)` for apps with their own migrator.
    public init(name: String, writer: any DatabaseWriter) throws {
        self.name = name
        self.writer = writer
        try LedjerSchema.migrate(writer)
    }

    /// Like `init(name:writer:)`, off the calling thread, for opening the database at launch.
    public static func open(name: String, writer: any DatabaseWriter) async throws -> GRDBAccumulator {
        try await Task.detached { try GRDBAccumulator(name: name, writer: writer) }.value
    }

    /// Same as `GRDBEventStore.registerMigrations(in:)`.
    public static func registerMigrations(in migrator: inout DatabaseMigrator) {
        LedjerSchema.registerMigrations(in: &migrator)
    }

    public var sequenceNumber: Position? {
        get async throws {
            let name = LedjerSchema.blob(self.name)
            return try await writer.read { db in
                try Int.fetchOne(db, sql: "SELECT position FROM ledjer_checkpoint WHERE name = ?", arguments: [name])
            }
        }
    }

    public func commit(_ operations: [GRDBOperation], through position: Position) async throws {
        try await write(operations, expecting: nil, through: position)
    }

    /// Fenced: fails with `PersistedAccumulatorConflict`, and writes nothing, if the stored
    /// checkpoint isn't `checkpoint`. So a stale view, e.g. one whose commit was still stuck
    /// when it was closed and replaced, can't apply events twice.
    public func commit(_ operations: [GRDBOperation], after checkpoint: Position?, through position: Position) async throws {
        try await write(operations, expecting: .some(checkpoint), through: position)
    }

    private func write(_ operations: [GRDBOperation], expecting fence: Position??, through position: Position) async throws {
        let name = LedjerSchema.blob(self.name)
        try await writer.write { db in
            if let expected = fence {
                let stored = try Int.fetchOne(db, sql: "SELECT position FROM ledjer_checkpoint WHERE name = ?", arguments: [name])
                guard stored == expected else { throw PersistedAccumulatorConflict(expected: expected, actual: stored) }
            }
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
        try db.execute(sql: "DELETE FROM ledjer_checkpoint WHERE name = ?", arguments: [LedjerSchema.blob(name)])
    }
}
