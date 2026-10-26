import Foundation
import GRDB
import Ledjer

/// An `EventStore` in a SQLite database, through GRDB.
///
/// Works with a `DatabaseQueue` or a `DatabasePool` (use a pool, so reads
/// don't wait for writes). The tables are prefixed `ledjer_`, so the store can
/// share a database with the app's own tables, e.g. its read models.
///
/// - Appends, including their condition check, run in one write transaction.
/// - `position` is a SQLite `AUTOINCREMENT` key: positions start at 1, are
///   never reused, and commit in order, so a reader never sees a gap.
/// - Subscriptions are driven by GRDB's `ValueObservation`, which notices
///   changes made through the same `DatabaseWriter` instance. Writes from
///   another process (an app extension, say) aren't observed.
public final class GRDBEventStore: EventStore {
    private let writer: any DatabaseWriter

    /// How many events a subscription fetches at a time.
    private static let batchSize = 500

    /// Creates the store, migrating the database if needed.
    ///
    /// Apps that run their own `DatabaseMigrator` should call
    /// `registerMigrations(in:)` on it, see there.
    public init(_ writer: any DatabaseWriter) throws {
        self.writer = writer
        try LedjerSchema.migrate(writer)
    }

    /// Like `init(_:)`, off the calling thread, for opening the database at launch.
    public static func open(_ writer: any DatabaseWriter) async throws -> GRDBEventStore {
        try await Task.detached { try GRDBEventStore(writer) }.value
    }

    /// Registers the migrations of the ledjer tables (events, tags and view checkpoints).
    ///
    /// Creating a `GRDBEventStore` or a `GRDBAccumulator` applies them as
    /// well, in any order and any number of times. An app that has its own
    /// `DatabaseMigrator` must register them there too if it sets
    /// `eraseDatabaseOnSchemaChange`: otherwise GRDB sees the ledjer tables as
    /// unknown schema and erases the database, events included, on a schema change.
    public static func registerMigrations(in migrator: inout DatabaseMigrator) {
        LedjerSchema.registerMigrations(in: &migrator)
    }

    // MARK: EventStore

    public func source(_ query: Query, startPosition: Position?) async throws -> [StoredEvent] {
        let after = max((startPosition ?? 0) - 1, 0)
        return try await writer.read { db in
            try Self.fetch(db, query, after: after, upTo: nil, limit: nil)
        }
    }

    @discardableResult
    public func append(_ events: [NewEvent], condition: AppendCondition?) async throws -> Position? {
        try await writer.write { db in
            if let condition,
                let (positions, arguments) = Self.positionsSQL(
                    for: condition.query, after: condition.after ?? 0, upTo: nil, limit: 1)
            {
                if try Int.fetchOne(db, sql: "SELECT 1 FROM (\(positions)) LIMIT 1", arguments: arguments) != nil {
                    throw LedjerError.appendConditionFailed
                }
            }
            var last: Position?
            for event in events {
                try db.execute(
                    sql: "INSERT INTO ledjer_event (type, metadata, payload) VALUES (?, ?, ?)",
                    arguments: [event.type.rawValue, try JSONCoding.encode(event.metadata), event.payload]
                )
                let position = Int(db.lastInsertedRowID)
                for (path, value) in event.tags {
                    try db.execute(
                        sql: "INSERT INTO ledjer_event_tag (position, name, value) VALUES (?, ?, ?)",
                        arguments: [position, path.description, value.encoded]
                    )
                }
                last = position
            }
            return last
        }
    }

    public func subscribe(
        _ query: Query,
        startPosition: Position?
    ) async throws -> AsyncThrowingStream<StoredEvent, any Error> {
        // Fixing the starting point before returning means that no event
        // appended after this call returns can be missed.
        let initiallyDelivered: Position
        if let startPosition {
            initiallyDelivered = max(startPosition - 1, 0)
        } else {
            initiallyDelivered = try await writer.read(Self.head)
        }
        let writer = self.writer
        let batchSize = Self.batchSize

        return AsyncThrowingStream { continuation in
            let task = Task {
                var delivered = initiallyDelivered
                do {
                    // Emits the head position now, and again after every change to the log.
                    let heads = ValueObservation.tracking(Self.head).values(in: writer)
                    for try await head in heads {
                        while delivered < head {
                            let after = delivered
                            let batch = try await writer.read { db in
                                try Self.fetch(db, query, after: after, upTo: head, limit: batchSize)
                            }
                            for event in batch { continuation.yield(event) }
                            // A short batch means everything up to `head` has been seen.
                            delivered = batch.count < batchSize ? head : (batch.last?.position ?? head)
                        }
                    }
                    continuation.finish()
                } catch {
                    continuation.finish(throwing: error)
                }
            }
            continuation.onTermination = { _ in task.cancel() }
        }
    }

    // MARK: SQL

    private static func head(_ db: Database) throws -> Position {
        try Int.fetchOne(db, sql: "SELECT COALESCE(MAX(position), 0) FROM ledjer_event") ?? 0
    }

    /// The events matching `query` at positions in `(after, upTo]`, in order.
    private static func fetch(
        _ db: Database,
        _ query: Query,
        after: Position,
        upTo: Position?,
        limit: Int?
    ) throws -> [StoredEvent] {
        guard var (sql, arguments) = positionsSQL(for: query, after: after, upTo: upTo, limit: limit) else { return [] }
        sql = "SELECT e.position, e.type, e.metadata, e.payload FROM ledjer_event e WHERE e.position IN (\(sql)) ORDER BY e.position"
        if let limit {
            sql += " LIMIT ?"
            arguments += [limit]
        }
        return try Row.fetchAll(db, sql: sql, arguments: arguments).map { row in
            StoredEvent(
                position: row["position"],
                type: EventType(row["type"] as String),
                metadata: try JSONCoding.decode(Metadata.self, from: row["metadata"]),
                payload: row["payload"]
            )
        }
    }

    /// SQL selecting the positions of the events of `query` within `(after, upTo]`, or `nil`
    /// if the query selects nothing.
    ///
    /// One branch per selector, combined with `UNION`. A branch with required
    /// tags is driven by the tag index, `(name, value, position)`: a range scan
    /// over one tag, checking the event's type and any further tags per hit.
    /// So the cost follows the number of events with that tag, not the length
    /// of the log. A branch without tags uses the `(type, position)` index.
    /// With a `limit`, each branch is limited as well, as the first `limit`
    /// of a union lie within the first `limit` of its branches.
    ///
    /// Tag values carry their type, so a UUID never matches its string.
    static func positionsSQL(
        for query: Query,
        after: Position,
        upTo: Position?,
        limit: Int?
    ) -> (sql: String, arguments: StatementArguments)? {
        let selectors = query.selectors.sorted {
            ($0.type.rawValue, $0.required.count) < ($1.type.rawValue, $1.required.count)
        }
        guard !selectors.isEmpty else { return nil }
        var arguments = StatementArguments()
        func bounds(_ column: String) -> String {
            arguments += [after]
            if let upTo {
                arguments += [upTo]
                return " AND \(column) > ? AND \(column) <= ?"
            }
            return " AND \(column) > ?"
        }
        let branches = selectors.map { selector -> String in
            let tags = selector.required.sorted { $0.key.description < $1.key.description }
            var sql: String
            if let (path, value) = tags.first {
                sql = "SELECT t0.position AS position FROM ledjer_event_tag t0 WHERE t0.name = ? AND t0.value = ?"
                arguments += [path.description, value.encoded]
                sql += bounds("t0.position")
                sql += " AND EXISTS (SELECT 1 FROM ledjer_event e WHERE e.position = t0.position AND e.type = ?)"
                arguments += [selector.type.rawValue]
                for (index, (path, value)) in tags.dropFirst().enumerated() {
                    let t = "t\(index + 1)"
                    sql += " AND EXISTS (SELECT 1 FROM ledjer_event_tag \(t) WHERE \(t).position = t0.position AND \(t).name = ? AND \(t).value = ?)"
                    arguments += [path.description, value.encoded]
                }
                sql += " ORDER BY t0.position"
            } else {
                sql = "SELECT e.position AS position FROM ledjer_event e WHERE e.type = ?"
                arguments += [selector.type.rawValue]
                sql += bounds("e.position")
                sql += " ORDER BY e.position"
            }
            if let limit {
                sql += " LIMIT ?"
                arguments += [limit]
            }
            return "SELECT position FROM (\(sql))"
        }
        return (branches.joined(separator: " UNION "), arguments)
    }
}
