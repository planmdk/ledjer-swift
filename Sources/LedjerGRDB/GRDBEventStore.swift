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
            if let condition {
                let (clause, arguments) = Self.clause(for: condition.query)
                let conflict = try Bool.fetchOne(
                    db,
                    sql: "SELECT EXISTS (SELECT 1 FROM ledjer_event e WHERE e.position > ? AND (\(clause)))",
                    arguments: StatementArguments([condition.after ?? 0]) + arguments
                )
                if conflict == true { throw LedjerError.appendConditionFailed }
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
        let (clause, arguments) = clause(for: query)
        var sql = "SELECT e.position, e.type, e.metadata, e.payload FROM ledjer_event e WHERE e.position > ?"
        var all = StatementArguments([after])
        if let upTo {
            sql += " AND e.position <= ?"
            all += [upTo]
        }
        sql += " AND (\(clause)) ORDER BY e.position"
        all += arguments
        if let limit {
            sql += " LIMIT ?"
            all += [limit]
        }
        return try Row.fetchAll(db, sql: sql, arguments: all).map { row in
            StoredEvent(
                position: row["position"],
                type: EventType(row["type"] as String),
                metadata: try JSONCoding.decode(Metadata.self, from: row["metadata"]),
                payload: row["payload"]
            )
        }
    }

    /// An SQL condition on `ledjer_event e` selecting the events of `query`.
    ///
    /// One alternative per selector: the event's type, and for each tag the
    /// selector requires, a matching row in `ledjer_event_tag`. Tag values
    /// carry their type, so a UUID never matches its string.
    private static func clause(for query: Query) -> (sql: String, arguments: StatementArguments) {
        let selectors = query.selectors.sorted {
            ($0.type.rawValue, $0.required.count) < ($1.type.rawValue, $1.required.count)
        }
        guard !selectors.isEmpty else { return ("0", []) }
        var arguments = StatementArguments()
        let alternatives = selectors.map { selector in
            var parts = ["e.type = ?"]
            arguments += [selector.type.rawValue]
            for (path, value) in selector.required.sorted(by: { $0.key.description < $1.key.description }) {
                parts.append(
                    "EXISTS (SELECT 1 FROM ledjer_event_tag t WHERE t.position = e.position AND t.name = ? AND t.value = ?)"
                )
                arguments += [path.description, value.encoded]
            }
            return "(" + parts.joined(separator: " AND ") + ")"
        }
        return (alternatives.joined(separator: " OR "), arguments)
    }
}
