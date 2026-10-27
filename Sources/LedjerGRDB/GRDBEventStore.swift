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

    /// The most selectors (event types, per tag bindings that apply to them) a query can have.
    /// Each selector is one branch of a `UNION`, and SQLite's default limit for the terms of a
    /// compound `SELECT` is 500.
    static let maxSelectors = 500

    /// Throws `LedjerError.invalidQuery` for a query with more selectors than SQLite can combine,
    /// instead of letting SQLite fail with "too many terms in compound SELECT".
    static func checkSelectorCount(_ query: EventQuery) throws {
        guard query.selectors.count <= maxSelectors else {
            throw LedjerError.invalidQuery(
                "the GRDB store supports at most \(maxSelectors) query selectors per query, "
                    + "but this one has \(query.selectors.count)")
        }
    }

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

    public func source(_ query: EventQuery, startPosition: Position?) async throws -> [StoredEvent] {
        try Self.checkSelectorCount(query)
        let after = max((startPosition ?? 0) - 1, 0)
        return try await writer.read { db in
            try Self.fetch(db, query, after: after, upTo: nil, limit: nil)
        }
    }

    @discardableResult
    public func append(_ events: [NewEvent], condition: AppendCondition?) async throws -> Position? {
        if let condition { try Self.checkSelectorCount(condition.query) }
        return try await writer.write { db in
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
                    arguments: [LedjerSchema.blob(event.type.rawValue), try JSONCoding.encode(event.metadata), event.payload]
                )
                let position = Int(db.lastInsertedRowID)
                for (path, value) in event.tags {
                    try db.execute(
                        sql: "INSERT INTO ledjer_event_tag (position, name, value) VALUES (?, ?, ?)",
                        arguments: [position, LedjerSchema.blob(path.description), LedjerSchema.blob(value.encoded)]
                    )
                }
                last = position
            }
            return last
        }
    }

    public func lastPosition(matching query: EventQuery, through position: Position) async throws -> Position? {
        try Self.checkSelectorCount(query)
        return try await writer.read { db in
            guard let (sql, arguments) = Self.lastPositionSQL(for: query, through: position) else { return nil }
            return try Int.fetchOne(db, sql: sql, arguments: arguments)
        }
    }

    public func subscribe(
        _ query: EventQuery,
        startPosition: Position?
    ) async throws -> AsyncThrowingStream<StoredEvent, any Error> {
        try Self.checkSelectorCount(query)
        // Fixing the starting point before returning means that no event
        // appended after this call returns can be missed.
        let initiallyDelivered: Position
        if let startPosition {
            initiallyDelivered = max(startPosition - 1, 0)
        } else {
            initiallyDelivered = try await writer.read(Self.head)
        }
        let subscription = Subscription(
            writer: writer, query: query, batchSize: Self.batchSize, delivered: initiallyDelivered)
        // Pull-based: events are read from the database only as the consumer asks for them,
        // so a slow consumer slows the reads down instead of piling events up in memory.
        return AsyncThrowingStream(unfolding: { try await subscription.next() })
    }

    /// The state of one subscription. A stream has a single consumer, which calls `next` one at a time
    /// (also after a cancellation, which just makes the next call return `nil`), so no locking.
    private final class Subscription: @unchecked Sendable {
        let writer: any DatabaseWriter
        let query: EventQuery
        let batchSize: Int
        /// Events up to here have been read from the database.
        var delivered: Position
        /// The newest head position seen. Events up to here can be read.
        var knownHead: Position
        var buffer: [StoredEvent] = []
        var bufferIndex = 0
        var heads: AsyncValueObservation<Position>.AsyncIterator?

        init(writer: any DatabaseWriter, query: EventQuery, batchSize: Int, delivered: Position) {
            self.writer = writer
            self.query = query
            self.batchSize = batchSize
            self.delivered = delivered
            self.knownHead = delivered
        }

        /// The next event, or `nil` when the consumer has been cancelled, as with the in-memory
        /// store: cancelling ends the stream, wherever the cancellation lands, and isn't an error.
        func next() async throws -> StoredEvent? {
            do {
                return try await nextEvent()
            } catch is CancellationError where Task.isCancelled {
                return nil
            }
        }

        private func nextEvent() async throws -> StoredEvent? {
            while true {
                if bufferIndex < buffer.count {
                    defer { bufferIndex += 1 }
                    return buffer[bufferIndex]
                }
                if delivered < knownHead {
                    let (after, upTo, limit, query) = (delivered, knownHead, batchSize, query)
                    let batch = try await writer.read { db in
                        try GRDBEventStore.fetch(db, query, after: after, upTo: upTo, limit: limit)
                    }
                    // A short batch means everything up to `upTo` has been seen.
                    delivered = batch.count < limit ? upTo : (batch.last?.position ?? upTo)
                    buffer = batch
                    bufferIndex = 0
                    continue
                }
                // Caught up: wait for the log to change. The observation emits the head
                // position when it starts, and again after every change.
                if heads == nil {
                    heads = ValueObservation.tracking(GRDBEventStore.head)
                        .values(in: writer, bufferingPolicy: .bufferingNewest(1))
                        .makeAsyncIterator()
                }
                guard let head = try await heads?.next() else { return nil }
                knownHead = max(knownHead, head)
            }
        }
    }

    // MARK: SQL

    fileprivate static func head(_ db: Database) throws -> Position {
        try Int.fetchOne(db, sql: "SELECT COALESCE(MAX(position), 0) FROM ledjer_event") ?? 0
    }

    /// The events matching `query` at positions in `(after, upTo]`, in order.
    fileprivate static func fetch(
        _ db: Database,
        _ query: EventQuery,
        after: Position,
        upTo: Position?,
        limit: Int?
    ) throws -> [StoredEvent] {
        guard let (sql, arguments) = eventsSQL(for: query, after: after, upTo: upTo, limit: limit) else { return [] }
        return try Row.fetchAll(db, sql: sql, arguments: arguments).map { row in
            StoredEvent(
                position: row["position"],
                type: EventType(LedjerSchema.string(row["type"] as Data)),
                metadata: try JSONCoding.decode(Metadata.self, from: row["metadata"]),
                payload: row["payload"]
            )
        }
    }

    /// SQL selecting the events of `query` within `(after, upTo]`, in order, or `nil` if the query selects nothing.
    static func eventsSQL(
        for query: EventQuery,
        after: Position,
        upTo: Position?,
        limit: Int?
    ) -> (sql: String, arguments: StatementArguments)? {
        guard var (sql, arguments) = positionsSQL(for: query, after: after, upTo: upTo, limit: limit) else { return nil }
        sql = "SELECT e.position, e.type, e.metadata, e.payload FROM ledjer_event e WHERE e.position IN (\(sql)) ORDER BY e.position"
        if let limit {
            sql += " LIMIT ?"
            arguments += [limit]
        }
        return (sql, arguments)
    }

    /// SQL for the position of the last event of `query` at or before `position`, or `nil` if the query selects nothing.
    static func lastPositionSQL(for query: EventQuery, through position: Position) -> (sql: String, arguments: StatementArguments)? {
        guard let (positions, arguments) = positionsSQL(for: query, after: 0, upTo: position, limit: 1, descending: true) else {
            return nil
        }
        return ("SELECT MAX(position) FROM (\(positions))", arguments)
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
        for query: EventQuery,
        after: Position,
        upTo: Position?,
        limit: Int?,
        descending: Bool = false
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
        let direction = descending ? " DESC" : ""
        let branches = selectors.map { selector -> String in
            let tags = selector.required.sorted { $0.key.description < $1.key.description }
            var sql: String
            if let (path, value) = tags.first {
                sql = "SELECT t0.position AS position FROM ledjer_event_tag t0 WHERE t0.name = ? AND t0.value = ?"
                arguments += [LedjerSchema.blob(path.description), LedjerSchema.blob(value.encoded)]
                sql += bounds("t0.position")
                sql += " AND EXISTS (SELECT 1 FROM ledjer_event e WHERE e.position = t0.position AND e.type = ?)"
                arguments += [LedjerSchema.blob(selector.type.rawValue)]
                for (index, (path, value)) in tags.dropFirst().enumerated() {
                    let t = "t\(index + 1)"
                    sql += " AND EXISTS (SELECT 1 FROM ledjer_event_tag \(t) WHERE \(t).position = t0.position AND \(t).name = ? AND \(t).value = ?)"
                    arguments += [LedjerSchema.blob(path.description), LedjerSchema.blob(value.encoded)]
                }
                sql += " ORDER BY t0.position\(direction)"
            } else {
                sql = "SELECT e.position AS position FROM ledjer_event e WHERE e.type = ?"
                arguments += [LedjerSchema.blob(selector.type.rawValue)]
                sql += bounds("e.position")
                sql += " ORDER BY e.position\(direction)"
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
