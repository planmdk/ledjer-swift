import Foundation

/// A position in an event store's global event order. The first event is at position 1.
public typealias Position = Int

public enum LedjerError: Error, CustomStringConvertible {
    /// A query is malformed.
    case invalidQuery(String)
    /// The condition of an append found matching events, so nothing was appended.
    case appendConditionFailed
    /// A view got an event for which not exactly one binding applies.
    case expectedExactlyOneBinding(eventType: EventType, matching: Int)
    /// A runtime tag binding differs from the one a view binding fixes.
    case tagBindingConflict(eventType: EventType, tags: [TagPath])
    /// `StoredEvent.decode` was asked for a different type than the event's.
    case eventTypeMismatch(expected: EventType, actual: EventType)

    public var description: String {
        switch self {
        case .invalidQuery(let why): "Invalid ledjer query: \(why)"
        case .appendConditionFailed: "Append condition failed: matching events exist"
        case .expectedExactlyOneBinding(let type, let n):
            "Expected exactly one matching binding per event, but \(n) match \(type)"
        case .tagBindingConflict(let type, let tags):
            "Tag bindings conflict with binding for \(type): \(tags.map(\.description).sorted().joined(separator: ", "))"
        case .eventTypeMismatch(let expected, let actual):
            "Expected event type \(expected), got \(actual)"
        }
    }
}

/// Makes an append conditional on there being no matching events.
///
/// The append fails with `LedjerError.appendConditionFailed` if the store
/// holds any event matching `query` at a position after `after`, evaluated
/// atomically with the append. With `after == nil`, any matching event fails
/// it.
///
/// Typically `after` is the position of the last event a decision was based on.
public struct AppendCondition: Sendable {
    public var query: Query
    /// Exclusive.
    public var after: Position?

    public init(query: Query, after: Position? = nil) {
        self.query = query
        self.after = after
    }
}

/// An append-only log of events, queried by event type and tags.
public protocol EventStore: Sendable {
    /// The events matching `query` from `startPosition` (inclusive) on, in
    /// position order.
    func source(_ query: Query, startPosition: Position?) async throws -> [StoredEvent]

    /// A stream of the events matching `query`.
    ///
    /// Without `startPosition`, only events appended after this call returns
    /// are delivered. With it, the matching events from that position
    /// (inclusive) on are delivered first, then new ones as they arrive.
    /// Events are delivered in order, each once. Ending the stream's
    /// iteration (or cancelling its task) ends the subscription.
    func subscribe(_ query: Query, startPosition: Position?) async throws -> AsyncThrowingStream<StoredEvent, any Error>

    /// Atomically appends `events`, subject to `condition`. Returns the
    /// position of the last appended event, or `nil` if `events` is empty.
    @discardableResult
    func append(_ events: [NewEvent], condition: AppendCondition?) async throws -> Position?
}

extension EventStore {
    public func source(_ query: Query) async throws -> [StoredEvent] {
        try await source(query, startPosition: nil)
    }

    public func subscribe(_ query: Query) async throws -> AsyncThrowingStream<StoredEvent, any Error> {
        try await subscribe(query, startPosition: nil)
    }

    @discardableResult
    public func append(_ events: [NewEvent]) async throws -> Position? {
        try await append(events, condition: nil)
    }

    /// Appends typed events, all with the same extra `metadata`.
    @discardableResult
    public func append(
        _ events: [any Event],
        metadata: Metadata = [:],
        condition: AppendCondition? = nil
    ) async throws -> Position? {
        try await append(events.map { try NewEvent($0, metadata: metadata) }, condition: condition)
    }
}
