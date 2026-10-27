import Foundation

/// A position in an event store's global event order. The first event is at position 1.
public typealias Position = Int

public enum LedjerError: Error, CustomStringConvertible {
    /// A query is malformed.
    case invalidQuery(String)
    /// The condition of an append found matching events, so nothing was appended.
    case appendConditionFailed
    /// Different Swift types declare the same event type, in one query or one view.
    case duplicateEventType(EventType, swiftTypes: [String])
    /// A view has two bindings for the same event type with the same tag bindings: every event they
    /// select would match both.
    case duplicateBinding(eventType: EventType)
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
        case .duplicateEventType(let type, let swiftTypes):
            "Event type \(type) is declared by more than one Swift type: \(swiftTypes.joined(separator: ", ")). "
                + "Event types must be unique"
        case .duplicateBinding(let type):
            "Two bindings for \(type) with the same tag bindings: every event they select would match both"
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
    public var query: EventQuery
    /// Exclusive.
    public var after: Position?

    public init(query: EventQuery, after: Position? = nil) {
        self.query = query
        self.after = after
    }
}

/// An append-only log of events, queried by event type and tags.
public protocol EventStore: Sendable {
    /// The events matching `query` from `startPosition` (inclusive) on, in
    /// position order.
    func source(_ query: EventQuery, startPosition: Position?) async throws -> [StoredEvent]

    /// A stream of the events matching `query`.
    ///
    /// Without `startPosition`, only events appended after this call returns
    /// are delivered. With it, the matching events from that position
    /// (inclusive) on are delivered first, then new ones as they arrive.
    /// Events are delivered in order, each once. Ending the stream's
    /// iteration (or cancelling its task) ends the subscription, cleanly: the stream
    /// finishes without an error. A stream has a single consumer: iterate it from one task.
    func subscribe(_ query: EventQuery, startPosition: Position?) async throws -> AsyncThrowingStream<StoredEvent, any Error>

    /// Atomically appends `events`, subject to `condition`. Returns the
    /// position of the last appended event, or `nil` if `events` is empty.
    @discardableResult
    func append(_ events: [NewEvent], condition: AppendCondition?) async throws -> Position?

    /// The position of the last event matching `query` at or before `position`, if any.
    ///
    /// Stores should implement this without reading the events. The default
    /// sources them all.
    func lastPosition(matching query: EventQuery, through position: Position) async throws -> Position?
}

extension EventStore {
    public func lastPosition(matching query: EventQuery, through position: Position) async throws -> Position? {
        try await source(query, startPosition: nil).last { $0.position <= position }?.position
    }

    public func source(_ query: EventQuery) async throws -> [StoredEvent] {
        try await source(query, startPosition: nil)
    }

    public func subscribe(_ query: EventQuery) async throws -> AsyncThrowingStream<StoredEvent, any Error> {
        try await subscribe(query, startPosition: nil)
    }

    @discardableResult
    public func append(_ events: [NewEvent]) async throws -> Position? {
        try await append(events, condition: nil)
    }

    /// Appends typed events, all with the same extra `metadata`.
    @discardableResult
    @_disfavoredOverload  // so that `append([])` means the `[NewEvent]` overload
    public func append(
        _ events: [any Event],
        metadata: Metadata = [:],
        condition: AppendCondition? = nil
    ) async throws -> Position? {
        try await append(events.map { try NewEvent($0, metadata: metadata) }, condition: condition)
    }
}
