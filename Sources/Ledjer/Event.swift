import Foundation

/// The unique name of an event type, e.g. `"orders/order-placed"`.
///
/// Names compare by their UTF-8 bytes, not by Unicode canonical equivalence (see `Scalar`).
public struct EventType: Hashable, Sendable, ExpressibleByStringLiteral, CustomStringConvertible, Codable {
    public let rawValue: String

    public init(_ rawValue: String) { self.rawValue = rawValue }
    public init(stringLiteral value: String) { self.rawValue = value }
    public var description: String { rawValue }

    public static func == (lhs: EventType, rhs: EventType) -> Bool { lhs.rawValue.isByteEqual(to: rhs.rawValue) }
    public func hash(into hasher: inout Hasher) { rawValue.hashBytes(into: &hasher) }
}

/// Names a value that events can be selected by: a key in an event's
/// payload or metadata.
///
/// Tag paths are labels, not JSON paths. `TagPath.payload("id")` is whatever
/// the event type's `EventTag` declares for `"id"`. Keys compare by their UTF-8 bytes (see `Scalar`).
public struct TagPath: Hashable, Sendable, CustomStringConvertible {
    public enum Source: String, Hashable, Sendable {
        case payload
        case metadata
    }

    public let source: Source
    public let key: String

    public init(source: Source, key: String) {
        self.source = source
        self.key = key
    }

    public static func payload(_ key: String) -> TagPath { .init(source: .payload, key: key) }
    public static func metadata(_ key: String) -> TagPath { .init(source: .metadata, key: key) }

    public var description: String { "\(source.rawValue).\(key)" }

    public static func == (lhs: TagPath, rhs: TagPath) -> Bool {
        lhs.source == rhs.source && lhs.key.isByteEqual(to: rhs.key)
    }

    public func hash(into hasher: inout Hasher) {
        hasher.combine(source)
        key.hashBytes(into: &hasher)
    }
}

/// Declares one tag of an event type: how to read the tag value from an
/// event (and its metadata).
public struct EventTag<E: Event>: Sendable {
    public let path: TagPath
    let extract: @Sendable (E, Metadata) -> Scalar?

    /// A payload tag, read from a property of the event.
    public init<V: ScalarConvertible>(_ key: String, _ keyPath: KeyPath<E, V> & Sendable) {
        self.path = .payload(key)
        self.extract = { event, _ in event[keyPath: keyPath].scalar }
    }

    /// A payload tag, read from an optional property. Events where the
    /// property is `nil` don't carry the tag.
    public init<V: ScalarConvertible>(_ key: String, _ keyPath: KeyPath<E, V?> & Sendable) {
        self.path = .payload(key)
        self.extract = { event, _ in event[keyPath: keyPath]?.scalar }
    }

    /// A metadata tag, e.g. `EventTag.metadata("version")`.
    public static func metadata(_ key: String) -> EventTag<E> {
        EventTag(path: .metadata(key), extract: { _, metadata in metadata[key] })
    }

    private init(path: TagPath, extract: @escaping @Sendable (E, Metadata) -> Scalar?) {
        self.path = path
        self.extract = extract
    }
}

/// An event type. Conforming types are the event payloads.
///
/// ```swift
/// struct OrderPlaced: Event {
///     static let eventType: EventType = "orders/order-placed"
///     static var tags: [EventTag<Self>] { [EventTag("orderId", \.orderId)] }
///     var orderId: UUID
///     var total: Int
/// }
/// ```
///
/// Event types are never versioned in place. To change an event, add a new
/// event type and teach the views about it. `version` is recorded in the
/// event's metadata for the cases where you do want to tell variants apart.
public protocol Event: Codable, Sendable {
    /// Must be unique across event types.
    static var eventType: EventType { get }
    /// Recorded as `version` in metadata. Defaults to 1.
    static var version: Int { get }
    /// The values events of this type can be selected by. Defaults to none.
    static var tags: [EventTag<Self>] { get }
}

extension Event {
    public static var version: Int { 1 }
    public static var tags: [EventTag<Self>] { [] }

    /// The tag paths of this type, usable through an existential metatype.
    static var tagPaths: Set<TagPath> { Set(tags.map(\.path)) }

    /// The values of this type's tags for `event`. Tags without a value are left out.
    static func tagValues(of event: Self, metadata: Metadata) -> [TagPath: Scalar] {
        var values: [TagPath: Scalar] = [:]
        for tag in tags {
            if let value = tag.extract(event, metadata) {
                values[tag.path] = value
            }
        }
        return values
    }
}

/// An event ready to be appended to an event store.
///
/// Type-erased: the payload is the event's JSON, and the tag values are
/// computed when the event is created, so stores need no knowledge of Swift types.
public struct NewEvent: Hashable, Sendable {
    public let type: EventType
    public let metadata: Metadata
    /// The event's JSON, with sorted keys.
    public let payload: Data
    public let tags: [TagPath: Scalar]

    public init(type: EventType, metadata: Metadata, payload: Data, tags: [TagPath: Scalar]) {
        self.type = type
        self.metadata = metadata
        self.payload = payload
        self.tags = tags
    }

    /// Creates a new event from `event`. Extra `metadata` (e.g. a correlation
    /// id) is stored alongside the automatic `version`.
    public init<E: Event>(_ event: E, metadata: Metadata = [:]) throws {
        var metadata = metadata
        metadata["version"] = .int(E.version)
        self.init(
            type: E.eventType,
            metadata: metadata,
            payload: try JSONCoding.encode(event),
            tags: E.tagValues(of: event, metadata: metadata)
        )
    }
}

/// An event as persisted in an event store.
public struct StoredEvent: Hashable, Sendable {
    /// The position in the store's global order. Starts at 1.
    public let position: Position
    public let type: EventType
    public let metadata: Metadata
    public let payload: Data

    public init(position: Position, type: EventType, metadata: Metadata, payload: Data) {
        self.position = position
        self.type = type
        self.metadata = metadata
        self.payload = payload
    }

    public func isType<E: Event>(_ type: E.Type) -> Bool { self.type == E.eventType }

    /// Decodes the payload as `E`. Throws if this isn't an `E` event.
    public func decode<E: Event>(_ type: E.Type = E.self) throws -> E {
        guard isType(E.self) else {
            throw LedjerError.eventTypeMismatch(expected: E.eventType, actual: self.type)
        }
        return try JSONCoding.decode(E.self, from: payload)
    }
}

/// JSON coding used for payloads and metadata. Shared with the GRDB store, not for clients: a store
/// that keeps metadata can encode `Metadata` (its `Scalar`s are `Codable`) any way it likes.
package enum JSONCoding {
    package static func encode<T: Encodable>(_ value: T) throws -> Data {
        let encoder = JSONEncoder()
        // Deterministic bytes, so equal events have equal payloads.
        encoder.outputFormatting = [.sortedKeys]
        return try encoder.encode(value)
    }

    package static func decode<T: Decodable>(_ type: T.Type, from data: Data) throws -> T {
        try JSONDecoder().decode(type, from: data)
    }
}
