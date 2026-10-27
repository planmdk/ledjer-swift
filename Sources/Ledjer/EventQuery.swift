import Foundation

/// What a query requires of a tag.
///
/// Literals are exact values: `[.payload("id"): "abc"]`. Use `.any` for a
/// wildcard (there is no magic `"*"` string).
public enum TagBinding: Hashable, Sendable {
    /// Matches any value, including events without the tag.
    case any
    /// Matches events whose tag has exactly this value, by type and value.
    case value(Scalar)

    public static func bound(_ value: some ScalarConvertible) -> TagBinding { .value(value.scalar) }
}

extension TagBinding: ExpressibleByStringLiteral, ExpressibleByIntegerLiteral, ExpressibleByBooleanLiteral {
    public init(stringLiteral value: String) { self = .value(.string(value)) }
    public init(integerLiteral value: Int) { self = .value(.int(value)) }
    public init(booleanLiteral value: Bool) { self = .value(.bool(value)) }
}

public typealias TagBindings = [TagPath: TagBinding]

/// An event type as queries see it: its name and the tags it declares.
public struct EventTypeInfo: Hashable, Sendable {
    public let type: EventType
    public let tags: Set<TagPath>

    init(_ eventType: any Event.Type) {
        self.type = eventType.eventType
        self.tags = eventType.tagPaths
    }
}

/// Throws `LedjerError.duplicateEventType` if two different Swift types share an event type.
///
/// Events are matched, and stored, by their event type's name, so two Swift types with the same name would
/// select each other's events and decode each other's payloads. The Clojure library has the same
/// requirement ("event type already registered": it only notes the clash, and the last definition wins).
/// There is no registry here, since Swift types are named where they are used, so this is checked where
/// types come together: in a query, and in a view's bindings.
func checkEventTypesAreUnique(_ declared: [(type: EventType, swift: any Event.Type)]) throws {
    var first: [EventType: any Event.Type] = [:]
    for (type, swift) in declared {
        guard let other = first[type] else {
            first[type] = swift
            continue
        }
        if ObjectIdentifier(other) != ObjectIdentifier(swift) {
            throw LedjerError.duplicateEventType(
                type, swiftTypes: [String(reflecting: other), String(reflecting: swift)].sorted())
        }
    }
}

/// Event types plus tag bindings, all of which must match.
///
/// An event matches when its type is one of the event types and, for each
/// bound tag that the event's type declares, its value equals the bound
/// value. Bindings for tags the event's type doesn't declare don't apply to
/// that type.
public struct EventQueryItem: Hashable, Sendable {
    public let eventTypes: Set<EventTypeInfo>
    public let tagBindings: TagBindings

    /// Throws `LedjerError.invalidQuery` if a binding names a tag that none
    /// of the event types declare: such a binding applies to nothing, so is a mistake.
    /// Throws `LedjerError.duplicateEventType` if two of the Swift types declare the same event type.
    public init(_ eventTypes: [any Event.Type], where tagBindings: TagBindings = [:]) throws {
        try checkEventTypesAreUnique(eventTypes.map { (type: $0.eventType, swift: $0) })
        let infos = Set(eventTypes.map { EventTypeInfo($0) })
        let known = infos.reduce(into: Set<TagPath>()) { $0.formUnion($1.tags) }
        let unknown = tagBindings.keys.filter { !known.contains($0) }
        guard unknown.isEmpty else {
            throw LedjerError.invalidQuery(
                "tag binding for a tag none of the event types have: "
                    + unknown.map(\.description).sorted().joined(separator: ", ")
            )
        }
        self.eventTypes = infos
        self.tagBindings = tagBindings
    }

    init(unchecked eventTypes: Set<EventTypeInfo>, tagBindings: TagBindings) {
        self.eventTypes = eventTypes
        self.tagBindings = tagBindings
    }
}

/// A set of query items, joined by logical OR.
///
/// Queries are validated when constructed, so stores never see an invalid one.
public struct EventQuery: Hashable, Sendable {
    public let items: Set<EventQueryItem>

    /// The query flattened to one selector per event type, with the bindings
    /// that apply to it. This is what stores execute.
    public let selectors: Set<Selector>

    public init(_ items: [EventQueryItem]) {
        let items = Set(items)
        self.items = items
        self.selectors = Self.selectors(of: items)
    }

    public init(_ item: EventQueryItem) { self.init([item]) }

    public static func == (lhs: EventQuery, rhs: EventQuery) -> Bool { lhs.items == rhs.items }
    public func hash(into hasher: inout Hasher) { hasher.combine(items) }

    /// A query of one item.
    public static func of(_ eventTypes: any Event.Type..., where tagBindings: TagBindings = [:]) throws -> EventQuery {
        EventQuery(try EventQueryItem(eventTypes, where: tagBindings))
    }

    /// One event type with the exact tag values it must have.
    public struct Selector: Hashable, Sendable {
        public let type: EventType
        public let required: [TagPath: Scalar]
    }

    private static func selectors(of items: Set<EventQueryItem>) -> Set<Selector> {
        var result = Set<Selector>()
        for item in items {
            for info in item.eventTypes {
                var required: [TagPath: Scalar] = [:]
                for (path, binding) in item.tagBindings where info.tags.contains(path) {
                    if case .value(let v) = binding { required[path] = v }
                }
                result.insert(Selector(type: info.type, required: required))
            }
        }
        return result
    }

    /// Whether an event of `type` with the tag values `tags` is selected.
    public func matches(type: EventType, tags: [TagPath: Scalar]) -> Bool {
        selectors.contains { selector in
            selector.type == type && selector.required.allSatisfy { tags[$0.key] == $0.value }
        }
    }
}
