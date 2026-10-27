import Foundation

// MARK: - Selecting and routing events (shared by state views and persisted views)

/// Selects the events a view binding applies to: an event type, optionally
/// narrowed by tag bindings. Type-erased, so bindings for different event types can live together.
struct EventSelector: Sendable {
    let eventType: EventTypeInfo
    let swiftType: any Event.Type
    let tagBindings: TagBindings
    /// The values of the event's tags, computed from the decoded event.
    let tagValues: @Sendable (StoredEvent) throws -> [TagPath: Scalar]

    init<E: Event>(_ type: E.Type, where tagBindings: TagBindings) {
        self.eventType = EventTypeInfo(type)
        self.swiftType = type
        self.tagBindings = tagBindings
        self.tagValues = { event in
            E.tagValues(of: try event.decode(E.self), metadata: event.metadata)
        }
    }

    /// Throws if a tag binding names a tag the event type doesn't have.
    func validate() throws {
        let unknown = tagBindings.keys.filter { !eventType.tags.contains($0) }
        guard unknown.isEmpty else {
            throw LedjerError.invalidQuery(
                "tag binding for a tag none of the event types have: "
                    + unknown.map(\.description).sorted().joined(separator: ", ")
            )
        }
    }

    /// Whether this binding applies to `event`: the type matches and every bound tag has the
    /// bound value. As in Clojure's `has-bindings?`, a bound tag that the event doesn't have
    /// is not held against it: only a different value does.
    func matches(_ event: StoredEvent) throws -> Bool {
        guard event.type == eventType.type else { return false }
        let required = tagBindings.compactMap { path, binding -> (TagPath, Scalar)? in
            if case .value(let v) = binding { (path, v) } else { nil }
        }
        if required.isEmpty { return true }
        let values = try tagValues(event)
        return required.allSatisfy { path, value in values[path].map { $0 == value } ?? true }
    }
}

/// Throws if the bindings of a view can't be right whatever events arrive: two Swift types that share an
/// event type (`LedjerError.duplicateEventType`), or two bindings of one type with the same tag bindings
/// (`LedjerError.duplicateBinding`, which would otherwise surface as "exactly one binding" with the first
/// event of that type). Views are plain values, built without `try`, so this runs when a view is run or
/// started, before anything is read.
///
/// Bindings of one type with different tag bindings are fine, and may overlap: whether they do depends on
/// the events, which is checked per event, by `route`.
func checkBindings(_ selectors: [EventSelector]) throws {
    try checkEventTypesAreUnique(selectors.map { (type: $0.eventType.type, swift: $0.swiftType) })
    for (index, selector) in selectors.enumerated() {
        let duplicated = selectors[..<index].contains {
            $0.eventType.type == selector.eventType.type && $0.tagBindings == selector.tagBindings
        }
        if duplicated { throw LedjerError.duplicateBinding(eventType: selector.eventType.type) }
    }
}

/// The handler of the one binding that applies to `event`. Throws unless exactly one does.
func route<Handler>(_ event: StoredEvent, among bindings: [(selector: EventSelector, handler: Handler)]) throws -> Handler {
    var matching: [Handler] = []
    for binding in bindings {
        if try binding.selector.matches(event) { matching.append(binding.handler) }
    }
    guard matching.count == 1 else {
        throw LedjerError.expectedExactlyOneBinding(eventType: event.type, matching: matching.count)
    }
    return matching[0]
}

/// The query selecting the events `selectors` apply to, with `runtime` tag
/// bindings narrowing each selector for the tags its event type has.
///
/// A `.any` on either side yields to the other. Otherwise a runtime binding
/// that differs from the selector's own is a conflict.
func query(for selectors: [EventSelector], narrowedBy runtime: TagBindings = [:]) throws -> EventQuery {
    try checkBindings(selectors)
    let known = selectors.reduce(into: Set<TagPath>()) { $0.formUnion($1.eventType.tags) }
    let unknown = runtime.keys.filter { !known.contains($0) }
    guard unknown.isEmpty else {
        throw LedjerError.invalidQuery(
            "tag binding for a tag none of the event types have: "
                + unknown.map(\.description).sorted().joined(separator: ", ")
        )
    }
    var items: [EventQueryItem] = []
    for selector in selectors {
        try selector.validate()
        let applicable = runtime.filter { selector.eventType.tags.contains($0.key) }
        let conflicts = selector.tagBindings.compactMap { path, own -> TagPath? in
            guard let theirs = applicable[path] else { return nil }
            return (own == theirs || own == .any || theirs == .any) ? nil : path
        }
        guard conflicts.isEmpty else {
            throw LedjerError.tagBindingConflict(eventType: selector.eventType.type, tags: conflicts)
        }
        var merged = applicable
        for (path, own) in selector.tagBindings where !(own == .any && applicable[path] != nil) {
            merged[path] = own
        }
        items.append(EventQueryItem(unchecked: [selector.eventType], tagBindings: merged))
    }
    return EventQuery(validated: items)
}

// MARK: - State views

/// One reducer of a state view: which events it applies to and how.
public struct StateViewBinding<Acc: Sendable>: Sendable {
    let selector: EventSelector
    let reduce: @Sendable (inout Acc, StoredEvent) throws -> Void

    /// Applies `reduce` to each `E` event, optionally only those with the given tag values.
    public static func on<E: Event>(
        _ type: E.Type,
        where tagBindings: TagBindings = [:],
        reduce: @escaping @Sendable (inout Acc, E) throws -> Void
    ) -> StateViewBinding {
        StateViewBinding(selector: EventSelector(type, where: tagBindings)) { acc, stored in
            try reduce(&acc, try stored.decode(E.self))
        }
    }

    /// Like `on(_:where:reduce:)`, with the stored event (position, metadata) too.
    public static func on<E: Event>(
        _ type: E.Type,
        where tagBindings: TagBindings = [:],
        reduce: @escaping @Sendable (inout Acc, E, StoredEvent) throws -> Void
    ) -> StateViewBinding {
        StateViewBinding(selector: EventSelector(type, where: tagBindings)) { acc, stored in
            try reduce(&acc, try stored.decode(E.self), stored)
        }
    }
}

/// A state view: a function of an event store that reduces the events it
/// is bound to into a value.
///
/// ```swift
/// let orderCount = StateView(initial: 0, bindings: [
///     .on(OrderPlaced.self) { count, _ in count += 1 },
///     .on(OrderCancelled.self) { count, _ in count -= 1 },
/// ])
/// let n = try await orderCount.run(on: store)
/// ```
///
/// Every event the view reads must have exactly one binding that applies, or
/// running the view throws. Running it also throws, before reading anything, if its bindings
/// can't be right whatever the events: two Swift types that declare the same event type
/// (`LedjerError.duplicateEventType`), or two bindings of a type with the same tag bindings
/// (`LedjerError.duplicateBinding`).
public struct StateView<Acc: Sendable>: Sendable {
    public let initial: Acc
    let bindings: [StateViewBinding<Acc>]

    public init(initial: Acc, bindings: [StateViewBinding<Acc>]) {
        self.initial = initial
        self.bindings = bindings
    }

    /// Reduces the events of `store`, in position order.
    ///
    /// `tagBindings` narrow each binding's query, for the tags its event type
    /// has. They must name a tag of at least one binding's event type, and
    /// must not conflict with the tags a binding fixes itself.
    public func run(on store: some EventStore, where tagBindings: TagBindings = [:]) async throws -> Acc {
        let selectors = bindings.map(\.selector)
        let events = try await store.source(query(for: selectors, narrowedBy: tagBindings), startPosition: nil)
        let routes = bindings.map { (selector: $0.selector, handler: $0.reduce) }
        var acc = initial
        for event in events {
            try route(event, among: routes)(&acc, event)
        }
        return acc
    }
}
