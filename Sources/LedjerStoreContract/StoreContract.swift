import Foundation
import Ledjer

// A framework-agnostic contract every `EventStore` must satisfy. Each check
// takes a factory for a fresh, empty store and throws `ContractFailure` when
// the store misbehaves, so any test framework can call it:
//
//     @Test func contract() async throws {
//         try await StoreContract.sourceWithUUIDTags { MyStore() }
//     }

public struct ContractFailure: Error, CustomStringConvertible {
    public let message: String
    public init(message: String) { self.message = message }
    public var description: String { message }
}

public typealias StoreFactory = @Sendable () async throws -> any EventStore

public func check(_ condition: Bool, _ message: @autoclosure () -> String, line: Int = #line) throws {
    if !condition { throw ContractFailure(message: "line \(line): \(message())") }
}

// MARK: - Contract events

public struct UUIDTagged: Event {
    public static let eventType: EventType = "contract/uuid-tagged"
    public static var tags: [EventTag<Self>] { [EventTag("id", \.id)] }
    public var id: UUID
    public var num: Int
    public init(id: UUID, num: Int) { (self.id, self.num) = (id, num) }
}

public struct OrderPlaced: Event {
    public static let eventType: EventType = "contract/order-placed"
    public static var tags: [EventTag<Self>] { [EventTag("orderId", \.orderId), EventTag("customer", \.customer)] }
    public var orderId: UUID
    public var customer: String?
    public init(orderId: UUID, customer: String? = nil) { (self.orderId, self.customer) = (orderId, customer) }
}

/// Carries an `orderId` but doesn't tag it.
public struct OrderNoted: Event {
    public static let eventType: EventType = "contract/order-noted"
    public var orderId: UUID
    public var note: String
    public init(orderId: UUID, note: String) { (self.orderId, self.note) = (orderId, note) }
}

public struct VersionTagged: Event {
    public static let eventType: EventType = "contract/version-tagged"
    public static let version = 2
    public static var tags: [EventTag<Self>] { [EventTag("id", \.id), .metadata("version")] }
    public var id: String
    public init(id: String) { self.id = id }
}

// MARK: - Subscription helper

/// Collects what a subscription delivers.
public final class Collector: @unchecked Sendable {
    private let lock = NSLock()
    private var _events: [StoredEvent] = []
    private var _error: (any Error)?
    private var task: Task<Void, Never>?

    public init(_ stream: AsyncThrowingStream<StoredEvent, any Error>) {
        task = Task { [self] in
            do {
                for try await event in stream {
                    lock.withLock { _events.append(event) }
                }
            } catch {
                lock.withLock { _error = error }
            }
        }
    }

    public var events: [StoredEvent] { lock.withLock { _events } }
    public var error: (any Error)? { lock.withLock { _error } }

    /// Waits until nothing has arrived for `quiet`, at most `timeout`, and returns what arrived.
    public func settled(quiet: Duration = .milliseconds(200), timeout: Duration = .seconds(20)) async throws -> [StoredEvent] {
        let clock = ContinuousClock()
        let deadline = clock.now + timeout
        var count = events.count
        while clock.now < deadline {
            try await Task.sleep(for: quiet)
            if let error { throw error }
            let now = events.count
            if now == count { return events }
            count = now
        }
        throw ContractFailure(message: "subscription did not settle")
    }

    public func cancel() { task?.cancel() }
}

public func orderIds(_ events: [StoredEvent]) throws -> [UUID] {
    try events.map { try $0.decode(OrderPlaced.self).orderId }
}

// MARK: - The contract

public enum StoreContract {
    /// Runs every check, each against a fresh store.
    public static func all(_ makeStore: StoreFactory) async throws {
        try await positionsStartAtOne(makeStore)
        try await sourceWithUUIDTags(makeStore)
        try await sourceSelectsByTagsOfTheEventsType(makeStore)
        try await metadataRoundTrips(makeStore)
        try await appendConditions(makeStore)
        try await subscribeFromNow(makeStore)
        try await subscribeWithStartPosition(makeStore)
        try await subscribeTerminationDoesNotDisturbAppends(makeStore)
    }

    public static func positionsStartAtOne(_ makeStore: StoreFactory) async throws {
        let store = try await makeStore()
        let empty = try await store.append([NewEvent]())
        try check(empty == nil, "appending nothing returns nil, got \(String(describing: empty))")
        let first = try await store.append([UUIDTagged(id: UUID(), num: 1)])
        try check(first == 1, "first position is 1, got \(String(describing: first))")
        let last = try await store.append([UUIDTagged(id: UUID(), num: 2), UUIDTagged(id: UUID(), num: 3)])
        try check(last == 3, "append returns the last position, got \(String(describing: last))")
        let all = try await store.source(Query.of(UUIDTagged.self))
        try check(all.map(\.position) == [1, 2, 3], "positions \(all.map(\.position))")
        let tail = try await store.source(Query.of(UUIDTagged.self), startPosition: 2)
        try check(tail.map(\.position) == [2, 3], "startPosition is inclusive: \(tail.map(\.position))")
        let decoded = try tail.map { try $0.decode(UUIDTagged.self).num }
        try check(decoded == [2, 3], "payloads round-trip: \(decoded)")
    }

    public static func sourceWithUUIDTags(_ makeStore: StoreFactory) async throws {
        let store = try await makeStore()
        let (id1, id2) = (UUID(), UUID())
        try await store.append([UUIDTagged(id: id1, num: 1), UUIDTagged(id: id2, num: 2)])
        func nums(_ query: Query) async throws -> [Int] {
            try await store.source(query).map { try $0.decode(UUIDTagged.self).num }
        }
        let path = TagPath.payload("id")
        let bound = try await nums(Query.of(UUIDTagged.self, where: [path: .bound(id1)]))
        try check(bound == [1], "bound to a UUID: \(bound)")
        let unbound = try await nums(Query.of(UUIDTagged.self))
        try check(unbound == [1, 2], "unbound: \(unbound)")
        let wildcard = try await nums(Query.of(UUIDTagged.self, where: [path: .any]))
        try check(wildcard == [1, 2], "wildcard: \(wildcard)")
        let wrongType = try await nums(Query.of(UUIDTagged.self, where: [path: .bound(id1.uuidString)]))
        try check(wrongType.isEmpty, "values match on type as well as value: \(wrongType)")
    }

    public static func sourceSelectsByTagsOfTheEventsType(_ makeStore: StoreFactory) async throws {
        let store = try await makeStore()
        let (wanted, other) = (UUID(), UUID())
        try await store.append([
            OrderPlaced(orderId: wanted, customer: "ann"),
            OrderPlaced(orderId: other, customer: "bob"),
            OrderPlaced(orderId: other),  // no customer: carries no customer tag
            // OrderNoted doesn't tag orderId, so a binding on it doesn't apply
            OrderNoted(orderId: other, note: "x"),
            VersionTagged(id: "v"),
        ])
        let byOrder = try await store.source(
            Query.of(OrderPlaced.self, OrderNoted.self, where: [.payload("orderId"): .bound(wanted)])
        )
        try check(byOrder.map(\.position) == [1, 4], "bindings apply only to types with the tag: \(byOrder.map(\.position))")

        let byCustomer = try await store.source(
            Query.of(OrderPlaced.self, where: [.payload("customer"): "bob"])
        )
        try check(byCustomer.map(\.position) == [2], "string tag: \(byCustomer.map(\.position))")

        let anyCustomer = try await store.source(
            Query.of(OrderPlaced.self, where: [.payload("customer"): .any])
        )
        try check(anyCustomer.map(\.position) == [1, 2, 3], "wildcard matches events without the tag: \(anyCustomer.map(\.position))")

        let byVersion = try await store.source(Query.of(VersionTagged.self, where: [.metadata("version"): 2]))
        try check(byVersion.map(\.position) == [5], "metadata tag: \(byVersion.map(\.position))")
        let wrongVersion = try await store.source(Query.of(VersionTagged.self, where: [.metadata("version"): 1]))
        try check(wrongVersion.isEmpty, "metadata tag, other value: \(wrongVersion.map(\.position))")

        let union = try await store.source(
            Query([
                try QueryItem([OrderPlaced.self], where: [.payload("customer"): "ann"]),
                try QueryItem([OrderPlaced.self], where: [.payload("customer"): "bob"]),
                try QueryItem([OrderNoted.self]),
            ])
        )
        try check(union.map(\.position) == [1, 2, 4], "items are joined by OR: \(union.map(\.position))")

        let overlapping = try await store.source(
            Query([
                try QueryItem([OrderPlaced.self]),
                try QueryItem([OrderPlaced.self], where: [.payload("customer"): "ann"]),
            ])
        )
        try check(overlapping.map(\.position) == [1, 2, 3], "an event matching several items is returned once: \(overlapping.map(\.position))")

        let invalid = Result { try QueryItem([OrderNoted.self], where: [.payload("orderId"): "x"]) }
        try check({ if case .failure = invalid { true } else { false } }(), "binding for a tag no type has is invalid")
    }

    public static func metadataRoundTrips(_ makeStore: StoreFactory) async throws {
        let store = try await makeStore()
        let correlation = UUID()
        try await store.append(
            [UUIDTagged(id: UUID(), num: 1)],
            metadata: ["correlation": .uuid(correlation), "happy": .bool(true), "n": .int(7), "who": .string("me")]
        )
        let events = try await store.source(Query.of(UUIDTagged.self))
        let expected: Metadata = [
            "correlation": .uuid(correlation), "happy": .bool(true), "n": .int(7), "who": .string("me"),
            "version": .int(1),
        ]
        try check(events.first?.metadata == expected, "metadata: \(String(describing: events.first?.metadata))")
    }

    public static func appendConditions(_ makeStore: StoreFactory) async throws {
        let store = try await makeStore()
        let order = UUID()
        let placed = try Query.of(OrderPlaced.self, where: [.payload("orderId"): .bound(order)])

        // Nothing matches yet, so a condition without `after` passes.
        let first = try await store.append(
            [OrderPlaced(orderId: order)],
            condition: AppendCondition(query: placed)
        )
        try check(first == 1, "condition passes when nothing matches: \(String(describing: first))")

        // Now a matching event exists.
        var failed = false
        do {
            try await store.append([OrderPlaced(orderId: order)], condition: AppendCondition(query: placed))
        } catch LedjerError.appendConditionFailed {
            failed = true
        }
        try check(failed, "condition fails when a matching event exists")
        let afterFailed = try await store.source(Query.of(OrderPlaced.self))
        try check(afterFailed.count == 1, "nothing is appended when the condition fails: \(afterFailed.count)")

        // `after` is exclusive: the event at position 1 is not after 1.
        let second = try await store.append(
            [OrderPlaced(orderId: order)],
            condition: AppendCondition(query: placed, after: 1)
        )
        try check(second == 2, "events at or before `after` don't count: \(String(describing: second))")

        // The event at position 2 is after 1.
        failed = false
        do {
            try await store.append([UUIDTagged(id: UUID(), num: 1)], condition: AppendCondition(query: placed, after: 1))
        } catch LedjerError.appendConditionFailed {
            failed = true
        }
        try check(failed, "events after `after` count")

        // Events of other aggregates or types don't conflict.
        let unrelated = try await store.append(
            [OrderPlaced(orderId: UUID())],
            condition: AppendCondition(query: placed, after: 2)
        )
        try check(unrelated == 3, "unrelated events don't conflict: \(String(describing: unrelated))")

        // The events of the append itself don't conflict with its condition.
        let own = try await store.append(
            [OrderPlaced(orderId: UUID()), OrderPlaced(orderId: UUID())],
            condition: AppendCondition(query: try Query.of(OrderPlaced.self), after: 3)
        )
        try check(own == 5, "an append doesn't conflict with itself: \(String(describing: own))")

        // An empty append still evaluates its condition.
        failed = false
        do {
            try await store.append([NewEvent](), condition: AppendCondition(query: placed))
        } catch LedjerError.appendConditionFailed {
            failed = true
        }
        try check(failed, "empty append evaluates its condition")
    }

    public static func subscribeFromNow(_ makeStore: StoreFactory) async throws {
        let store = try await makeStore()
        let query = try Query.of(OrderPlaced.self)

        // Every subscription gets every event, and only those appended after subscribing.
        let before = UUID()
        try await store.append([OrderPlaced(orderId: before)])
        let c1 = Collector(try await store.subscribe(query))
        let c2 = Collector(try await store.subscribe(query))
        let ids = (0..<3).map { _ in UUID() }
        try await store.append(ids.map { OrderPlaced(orderId: $0) })
        let got1 = try orderIds(try await c1.settled())
        let got2 = try orderIds(try await c2.settled())
        try check(got1 == ids, "all events of one append, in order, only new ones: \(got1)")
        try check(got2 == ids, "every subscription receives events: \(got2)")

        // Selection works like source does.
        let wanted = UUID()
        let tagged = try Query.of(OrderPlaced.self, OrderNoted.self, where: [.payload("orderId"): .bound(wanted)])
        let c3 = Collector(try await store.subscribe(tagged))
        try await store.append([
            OrderPlaced(orderId: wanted), OrderPlaced(orderId: UUID()), OrderNoted(orderId: UUID(), note: "x"),
        ])
        let got3 = try await c3.settled()
        let sourced = try await store.source(tagged, startPosition: got3.first?.position)
        try check(got3.count == 2 && got3 == sourced, "tag bindings select like source does: \(got3.count)")

        // An event matching several items is delivered once.
        let id = UUID()
        let c4 = Collector(
            try await store.subscribe(
                Query([
                    try QueryItem([OrderPlaced.self]),
                    try QueryItem([OrderPlaced.self], where: [.payload("orderId"): .bound(id)]),
                ])
            )
        )
        try await store.append([OrderPlaced(orderId: id)])
        let got4 = try orderIds(try await c4.settled())
        try check(got4 == [id], "delivered once: \(got4)")
        [c1, c2, c3, c4].forEach { $0.cancel() }
    }

    public static func subscribeWithStartPosition(_ makeStore: StoreFactory) async throws {
        let query = try Query.of(OrderPlaced.self)
        func appendOrders(_ store: any EventStore, _ n: Int) async throws -> [UUID] {
            let ids = (0..<n).map { _ in UUID() }
            try await store.append(ids.map { OrderPlaced(orderId: $0) })
            return ids
        }

        // Replays from the start position, inclusive, then continues live.
        do {
            let store = try await makeStore()
            let ids = try await appendOrders(store, 5)
            let c = Collector(try await store.subscribe(query, startPosition: 3))
            let live = UUID()
            try await store.append([OrderPlaced(orderId: live)])
            let got = try orderIds(try await c.settled())
            try check(got == Array(ids[2...]) + [live], "replay then live: \(got.count)")
            c.cancel()
        }

        // A start position of 0 or 1 replays everything.
        do {
            let store = try await makeStore()
            let ids = try await appendOrders(store, 3)
            for start in [0, 1] {
                let c = Collector(try await store.subscribe(query, startPosition: start))
                let got = try orderIds(try await c.settled())
                try check(got == ids, "start \(start) replays everything: \(got.count)")
                c.cancel()
            }
        }

        // A start position in the future delivers events from that position on.
        do {
            let store = try await makeStore()
            _ = try await appendOrders(store, 1)
            let c = Collector(try await store.subscribe(query, startPosition: 3))
            let ids = try await appendOrders(store, 3)  // positions 2, 3, 4
            let got = try orderIds(try await c.settled())
            try check(got == Array(ids[1...]), "future start: \(got.count)")
            c.cancel()
        }

        // Replayed events are selected like live events.
        do {
            let store = try await makeStore()
            let wanted = UUID()
            try await store.append([
                OrderPlaced(orderId: wanted), OrderNoted(orderId: UUID(), note: "x"), OrderPlaced(orderId: UUID()),
            ])
            let tagged = try Query.of(OrderPlaced.self, where: [.payload("orderId"): .bound(wanted)])
            let c = Collector(try await store.subscribe(tagged, startPosition: 0))
            let got = try orderIds(try await c.settled())
            try check(got == [wanted], "replay selects by tags: \(got.count)")
            c.cancel()
        }

        // Replays more events than any internal batch holds.
        do {
            let store = try await makeStore()
            let ids = try await appendOrders(store, 3000)
            let c = Collector(try await store.subscribe(query, startPosition: 0))
            let got = try orderIds(try await c.settled())
            try check(got == ids, "large replay: \(got.count)")
            c.cancel()
        }

        // Events appended while replaying are delivered once, in order.
        do {
            let store = try await makeStore()
            let ids = try await appendOrders(store, 200)
            let c = Collector(try await store.subscribe(query, startPosition: 0))
            var live: [UUID] = []
            for _ in 0..<20 { live += try await appendOrders(store, 1) }
            let got = try orderIds(try await c.settled())
            try check(got == ids + live, "no gaps or duplicates: \(got.count) of \(ids.count + live.count)")
            c.cancel()
        }
    }

    public static func subscribeTerminationDoesNotDisturbAppends(_ makeStore: StoreFactory) async throws {
        let store = try await makeStore()
        let c = Collector(try await store.subscribe(Query.of(OrderPlaced.self)))
        c.cancel()
        try await Task.sleep(for: .milliseconds(100))
        let position = try await store.append([OrderPlaced(orderId: UUID())])
        try check(position == 1, "append after a cancelled subscription: \(String(describing: position))")
    }
}
