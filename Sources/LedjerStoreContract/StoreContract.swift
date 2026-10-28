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

// MARK: - The contract

public enum StoreContract {
    /// Runs every check, each against a fresh store.
    public static func all(_ makeStore: StoreFactory) async throws {
        try await positionsStartAtOne(makeStore)
        try await sourceWithUUIDTags(makeStore)
        try await sourceSelectsByTagsOfTheEventsType(makeStore)
        try await metadataRoundTrips(makeStore)
        try await intAndBoolTags(makeStore)
        try await nulCharactersAreSignificant(makeStore)
        try await unicodeNormalizationIsSignificant(makeStore)
        try await appendConditions(makeStore)
        try await failedConditionalAppendsAreNotDelivered(makeStore)
        try await concurrentConditionalAppendsAreAtomic(makeStore)
        try await lastPositions(makeStore)
        try await subscribeFromNow(makeStore)
        try await subscribeWithStartPosition(makeStore)
        try await startPositionsBelowOneReadEverything(makeStore)
        try await subscribeTerminationDoesNotDisturbAppends(makeStore)
        try await cancellingAConsumerEndsTheStreamWithoutAnError(makeStore)
    }

    public static func positionsStartAtOne(_ makeStore: StoreFactory) async throws {
        let store = try await makeStore()
        let empty = try await store.append([NewEvent]())
        try check(empty == nil, "appending nothing returns nil, got \(String(describing: empty))")
        let first = try await store.append([UUIDTagged(id: UUID(), num: 1)])
        try check(first == 1, "first position is 1, got \(String(describing: first))")
        let last = try await store.append([UUIDTagged(id: UUID(), num: 2), UUIDTagged(id: UUID(), num: 3)])
        try check(last == 3, "append returns the last position, got \(String(describing: last))")
        let all = try await store.source(EventQuery.of(UUIDTagged.self))
        try check(all.map(\.position) == [1, 2, 3], "positions \(all.map(\.position))")
        let tail = try await store.source(EventQuery.of(UUIDTagged.self), startPosition: 2)
        try check(tail.map(\.position) == [2, 3], "startPosition is inclusive: \(tail.map(\.position))")
        let decoded = try tail.map { try $0.decode(UUIDTagged.self).num }
        try check(decoded == [2, 3], "payloads round-trip: \(decoded)")
    }

    public static func sourceWithUUIDTags(_ makeStore: StoreFactory) async throws {
        let store = try await makeStore()
        let (id1, id2) = (UUID(), UUID())
        try await store.append([UUIDTagged(id: id1, num: 1), UUIDTagged(id: id2, num: 2)])
        func nums(_ query: EventQuery) async throws -> [Int] {
            try await store.source(query).map { try $0.decode(UUIDTagged.self).num }
        }
        let path = TagPath.payload("id")
        let bound = try await nums(EventQuery.of(UUIDTagged.self, where: [path: .bound(id1)]))
        try check(bound == [1], "bound to a UUID: \(bound)")
        let unbound = try await nums(EventQuery.of(UUIDTagged.self))
        try check(unbound == [1, 2], "unbound: \(unbound)")
        let wildcard = try await nums(EventQuery.of(UUIDTagged.self, where: [path: .any]))
        try check(wildcard == [1, 2], "wildcard: \(wildcard)")
        let wrongType = try await nums(EventQuery.of(UUIDTagged.self, where: [path: .bound(id1.uuidString)]))
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
            EventQuery.of(OrderPlaced.self, OrderNoted.self, where: [.payload("orderId"): .bound(wanted)])
        )
        try check(byOrder.map(\.position) == [1, 4], "bindings apply only to types with the tag: \(byOrder.map(\.position))")

        let byCustomer = try await store.source(
            EventQuery.of(OrderPlaced.self, where: [.payload("customer"): "bob"])
        )
        try check(byCustomer.map(\.position) == [2], "string tag: \(byCustomer.map(\.position))")

        let anyCustomer = try await store.source(
            EventQuery.of(OrderPlaced.self, where: [.payload("customer"): .any])
        )
        try check(anyCustomer.map(\.position) == [1, 2, 3], "wildcard matches events without the tag: \(anyCustomer.map(\.position))")

        let byVersion = try await store.source(EventQuery.of(VersionTagged.self, where: [.metadata("version"): 2]))
        try check(byVersion.map(\.position) == [5], "metadata tag: \(byVersion.map(\.position))")
        let wrongVersion = try await store.source(EventQuery.of(VersionTagged.self, where: [.metadata("version"): 1]))
        try check(wrongVersion.isEmpty, "metadata tag, other value: \(wrongVersion.map(\.position))")

        let union = try await store.source(
            try EventQuery([
                try EventQueryItem([OrderPlaced.self], where: [.payload("customer"): "ann"]),
                try EventQueryItem([OrderPlaced.self], where: [.payload("customer"): "bob"]),
                try EventQueryItem([OrderNoted.self]),
            ])
        )
        try check(union.map(\.position) == [1, 2, 4], "items are joined by OR: \(union.map(\.position))")

        let overlapping = try await store.source(
            try EventQuery([
                try EventQueryItem([OrderPlaced.self]),
                try EventQueryItem([OrderPlaced.self], where: [.payload("customer"): "ann"]),
            ])
        )
        try check(overlapping.map(\.position) == [1, 2, 3], "an event matching several items is returned once: \(overlapping.map(\.position))")

        let invalid = Result { try EventQueryItem([OrderNoted.self], where: [.payload("orderId"): "x"]) }
        try check({ if case .failure = invalid { true } else { false } }(), "binding for a tag no type has is invalid")
    }

    /// `Int` and `Bool` tag values match by type and value, in queries, `lastPosition` and conditions.
    public static func intAndBoolTags(_ makeStore: StoreFactory) async throws {
        let store = try await makeStore()
        try await store.append([
            Flagged(n: 1, active: true),  // 1
            Flagged(n: 1, active: false),  // 2
            Flagged(n: 2, active: true),  // 3
            Flagged(n: 1, active: true),  // 4
            Flagged(n: -1, active: false),  // 5
            Flagged(n: Int.max, active: true),  // 6
        ])
        func positions(_ bindings: TagBindings) async throws -> [Position] {
            try await store.source(EventQuery.of(Flagged.self, where: bindings)).map(\.position)
        }
        let (n, active) = (TagPath.payload("n"), TagPath.payload("active"))
        let one = try await positions([n: .bound(1)])
        try check(one == [1, 2, 4], "Int tag: \(one)")
        let negative = try await positions([n: .bound(-1)])
        try check(negative == [5], "negative Int tag: \(negative)")
        let largest = try await positions([n: .bound(Int.max)])
        try check(largest == [6], "largest Int tag: \(largest)")
        let isActive = try await positions([active: .bound(true)])
        try check(isActive == [1, 3, 4, 6], "Bool tag, true: \(isActive)")
        let inactive = try await positions([active: .bound(false)])
        try check(inactive == [2, 5], "Bool tag, false: \(inactive)")
        let both = try await positions([n: .bound(1), active: .bound(true)])
        try check(both == [1, 4], "Int and Bool tags together: \(both)")
        let anyN = try await positions([n: .any, active: .bound(false)])
        try check(anyN == [2, 5], "wildcard and Bool tag: \(anyN)")

        // Values match on type: neither "1" nor true is the Int 1, and neither "true" nor 1 is the Bool true.
        let stringOne = try await positions([n: .bound("1")])
        try check(stringOne.isEmpty, "the string \"1\" doesn't match the Int 1: \(stringOne)")
        let boolForInt = try await positions([n: .bound(true)])
        try check(boolForInt.isEmpty, "true doesn't match the Int 1: \(boolForInt)")
        let intForBool = try await positions([active: .bound(1)])
        try check(intForBool.isEmpty, "the Int 1 doesn't match the Bool true: \(intForBool)")
        let stringForBool = try await positions([active: .bound("true")])
        try check(stringForBool.isEmpty, "the string \"true\" doesn't match the Bool true: \(stringForBool)")

        let lastOne = try await store.lastPosition(
            matching: EventQuery.of(Flagged.self, where: [n: .bound(1)]), through: 3)
        try check(lastOne == 2, "lastPosition by an Int tag: \(String(describing: lastOne))")
        let lastInactive = try await store.lastPosition(
            matching: EventQuery.of(Flagged.self, where: [active: .bound(false)]), through: 6)
        try check(lastInactive == 5, "lastPosition by a Bool tag: \(String(describing: lastInactive))")

        var failed = false
        do {
            try await store.append(
                [Flagged(n: 9, active: false)],
                condition: AppendCondition(query: try EventQuery.of(Flagged.self, where: [n: .bound(2), active: .bound(true)]))
            )
        } catch LedjerError.appendConditionFailed {
            failed = true
        }
        try check(failed, "a condition on Int and Bool tags fails when an event matches")
        let passed = try await store.append(
            [Flagged(n: 9, active: false)],
            condition: AppendCondition(query: try EventQuery.of(Flagged.self, where: [n: .bound(2), active: .bound(false)]))
        )
        try check(passed == 7, "a condition on Int and Bool tags passes when none matches: \(String(describing: passed))")
    }

    /// A NUL character is an ordinary character: `"alice\0evil"` is neither `"alice"` nor `"alice\0"`, in tag
    /// values, tag names and event types. (A store that hands strings to a C API as NUL-terminated text, as
    /// SQLite's `bind_text` with length -1 does, cuts them at the NUL and fails this.)
    public static func nulCharactersAreSignificant(_ makeStore: StoreFactory) async throws {
        let nul = "\u{0}"
        let name = TagPath.payload("name")

        // Tag values.
        let store = try await makeStore()
        try await store.append([StringTagged(name: "alice" + nul + "evil")])  // 1
        func positions(_ value: String) async throws -> [Position] {
            try await store.source(EventQuery.of(StringTagged.self, where: [name: .bound(value)])).map(\.position)
        }
        func fails(_ value: String) async throws -> Bool {
            do {
                try await store.append(
                    [StringTagged(name: "unrelated")],
                    condition: AppendCondition(query: try EventQuery.of(StringTagged.self, where: [name: .bound(value)])))
                return false
            } catch LedjerError.appendConditionFailed {
                return true
            }
        }
        let prefix = try await positions("alice")
        try check(prefix.isEmpty, "\"alice\" doesn't match \"alice\\0evil\": \(prefix)")
        let whole = try await positions("alice" + nul + "evil")
        try check(whole == [1], "\"alice\\0evil\" matches itself: \(whole)")
        let prefixFails = try await fails("alice")
        try check(!prefixFails, "a condition on \"alice\" passes when only \"alice\\0evil\" exists")
        let wholeFails = try await fails("alice" + nul + "evil")
        try check(wholeFails, "a condition on \"alice\\0evil\" fails")
        // 2 is the unrelated event appended by the passing condition above, 3 the next one.
        try await store.append([StringTagged(name: "alice"), StringTagged(name: "alice" + nul), StringTagged(name: "")])  // 3, 4, 5
        let alice = try await positions("alice")
        try check(alice == [3], "\"alice\" and \"alice\\0evil\" coexist: \(alice)")
        let trailing = try await positions("alice" + nul)
        try check(trailing == [4], "a trailing NUL counts: \(trailing)")
        let empty = try await positions("")
        try check(empty == [5], "the empty string is a value: \(empty)")
        let other = try await positions("alice" + nul + "other")
        try check(other.isEmpty, "a different suffix after the NUL: \(other)")
        let both = try await store.source(
            try EventQuery([
                try EventQueryItem([StringTagged.self], where: [name: .bound("alice")]),
                try EventQueryItem([StringTagged.self], where: [name: .bound("alice" + nul + "evil")]),
            ])
        ).map(\.position)
        try check(both == [1, 3], "a query for both: \(both)")
        let last = try await store.lastPosition(
            matching: EventQuery.of(StringTagged.self, where: [name: .bound("alice")]), through: 5)
        try check(last == 3, "lastPosition: \(String(describing: last))")
        let decoded = try await store.source(EventQuery.of(StringTagged.self, where: [name: .bound("alice" + nul + "evil")]))
            .map { try $0.decode(StringTagged.self).name }
        try check(decoded == ["alice" + nul + "evil"], "the payload keeps the NUL")

        // Tag names: two tags of one event whose names differ only after a NUL.
        let keys = try await makeStore()
        try await keys.append([NulKeys(plain: "1", other: "2")])
        func keyed(_ key: String, _ value: String) async throws -> [Position] {
            try await keys.source(EventQuery.of(NulKeys.self, where: [.payload(key): .bound(value)])).map(\.position)
        }
        let plainTag = try await keyed("k", "1")
        try check(plainTag == [1], "tag \"k\": \(plainTag)")
        let nulTag = try await keyed("k" + nul + "x", "2")
        try check(nulTag == [1], "tag \"k\\0x\": \(nulTag)")
        let mixedUp = try await keyed("k", "2") + keyed("k" + nul + "x", "1")
        try check(mixedUp.isEmpty, "the two tags don't mix: \(mixedUp)")

        // Event types that differ only after a NUL.
        let types = try await makeStore()
        try await types.append([NulTypeShort(), NulTypeLong(), NulTypeShort()])
        let short = try await types.source(EventQuery.of(NulTypeShort.self)).map(\.position)
        try check(short == [1, 3], "event type \"contract/nul\": \(short)")
        let long = try await types.source(EventQuery.of(NulTypeLong.self)).map(\.position)
        try check(long == [2], "event type \"contract/nul\\0long\": \(long)")
        let longEvents = try await types.source(EventQuery.of(NulTypeLong.self))
        try check(longEvents.map(\.type) == [NulTypeLong.eventType], "the stored type keeps the NUL")
    }

    /// Strings are equal when their UTF-8 bytes are, not when they are canonically equivalent, as Swift's `String`
    /// has it: the precomposed "café" (U+00E9) and "cafe\u{301}" are different tag values, tag names and event types.
    public static func unicodeNormalizationIsSignificant(_ makeStore: StoreFactory) async throws {
        let (nfc, nfd) = ("caf\u{e9}", "cafe\u{301}")
        try check(nfc.utf8.count != nfd.utf8.count, "the two spellings differ in bytes")
        let name = TagPath.payload("name")

        // Tag values.
        let store = try await makeStore()
        try await store.append([StringTagged(name: nfc)])  // 1
        func positions(_ value: String) async throws -> [Position] {
            try await store.source(EventQuery.of(StringTagged.self, where: [name: .bound(value)])).map(\.position)
        }
        let early = try await positions(nfd)
        try check(early.isEmpty, "NFD doesn't match NFC: \(early)")
        let passed = try await store.append(
            [StringTagged(name: nfd)],  // 2
            condition: AppendCondition(query: try EventQuery.of(StringTagged.self, where: [name: .bound(nfd)])))
        try check(passed == 2, "a condition on NFD passes when only NFC exists: \(String(describing: passed))")
        let failedOnNFC: Bool
        do {
            try await store.append(
                [StringTagged(name: "x")],
                condition: AppendCondition(query: try EventQuery.of(StringTagged.self, where: [name: .bound(nfc)])))
            failedOnNFC = false
        } catch LedjerError.appendConditionFailed {
            failedOnNFC = true
        }
        try check(failedOnNFC, "a condition on NFC fails when NFC exists")
        let (onlyNFC, onlyNFD) = (try await positions(nfc), try await positions(nfd))
        try check(onlyNFC == [1] && onlyNFD == [2], "both coexist: NFC \(onlyNFC), NFD \(onlyNFD)")
        let both = try await store.source(
            try EventQuery([
                try EventQueryItem([StringTagged.self], where: [name: .bound(nfc)]),
                try EventQueryItem([StringTagged.self], where: [name: .bound(nfd)]),
            ])
        ).map(\.position)
        try check(both == [1, 2], "a query with both forms returns both: \(both)")
        let last = try await store.lastPosition(
            matching: EventQuery.of(StringTagged.self, where: [name: .bound(nfc)]), through: 2)
        try check(last == 1, "lastPosition: \(String(describing: last))")
        let names = try await store.source(EventQuery.of(StringTagged.self)).map { try $0.decode(StringTagged.self).name }
        try check(
            names.count == 2 && names[0].utf8.elementsEqual(nfc.utf8) && names[1].utf8.elementsEqual(nfd.utf8),
            "the payloads keep their spelling")

        // Tag names: two tags of one event that differ only in normalization.
        let keys = try await makeStore()
        try await keys.append([NormalizedKeys(precomposed: "1", decomposed: "2")])
        func keyed(_ key: String, _ value: String) async throws -> [Position] {
            try await keys.source(EventQuery.of(NormalizedKeys.self, where: [.payload(key): .bound(value)])).map(\.position)
        }
        let a = try await keyed(nfc, "1")
        let b = try await keyed(nfd, "2")
        let crossed = try await keyed(nfc, "2") + keyed(nfd, "1")
        try check(a == [1] && b == [1] && crossed.isEmpty, "tag names: \(a) \(b) \(crossed)")

        // Event types.
        let types = try await makeStore()
        try await types.append([NFCType(), NFDType(), NFCType()])
        let precomposed = try await types.source(EventQuery.of(NFCType.self)).map(\.position)
        let decomposed = try await types.source(EventQuery.of(NFDType.self)).map(\.position)
        try check(precomposed == [1, 3] && decomposed == [2], "event types: \(precomposed) \(decomposed)")
        let bothTypes = try await types.source(EventQuery.of(NFCType.self, NFDType.self)).map(\.position)
        try check(bothTypes == [1, 2, 3], "a query for both types: \(bothTypes)")
    }

    /// A conditional append that fails is never seen by subscribers: they get the events of the
    /// appends that succeed, in order, without gaps or duplicates.
    public static func failedConditionalAppendsAreNotDelivered(_ makeStore: StoreFactory) async throws {
        let store = try await makeStore()
        let order = UUID()
        let placed = try EventQuery.of(OrderPlaced.self, where: [.payload("orderId"): .bound(order)])
        try await store.append([OrderPlaced(orderId: order)])  // 1
        let live = Collector(try await store.subscribe(EventQuery.of(OrderPlaced.self)))
        let replaying = Collector(try await store.subscribe(EventQuery.of(OrderPlaced.self), startPosition: 1))

        var failures = 0
        for _ in 0..<3 {
            do {
                try await store.append(
                    [OrderPlaced(orderId: UUID()), OrderPlaced(orderId: UUID())],
                    condition: AppendCondition(query: placed))
            } catch LedjerError.appendConditionFailed {
                failures += 1
            }
        }
        try check(failures == 3, "the conditional appends fail: \(failures) of 3")

        let accepted = (0..<2).map { _ in UUID() }
        let last = try await store.append(
            accepted.map { OrderPlaced(orderId: $0) },
            condition: AppendCondition(query: placed, after: 1))
        try check(last == 3, "the failed appends took no positions: \(String(describing: last))")

        let expectedPositions = [2, 3]
        let seen = try await live.settled()
        try check(seen.map(\.position) == expectedPositions, "a live subscriber sees only the appended events: \(seen.map(\.position))")
        try check(try orderIds(seen) == accepted, "in order")
        let replayed = try await replaying.settled()
        try check(replayed.map(\.position) == [1] + expectedPositions, "a replaying subscriber too: \(replayed.map(\.position))")
        live.cancel()
        replaying.cancel()
    }

    public static func metadataRoundTrips(_ makeStore: StoreFactory) async throws {
        let store = try await makeStore()
        let correlation = UUID()
        try await store.append(
            [UUIDTagged(id: UUID(), num: 1)],
            metadata: ["correlation": .uuid(correlation), "happy": .bool(true), "n": .int(7), "who": .string("me")]
        )
        let events = try await store.source(EventQuery.of(UUIDTagged.self))
        let expected: Metadata = [
            "correlation": .uuid(correlation), "happy": .bool(true), "n": .int(7), "who": .string("me"),
            "version": .int(1),
        ]
        try check(events.first?.metadata == expected, "metadata: \(String(describing: events.first?.metadata))")
    }

    public static func appendConditions(_ makeStore: StoreFactory) async throws {
        let store = try await makeStore()
        let order = UUID()
        let placed = try EventQuery.of(OrderPlaced.self, where: [.payload("orderId"): .bound(order)])

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
        let afterFailed = try await store.source(EventQuery.of(OrderPlaced.self))
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
            condition: AppendCondition(query: try EventQuery.of(OrderPlaced.self), after: 3)
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

    /// The condition is evaluated atomically with the append: of several concurrent appends that each
    /// require that no matching event exists, exactly one succeeds.
    public static func concurrentConditionalAppendsAreAtomic(_ makeStore: StoreFactory) async throws {
        let store = try await makeStore()
        let writers = 8
        for round in 0..<50 {
            let order = UUID()
            let placed = try EventQuery.of(OrderPlaced.self, where: [.payload("orderId"): .bound(order)])
            let successes = try await withThrowingTaskGroup(of: Bool.self) { group in
                for _ in 0..<writers {
                    group.addTask {
                        do {
                            try await store.append([OrderPlaced(orderId: order)], condition: AppendCondition(query: placed))
                            return true
                        } catch LedjerError.appendConditionFailed {
                            return false
                        }
                    }
                }
                return try await group.reduce(0) { $0 + ($1 ? 1 : 0) }
            }
            try check(successes == 1, "round \(round): \(successes) of \(writers) concurrent conditional appends succeeded, expected 1")
            let stored = try await store.source(placed).count
            try check(stored == 1, "round \(round): the log holds \(stored) matching events, expected 1")
        }
    }

    public static func lastPositions(_ makeStore: StoreFactory) async throws {
        let store = try await makeStore()
        let (a, b) = (UUID(), UUID())
        try await store.append([
            OrderPlaced(orderId: a),  // 1
            OrderNoted(orderId: a, note: "x"),  // 2
            OrderPlaced(orderId: b),  // 3
            OrderPlaced(orderId: a),  // 4
        ])
        func last(_ query: EventQuery, through: Position) async throws -> Position? {
            try await store.lastPosition(matching: query, through: through)
        }
        let placed = try EventQuery.of(OrderPlaced.self)
        for (through, expected) in [(4, 4), (3, 3), (2, 1), (1, 1), (0, nil), (100, 4)] as [(Int, Int?)] {
            let got = try await last(placed, through: through)
            try check(got == expected, "last placed through \(through): \(String(describing: got)), expected \(String(describing: expected))")
        }
        let ofA = try EventQuery.of(OrderPlaced.self, where: [.payload("orderId"): .bound(a)])
        let gotA = try await last(ofA, through: 3)
        try check(gotA == 1, "tagged: \(String(describing: gotA))")
        let ofB = try EventQuery.of(OrderPlaced.self, where: [.payload("orderId"): .bound(b)])
        let gotB = try await last(ofB, through: 4)
        try check(gotB == 3, "tagged, another: \(String(describing: gotB))")
        let none = try await last(try EventQuery.of(OrderPlaced.self, where: [.payload("orderId"): .bound(UUID())]), through: 4)
        try check(none == nil, "nothing matches: \(String(describing: none))")
        let union = try EventQuery([try EventQueryItem([OrderNoted.self]), try EventQueryItem([OrderPlaced.self], where: [.payload("orderId"): .bound(b)])])
        let gotUnion = try await last(union, through: 4)
        try check(gotUnion == 3, "union: \(String(describing: gotUnion))")
        let unionEarly = try await last(union, through: 2)
        try check(unionEarly == 2, "union, earlier: \(String(describing: unionEarly))")
    }

    public static func subscribeFromNow(_ makeStore: StoreFactory) async throws {
        let store = try await makeStore()
        let query = try EventQuery.of(OrderPlaced.self)

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
        let tagged = try EventQuery.of(OrderPlaced.self, OrderNoted.self, where: [.payload("orderId"): .bound(wanted)])
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
                try EventQuery([
                    try EventQueryItem([OrderPlaced.self]),
                    try EventQueryItem([OrderPlaced.self], where: [.payload("orderId"): .bound(id)]),
                ])
            )
        )
        try await store.append([OrderPlaced(orderId: id)])
        let got4 = try orderIds(try await c4.settled())
        try check(got4 == [id], "delivered once: \(got4)")
        [c1, c2, c3, c4].forEach { $0.cancel() }
    }

    public static func subscribeWithStartPosition(_ makeStore: StoreFactory) async throws {
        let query = try EventQuery.of(OrderPlaced.self)
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
            let tagged = try EventQuery.of(OrderPlaced.self, where: [.payload("orderId"): .bound(wanted)])
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

    /// A start position below 1, `Int.min` included, is the same as 1: everything. (A store that
    /// computes with the position, e.g. `startPosition - 1`, must not overflow on it.)
    public static func startPositionsBelowOneReadEverything(_ makeStore: StoreFactory) async throws {
        let store = try await makeStore()
        let ids = (0..<3).map { _ in UUID() }
        try await store.append(ids.map { OrderPlaced(orderId: $0) })
        let query = try EventQuery.of(OrderPlaced.self)
        for start in [1, 0, -1, Int.min + 1, Int.min] {
            let sourced = try await store.source(query, startPosition: start)
            try check(sourced.map(\.position) == [1, 2, 3], "source from \(start): \(sourced.map(\.position))")
            let c = Collector(try await store.subscribe(query, startPosition: start))
            let got = try orderIds(try await c.settled())
            try check(got == ids, "subscribe from \(start) replays everything: \(got.count)")
            c.cancel()
        }
    }

    public static func subscribeTerminationDoesNotDisturbAppends(_ makeStore: StoreFactory) async throws {
        let store = try await makeStore()
        let c = Collector(try await store.subscribe(EventQuery.of(OrderPlaced.self)))
        c.cancel()
        try await Task.sleep(for: .milliseconds(100))
        let position = try await store.append([OrderPlaced(orderId: UUID())])
        try check(position == 1, "append after a cancelled subscription: \(String(describing: position))")
        try check(c.error == nil, "a cancelled subscription finishes without an error: \(String(describing: c.error))")
    }

    /// Cancelling a consumer ends its stream without an error, also when the cancellation lands in the
    /// middle of a read of the log (which in a database-backed store may throw a `CancellationError`).
    public static func cancellingAConsumerEndsTheStreamWithoutAnError(_ makeStore: StoreFactory) async throws {
        let store = try await makeStore()
        try await store.append((0..<4000).map { UUIDTagged(id: UUID(), num: $0) })
        let query = try EventQuery.of(UUIDTagged.self)

        var failures: [String] = []
        for round in 0..<60 {
            let stream = try await store.subscribe(query, startPosition: 0)
            let consumer = Task { () throws -> Int in
                var seen = 0
                for try await _ in stream { seen += 1 }
                return seen
            }
            try await Task.sleep(for: .microseconds(round * 150))
            consumer.cancel()
            if case .failure(let error) = await consumer.result { failures.append("\(error)") }
        }
        try check(failures.isEmpty, "cancelling a consumer failed its stream: \(failures)")
    }
}

// The events and helpers of the checks live in the namespace of `StoreContract`, so that they
// don't pollute that of the test target that imports this module.
extension StoreContract {
    static func check(_ condition: Bool, _ message: @autoclosure () -> String, line: Int = #line) throws {
        if !condition { throw ContractFailure(message: "line \(line): \(message())") }
    }

    // MARK: - Contract events

    package struct UUIDTagged: Event {
        package static let eventType: EventType = "contract/uuid-tagged"
        package static var tags: [EventTag<Self>] { [EventTag("id", \.id)] }
        package var id: UUID
        package var num: Int
        package init(id: UUID, num: Int) { (self.id, self.num) = (id, num) }
    }

    package struct OrderPlaced: Event {
        package static let eventType: EventType = "contract/order-placed"
        package static var tags: [EventTag<Self>] { [EventTag("orderId", \.orderId), EventTag("customer", \.customer)] }
        package var orderId: UUID
        package var customer: String?
        package init(orderId: UUID, customer: String? = nil) { (self.orderId, self.customer) = (orderId, customer) }
    }

    /// Carries an `orderId` but doesn't tag it.
    package struct OrderNoted: Event {
        package static let eventType: EventType = "contract/order-noted"
        package var orderId: UUID
        package var note: String
        package init(orderId: UUID, note: String) { (self.orderId, self.note) = (orderId, note) }
    }

    /// Tags of the other scalar types: an `Int` and a `Bool`.
    package struct Flagged: Event {
        package static let eventType: EventType = "contract/flagged"
        package static var tags: [EventTag<Self>] { [EventTag("n", \.n), EventTag("active", \.active)] }
        package var n: Int
        package var active: Bool
        package init(n: Int, active: Bool) { (self.n, self.active) = (n, active) }
    }

    /// A string tag, for values with characters that strings are easily mishandled by.
    package struct StringTagged: Event {
        package static let eventType: EventType = "contract/string-tagged"
        package static var tags: [EventTag<Self>] { [EventTag("name", \.name)] }
        package var name: String
        package init(name: String) { self.name = name }
    }

    /// Two tags whose names differ only after a NUL.
    package struct NulKeys: Event {
        package static let eventType: EventType = "contract/nul-keys"
        package static var tags: [EventTag<Self>] { [EventTag("k", \.plain), EventTag("k\u{0}x", \.other)] }
        package var plain: String
        package var other: String
        package init(plain: String, other: String) { (self.plain, self.other) = (plain, other) }
    }

    package struct NulTypeShort: Event {
        package static let eventType: EventType = "contract/nul"
        package init() {}
    }

    /// Its type differs from `NulTypeShort`'s only after a NUL.
    package struct NulTypeLong: Event {
        package static let eventType: EventType = "contract/nul\u{0}long"
        package init() {}
    }

    /// Two tags whose names are canonically equivalent but not byte-equal ("café", precomposed and decomposed).
    package struct NormalizedKeys: Event {
        package static let eventType: EventType = "contract/normalized-keys"
        package static var tags: [EventTag<Self>] {
            [EventTag("caf\u{e9}", \.precomposed), EventTag("cafe\u{301}", \.decomposed)]
        }
        package var precomposed: String
        package var decomposed: String
        package init(precomposed: String, decomposed: String) { (self.precomposed, self.decomposed) = (precomposed, decomposed) }
    }

    package struct NFCType: Event {
        package static let eventType: EventType = "contract/caf\u{e9}"
        package init() {}
    }

    /// Its type is canonically equivalent to `NFCType`'s, but not byte-equal.
    package struct NFDType: Event {
        package static let eventType: EventType = "contract/cafe\u{301}"
        package init() {}
    }

    package struct VersionTagged: Event {
        package static let eventType: EventType = "contract/version-tagged"
        package static let version = 2
        package static var tags: [EventTag<Self>] { [EventTag("id", \.id), .metadata("version")] }
        package var id: String
        package init(id: String) { self.id = id }
    }

    // MARK: - Subscription helper

    /// Collects what a subscription delivers.
    final class Collector: @unchecked Sendable {
        private let lock = NSLock()
        private var _events: [StoredEvent] = []
        private var _error: (any Error)?
        private var task: Task<Void, Never>?

        init(_ stream: AsyncThrowingStream<StoredEvent, any Error>) {
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

        var events: [StoredEvent] { lock.withLock { _events } }
        var error: (any Error)? { lock.withLock { _error } }

        /// Waits until nothing has arrived for `quiet`, at most `timeout`, and returns what arrived.
        func settled(quiet: Duration = .milliseconds(200), timeout: Duration = .seconds(20)) async throws -> [StoredEvent] {
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

        func cancel() { task?.cancel() }
    }

    static func orderIds(_ events: [StoredEvent]) throws -> [UUID] {
        try events.map { try $0.decode(OrderPlaced.self).orderId }
    }
}
