import Foundation
import Ledjer
import LedjerStoreContract
import Testing

struct TestEvent2: Event {
    static let eventType: EventType = "test/event-2"
    static var tags: [EventTag<Self>] { [EventTag("id", \.id), .metadata("version")] }
    var id: String
    var num: Int
}

@Suite struct EventTests {
    @Test func newEventCarriesTypeMetadataPayloadAndTags() throws {
        let event = try NewEvent(TestEvent2(id: "test-123", num: 42), metadata: ["happy": .bool(true)])
        #expect(event.type == "test/event-2")
        #expect(event.metadata == ["happy": .bool(true), "version": .int(1)])
        #expect(event.tags == [.payload("id"): .string("test-123"), .metadata("version"): .int(1)])
        #expect(String(decoding: event.payload, as: UTF8.self) == #"{"id":"test-123","num":42}"#)
    }

    @Test func versionIsRecordedInMetadata() throws {
        #expect(try NewEvent(VersionTagged(id: "a")).metadata["version"] == .int(2))
    }

    @Test func theEventsVersionOverridesUserMetadata() throws {
        // As in Clojure: (merge metadata {:version v}).
        let event = try NewEvent(VersionTagged(id: "a"), metadata: ["version": .int(99), "other": .int(1)])
        #expect(event.metadata == ["version": .int(2), "other": .int(1)])
    }

    @Test func scalarEncodingRoundTrips() {
        let id = UUID()
        var scalars: [Scalar] = [
            .string(""), .string("a:b"), .string(":"), .string("::x"), .string("string:int:1"), .string("café"), .string("cafe\u{301}"),
            .string("\u{301}"), .string("日本語 🎉"), .string("a\0b"), .string("\0"), .string("\r\n"),
            .int(0), .int(-3), .int(7), .int(Int.max), .int(Int.min), .bool(true), .bool(false), .uuid(id), .uuid(UUID(uuid: (0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0))),
        ]
        #if _pointerBitWidth(_64)
        scalars.append(UInt32.max.scalar)
        #endif
        for scalar in scalars {
            let decoded = Scalar(encoded: scalar.encoded)
            #expect(decoded == scalar, "\(scalar)")
            #expect(decoded?.encoded.utf8.elementsEqual(scalar.encoded.utf8) == true, "\(scalar)")
        }
    }

    @Test func nonCanonicalScalarEncodingsAreRejected() throws {
        let id = UUID(uuidString: "6F9619FF-8B86-D011-B42D-00C04FC964FF")!
        #expect(Scalar(encoded: "uuid:" + id.uuidString) == .uuid(id))
        #expect(Scalar(encoded: "uuid:" + id.uuidString.lowercased()) == nil)
        let rejected = [
            "", "int", "string", "bool:", "x:1", "String:a", " string:a", "string :a",
            "int:+5", "int:05", "int:00", "int:-0", "int:+0", "int:-05", "int:", "int:-", "int: 5", "int:5 ", "int:1.0", "int:1e3", "int:٣",
            "int:9223372036854775808", "int:-9223372036854775809",
            "bool:True", "bool:TRUE", "bool:1", "bool:yes", "bool: true",
            "uuid:", "uuid:not-a-uuid", "uuid:" + id.uuidString + " ",
            "uuid:" + id.uuidString.replacingOccurrences(of: "-", with: ""),
        ]
        for input in rejected {
            #expect(Scalar(encoded: input) == nil, "\(input)")
        }
        // Also by the Codable conformance, which decoders of stored metadata go through.
        #expect(throws: DecodingError.self) { try JSONDecoder().decode([Scalar].self, from: Data(#"["int:+5"]"#.utf8)) }
        #expect(try JSONDecoder().decode([Scalar].self, from: Data(#"["int:5"]"#.utf8)) == [.int(5)])
    }

    @Test func optionalTagsAreLeftOutWhenNil() throws {
        let without = try NewEvent(OrderPlaced(orderId: UUID()))
        #expect(without.tags.keys.contains(.payload("customer")) == false)
        let with = try NewEvent(OrderPlaced(orderId: UUID(), customer: "ann"))
        #expect(with.tags[.payload("customer")] == .string("ann"))
    }

    @Test func scalarsCompareByTypeAndValue() {
        #expect(Scalar.string("1") != Scalar.int(1))
        let id = UUID()
        #expect(Scalar.uuid(id) != Scalar.string(id.uuidString))
        for scalar in [Scalar.string("a:b"), .int(-3), .bool(false), .uuid(id)] {
            #expect(Scalar(encoded: scalar.encoded) == scalar)
        }
    }

    @Test func fixedWidthIntegersFittingAnIntAreTagValues() {
        #expect(Int8(-8).scalar == .int(-8))
        #expect(Int16(16).scalar == .int(16))
        #expect(Int32.min.scalar == .int(Int(Int32.min)))
        #expect(UInt8.max.scalar == .int(255))
        #expect(UInt16.max.scalar == .int(65535))
    }

    // `Int64` and `UInt32` are tag values only where `Int` is 64 bits wide.
    #if _pointerBitWidth(_64)
    struct Sized: Event {
        static let eventType: EventType = "event-test/sized"
        static var tags: [EventTag<Self>] { [EventTag("small", \.small), EventTag("big", \.big), EventTag("count", \.count)] }
        var small: Int8
        var big: Int64
        var count: UInt32?
    }

    @Test func wideIntegersAreTagValuesOnA64BitInt() throws {
        #expect(Int64.max.scalar == .int(Int.max))
        #expect(UInt32.max.scalar == .int(4_294_967_295))

        let event = try NewEvent(Sized(small: -3, big: 1 << 40, count: UInt32.max))
        #expect(event.tags[.payload("small")] == .int(-3))
        #expect(event.tags[.payload("big")] == .int(1 << 40))
        #expect(event.tags[.payload("count")] == .int(4_294_967_295))
        #expect(try NewEvent(Sized(small: 0, big: 0, count: nil)).tags[.payload("count")] == nil)

        // Bound like an Int of the same value.
        let query = try EventQuery.of(Sized.self, where: [.payload("big"): .bound(Int64(1) << 40), .payload("small"): .bound(Int(-3))])
        #expect(query.matches(type: Sized.eventType, tags: event.tags))
    }
    #endif

    @Test func eventTypeIsCodedAsABareString() throws {
        let type: EventType = "orders/order-placed"
        let encoder = JSONEncoder()
        encoder.outputFormatting = .withoutEscapingSlashes
        let json = try encoder.encode([type])
        #expect(String(decoding: json, as: UTF8.self) == #"["orders/order-placed"]"#)
        #expect(try JSONDecoder().decode([EventType].self, from: json) == [type])
        #expect(try JSONDecoder().decode([EventType].self, from: Data(#"["a","b"]"#.utf8)) == ["a", "b"])
        // Keeps the exact spelling.
        let nfd = EventType("cafe\u{301}")
        #expect(try JSONDecoder().decode([EventType].self, from: JSONEncoder().encode([nfd])) == [nfd])
        #expect(throws: DecodingError.self) { try JSONDecoder().decode([EventType].self, from: Data(#"[{"rawValue":"a"}]"#.utf8)) }
    }

    @Test func decodeChecksTheType() throws {
        let stored = StoredEvent(position: 1, type: "test/event-2", metadata: [:], payload: Data(#"{"id":"a","num":1}"#.utf8))
        #expect(try stored.decode(TestEvent2.self).num == 1)
        #expect(throws: LedjerError.self) { try stored.decode(OrderNoted.self) }
    }
}

@Suite struct EventQueryTests {
    @Test func validQueries() throws {
        _ = try EventQuery.of(OrderPlaced.self)
        _ = try EventQuery.of(OrderPlaced.self, where: [.payload("orderId"): .any])
        // A binding needs only to be a tag of one of the event types.
        _ = try EventQuery.of(OrderPlaced.self, OrderNoted.self, where: [.payload("orderId"): .bound(UUID())])
    }

    @Test func twoSwiftTypesWithOneEventTypeAreInvalid() throws {
        #expect(throws: LedjerError.self) { try EventQuery.of(SharedNameA.self, SharedNameB.self) }
        #expect(throws: LedjerError.self) { try EventQueryItem([SharedNameB.self, OrderPlaced.self, SharedNameA.self]) }
        // The same type twice is just one type.
        #expect(try EventQuery.of(SharedNameA.self, SharedNameA.self).selectors.count == 1)
    }

    @Test func twoSwiftTypesWithOneEventTypeAreInvalidAcrossItems() throws {
        let a = try EventQueryItem([SharedNameA.self], where: [.payload("id"): "a"])
        let b = try EventQueryItem([SharedNameB.self], where: [.payload("id"): "b"])
        #expect(throws: LedjerError.self) { try EventQuery([a, b]) }
        #expect(throws: LedjerError.self) { try EventQuery([try EventQueryItem([OrderPlaced.self]), a, b]) }
        do {
            _ = try EventQuery([b, a])
            Issue.record("expected duplicateEventType")
        } catch LedjerError.duplicateEventType(let type, let swiftTypes) {
            #expect(type == SharedNameA.eventType)
            #expect(swiftTypes.count == 2)
        }
    }

    @Test func oneSwiftTypeInSeveralItemsIsFine() throws {
        let query = try EventQuery([
            try EventQueryItem([SharedNameA.self], where: [.payload("id"): "a"]),
            try EventQueryItem([SharedNameA.self], where: [.payload("id"): "b"]),
            try EventQueryItem([SharedNameA.self]),
        ])
        #expect(query.items.count == 3)
        #expect(query.selectors.count == 3)
    }

    @Test func unknownTagPathIsInvalid() {
        #expect(throws: LedjerError.self) { try EventQuery.of(OrderNoted.self, where: [.payload("orderId"): "x"]) }
        #expect(throws: LedjerError.self) { try EventQuery.of(OrderPlaced.self, where: [.metadata("version"): 1]) }
    }

    @Test func selectorsOnlyKeepBindingsOfTheTypesTags() throws {
        let id = UUID()
        let query = try EventQuery.of(OrderPlaced.self, OrderNoted.self, where: [.payload("orderId"): .bound(id)])
        let byType = Dictionary(uniqueKeysWithValues: query.selectors.map { ($0.type, $0.required) })
        #expect(byType["contract/order-placed"] == [.payload("orderId"): .uuid(id)])
        #expect(byType["contract/order-noted"] == [:])
    }

    /// Swift's `String` equates canonically equivalent spellings; queries must not.
    @Test func spellingsOfTheSameTextAreNotDeduplicated() throws {
        let (nfc, nfd) = ("caf\u{e9}", "cafe\u{301}")
        #expect(nfc == nfd)  // what Swift thinks; and what the types below don't

        #expect(Set([Scalar.string(nfc), .string(nfd)]).count == 2)
        #expect(Set([EventType(nfc), EventType(nfd)]).count == 2)
        #expect(Set([TagPath.payload(nfc), .payload(nfd)]).count == 2)
        #expect([TagPath.payload(nfc): 1][.payload(nfd)] == nil)
        #expect(Scalar.string(nfc) == Scalar.string(String(nfc)))

        let name = TagPath.payload("name")
        let query = try EventQuery([
            try EventQueryItem([StoreContract.StringTagged.self], where: [name: .bound(nfc)]),
            try EventQueryItem([StoreContract.StringTagged.self], where: [name: .bound(nfd)]),
        ])
        #expect(query.items.count == 2)
        #expect(query.selectors.count == 2)
        #expect(query.matches(type: "contract/string-tagged", tags: [name: .string(nfc)]))
        #expect(query.matches(type: "contract/string-tagged", tags: [name: .string(nfd)]))
        let onlyNFC = try EventQuery.of(StoreContract.StringTagged.self, where: [name: .bound(nfc)])
        #expect(!onlyNFC.matches(type: "contract/string-tagged", tags: [name: .string(nfd)]))
        #expect(!onlyNFC.matches(type: EventType("contract/string-tagged"), tags: [TagPath.payload(nfc): .string(nfc)]))

        let types = try EventQuery.of(StoreContract.NFCType.self, StoreContract.NFDType.self)
        #expect(types.selectors.count == 2)
        #expect(!(try EventQuery.of(StoreContract.NFCType.self)).matches(type: StoreContract.NFDType.eventType, tags: [:]))

        let keys = try EventQuery.of(StoreContract.NormalizedKeys.self, where: [.payload(nfc): "1", .payload(nfd): "2"])
        #expect(keys.selectors.first?.required.count == 2)
    }

    @Test func matching() throws {
        let id = UUID()
        let query = try EventQuery.of(OrderPlaced.self, where: [.payload("orderId"): .bound(id), .payload("customer"): .any])
        #expect(query.matches(type: "contract/order-placed", tags: [.payload("orderId"): .uuid(id)]))
        #expect(!query.matches(type: "contract/order-placed", tags: [.payload("orderId"): .uuid(UUID())]))
        #expect(!query.matches(type: "contract/order-placed", tags: [:]))
        #expect(!query.matches(type: "contract/order-noted", tags: [.payload("orderId"): .uuid(id)]))
    }
}
