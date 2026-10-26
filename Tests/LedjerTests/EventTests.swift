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

    @Test func decodeChecksTheType() throws {
        let stored = StoredEvent(position: 1, type: "test/event-2", metadata: [:], payload: Data(#"{"id":"a","num":1}"#.utf8))
        #expect(try stored.decode(TestEvent2.self).num == 1)
        #expect(throws: LedjerError.self) { try stored.decode(OrderNoted.self) }
    }
}

@Suite struct QueryTests {
    @Test func validQueries() throws {
        _ = try Query.of(OrderPlaced.self)
        _ = try Query.of(OrderPlaced.self, where: [.payload("orderId"): .any])
        // A binding needs only to be a tag of one of the event types.
        _ = try Query.of(OrderPlaced.self, OrderNoted.self, where: [.payload("orderId"): .bound(UUID())])
    }

    @Test func unknownTagPathIsInvalid() {
        #expect(throws: LedjerError.self) { try Query.of(OrderNoted.self, where: [.payload("orderId"): "x"]) }
        #expect(throws: LedjerError.self) { try Query.of(OrderPlaced.self, where: [.metadata("version"): 1]) }
    }

    @Test func selectorsOnlyKeepBindingsOfTheTypesTags() throws {
        let id = UUID()
        let query = try Query.of(OrderPlaced.self, OrderNoted.self, where: [.payload("orderId"): .bound(id)])
        let byType = Dictionary(uniqueKeysWithValues: query.selectors.map { ($0.type, $0.required) })
        #expect(byType["contract/order-placed"] == [.payload("orderId"): .uuid(id)])
        #expect(byType["contract/order-noted"] == [:])
    }

    @Test func matching() throws {
        let id = UUID()
        let query = try Query.of(OrderPlaced.self, where: [.payload("orderId"): .bound(id), .payload("customer"): .any])
        #expect(query.matches(type: "contract/order-placed", tags: [.payload("orderId"): .uuid(id)]))
        #expect(!query.matches(type: "contract/order-placed", tags: [.payload("orderId"): .uuid(UUID())]))
        #expect(!query.matches(type: "contract/order-placed", tags: [:]))
        #expect(!query.matches(type: "contract/order-noted", tags: [.payload("orderId"): .uuid(id)]))
    }
}
