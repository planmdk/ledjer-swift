import Foundation
@testable import Ledjer
import Testing

/// Counts how often it is decoded.
private struct Decoded: Event {
    static let eventType: EventType = "routing-test/decoded"
    static var tags: [EventTag<Self>] { [EventTag("a", \.a), EventTag("b", \.b), EventTag("c", \.c)] }
    static let decodes = Counter()

    var a: String
    var b: String
    var c: String

    init(a: String, b: String, c: String) { (self.a, self.b, self.c) = (a, b, c) }

    enum CodingKeys: String, CodingKey { case a, b, c }
    init(from decoder: any Decoder) throws {
        Self.decodes.increment()
        let container = try decoder.container(keyedBy: CodingKeys.self)
        (a, b, c) = (try container.decode(String.self, forKey: .a), try container.decode(String.self, forKey: .b), try container.decode(String.self, forKey: .c))
    }
}

@Suite(.serialized) struct RoutingTests {
    /// An event is decoded once to be routed, however many tag-bound bindings are tried on it.
    @Test func anEventIsDecodedOnceForAllTheBindingsTriedOnIt() async throws {
        let store = InMemoryEventStore()
        try await store.append([Decoded(a: "1", b: "2", c: "3")])
        let event = try #require(try await store.source(EventQuery.of(Decoded.self), startPosition: nil).first)

        let routes = [
            (selector: EventSelector(Decoded.self, where: [.payload("a"): "x"]), handler: "a"),
            (selector: EventSelector(Decoded.self, where: [.payload("b"): "2"]), handler: "b"),
            (selector: EventSelector(Decoded.self, where: [.payload("c"): "y"]), handler: "c"),
        ]
        let before = Decoded.decodes.value
        #expect(try route(event, among: routes) == "b")
        #expect(Decoded.decodes.value - before == 1)
    }

    @Test func eventsOfOtherTypesAreNotDecoded() async throws {
        let store = InMemoryEventStore()
        try await store.append([Decoded(a: "1", b: "2", c: "3")])
        let event = try #require(try await store.source(EventQuery.of(Decoded.self), startPosition: nil).first)
        let routes = [
            (selector: EventSelector(Decoded.self, where: [:]), handler: "unbound"),
            (selector: EventSelector(TestEvent.self, where: [.payload("id"): "x"]), handler: "other"),
        ]
        let before = Decoded.decodes.value
        #expect(try route(event, among: routes) == "unbound")
        #expect(Decoded.decodes.value == before)
    }
}
