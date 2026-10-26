import Foundation
import Ledjer
import LedjerStoreContract
import Testing

struct TestEvent: Event {
    static let eventType: EventType = "view-test/event"
    static var tags: [EventTag<Self>] { [EventTag("id", \.id)] }
    var id: String
    var num: Int
}

struct TestEventWithVersionTag: Event {
    static let eventType: EventType = "view-test/event-2"
    static var tags: [EventTag<Self>] { [EventTag("id", \.id), .metadata("version")] }
    var id: String
    var num: Int
}

private func expectInvalidQuery(_ body: () async throws -> some Any) async {
    do {
        _ = try await body()
        Issue.record("expected an invalid query error")
    } catch LedjerError.invalidQuery {
    } catch {
        Issue.record("expected an invalid query error, got \(error)")
    }
}

@Suite struct StateViewTests {
    /// +num for TestEvent, -num for TestEventWithVersionTag for id abc-222
    let view = StateView(initial: 0, bindings: [
        .on(TestEvent.self) { acc, e in acc += e.num },
        .on(TestEventWithVersionTag.self, where: [.payload("id"): "abc-222"]) { acc, e in acc -= e.num },
    ])

    @Test func twoEventsOneWithTagBindings() async throws {
        let store = InMemoryEventStore()
        try await store.append([
            TestEvent(id: "abc-123", num: 10),
            TestEvent(id: "abc-222", num: 10),
            TestEventWithVersionTag(id: "abc-222", num: 5),
        ])
        #expect(try await view.run(on: store) == 15)
        #expect(try await view.run(on: store, where: [.payload("id"): "abc-222"]) == 5)
        #expect(try await view.run(on: store, where: [.payload("id"): .any]) == 15)
    }

    @Test func runtimeBindingsConflictingWithABindingsOwn() async throws {
        let store = InMemoryEventStore()
        try await store.append([TestEvent(id: "abc-123", num: 10), TestEventWithVersionTag(id: "abc-222", num: 5)])
        await #expect(throws: LedjerError.self) {
            try await view.run(on: store, where: [.payload("id"): "abc-123"])
        }
    }

    @Test func runtimeBindingsNarrowBindingsWithoutTheirOwn() async throws {
        let view = StateView(initial: 0, bindings: [
            .on(TestEvent.self) { acc, e in acc += e.num },
            .on(TestEventWithVersionTag.self) { acc, e in acc -= e.num },
        ])
        let store = InMemoryEventStore()
        try await store.append([
            TestEvent(id: "abc-123", num: 10),
            TestEvent(id: "abc-222", num: 10),
            TestEventWithVersionTag(id: "abc-222", num: 5),
        ])
        #expect(try await view.run(on: store, where: [.payload("id"): "abc-222"]) == 5)
    }

    @Test func runtimeBindingsOnlyApplyToTypesHavingTheTag() async throws {
        // Only TestEventWithVersionTag has the metadata version tag.
        let view = StateView(initial: 0, bindings: [
            .on(TestEvent.self) { acc, e in acc += e.num },
            .on(TestEventWithVersionTag.self) { acc, e in acc -= e.num },
        ])
        let store = InMemoryEventStore()
        try await store.append([
            TestEvent(id: "abc-123", num: 10),
            TestEvent(id: "abc-222", num: 10),
            TestEventWithVersionTag(id: "abc-222", num: 5),
        ])
        #expect(try await view.run(on: store, where: [.metadata("version"): 1]) == 15)
        #expect(try await view.run(on: store, where: [.metadata("version"): 2]) == 20)
    }

    @Test func runtimeBindingForATagNoTypeHasIsInvalid() async throws {
        let view = StateView(initial: 0, bindings: [
            .on(TestEvent.self) { acc, e in acc += e.num },
        ])
        let store = InMemoryEventStore()
        await expectInvalidQuery { try await view.run(on: store, where: [.payload("nope"): "x"]) }
        await expectInvalidQuery { try await view.run(on: store, where: [.metadata("version"): 1]) }
    }

    @Test func aBindingForATagItsTypeDoesNotHaveIsInvalid() async throws {
        let view = StateView(initial: 0, bindings: [
            .on(TestEvent.self, where: [.payload("nope"): "x"]) { acc, e in acc += e.num },
        ])
        await expectInvalidQuery { try await view.run(on: InMemoryEventStore()) }
    }

    @Test func tooManyMatchingBindings() async throws {
        let view = StateView(initial: 0, bindings: [
            .on(TestEvent.self) { acc, e in acc += e.num },
            .on(TestEvent.self, where: [.payload("id"): "abc-123"]) { acc, e in acc -= e.num },
        ])
        let store = InMemoryEventStore()
        try await store.append([TestEvent(id: "abc-123", num: 10)])
        await #expect(throws: LedjerError.self) { try await view.run(on: store) }
    }

    @Test func reducersSeeTheStoredEvent() async throws {
        let view = StateView(initial: [Position](), bindings: [
            .on(TestEvent.self) { acc, _, stored in acc.append(stored.position) },
        ])
        let store = InMemoryEventStore()
        try await store.append([TestEvent(id: "a", num: 1), TestEventWithVersionTag(id: "a", num: 1), TestEvent(id: "b", num: 1)])
        #expect(try await view.run(on: store) == [1, 3])
    }

    @Test func emptyStoreYieldsTheInitialValue() async throws {
        #expect(try await view.run(on: InMemoryEventStore()) == 0)
    }
}
