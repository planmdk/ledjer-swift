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

/// Two Swift types that, wrongly, share an event type.
struct SharedNameA: Event {
    static let eventType: EventType = "view-test/shared-name"
    static var tags: [EventTag<Self>] { [EventTag("id", \.id)] }
    var id: String
    var num: Int
}

struct SharedNameB: Event {
    static let eventType: EventType = "view-test/shared-name"
    static var tags: [EventTag<Self>] { [EventTag("id", \.id)] }
    var id: String
    var num: Int
}

struct MaybeTagged: Event {
    static let eventType: EventType = "view-test/maybe-tagged"
    static var tags: [EventTag<Self>] { [EventTag("id", \.id)] }
    var id: String?
    var num: Int
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

    @Test func aBindingsOwnAnyYieldsToARuntimeValue() async throws {
        let view = StateView(initial: [String](), bindings: [
            .on(TestEvent.self, where: [.payload("id"): .any]) { acc, e in acc.append(e.id) },
        ])
        let store = InMemoryEventStore()
        try await store.append([TestEvent(id: "x", num: 1), TestEvent(id: "y", num: 1), TestEvent(id: "x", num: 1)])
        #expect(try await view.run(on: store, where: [.payload("id"): "x"]) == ["x", "x"])
        #expect(try await view.run(on: store) == ["x", "y", "x"])
    }

    @Test func aRuntimeAnyYieldsToABindingsOwnValue() async throws {
        let view = StateView(initial: [String](), bindings: [
            .on(TestEvent.self, where: [.payload("id"): "x"]) { acc, e in acc.append(e.id) },
        ])
        let store = InMemoryEventStore()
        try await store.append([TestEvent(id: "x", num: 1), TestEvent(id: "y", num: 1)])
        #expect(try await view.run(on: store, where: [.payload("id"): .any]) == ["x"])
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

    @Test func twoSwiftTypesWithOneEventTypeAreRefusedBeforeAnythingIsRead() async throws {
        let view = StateView(initial: 0, bindings: [
            .on(SharedNameA.self, where: [.payload("id"): "a"]) { acc, e in acc += e.num },
            .on(SharedNameB.self, where: [.payload("id"): "b"]) { acc, e in acc -= e.num },
        ])
        // The store is empty and would be fine: the view fails on its bindings, not on an event.
        do {
            _ = try await view.run(on: InMemoryEventStore())
            Issue.record("expected a duplicate event type")
        } catch LedjerError.duplicateEventType(let type, let swiftTypes) {
            #expect(type == "view-test/shared-name")
            #expect(swiftTypes.count == 2 && swiftTypes.allSatisfy { $0.contains("SharedName") })
        }
        // The same Swift type bound twice, with different tags, is the normal case.
        let fine = StateView(initial: 0, bindings: [
            .on(SharedNameA.self, where: [.payload("id"): "a"]) { acc, e in acc += e.num },
            .on(SharedNameA.self, where: [.payload("id"): "b"]) { acc, e in acc -= e.num },
        ])
        #expect(try await fine.run(on: InMemoryEventStore()) == 0)
    }

    @Test func aBindingRepeatedWithTheSameTagBindingsIsRefusedBeforeAnythingIsRead() async throws {
        for repeated: TagBindings in [[:], [.payload("id"): "x"]] {
            let view = StateView(initial: 0, bindings: [
                .on(TestEvent.self, where: repeated) { acc, e in acc += e.num },
                .on(TestEvent.self, where: repeated) { acc, e in acc -= e.num },
            ])
            do {
                _ = try await view.run(on: InMemoryEventStore())  // no events: it used to pass
                Issue.record("expected a duplicate binding")
            } catch LedjerError.duplicateBinding(let type) {
                #expect(type == TestEvent.eventType)
            }
        }
    }

    @Test func anyBindingsCountAsNoBindingForTheDuplicateCheck() async throws {
        let id = TagPath.payload("id"), num = TagPath.metadata("version")
        // Each pair matches the same events.
        let pairs: [(TagBindings, TagBindings)] = [
            ([:], [id: .any]),
            ([id: .any], [:]),
            ([id: .any], [num: .any]),
            ([id: .any, num: 1], [num: 1]),
            ([num: 1], [id: .any, num: 1]),
        ]
        for (first, second) in pairs {
            let view = StateView(initial: 0, bindings: [
                .on(TestEventWithVersionTag.self, where: first) { acc, e in acc += e.num },
                .on(TestEventWithVersionTag.self, where: second) { acc, e in acc -= e.num },
            ])
            do {
                _ = try await view.run(on: InMemoryEventStore())
                Issue.record("expected a duplicate binding for \(first) and \(second)")
            } catch LedjerError.duplicateBinding(let type) {
                #expect(type == TestEventWithVersionTag.eventType)
            }
        }
    }

    @Test func bindingsWithDifferentRequiredValuesAreNotDuplicates() async throws {
        let id = TagPath.payload("id"), num = TagPath.metadata("version")
        let pairs: [(TagBindings, TagBindings)] = [
            ([id: "a"], [id: "b"]),
            ([id: .any], [id: "b"]),
            ([id: "a"], [:]),
            ([id: .any, num: 1], [num: 2]),
            ([id: "a"], [num: 1]),
        ]
        for (first, second) in pairs {
            let view = StateView(initial: 0, bindings: [
                .on(TestEventWithVersionTag.self, where: first) { acc, e in acc += e.num },
                .on(TestEventWithVersionTag.self, where: second) { acc, e in acc -= e.num },
            ])
            // No events: nothing is routed, and the early check lets the view through.
            #expect(try await view.run(on: InMemoryEventStore()) == 0, "\(first) and \(second)")
        }
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

    // Routing follows Clojure's has-bindings?: a bound tag that the event lacks is ignored, only
    // a different value rules a binding out.

    @Test func tagBoundBindingsDoNotSelectEventsWithoutTheTag() async throws {
        let store = InMemoryEventStore()
        try await store.append([MaybeTagged(id: "x", num: 1), MaybeTagged(id: nil, num: 10), MaybeTagged(id: "y", num: 100)])

        // The queries of both bindings ask for a tag value, so neither selects the untagged event.
        let view = StateView(initial: 0, bindings: [
            .on(MaybeTagged.self, where: [.payload("id"): "x"]) { acc, e in acc += e.num },
            .on(MaybeTagged.self, where: [.payload("id"): "y"]) { acc, e in acc += e.num },
        ])
        #expect(try await view.run(on: store) == 101)
    }

    @Test func anEventWithoutTheTagMatchesEveryTaggedBindingOfItsType() async throws {
        let store = InMemoryEventStore()
        try await store.append([MaybeTagged(id: nil, num: 10)])
        // Selected through the unbound binding, but the tagged one isn't ruled out either.
        // Like Clojure, that makes two matching bindings, which is an error.
        let view = StateView(initial: 0, bindings: [
            .on(MaybeTagged.self) { acc, e in acc += e.num },
            .on(MaybeTagged.self, where: [.payload("id"): "x"]) { acc, e in acc -= e.num },
        ])
        do {
            _ = try await view.run(on: store)
            Issue.record("expected two matching bindings")
        } catch LedjerError.expectedExactlyOneBinding(_, let matching) {
            #expect(matching == 2)
        }
    }

    @Test func aDifferentValueRulesABindingOut() async throws {
        let store = InMemoryEventStore()
        try await store.append([MaybeTagged(id: "z", num: 10)])
        let view = StateView(initial: 0, bindings: [
            .on(MaybeTagged.self) { acc, e in acc += e.num },
            .on(MaybeTagged.self, where: [.payload("id"): "x"]) { acc, e in acc -= e.num },
        ])
        #expect(try await view.run(on: store) == 10)
    }
}
