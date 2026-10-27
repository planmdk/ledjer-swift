import Ledjer
import LedjerStoreContract
import Testing

/// A store that checks the condition of an append and appends in separate steps.
private struct RacyStore: EventStore {
    let inner = InMemoryEventStore()

    func source(_ query: Query, startPosition: Position?) async throws -> [StoredEvent] {
        try await inner.source(query, startPosition: startPosition)
    }

    func subscribe(_ query: Query, startPosition: Position?) async throws -> AsyncThrowingStream<StoredEvent, any Error> {
        try await inner.subscribe(query, startPosition: startPosition)
    }

    func append(_ events: [NewEvent], condition: AppendCondition?) async throws -> Position? {
        if let condition {
            let later = try await inner.source(condition.query, startPosition: (condition.after ?? 0) + 1)
            if !later.isEmpty { throw LedjerError.appendConditionFailed }
            await Task.yield()
        }
        return try await inner.append(events, condition: nil)
    }
}

@Suite struct InMemoryEventStoreTests {
    @Test func theContractDetectsANonAtomicConditionalAppend() async throws {
        await #expect(throws: ContractFailure.self) {
            try await StoreContract.concurrentConditionalAppendsAreAtomic { RacyStore() }
        }
    }

    static let make: StoreFactory = { InMemoryEventStore() }

    @Test func positionsStartAtOne() async throws { try await StoreContract.positionsStartAtOne(Self.make) }
    @Test func sourceWithUUIDTags() async throws { try await StoreContract.sourceWithUUIDTags(Self.make) }
    @Test func sourceSelectsByTags() async throws { try await StoreContract.sourceSelectsByTagsOfTheEventsType(Self.make) }
    @Test func metadataRoundTrips() async throws { try await StoreContract.metadataRoundTrips(Self.make) }
    @Test func intAndBoolTags() async throws { try await StoreContract.intAndBoolTags(Self.make) }
    @Test func nulCharactersAreSignificant() async throws { try await StoreContract.nulCharactersAreSignificant(Self.make) }
    @Test func unicodeNormalizationIsSignificant() async throws {
        try await StoreContract.unicodeNormalizationIsSignificant(Self.make)
    }
    @Test func appendConditions() async throws { try await StoreContract.appendConditions(Self.make) }
    @Test func failedConditionalAppendsAreNotDelivered() async throws {
        try await StoreContract.failedConditionalAppendsAreNotDelivered(Self.make)
    }
    @Test func concurrentConditionalAppendsAreAtomic() async throws {
        try await StoreContract.concurrentConditionalAppendsAreAtomic(Self.make)
    }
    @Test func lastPositions() async throws { try await StoreContract.lastPositions(Self.make) }
    @Test func subscribeFromNow() async throws { try await StoreContract.subscribeFromNow(Self.make) }
    @Test func subscribeWithStartPosition() async throws { try await StoreContract.subscribeWithStartPosition(Self.make) }
    @Test func subscribeTermination() async throws {
        try await StoreContract.subscribeTerminationDoesNotDisturbAppends(Self.make)
    }
    @Test func cancellingAConsumerEndsTheStreamWithoutAnError() async throws {
        try await StoreContract.cancellingAConsumerEndsTheStreamWithoutAnError(Self.make)
    }
}
