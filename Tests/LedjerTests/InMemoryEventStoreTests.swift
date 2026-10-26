import Ledjer
import LedjerStoreContract
import Testing

@Suite struct InMemoryEventStoreTests {
    static let make: StoreFactory = { InMemoryEventStore() }

    @Test func positionsStartAtOne() async throws { try await StoreContract.positionsStartAtOne(Self.make) }
    @Test func sourceWithUUIDTags() async throws { try await StoreContract.sourceWithUUIDTags(Self.make) }
    @Test func sourceSelectsByTags() async throws { try await StoreContract.sourceSelectsByTagsOfTheEventsType(Self.make) }
    @Test func metadataRoundTrips() async throws { try await StoreContract.metadataRoundTrips(Self.make) }
    @Test func appendConditions() async throws { try await StoreContract.appendConditions(Self.make) }
    @Test func subscribeFromNow() async throws { try await StoreContract.subscribeFromNow(Self.make) }
    @Test func subscribeWithStartPosition() async throws { try await StoreContract.subscribeWithStartPosition(Self.make) }
    @Test func subscribeTermination() async throws {
        try await StoreContract.subscribeTerminationDoesNotDisturbAppends(Self.make)
    }
}
