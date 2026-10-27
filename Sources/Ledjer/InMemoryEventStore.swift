import Foundation

/// An event store that keeps everything in memory. Meant for tests (it is
/// what `gwt` and `gt` use) and as the reference for store behavior.
public actor InMemoryEventStore: EventStore {
    private struct Entry {
        let event: StoredEvent
        let tags: [TagPath: Scalar]
    }

    private struct Subscriber {
        let query: EventQuery
        /// Events before this position aren't delivered.
        let minimumPosition: Position
        let continuation: AsyncThrowingStream<StoredEvent, any Error>.Continuation

        func wants(_ entry: Entry) -> Bool {
            entry.event.position >= minimumPosition
                && query.matches(type: entry.event.type, tags: entry.tags)
        }
    }

    private var entries: [Entry] = []
    private var subscribers: [UUID: Subscriber] = [:]

    public init() {}

    public func source(_ query: EventQuery, startPosition: Position?) async throws -> [StoredEvent] {
        let start = startPosition ?? 0
        return entries
            .filter { $0.event.position >= start && query.matches(type: $0.event.type, tags: $0.tags) }
            .map(\.event)
    }

    public func lastPosition(matching query: EventQuery, through position: Position) async throws -> Position? {
        entries.last { $0.event.position <= position && query.matches(type: $0.event.type, tags: $0.tags) }?.event.position
    }

    /// Every event from `startPosition` (inclusive) on, whatever its type.
    public func allEvents(startPosition: Position? = nil) -> [StoredEvent] {
        let start = startPosition ?? 0
        return entries.map(\.event).filter { $0.position >= start }
    }

    public func subscribe(
        _ query: EventQuery,
        startPosition: Position?
    ) async throws -> AsyncThrowingStream<StoredEvent, any Error> {
        let (stream, continuation) = AsyncThrowingStream<StoredEvent, any Error>.makeStream()
        // Replay and registration happen in one actor turn, so no event is
        // missed or delivered twice. A start position beyond the end of the
        // log simply delivers events from that position on.
        let subscriber = Subscriber(
            query: query,
            minimumPosition: startPosition ?? entries.count + 1,
            continuation: continuation
        )
        for entry in entries where subscriber.wants(entry) {
            continuation.yield(entry.event)
        }
        let id = UUID()
        subscribers[id] = subscriber
        continuation.onTermination = { [weak self] _ in
            Task { await self?.removeSubscriber(id) }
        }
        return stream
    }

    @discardableResult
    public func append(_ events: [NewEvent], condition: AppendCondition?) async throws -> Position? {
        if let condition {
            let after = condition.after ?? 0
            let conflict = entries.contains {
                $0.event.position > after && condition.query.matches(type: $0.event.type, tags: $0.tags)
            }
            if conflict { throw LedjerError.appendConditionFailed }
        }
        let appended = events.enumerated().map { offset, event in
            Entry(
                event: StoredEvent(
                    position: entries.count + offset + 1,
                    type: event.type,
                    metadata: event.metadata,
                    payload: event.payload
                ),
                tags: event.tags
            )
        }
        entries.append(contentsOf: appended)
        for subscriber in subscribers.values {
            for entry in appended where subscriber.wants(entry) {
                subscriber.continuation.yield(entry.event)
            }
        }
        return appended.last?.event.position
    }

    private func removeSubscriber(_ id: UUID) {
        subscribers[id] = nil
    }
}
