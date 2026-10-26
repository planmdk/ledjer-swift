import Foundation

/// Something that makes an automation run.
public struct Trigger: Sendable {
    enum Kind: Sendable {
        case events(Query)
        case signal(AsyncStream<Void>)
    }
    let kind: Kind

    /// Fires for each new `E` event.
    public static func event<E: Event>(_ type: E.Type) -> Trigger {
        Trigger(kind: .events(Query(QueryItem(unchecked: [EventTypeInfo(type)], tagBindings: [:]))))
    }

    /// Fires for each new `E` event with the given tag values.
    public static func event<E: Event>(_ type: E.Type, where tagBindings: TagBindings) throws -> Trigger {
        Trigger(kind: .events(try Query(QueryItem([type], where: tagBindings))))
    }

    /// Fires for each element of `stream`, e.g. `Trigger.periodic(every:)`.
    public static func signal(_ stream: AsyncStream<Void>) -> Trigger {
        Trigger(kind: .signal(stream))
    }

    /// Fires every `interval`, until the automation stops.
    public static func periodic(every interval: Duration) -> Trigger {
        let (stream, continuation) = AsyncStream<Void>.makeStream()
        let ticker = Task {
            while !Task.isCancelled {
                try? await Task.sleep(for: interval)
                if Task.isCancelled { break }
                continuation.yield()
            }
            continuation.finish()
        }
        continuation.onTermination = { _ in ticker.cancel() }
        return .signal(stream)
    }
}

/// A running automation. It runs until stopped, so keep the handle and call `stop()`.
public final class Automation: Sendable {
    private let tasks: [Task<Void, Never>]
    private let signals: AsyncStream<Void>.Continuation

    init(tasks: [Task<Void, Never>], signals: AsyncStream<Void>.Continuation) {
        self.tasks = tasks
        self.signals = signals
    }

    /// Stops listening for triggers. A run of the body in progress is cancelled.
    public func stop() {
        signals.finish()
        tasks.forEach { $0.cancel() }
    }
}

/// Starts an automation: whenever a trigger fires, `body` runs with the event store.
///
/// The body typically reads state views and appends events. Runs of the
/// body never overlap. Subscriptions are in place when this returns, so
/// events appended afterwards trigger the automation.
///
/// A body that throws is reported to `onError`, and the automation keeps running.
public func automation(
    on store: some EventStore,
    triggers: [Trigger],
    onError: @escaping @Sendable (any Error) -> Void = { _ in },
    body: @escaping @Sendable (any EventStore) async throws -> Void
) async throws -> Automation {
    let (signals, fire) = AsyncStream<Void>.makeStream()
    var forwarders: [Task<Void, Never>] = []
    do {
        for trigger in triggers {
            switch trigger.kind {
            case .events(let query):
                let stream = try await store.subscribe(query, startPosition: nil)
                forwarders.append(Task {
                    do { for try await _ in stream { fire.yield() } } catch { onError(error) }
                })
            case .signal(let stream):
                forwarders.append(Task {
                    for await _ in stream { fire.yield() }
                })
            }
        }
    } catch {
        forwarders.forEach { $0.cancel() }
        throw error
    }
    let anyStore: any EventStore = store
    let runner = Task {
        for await _ in signals {
            if Task.isCancelled { break }
            do { try await body(anyStore) } catch { onError(error) }
        }
    }
    return Automation(tasks: forwarders + [runner], signals: fire)
}
