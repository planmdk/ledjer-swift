import Foundation

/// Something that makes an automation run.
///
/// A trigger is a description, not a running thing: each automation that
/// uses it starts its own listener, so a trigger can be shared freely.
public struct Trigger: Sendable {
    enum Kind: Sendable {
        case events(Query)
        case signal(@Sendable () -> AsyncStream<Void>)
        case periodic(Duration)
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

    /// Fires for each element of the stream `make` returns. `make` is called
    /// once per automation that uses the trigger, when the automation starts,
    /// so it should return a fresh stream each time. A stream that several
    /// automations share would split its elements between them.
    public static func signal(_ make: @escaping @Sendable () -> AsyncStream<Void>) -> Trigger {
        Trigger(kind: .signal(make))
    }

    /// Fires every `interval`, from when the automation starts until it stops.
    public static func periodic(every interval: Duration) -> Trigger {
        Trigger(kind: .periodic(interval))
    }
}

/// A running automation. Keep the handle for as long as it should run: it stops
/// when stopped with `stop()`, or when the handle is dropped.
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

    deinit { stop() }
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
            case .signal(let make):
                let stream = make()
                forwarders.append(Task {
                    for await _ in stream { fire.yield() }
                })
            case .periodic(let interval):
                forwarders.append(Task {
                    while !Task.isCancelled {
                        try? await Task.sleep(for: interval)
                        if Task.isCancelled { break }
                        fire.yield()
                    }
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
