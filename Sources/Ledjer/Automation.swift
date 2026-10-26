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
/// when stopped with `stop()`, when its event subscription fails, or when the handle is dropped.
public final class Automation: @unchecked Sendable {
    private let lock = NSLock()
    private var tasks: [Task<Void, Never>] = []
    private var running = true
    private let signals: AsyncStream<Void>.Continuation

    init(signals: AsyncStream<Void>.Continuation) {
        self.signals = signals
    }

    fileprivate func add(_ task: Task<Void, Never>) {
        let stopped = lock.withLock {
            if running { tasks.append(task) }
            return !running
        }
        if stopped { task.cancel() }
    }

    /// Whether the automation still listens for triggers.
    public var isRunning: Bool { lock.withLock { running } }

    /// Stops listening for triggers. A run of the body in progress is cancelled.
    public func stop() {
        let toCancel = lock.withLock {
            running = false
            defer { tasks = [] }
            return tasks
        }
        signals.finish()
        toCancel.forEach { $0.cancel() }
    }

    deinit { stop() }
}

/// Starts an automation: whenever a trigger fires, `body` runs with the event store.
///
/// The body typically reads state views and appends events. Runs of the
/// body never overlap. Subscriptions are in place when this returns, so
/// events appended afterwards trigger the automation.
///
/// A body that throws is reported to `onError`, and the automation keeps
/// running. Errors that come of the automation being stopped, a body or a
/// subscription cancelled in flight, are not reported. If an event subscription fails, the failure is reported to
/// `onError` and the whole automation stops (`isRunning` becomes `false`),
/// rather than carrying on deaf to some of its triggers.
public func automation(
    on store: some EventStore,
    triggers: [Trigger],
    onError: @escaping @Sendable (any Error) -> Void = { _ in },
    body: @escaping @Sendable (any EventStore) async throws -> Void
) async throws -> Automation {
    let (signals, fire) = AsyncStream<Void>.makeStream()
    let handle = Automation(signals: fire)
    do {
        for trigger in triggers {
            switch trigger.kind {
            case .events(let query):
                let stream = try await store.subscribe(query, startPosition: nil)
                handle.add(Task { [weak handle] in
                    do {
                        for try await _ in stream { fire.yield() }
                    } catch {
                        // Stopping cancels this task, and whatever the store was doing then fails with
                        // it: that is the stop, not a failure.
                        if Task.isCancelled { return }
                        onError(error)
                        handle?.stop()
                    }
                })
            case .signal(let make):
                let stream = make()
                handle.add(Task {
                    for await _ in stream { fire.yield() }
                })
            case .periodic(let interval):
                handle.add(Task {
                    while !Task.isCancelled {
                        try? await Task.sleep(for: interval)
                        if Task.isCancelled { break }
                        fire.yield()
                    }
                })
            }
        }
    } catch {
        handle.stop()
        throw error
    }
    let anyStore: any EventStore = store
    handle.add(Task {
        for await _ in signals {
            if Task.isCancelled { break }
            do {
                try await body(anyStore)
            } catch {
                // A body that was cancelled by `stop()` ends however it ends, e.g. with a
                // CancellationError or its URLSession's cancellation. Not a failure.
                if !Task.isCancelled { onError(error) }
            }
        }
    })
    return handle
}
