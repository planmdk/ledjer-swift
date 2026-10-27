import Foundation

/// Something that makes an automation run.
///
/// A trigger is a description, not a running thing: each automation that
/// uses it starts its own listener, so a trigger can be shared freely.
public struct Trigger: Sendable {
    enum Kind: Sendable {
        case events(EventQuery)
        case signal(@Sendable () -> AsyncStream<Void>)
        case periodic(Duration)
    }
    let kind: Kind

    /// Fires for each new `E` event.
    public static func event<E: Event>(_ type: E.Type) -> Trigger {
        Trigger(kind: .events(EventQuery(EventQueryItem(unchecked: [EventTypeInfo(type)], tagBindings: [:]))))
    }

    /// Fires for each new `E` event with the given tag values.
    public static func event<E: Event>(_ type: E.Type, where tagBindings: TagBindings) throws -> Trigger {
        Trigger(kind: .events(try EventQuery(EventQueryItem([type], where: tagBindings))))
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

public enum AutomationError: Error, CustomStringConvertible {
    /// An event subscription ended without an error, so its trigger can't fire any more.
    case subscriptionEnded

    public var description: String {
        switch self {
        case .subscriptionEnded: "An event subscription of the automation ended"
        }
    }
}

/// Lets a periodic or signal trigger leave at most one run waiting for the body, however slow it is.
///
/// Event triggers don't use it: each event is a run, as in Clojure. A periodic tick or a signal says
/// "run now", which one pending run says just as well as a hundred, and an unbounded backlog of
/// them would run back to back, and grow, for as long as the body is slow.
final class TickGate: @unchecked Sendable {
    private let lock = NSLock()
    private var pending = false

    /// Whether a tick should be passed on: not while an earlier one still waits.
    func arm() -> Bool {
        lock.withLock {
            if pending { return false }
            pending = true
            return true
        }
    }

    /// The pending tick is being served: the next one is a new one.
    func clear() { lock.withLock { pending = false } }
}

/// A running automation. Keep the handle for as long as it should run: it stops
/// when stopped with `stop()`, when its event subscription fails, or when the handle is dropped.
public final class Automation: @unchecked Sendable {
    private let lock = NSLock()
    private var tasks: [Task<Void, Never>] = []
    private var running = true
    private let signals: AsyncStream<TickGate?>.Continuation

    init(signals: AsyncStream<TickGate?>.Continuation) {
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

    /// Stops listening for triggers, and cancels the run of the body in progress. No further body starts.
    ///
    /// Returns without waiting for that run, as `stop` of the Clojure library does (which doesn't interrupt it
    /// at all): a body that ignores cancellation can still be running afterwards. Bodies should honour
    /// cancellation and be idempotent, since runs of different automations (e.g. a new one started right
    /// after this stop) can overlap.
    public func stop() {
        let toCancel = lock.withLock {
            running = false
            defer { tasks = [] }
            return tasks
        }
        // Cancel first: elements already buffered in `signals` are still delivered after `finish()`,
        // and a body that starts then must find itself cancelled.
        toCancel.forEach { $0.cancel() }
        signals.finish()
    }

    deinit { stop() }
}

/// Starts an automation: whenever a trigger fires, `body` runs with the event store.
///
/// The body typically reads state views and appends events. Runs of the
/// body never overlap (within this automation). Each event of an event trigger is a run. Periodic and
/// signal triggers coalesce: ticks that arrive while the body is busy (or while
/// a run is already waiting) make one more run, not one each. Subscriptions are in place when this returns, so
/// events appended afterwards trigger the automation.
///
/// A body that throws is reported to `onError` (by default logged, see
/// `ErrorReporter.default`), and the automation keeps
/// running. Errors that come of the automation being stopped, a body or a
/// subscription cancelled in flight, are not reported. If an event subscription fails, or ends, that is
/// reported to `onError` and the whole automation stops (`isRunning` becomes `false`),
/// rather than carrying on deaf to some of its triggers.
public func automation(
    on store: some EventStore,
    triggers: [Trigger],
    onError: @escaping @Sendable (any Error) -> Void = ErrorReporter.default,
    body: @escaping @Sendable (any EventStore) async throws -> Void
) async throws -> Automation {
    let (signals, fire) = AsyncStream<TickGate?>.makeStream()
    let handle = Automation(signals: fire)
    do {
        for trigger in triggers {
            switch trigger.kind {
            case .events(let query):
                let stream = try await store.subscribe(query, startPosition: nil)
                handle.add(Task { [weak handle] in
                    do {
                        for try await _ in stream { fire.yield(nil) }
                        // The stream is over without anyone having stopped us: this trigger would never fire again.
                        if Task.isCancelled { return }
                        onError(AutomationError.subscriptionEnded)
                        handle?.stop()
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
                let gate = TickGate()
                handle.add(Task {
                    for await _ in stream where gate.arm() { fire.yield(gate) }
                })
            case .periodic(let interval):
                let gate = TickGate()
                handle.add(Task {
                    while !Task.isCancelled {
                        try? await Task.sleep(for: interval)
                        if Task.isCancelled { break }
                        if gate.arm() { fire.yield(gate) }
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
        for await gate in signals {
            if Task.isCancelled { break }
            gate?.clear()  // ticks that come while the body runs make one more run, not one each
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
