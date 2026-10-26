import Foundation

// MARK: - Accumulator

/// Where a persisted view keeps its state, e.g. a database table.
///
/// The view hands the accumulator batches of operations along with the
/// position of the last event the batch covers. The accumulator must make
/// the operations and the position durable together, so that after a crash
/// `sequenceNumber` says exactly which events are reflected in the state.
public protocol PersistedAccumulator: Sendable {
    /// What reducers emit and `commit` applies, e.g. "insert this row".
    associatedtype Operation: Sendable

    /// The position of the last committed event, `nil` for a fresh accumulator.
    var sequenceNumber: Position? { get async throws }

    /// Applies `operations` and records `position` as the last handled event, atomically.
    ///
    /// Once this returns, the committed state must be readable and
    /// `sequenceNumber` must return `position`. If it throws, the view may
    /// retry the same commit.
    func commit(_ operations: [Operation], through position: Position) async throws

    /// Called when the view closes, for releasing resources. Does nothing by default.
    func close() async
}

extension PersistedAccumulator {
    public func close() async {}
}

// MARK: - Definition

/// One reducer of a persisted view: which events it applies to, and the
/// operations it emits for them. Reducers are pure, they don't do IO.
public struct PersistedStateViewBinding<Operation: Sendable>: Sendable {
    let selector: EventSelector
    let apply: @Sendable (StoredEvent) throws -> [Operation]

    public static func on<E: Event>(
        _ type: E.Type,
        where tagBindings: TagBindings = [:],
        apply: @escaping @Sendable (E) throws -> [Operation]
    ) -> PersistedStateViewBinding {
        PersistedStateViewBinding(selector: EventSelector(type, where: tagBindings)) { stored in
            try apply(try stored.decode(E.self))
        }
    }

    public static func on<E: Event>(
        _ type: E.Type,
        where tagBindings: TagBindings = [:],
        apply: @escaping @Sendable (E, StoredEvent) throws -> [Operation]
    ) -> PersistedStateViewBinding {
        PersistedStateViewBinding(selector: EventSelector(type, where: tagBindings)) { stored in
            try apply(try stored.decode(E.self), stored)
        }
    }
}

public struct PersistedViewOptions: Sendable {
    /// Events per commit.
    public var maxSize = 64
    /// The longest an applied event waits for its commit.
    public var maxInterval: Duration = .milliseconds(200)
    /// How often a failed commit is retried.
    public var commitRetries = 3
    /// The delay before the first retry, doubled for each following retry.
    public var commitBackoff: Duration = .milliseconds(100)
    /// Called once, after the view has stopped on a failure. The failure is
    /// also thrown by `accumulator()` and `awaitPosition`.
    public var onError: (@Sendable (any Error) -> Void)?

    public init() {}
}

/// Why a persisted view failed.
public enum PersistedViewError: Error, CustomStringConvertible {
    /// Subscribing failed, or the subscription ended unexpectedly (`underlying == nil`).
    case subscribe(startPosition: Position, underlying: (any Error)?)
    /// No (or more than one) binding applies to an event, or a reducer threw. Events are never skipped.
    case reduce(eventType: EventType, position: Position, underlying: any Error)
    /// Committing failed after all retries.
    case commit(position: Position, attempts: Int, underlying: any Error)
    /// `awaitPosition` timed out.
    case awaitTimedOut(position: Position, target: Position, committed: Position?)

    public var description: String {
        switch self {
        case .subscribe(let start, let underlying):
            "Persisted view subscription from \(start) failed: \(underlying.map { "\($0)" } ?? "closed unexpectedly")"
        case .reduce(let type, let position, let underlying):
            "Persisted view reducer failed on \(type) at \(position): \(underlying)"
        case .commit(let position, let attempts, let underlying):
            "Persisted view commit through \(position) failed after \(attempts) attempts: \(underlying)"
        case .awaitTimedOut(let position, let target, let committed):
            "Timed out awaiting position \(position) (target \(target), committed \(committed.map(String.init) ?? "nothing"))"
        }
    }
}

/// A persisted state view: instead of reducing to a value on each call, it
/// keeps an accumulator up to date from a subscription.
///
/// ```swift
/// let totals = PersistedStateView<MyOp>(bindings: [
///     .on(OrderPlaced.self) { [.insert($0)] },
/// ])
/// let view = try await totals.start(on: store, accumulator: acc)
/// ...
/// await view.close()
/// ```
public struct PersistedStateView<Operation: Sendable>: Sendable {
    let bindings: [PersistedStateViewBinding<Operation>]

    public init(bindings: [PersistedStateViewBinding<Operation>]) {
        self.bindings = bindings
    }

    /// Starts keeping `accumulator` up to date with the events of `store`,
    /// resuming after the accumulator's last committed position.
    ///
    /// Events are committed every `options.maxSize` events or
    /// `options.maxInterval`, whichever comes first. The returned handle
    /// must be closed.
    public func start<A: PersistedAccumulator>(
        on store: some EventStore,
        accumulator: A,
        options: PersistedViewOptions = .init()
    ) async throws -> PersistedViewHandle<A> where A.Operation == Operation {
        let query = try query(for: bindings.map(\.selector))
        let handle = PersistedViewHandle(
            store: store,
            routes: bindings.map { (selector: $0.selector, handler: $0.apply) },
            query: query,
            accumulator: accumulator,
            options: options
        )
        try await handle.begin()
        return handle
    }
}

// MARK: - Running view

/// A running persisted view. Failures stop it: it never skips an event.
/// Once failed, `accumulator()` and `awaitPosition` throw the failure. Recover
/// by closing the view and starting a new one, which resumes after the
/// accumulator's last committed position.
public actor PersistedViewHandle<Accumulator: PersistedAccumulator> {
    typealias Route = (selector: EventSelector, handler: @Sendable (StoredEvent) throws -> [Accumulator.Operation])

    private enum Message: Sendable {
        case event(StoredEvent)
        case flush(generation: Int)
        case subscriptionEnded((any Error)?)
        case stop
    }

    private let store: any EventStore
    private let routes: [Route]
    private let query: Query
    private let acc: Accumulator
    private let options: PersistedViewOptions

    private var failure: (any Error)?
    private var closed = false
    private var startPosition: Position = 0
    private var inbox: AsyncStream<Message>.Continuation?
    private var worker: Task<Void, Never>?
    private var pump: Task<Void, Never>?

    init(store: any EventStore, routes: [Route], query: Query, accumulator: Accumulator, options: PersistedViewOptions) {
        self.store = store
        self.routes = routes
        self.query = query
        self.acc = accumulator
        self.options = options
    }

    // MARK: Public

    /// The accumulator. Throws the view's failure if it has failed.
    public func accumulator() throws -> Accumulator {
        if let failure { throw failure }
        return acc
    }

    /// Waits until the view has committed every event it applies up to and
    /// including `position`, e.g. as returned by `append`. Returns immediately
    /// if `position` is `nil` or no such event applies to the view.
    ///
    /// Throws the view's failure, or `PersistedViewError.awaitTimedOut`.
    public func awaitPosition(_ position: Position?, timeout: Duration = .seconds(1)) async throws {
        guard let position else { return }
        let committed = try await acc.sequenceNumber
        let pending = try await store.source(query, startPosition: committed.map { $0 + 1 } ?? 0)
        guard let target = pending.map(\.position).filter({ $0 <= position }).max() else { return }

        let clock = ContinuousClock()
        let deadline = clock.now + timeout
        while true {
            if let failure { throw failure }
            let current = try await acc.sequenceNumber
            if let current, current >= target { return }
            if clock.now > deadline {
                throw PersistedViewError.awaitTimedOut(position: position, target: target, committed: current)
            }
            try await Task.sleep(for: .milliseconds(10))
        }
    }

    /// Flushes the uncommitted events, stops the view and closes the
    /// accumulator. Closing more than once does nothing.
    public func close() async {
        guard !closed else { return }
        closed = true
        inbox?.yield(.stop)
        await worker?.value
        pump?.cancel()
        inbox?.finish()
        await acc.close()
    }

    // MARK: Internals

    func begin() async throws {
        let committed = try await acc.sequenceNumber
        let start = committed.map { $0 + 1 } ?? 0
        startPosition = start
        let stream: AsyncThrowingStream<StoredEvent, any Error>
        do {
            stream = try await store.subscribe(query, startPosition: start)
        } catch {
            throw PersistedViewError.subscribe(startPosition: start, underlying: error)
        }
        let (messages, inbox) = AsyncStream<Message>.makeStream()
        self.inbox = inbox
        pump = Task.detached {
            do {
                for try await event in stream { inbox.yield(.event(event)) }
                inbox.yield(.subscriptionEnded(nil))
            } catch {
                inbox.yield(.subscriptionEnded(error))
            }
        }
        worker = Task {
            do {
                try await self.work(messages, inbox: inbox)
            } catch {
                self.fail(error)
            }
        }
    }

    private func fail(_ error: any Error) {
        guard failure == nil else { return }
        failure = error
        pump?.cancel()
        options.onError?(error)
    }

    /// Applies events as they arrive and commits them in batches, until told
    /// to stop or something fails. All batching state is local to this one loop.
    private nonisolated func work(_ messages: AsyncStream<Message>, inbox: AsyncStream<Message>.Continuation) async throws {
        var operations: [Accumulator.Operation] = []
        var count = 0
        var lastPosition: Position?
        var timer: Task<Void, Never>?
        var generation = 0

        func commit() async throws {
            timer?.cancel()
            timer = nil
            guard let position = lastPosition else { return }
            try await commitWithRetries(operations, through: position)
            operations = []
            count = 0
            lastPosition = nil
        }

        defer { timer?.cancel() }
        for await message in messages {
            switch message {
            case .event(let event):
                do {
                    operations += try route(event, among: routes)(event)
                } catch {
                    throw PersistedViewError.reduce(eventType: event.type, position: event.position, underlying: error)
                }
                count += 1
                lastPosition = event.position
                if count >= options.maxSize {
                    try await commit()
                } else if timer == nil {
                    generation += 1
                    let current = generation
                    let interval = options.maxInterval
                    timer = Task {
                        try? await Task.sleep(for: interval)
                        inbox.yield(.flush(generation: current))
                    }
                }
            case .flush(let flushed):
                if timer != nil, flushed == generation { try await commit() }
            case .subscriptionEnded(let error):
                throw PersistedViewError.subscribe(startPosition: await startPosition, underlying: error)
            case .stop:
                try await commit()
                return
            }
        }
    }

    private nonisolated func commitWithRetries(_ operations: [Accumulator.Operation], through position: Position) async throws {
        var attempt = 1
        var backoff = options.commitBackoff
        while true {
            do {
                try await acc.commit(operations, through: position)
                return
            } catch {
                if error is CancellationError || attempt > options.commitRetries {
                    throw PersistedViewError.commit(position: position, attempts: attempt, underlying: error)
                }
                try await Task.sleep(for: backoff)
                backoff = backoff * 2
                attempt += 1
            }
        }
    }
}
