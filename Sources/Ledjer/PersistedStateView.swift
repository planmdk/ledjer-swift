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

    /// Like `commit(_:through:)`, and what the view actually calls. `checkpoint` is the
    /// position the view saw committed last, `nil` if none: the position its batch continues from.
    ///
    /// An accumulator that can check it should throw `PersistedAccumulatorConflict`, without
    /// applying anything, if its own position is not `checkpoint`. That fences off a stale view,
    /// e.g. one that was closed on a timeout while a commit of it was still on its way, from
    /// applying events that a newer view has handled since. The view doesn't retry a conflict, with
    /// one exception: if a *retry* of a commit that failed finds the accumulator at exactly `position`,
    /// the view takes it that the failed attempt did land and only its acknowledgement was lost, and
    /// treats the commit as done. (So an accumulator may report such a conflict, with `actual: position`,
    /// instead of applying a batch twice.) The default calls `commit(_:through:)`.
    func commit(_ operations: [Operation], after checkpoint: Position?, through position: Position) async throws

    /// Called when the view closes, for releasing resources. Does nothing by default.
    func close() async
}

extension PersistedAccumulator {
    public func commit(_ operations: [Operation], after checkpoint: Position?, through position: Position) async throws {
        try await commit(operations, through: position)
    }

    public func close() async {}
}

/// An accumulator refused a commit because its position is not the one the committing view expected:
/// another view has committed since.
public struct PersistedAccumulatorConflict: Error, CustomStringConvertible {
    public let expected: Position?
    public let actual: Position?

    public init(expected: Position?, actual: Position?) {
        self.expected = expected
        self.actual = actual
    }

    public var description: String {
        "Commit conflict: expected the accumulator at \(expected.map(String.init) ?? "nothing"), "
            + "but it is at \(actual.map(String.init) ?? "nothing")"
    }
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
    /// Events per commit. At least 1.
    public var maxSize = 64
    /// The longest an applied event waits for its commit.
    public var maxInterval: Duration = .milliseconds(200)
    /// How often a failed commit is retried. At least 0.
    public var commitRetries = 3
    /// The delay before the first retry, doubled for each following retry.
    public var commitBackoff: Duration = .milliseconds(100)
    /// How long `close()` waits for the view to flush and stop before it
    /// gives up waiting and closes the accumulator anyway.
    public var closeTimeout: Duration = .seconds(5)
    /// Called once, after the view has stopped on a failure. The failure is
    /// also thrown by `accumulator()` and `awaitPosition`, and available as `failure`.
    /// By default it is logged (`ErrorReporter.default`); `nil` silences it.
    public var onError: (@Sendable (any Error) -> Void)? = ErrorReporter.default

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
    /// The view has been closed.
    case closed

    public var description: String {
        switch self {
        case .subscribe(let start, let underlying):
            "Persisted view subscription from \(start) failed: \(underlying.map { "\($0)" } ?? "closed unexpectedly")"
        case .reduce(let type, let position, let underlying):
            "Persisted view reducer failed on \(type) at \(position): \(underlying)"
        case .commit(let position, let attempts, let underlying):
            "Persisted view commit through \(position) failed after \(attempts) attempts: \(underlying)"
        case .closed:
            "The persisted view is closed"
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
    /// must be closed. If the view can't start, `start` closes the accumulator and throws.
    public func start<A: PersistedAccumulator>(
        on store: some EventStore,
        accumulator: A,
        options: PersistedViewOptions = .init()
    ) async throws -> PersistedViewHandle<A> where A.Operation == Operation {
        // The accumulator is the view's from here on, also when the view doesn't get to start.
        do {
            let query = try query(for: bindings.map(\.selector))
            let engine = PersistedViewEngine(
                store: store,
                routes: bindings.map { (selector: $0.selector, handler: $0.apply) },
                query: query,
                accumulator: accumulator,
                options: options
            )
            try await engine.begin()
            return PersistedViewHandle(engine)
        } catch {
            await accumulator.close()
            throw error
        }
    }
}

// MARK: - Running view

/// A running persisted view. Failures stop it: it never skips an event.
/// Once failed, `accumulator()` and `awaitPosition` throw the failure. Recover
/// by closing the view and starting a new one, which resumes after the
/// accumulator's last committed position.
///
/// Call `close()` when done. A handle that is dropped without being closed
/// shuts the view down in the background.
public final class PersistedViewHandle<Accumulator: PersistedAccumulator>: Sendable {
    private let engine: PersistedViewEngine<Accumulator>

    init(_ engine: PersistedViewEngine<Accumulator>) {
        self.engine = engine
    }

    deinit {
        // The engine doesn't reference the handle, so it can outlive it for as long as this takes.
        let engine = self.engine
        Task { await engine.shutdown() }
    }

    /// The accumulator. Throws the view's failure if it has failed, and
    /// `PersistedViewError.closed` once the view is closed (the accumulator has been closed too).
    public func accumulator() throws -> Accumulator {
        try engine.accumulator()
    }

    /// Waits until the view has committed every event it applies up to and
    /// including `position`, e.g. as returned by `append`. Returns immediately
    /// if `position` is `nil` or no such event applies to the view.
    ///
    /// Throws the view's failure, `PersistedViewError.awaitTimedOut`, or
    /// `PersistedViewError.closed` if the view is, or gets, closed.
    public func awaitPosition(_ position: Position?, timeout: Duration = .seconds(1)) async throws {
        try await engine.awaitPosition(position, timeout: timeout)
    }

    /// The failure that stopped the view, if any. Unlike `accumulator()`, still
    /// available after `close()`, e.g. for a failure of its final flush.
    public var failure: (any Error)? { engine.failure }

    /// Stops the view, and closes the accumulator.
    ///
    /// Stopping takes priority over events that are queued up: the view commits
    /// the events it has already applied, and leaves the rest for the next
    /// run, which resumes after the accumulator's last committed position. Waits
    /// for that at most `options.closeTimeout`. Closing more than once, or
    /// concurrently, shares one shutdown: every call returns once it is done.
    ///
    /// A failure of the final commit doesn't throw: see `failure` and `options.onError`.
    ///
    /// If the worker doesn't stop within the timeout, for instance because a commit is stuck,
    /// `close()` cancels it and returns anyway: it starts no further commit, but one already
    /// in flight can't be called back. An accumulator that fences its commits (see
    /// `PersistedAccumulator.commit(_:after:through:)`, `GRDBAccumulator` does) refuses it if a newer
    /// view has committed in the meantime.
    public func close() async {
        await engine.shutdown()
    }
}

/// What the worker task writes and the handle reads.
final class PersistedViewState: @unchecked Sendable {
    private let lock = NSLock()
    private var _failure: (any Error)?
    private var _closed = false
    private var _committed: Position?

    /// The position of the last event the accumulator has committed.
    var committed: Position? { lock.withLock { _committed } }
    func setCommitted(_ position: Position?) { lock.withLock { _committed = position } }

    var failure: (any Error)? { lock.withLock { _failure } }

    /// Throws `PersistedViewError.closed` after the view has been closed, otherwise its failure, if any.
    func check() throws {
        try lock.withLock {
            if _closed { throw PersistedViewError.closed }
            if let _failure { throw _failure }
        }
    }

    func markClosed() { lock.withLock { _closed = true } }

    /// Records the first failure. Returns whether this was it.
    func fail(_ error: any Error) -> Bool {
        lock.withLock {
            guard _failure == nil else { return false }
            _failure = error
            return true
        }
    }
}

/// The machinery of a running persisted view. Tasks capture the engine and
/// never the handle, so dropping the handle can shut the view down.
final class PersistedViewEngine<Accumulator: PersistedAccumulator>: @unchecked Sendable {
    typealias Route = (selector: EventSelector, handler: @Sendable (StoredEvent) throws -> [Accumulator.Operation])

    enum Message: Sendable {
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
    private let state = PersistedViewState()

    private let lock = NSLock()
    private var inbox: Inbox<Message>?
    private var worker: Task<Void, Never>?
    private var pump: Task<Void, Never>?
    private var shutdownTask: Task<Void, Never>?

    init(store: any EventStore, routes: [Route], query: Query, accumulator: Accumulator, options: PersistedViewOptions) {
        self.store = store
        self.routes = routes
        self.query = query
        self.acc = accumulator
        var options = options
        options.maxSize = max(1, options.maxSize)
        options.commitRetries = max(0, options.commitRetries)
        self.options = options
    }

    // MARK: Handle operations

    var failure: (any Error)? { state.failure }

    func accumulator() throws -> Accumulator {
        try state.check()
        return acc
    }

    func awaitPosition(_ position: Position?, timeout: Duration) async throws {
        try state.check()
        guard let position else { return }
        // The last event the view applies up to `position`: one lookup, without reading events.
        guard let target = try await store.lastPosition(matching: query, through: position) else { return }

        // The worker records what it commits, so waiting doesn't touch the database.
        let clock = ContinuousClock()
        let deadline = clock.now + timeout
        while true {
            try state.check()
            let committed = state.committed
            if let committed, committed >= target { return }
            if clock.now > deadline {
                throw PersistedViewError.awaitTimedOut(position: position, target: target, committed: committed)
            }
            try await Task.sleep(for: .milliseconds(10))
        }
    }

    /// Stops the view and closes the accumulator, once, however often it is called.
    func shutdown() async {
        let task: Task<Void, Never> = lock.withLock {
            if let shutdownTask { return shutdownTask }
            state.markClosed()
            let task = Task { [self] in
                let (inbox, worker, pump) = lock.withLock { (self.inbox, self.worker, self.pump) }
                // Control lane: the worker takes it before any queued event.
                inbox?.post(.stop)
                if let worker {
                    let finished = await waitForCompletion(of: worker, timeout: options.closeTimeout)
                    // A worker that is still going must not commit after this: the accumulator is
                    // about to be closed, and a new view may take over from the checkpoint.
                    if !finished { worker.cancel() }
                }
                pump?.cancel()
                inbox?.close()
                await acc.close()
            }
            shutdownTask = task
            return task
        }
        await task.value
    }

    // MARK: Running

    func begin() async throws {
        let committed = try await acc.sequenceNumber
        let start = committed.map { $0 + 1 } ?? 0
        let stream: AsyncThrowingStream<StoredEvent, any Error>
        do {
            stream = try await store.subscribe(query, startPosition: start)
        } catch {
            throw PersistedViewError.subscribe(startPosition: start, underlying: error)
        }
        state.setCommitted(committed)
        // Bounded, so that a worker busy committing holds the pump back, which holds back the subscription.
        let inbox = Inbox<Message>(capacity: options.maxSize)
        let pump = Task.detached {
            do {
                // Once the worker has closed the inbox nobody reads: stop reading the log too.
                for try await event in stream {
                    if !(await inbox.send(.event(event))) { return }
                }
                await inbox.send(.subscriptionEnded(nil))
            } catch {
                await inbox.send(.subscriptionEnded(error))
            }
        }
        let worker = Task { [self] in
            defer { inbox.close() }
            do {
                try await work(inbox, startPosition: start)
            } catch where Task.isCancelled {
                // A shutdown that gave up waiting cancelled the worker, and `close()` has returned: the
                // view is closed, so whatever this worker ends with (the cancellation itself, or a stale
                // commit refused by the fence because a new view took over) is not its failure to report.
                // A CancellationError thrown while the worker was not cancelled is a failure, below.
            } catch {
                if state.fail(error) {
                    pump.cancel()
                    options.onError?(error)
                }
            }
        }
        lock.withLock {
            self.inbox = inbox
            self.pump = pump
            self.worker = worker
        }
    }

    /// Applies events as they arrive and commits them in batches, until told
    /// to stop or something fails. All batching state is local to this one loop.
    private func work(_ inbox: Inbox<Message>, startPosition: Position) async throws {
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
            state.setCommitted(position)
            operations = []
            count = 0
            lastPosition = nil
        }

        defer { timer?.cancel() }
        while let message = await inbox.receive() {
            try Task.checkCancellation()
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
                        if Task.isCancelled { return }  // a commit got there first
                        inbox.post(.flush(generation: current))
                    }
                }
            case .flush(let flushed):
                if timer != nil, flushed == generation { try await commit() }
            case .subscriptionEnded(let error):
                throw PersistedViewError.subscribe(startPosition: startPosition, underlying: error)
            case .stop:
                try await commit()
                return
            }
        }
    }

    private func commitWithRetries(_ operations: [Accumulator.Operation], through position: Position) async throws {
        var attempt = 1
        var backoff = options.commitBackoff
        // What this view last saw committed. Only this worker changes it, between commits.
        let checkpoint = state.committed
        while true {
            // Never start a commit once the view has given up on its worker.
            try Task.checkCancellation()
            do {
                try await acc.commit(operations, after: checkpoint, through: position)
                return
            } catch {
                // Our own cancellation ends the view quietly. A CancellationError that the accumulator
                // throws for its own reasons doesn't: it is a failed commit like any other.
                if error is CancellationError, Task.isCancelled { throw error }
                // A retry that finds the accumulator exactly at `position` means that an earlier attempt
                // did land, and only its acknowledgement was lost: the commit is done, not in conflict
                // with itself.
                if attempt > 1, let conflict = error as? PersistedAccumulatorConflict, conflict.actual == position {
                    return
                }
                // Any other conflict doesn't go away by trying again.
                if error is PersistedAccumulatorConflict || attempt > options.commitRetries {
                    throw PersistedViewError.commit(position: position, attempts: attempt, underlying: error)
                }
                try await Task.sleep(for: backoff)
                backoff = backoff * 2
                attempt += 1
            }
        }
    }
}

/// Waits for `task` to finish, but at most `timeout`. Returns whether it finished.
/// The task is left running if it didn't, and so is the waiter on it, until it does.
private func waitForCompletion(of task: Task<Void, Never>, timeout: Duration) async -> Bool {
    /// Resumes its continuation with whichever result arrives first.
    final class Once: @unchecked Sendable {
        private let lock = NSLock()
        private var continuation: CheckedContinuation<Bool, Never>?
        init(_ continuation: CheckedContinuation<Bool, Never>) { self.continuation = continuation }
        func resume(_ finished: Bool) {
            let pending = lock.withLock { () -> CheckedContinuation<Bool, Never>? in
                defer { continuation = nil }
                return continuation
            }
            pending?.resume(returning: finished)
        }
    }
    return await withCheckedContinuation { (continuation: CheckedContinuation<Bool, Never>) in
        let once = Once(continuation)
        let timer = Task {
            try? await Task.sleep(for: timeout)
            once.resume(false)
        }
        Task {
            await task.value
            timer.cancel()
            once.resume(true)
        }
    }
}
