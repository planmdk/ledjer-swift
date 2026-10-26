import Foundation

/// A bounded, single-consumer message queue with a priority lane.
///
/// - `send` is for data: it suspends while `capacity` messages are queued, so
///   a producer reading from a subscription stops reading when the consumer
///   falls behind. This is what carries backpressure from a slow commit all
///   the way back to the database.
/// - `post` is for control messages: it never waits, and the consumer
///   receives control messages before data.
///
/// `close` ends it: blocked senders are released, and `receive` returns
/// `nil` once the queues are empty.
final class Inbox<Message: Sendable>: @unchecked Sendable {
    private let lock = NSLock()
    private let capacity: Int
    private var control: [Message] = []
    private var data: [Message] = []
    private var blocked: [(message: Message, sender: CheckedContinuation<Void, Never>)] = []
    private var receiver: CheckedContinuation<Message?, Never>?
    private var closed = false

    init(capacity: Int) {
        self.capacity = max(1, capacity)
    }

    /// Queues a control message, ahead of all data.
    func post(_ message: Message) {
        lock.lock()
        if closed {
            lock.unlock()
            return
        }
        if let waiting = receiver {
            receiver = nil
            lock.unlock()
            waiting.resume(returning: message)
        } else {
            control.append(message)
            lock.unlock()
        }
    }

    /// Queues a data message, waiting for room.
    func send(_ message: Message) async {
        await withCheckedContinuation { (sender: CheckedContinuation<Void, Never>) in
            lock.lock()
            if closed {
                lock.unlock()
                sender.resume()
            } else if let waiting = receiver {
                // A waiting receiver means both queues are empty.
                receiver = nil
                lock.unlock()
                waiting.resume(returning: message)
                sender.resume()
            } else if data.count < capacity {
                data.append(message)
                lock.unlock()
                sender.resume()
            } else {
                blocked.append((message, sender))
                lock.unlock()
            }
        }
    }

    /// The next message: control before data. Waits if there is none, and
    /// returns `nil` once closed and drained.
    func receive() async -> Message? {
        await withCheckedContinuation { (continuation: CheckedContinuation<Message?, Never>) in
            lock.lock()
            if !control.isEmpty {
                let message = control.removeFirst()
                lock.unlock()
                continuation.resume(returning: message)
            } else if !data.isEmpty {
                let message = data.removeFirst()
                // Room for the longest-blocked sender.
                var released: CheckedContinuation<Void, Never>?
                if !blocked.isEmpty {
                    let next = blocked.removeFirst()
                    data.append(next.message)
                    released = next.sender
                }
                lock.unlock()
                released?.resume()
                continuation.resume(returning: message)
            } else if closed {
                lock.unlock()
                continuation.resume(returning: nil)
            } else {
                receiver = continuation
                lock.unlock()
            }
        }
    }

    func close() {
        lock.lock()
        closed = true
        let senders = blocked
        blocked = []
        let waiting = receiver
        receiver = nil
        lock.unlock()
        senders.forEach { $0.sender.resume() }
        waiting?.resume(returning: nil)
    }
}
