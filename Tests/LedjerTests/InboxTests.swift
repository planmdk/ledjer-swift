import Foundation
import Testing

@testable import Ledjer

@Suite struct InboxTests {
    @Test func sendWaitsForRoomAndKeepsOrder() async throws {
        let inbox = Inbox<Int>(capacity: 2)
        await inbox.send(1)
        await inbox.send(2)
        let sent = Counter()
        let blocked = Task {
            await inbox.send(3)
            sent.increment()
        }
        try await Task.sleep(for: .milliseconds(50))
        #expect(sent.value == 0, "the third send waits: the inbox is full")

        #expect(await inbox.receive() == 1)
        await blocked.value
        #expect(sent.value == 1)
        #expect(await inbox.receive() == 2)
        #expect(await inbox.receive() == 3)
    }

    @Test func controlMessagesComeFirstAndNeverWait() async {
        let inbox = Inbox<Int>(capacity: 1)
        await inbox.send(1)
        inbox.post(98)
        inbox.post(99)
        #expect(await inbox.receive() == 98)
        #expect(await inbox.receive() == 99)
        #expect(await inbox.receive() == 1)
    }

    @Test func aWaitingReceiverGetsWhatArrives() async throws {
        let inbox = Inbox<Int>(capacity: 1)
        let received = Task { await inbox.receive() }
        try await Task.sleep(for: .milliseconds(20))
        await inbox.send(7)
        #expect(await received.value == 7)

        let second = Task { await inbox.receive() }
        try await Task.sleep(for: .milliseconds(20))
        inbox.post(8)
        #expect(await second.value == 8)
    }

    @Test func closingReleasesBlockedSendersAndEndsTheReceiver() async throws {
        let inbox = Inbox<Int>(capacity: 1)
        await inbox.send(1)
        let blocked = Task { await inbox.send(2) }
        try await Task.sleep(for: .milliseconds(20))
        inbox.close()
        await blocked.value  // released, not hanging
        await inbox.send(3)  // dropped
        inbox.post(4)  // dropped
        #expect(await inbox.receive() == 1)  // what was queued is still delivered
        #expect(await inbox.receive() == nil)

        let waiting = Inbox<Int>(capacity: 1)
        let receiver = Task { await waiting.receive() }
        try await Task.sleep(for: .milliseconds(20))
        waiting.close()
        #expect(await receiver.value == nil)
    }
}

@Suite struct InboxStressTests {
    /// Several producers, a poster and one consumer at full speed: everything arrives exactly
    /// once, each producer's messages in order, and control messages overtake data.
    @Test func everythingArrivesOnceAndInOrder() async throws {
        let inbox = Inbox<Int>(capacity: 3)
        let producers = 4
        let perProducer = 500
        // Producer p sends p * 1_000_000 + i. Control messages are negative.
        let controls = 100

        let consumer = Task { () -> [Int] in
            var received: [Int] = []
            while received.count < producers * perProducer + controls, let message = await inbox.receive() {
                received.append(message)
            }
            return received
        }
        await withTaskGroup(of: Void.self) { group in
            for p in 0..<producers {
                group.addTask {
                    for i in 0..<perProducer { await inbox.send(p * 1_000_000 + i) }
                }
            }
            group.addTask {
                for i in 1...controls {
                    inbox.post(-i)
                    await Task.yield()
                }
            }
        }
        let received = await consumer.value
        #expect(received.count == producers * perProducer + controls)
        #expect(Set(received).count == received.count, "a message was delivered twice")
        for p in 0..<producers {
            let mine = received.filter { $0 / 1_000_000 == p && $0 >= 0 }
            #expect(mine == (0..<perProducer).map { p * 1_000_000 + $0 }, "producer \(p) lost or reordered messages")
        }
        #expect(received.filter { $0 < 0 } == (1...controls).map { -$0 }, "control messages are in order")
    }

    /// Closing in the middle of the traffic never hangs a sender or the receiver, and never duplicates a message.
    @Test func closingInTheMiddleReleasesEveryone() async throws {
        for round in 0..<50 {
            let inbox = Inbox<Int>(capacity: 2)
            let consumer = Task { () -> [Int] in
                var received: [Int] = []
                while let message = await inbox.receive() { received.append(message) }
                return received
            }
            let producers = Task {
                await withTaskGroup(of: Void.self) { group in
                    for p in 0..<3 {
                        group.addTask { for i in 0..<200 { await inbox.send(p * 1000 + i) } }
                    }
                    group.addTask { for i in 1...50 { inbox.post(-i) } }
                }
            }
            try await Task.sleep(for: .microseconds(round * 40))
            inbox.close()
            await producers.value  // every sender was released
            let received = await consumer.value  // and the receiver saw the end
            #expect(Set(received).count == received.count, "round \(round): a message was delivered twice")
        }
    }
}
