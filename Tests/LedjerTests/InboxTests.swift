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
