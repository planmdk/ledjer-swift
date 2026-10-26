import Foundation
@testable import Ledjer
import Testing

private struct Marker: Error, CustomStringConvertible {
    var description: String { "ReportedMarker" }
}

private let failingView = PersistedStateView<Add>(bindings: [
    .on(Counted.self) { _ in throw Marker() },
])

/// Runs `body` with standard error redirected into a pipe, and returns what was written to it.
/// Only for tests that don't run in parallel with others that write to standard error.
private func captureStandardError(_ body: () async throws -> Void) async throws -> String {
    let pipe = Pipe()
    let saved = dup(STDERR_FILENO)
    dup2(pipe.fileHandleForWriting.fileDescriptor, STDERR_FILENO)
    do {
        try await body()
    } catch {
        dup2(saved, STDERR_FILENO)
        close(saved)
        throw error
    }
    dup2(saved, STDERR_FILENO)
    close(saved)
    try pipe.fileHandleForWriting.close()
    return String(decoding: pipe.fileHandleForReading.readDataToEndOfFile(), as: UTF8.self)
}

@Suite struct ErrorReporterTests {
    @Test func writesTheErrorToTheHandle() throws {
        let pipe = Pipe()
        ErrorReporter.write(Marker(), to: pipe.fileHandleForWriting)
        try pipe.fileHandleForWriting.close()
        let written = String(decoding: pipe.fileHandleForReading.readDataToEndOfFile(), as: UTF8.self)
        #expect(written == "ledjer: ReportedMarker\n")
    }

    /// `FileHandle.write(_: Data)` would raise an exception here, and take the process down on Apple platforms.
    @Test func aFailingWriteIsNotACrash() throws {
        let pipe = Pipe()
        let closed = pipe.fileHandleForWriting
        try closed.close()
        ErrorReporter.write(Marker(), to: closed)
    }

    @Test func failuresAreReportedByDefault() {
        #expect(PersistedViewOptions().onError != nil)
    }
}

#if !canImport(os)
// Where the default is standard error, that can be observed.
@Suite(.serialized) struct DefaultErrorReportingTests {
    @Test func aFailedPersistedViewReportsByDefault() async throws {
        let store = InMemoryEventStore()
        let output = try await captureStandardError {
            let view = try await failingView.start(on: store, accumulator: TotalsAccumulator())
            try await store.append([Counted(id: "a", num: 1)])
            try await eventually { view.failure != nil }
            try await Task.sleep(for: .milliseconds(100))
            await view.close()
        }
        #expect(output.contains("ledjer:"), "got: \(output)")
    }

    @Test func aFailedAutomationBodyReportsByDefault() async throws {
        let store = InMemoryEventStore()
        let output = try await captureStandardError {
            let running = try await automation(on: store, triggers: [.event(Counted.self)]) { _ in throw Marker() }
            try await store.append([Counted(id: "a", num: 1)])
            try await Task.sleep(for: .milliseconds(200))
            running.stop()
        }
        #expect(output.contains("ledjer: ReportedMarker"), "got: \(output)")
    }

    @Test func gtReportsNothingForAFailureItThrows() async throws {
        let output = try await captureStandardError {
            do {
                try await gt(view: failingView, accumulator: TotalsAccumulator(), given: [Counted(id: "a", num: 1)], then: { _ in true })
                Issue.record("expected the scenario to throw")
            } catch is PersistedViewError {}
        }
        #expect(!output.contains("ledjer:"), "got: \(output)")
    }
}
#endif
