import Foundation
#if canImport(os)
import os
#endif

/// Ready-made `onError` handlers.
public enum ErrorReporter {
    /// The default `onError` of persisted views and automations: a failure is never silent by default.
    ///
    /// In Clojure the equivalent failures end in the thread's uncaught exception handler, which prints
    /// them. Here that is the unified log (`os.Logger`, subsystem `dk.planm.ledjer`, error level) on
    /// Apple platforms, where standard error doesn't reach the device logs of a shipped app, and
    /// `standardError` elsewhere. Pass your own handler to log somewhere else (a restart is a good
    /// thing for it to trigger), or `nil` to a persisted view's options to silence it.
    public static let `default`: @Sendable (any Error) -> Void = {
        #if canImport(os)
        let logger = Logger(subsystem: "dk.planm.ledjer", category: "errors")
        return { error in
            logger.error("ledjer: \(String(describing: error), privacy: .public)")
        }
        #else
        return standardError
        #endif
    }()

    /// Writes `ledjer: <error>` to standard error.
    public static let standardError: @Sendable (any Error) -> Void = { error in
        write(error, to: standardErrorHandle)
    }

    private static let handleLock = NSLock()
    nonisolated(unsafe) private static var handle = FileHandle.standardError

    /// What `standardError` writes to. Tests point it at a file: redirecting the process's own
    /// descriptor would swallow the output of every test running in parallel, and block them once
    /// whatever reads it falls behind.
    static var standardErrorHandle: FileHandle {
        get { handleLock.withLock { handle } }
        set { handleLock.withLock { handle = newValue } }
    }

    /// `FileHandle.write(_: Data)` raises an Objective-C exception, which Swift can't catch, when the
    /// write fails (a closed descriptor, a broken pipe). Reporting an error must not crash the app.
    static func write(_ error: any Error, to handle: FileHandle) {
        try? handle.write(contentsOf: Data("ledjer: \(error)\n".utf8))
    }
}
