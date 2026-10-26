import Foundation

/// Ready-made `onError` handlers.
public enum ErrorReporter {
    /// Writes `ledjer: <error>` to standard error.
    ///
    /// The default `onError` of persisted views and automations. In Clojure the equivalent failures
    /// end in the thread's uncaught exception handler, which prints them, so a failure is never silent
    /// by default. Pass your own handler to log elsewhere (a restart is a good thing for it to
    /// trigger), or `nil` to a persisted view's options to silence it.
    public static let standardError: @Sendable (any Error) -> Void = { error in
        FileHandle.standardError.write(Data("ledjer: \(error)\n".utf8))
    }
}
