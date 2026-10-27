// Identity of the strings that queries match on: event types, tag names and tag values.
//
// Swift's `String` is equal to another when the two are *canonically equivalent*, so the precomposed
// "café" (U+00E9) equals "cafe\u{301}", and they hash alike. The Clojure library, and the GRDB store, which
// compares the UTF-8 bytes, treat them as different values. Everything that decides whether an event
// matches (and the sets and dictionaries that deduplicate query selectors) therefore compares strings by
// their UTF-8 bytes instead, so that all stores select the same events.

extension String {
    /// Whether `self` and `other` have the same UTF-8 bytes.
    func isByteEqual(to other: String) -> Bool {
        // Equal bytes are always canonically equivalent, so the cheap test can rule most pairs out.
        self == other && utf8.elementsEqual(other.utf8)
    }

    /// Hashes the UTF-8 bytes (and their number), consistently with `isByteEqual(to:)`.
    func hashBytes(into hasher: inout Hasher) {
        hasher.combine(utf8.count)
        let done: Void? = utf8.withContiguousStorageIfAvailable {
            hasher.combine(bytes: UnsafeRawBufferPointer($0))
        }
        if done == nil {
            for byte in utf8 { hasher.combine(byte) }
        }
    }
}
