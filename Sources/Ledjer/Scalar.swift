import Foundation

/// A scalar value usable as a tag value or a metadata value.
///
/// Scalars compare by type *and* value, so `.string("1")` never equals
/// `.int(1)`, and a UUID never equals its string representation.
///
/// Strings compare by their UTF-8 bytes, not by Unicode canonical equivalence as Swift's `String` does, so
/// `.string("café")` (precomposed) differs from `.string("cafe\u{301}")`, as in the Clojure library and the GRDB store.
public enum Scalar: Hashable, Sendable {
    case string(String)
    case int(Int)
    case bool(Bool)
    case uuid(UUID)

    public static func == (lhs: Scalar, rhs: Scalar) -> Bool {
        switch (lhs, rhs) {
        case (.string(let a), .string(let b)): a.isByteEqual(to: b)
        case (.int(let a), .int(let b)): a == b
        case (.bool(let a), .bool(let b)): a == b
        case (.uuid(let a), .uuid(let b)): a == b
        default: false
        }
    }

    public func hash(into hasher: inout Hasher) {
        switch self {
        case .string(let v):
            hasher.combine(0)
            v.hashBytes(into: &hasher)
        case .int(let v):
            hasher.combine(1)
            hasher.combine(v)
        case .bool(let v):
            hasher.combine(2)
            hasher.combine(v)
        case .uuid(let v):
            hasher.combine(3)
            hasher.combine(v)
        }
    }
}

extension Scalar {
    /// A type-prefixed string encoding, e.g. `uuid:6F9…`, `int:42`. Stores
    /// can use it to match on type and value with plain string equality.
    ///
    /// Public for third-party `EventStore`s: they have to persist the tags and the metadata of events, and
    /// this is the encoding that `Scalar`'s `Codable` conformance and the GRDB store use. Compare the
    /// result by bytes (see the notes on `Scalar`), not with a collation that folds case or normalizes.
    public var encoded: String {
        switch self {
        case .string(let v): "string:\(v)"
        case .int(let v): "int:\(v)"
        case .bool(let v): "bool:\(v)"
        case .uuid(let v): "uuid:\(v.uuidString)"
        }
    }

    /// The inverse of `encoded`. `nil` if `encoded` isn't one.
    public init?(encoded: String) {
        guard let colon = encoded.firstIndex(of: ":") else { return nil }
        let prefix = encoded[..<colon]
        let rest = String(encoded[encoded.index(after: colon)...])
        switch prefix {
        case "string": self = .string(rest)
        case "int":
            guard let v = Int(rest) else { return nil }
            self = .int(v)
        case "bool":
            guard let v = Bool(rest) else { return nil }
            self = .bool(v)
        case "uuid":
            guard let v = UUID(uuidString: rest) else { return nil }
            self = .uuid(v)
        default: return nil
        }
    }
}

extension Scalar: Codable {
    public init(from decoder: any Decoder) throws {
        let string = try decoder.singleValueContainer().decode(String.self)
        guard let scalar = Scalar(encoded: string) else {
            throw DecodingError.dataCorrupted(
                .init(codingPath: decoder.codingPath, debugDescription: "Invalid scalar: \(string)")
            )
        }
        self = scalar
    }

    public func encode(to encoder: any Encoder) throws {
        var container = encoder.singleValueContainer()
        try container.encode(encoded)
    }
}

/// Types that can be used as tag values.
public protocol ScalarConvertible: Sendable {
    var scalar: Scalar { get }
}

extension String: ScalarConvertible { public var scalar: Scalar { .string(self) } }
extension Int: ScalarConvertible { public var scalar: Scalar { .int(self) } }
extension Bool: ScalarConvertible { public var scalar: Scalar { .bool(self) } }
extension UUID: ScalarConvertible { public var scalar: Scalar { .uuid(self) } }
extension Scalar: ScalarConvertible { public var scalar: Scalar { self } }

/// Event metadata. Ledjer adds `version` to every event's metadata.
///
/// A plain dictionary, so its *keys* are Swift strings: canonically equivalent spellings of a key are one key
/// (they never take part in matching, but `EventTag.metadata(_:)` looks its key up here). The same
/// goes for every store, since tags are computed before an event reaches it. Values are `Scalar`s, which are exact.
public typealias Metadata = [String: Scalar]
