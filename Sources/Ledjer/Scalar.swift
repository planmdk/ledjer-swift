import Foundation

/// A scalar value usable as a tag value or a metadata value.
///
/// Scalars compare by type *and* value, so `.string("1")` never equals
/// `.int(1)`, and a UUID never equals its string representation.
public enum Scalar: Hashable, Sendable {
    case string(String)
    case int(Int)
    case bool(Bool)
    case uuid(UUID)
}

extension Scalar {
    /// A type-prefixed string encoding, e.g. `uuid:6F9…`, `int:42`. Stores
    /// can use it to match on type and value with plain string equality.
    public var encoded: String {
        switch self {
        case .string(let v): "string:\(v)"
        case .int(let v): "int:\(v)"
        case .bool(let v): "bool:\(v)"
        case .uuid(let v): "uuid:\(v.uuidString)"
        }
    }

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
public typealias Metadata = [String: Scalar]
