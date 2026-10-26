// swift-tools-version:6.1
import PackageDescription

let package = Package(
    name: "Ledjer",
    platforms: [
        .iOS(.v16),
        .macOS(.v13),
    ],
    products: [
        // Core: events, queries, views, automations, in-memory store. No dependencies.
        .library(name: "Ledjer", targets: ["Ledjer"]),
        // GRDB (SQLite) event store and accumulator. Pulls in GRDB.
        .library(name: "LedjerGRDB", targets: ["LedjerGRDB"]),
    ],
    dependencies: [
        .package(url: "https://github.com/groue/GRDB.swift", from: "7.5.0"),
    ],
    targets: [
        .target(name: "Ledjer"),
        .target(
            name: "LedjerGRDB",
            dependencies: [
                "Ledjer",
                .product(name: "GRDB", package: "GRDB.swift"),
            ]
        ),
        // Shared, framework-agnostic store contract checks. Not a product.
        .target(name: "LedjerStoreContract", dependencies: ["Ledjer"]),
        .testTarget(
            name: "LedjerTests",
            dependencies: ["Ledjer", "LedjerStoreContract"]
        ),
        .testTarget(
            name: "LedjerGRDBTests",
            dependencies: ["Ledjer", "LedjerGRDB", "LedjerStoreContract"]
        ),
    ]
)
