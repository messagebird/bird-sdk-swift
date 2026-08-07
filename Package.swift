// swift-tools-version: 6.0
import PackageDescription

// The package name matches the mirror repo (github.com/messagebird/bird-sdk-swift),
// which is the identity SwiftPM resolves a URL dependency by, so a second
// product (a generated `Bird` API SDK alongside this hand-written realtime
// client) is additive here and never renames what consumers already reference.
//
// Tools version 6.0 is load-bearing twice over: it turns on the Swift 6 language
// mode, and it is what makes SwiftPM link the bundled swift-testing that
// Tests/ imports. Dropping to 5.x makes the test suite unbuildable.
let package = Package(
    name: "bird-sdk-swift",
    platforms: [
        .iOS(.v15),
        .macOS(.v12),
        .tvOS(.v15),
        .watchOS(.v8),
    ],
    products: [
        .library(name: "BirdRealtime", targets: ["BirdRealtime"])
    ],
    targets: [
        .target(name: "BirdRealtime"),
        // A target but deliberately not a product: `swift run BirdRealtimeDemo`
        // still drives a live edge from this checkout, while a consumer resolving
        // the package sees only the library.
        .executableTarget(
            name: "BirdRealtimeDemo",
            dependencies: ["BirdRealtime"]
        ),
        .testTarget(
            name: "BirdRealtimeTests",
            dependencies: ["BirdRealtime"]
        ),
    ]
)
