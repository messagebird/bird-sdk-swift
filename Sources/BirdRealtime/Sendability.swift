import Foundation

/// Carries a decoded JSON payload across a queue hop.
///
/// Event payloads are `Any?`, the same opaque shape the sibling clients expose
/// (`unknown` in TypeScript, `JsonElement` in Kotlin), so the compiler cannot
/// prove them `Sendable`, but the values are Foundation objects produced by
/// `JSONSerialization` and never written after decode. The box is the one place
/// that promise is made, rather than spreading `@unchecked` across every
/// delivery site. Never put mutable state in here.
struct UncheckedSendableBox<Value>: @unchecked Sendable {
    let value: Value

    init(_ value: Value) {
        self.value = value
    }
}
