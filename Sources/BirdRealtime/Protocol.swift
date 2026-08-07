import Foundation
// URLRequest/URLSession live in FoundationNetworking on non-Apple platforms.
#if canImport(FoundationNetworking)
import FoundationNetworking
#endif

/// The Bird Realtime wire protocol. A frame is JSON: `{ event, channel?, data? }`.
/// `data` often arrives as a JSON-encoded string (double-encoded) on the
/// server→client direction; `Frame.decode` normalizes it back to a value.
///
/// System and lifecycle events live under two reserved namespaces — `bird:`
/// (protocol/system) and `bird_internal:` (subscription internals).
/// Application event names are free-form and must not use these prefixes.
public enum BirdProtocol {
    public static let system = "bird:"
    public static let internalPrefix = "bird_internal:"

    /// Events the client sends to the server.
    enum Outbound {
        static let subscribe = "bird:subscribe"
        static let unsubscribe = "bird:unsubscribe"
        static let ping = "bird:ping"
        static let pong = "bird:pong"
        static let signin = "bird:signin"
    }

    /// Events the server sends to the client.
    enum Inbound {
        static let connectionEstablished = "bird:connection_established"
        static let error = "bird:error"
        static let ping = "bird:ping"
        static let pong = "bird:pong"
        static let signinSuccess = "bird:signin_success"
        static let subscriptionSucceeded = "bird_internal:subscription_succeeded"
        static let subscriptionError = "bird_internal:subscription_error"
        static let connectionCount = "bird_internal:connection_count"
        static let memberAdded = "bird_internal:member_added"
        static let memberRemoved = "bird_internal:member_removed"
    }

    /// User-facing lifecycle events, re-emitted from their `bird_internal:` origin.
    public enum Event {
        public static let subscriptionSucceeded = "bird:subscription_succeeded"
        public static let subscriptionError = "bird:subscription_error"
        public static let connectionCount = "bird:connection_count"
        public static let memberAdded = "bird:member_added"
        public static let memberRemoved = "bird:member_removed"
    }

    /// Prefix reserved for client-originated events (`Channel.trigger`).
    public static let clientEventPrefix = "client-"

    /// True for events the SDK handles internally and never surfaces to bindings.
    static func isInternal(_ event: String) -> Bool {
        event.hasPrefix(internalPrefix)
    }

    /// The edge's reserved channel family for events addressed to one signed-in
    /// member. `#` is otherwise an illegal channel character, so these names
    /// cannot be created or subscribed to as ordinary channels: the edge admits
    /// a connection only when the id in the name matches the identity it signed
    /// in as, which is why that subscribe carries no auth payload. The "user"
    /// spelling is fixed upstream and never appears in this SDK's surface.
    static let memberChannelPrefix = "#server-to-user-"

    /// The reserved channel carrying events addressed to `memberId`.
    static func memberChannelName(_ memberId: String) -> String {
        "\(memberChannelPrefix)\(memberId)"
    }
}

/// One wire frame. `data` is any JSON value, held as a decoded Foundation
/// object (`String`, `NSNumber`, `[String: Any]`, `[Any]`, `NSNull`).
struct Frame {
    var event: String
    var channel: String?
    var data: Any?

    /// Encode for the socket. Outbound `data` is sent as a JSON value — the
    /// double-encoding exists only on the server→client direction; a subscribe
    /// with stringified data is rejected by the edge.
    func encoded() -> String? {
        var out: [String: Any] = ["event": event]
        if let channel { out["channel"] = channel }
        if let data { out["data"] = data }
        guard JSONSerialization.isValidJSONObject(out),
              let bytes = try? JSONSerialization.data(withJSONObject: out)
        else { return nil }
        return String(decoding: bytes, as: UTF8.self)
    }

    /// Decode a socket message. `data` may arrive as a JSON-encoded string
    /// (the common case) or already-parsed; both are normalized to a value.
    /// Returns nil for a message that isn't a valid frame.
    static func decode(_ raw: String) -> Frame? {
        guard let bytes = raw.data(using: .utf8),
              let msg = try? JSONSerialization.jsonObject(
                  with: bytes, options: [.fragmentsAllowed]
              ) as? [String: Any],
              let event = msg["event"] as? String
        else { return nil }
        var data = msg["data"]
        if let s = data as? String, let inner = s.data(using: .utf8),
           let parsed = try? JSONSerialization.jsonObject(
               with: inner, options: [.fragmentsAllowed]
           ) {
            data = parsed
        }
        return Frame(event: event, channel: msg["channel"] as? String, data: data)
    }
}
