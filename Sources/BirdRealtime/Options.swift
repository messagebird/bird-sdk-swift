import Foundation
// URLRequest/URLSession live in FoundationNetworking on non-Apple platforms.
#if canImport(FoundationNetworking)
import FoundationNetworking
#endif

/// Connection state, mirroring the JS client's vocabulary.
public enum ConnectionState: String, Sendable {
    case initialized
    case connecting
    case connected
    case unavailable
    case disconnected
    case failed
}

/// The auth payload for a private/presence subscribe, produced by the
/// customer's backend (which holds the app secret and computes the signature).
public struct ChannelAuth: Sendable {
    public var auth: String
    public var memberData: String?

    public init(auth: String, memberData: String? = nil) {
        self.auth = auth
        self.memberData = memberData
    }
}

/// Authorizes a private/presence subscription. The default implementation
/// POSTs `{connection_id, channel_name}` to `authEndpoint`.
public typealias Authorizer = @Sendable (
    _ connectionId: String, _ channelName: String
) async throws -> ChannelAuth

/// The payload from the customer's member-auth endpoint. Unlike channel
/// authorization, `memberData` is required — it is the identity itself.
public struct MemberAuth: Sendable {
    public var auth: String
    /// JSON string carrying `member_id` and optional `member_info`.
    public var memberData: String

    public init(auth: String, memberData: String) {
        self.auth = auth
        self.memberData = memberData
    }
}

/// Authorizes this connection's identity. The default implementation POSTs
/// `{connection_id}` to `memberAuthEndpoint`.
public typealias MemberAuthorizer = @Sendable (
    _ connectionId: String
) async throws -> MemberAuth

/// The member this connection signed in as.
public struct SignedInMember: @unchecked Sendable {
    public let memberId: String
    /// The customer-defined info blob, if the signed identity carried one.
    /// Opaque `Any?` like the event payloads; see `UncheckedSendableBox`.
    public let memberInfo: Any?

    public init(memberId: String, memberInfo: Any? = nil) {
        self.memberId = memberId
        self.memberInfo = memberInfo
    }
}

/// A presence-channel member.
public struct Member: @unchecked Sendable {
    public let memberId: String
    /// The customer-defined info blob, if the identity carried one.
    /// Opaque `Any?` like the event payloads; see `UncheckedSendableBox`.
    public let memberInfo: Any?

    public init(memberId: String, memberInfo: Any? = nil) {
        self.memberId = memberId
        self.memberInfo = memberInfo
    }
}

/// Root of every error this SDK throws, so a caller can catch one type and
/// branch on the specific ones that carry structure. The other Bird SDKs express
/// the same hierarchy as a class tree (ADR-0042 section 1); these are value
/// types, so the root is a protocol.
public protocol BirdRealtimeError: Error, Sendable {
    var message: String { get }
}

/// A connection-level error from the server or the SDK.
public struct RealtimeError: BirdRealtimeError {
    /// The server's close/error code, when there is one (e.g. 4004 over
    /// quota, 4009 terminated by the API). Nil for SDK-detected failures.
    ///
    /// A wire code only. An HTTP status from an authorization round-trip is not
    /// one of these and arrives as `RealtimeAuthError.status`, so a caller
    /// branching on 4009 can never be handed a 403.
    public var code: Int?
    public var message: String
}

/// The customer's authorization endpoint failed or answered something unusable.
/// Carries where it went and what came back, because the fix is almost always in
/// that endpoint rather than in this client.
public struct RealtimeAuthError: BirdRealtimeError {
    /// The endpoint the default authorizer called.
    public var endpoint: URL
    /// HTTP status of the response, or 0 when the request never completed.
    public var status: Int
    public var message: String

    public init(endpoint: URL, status: Int, message: String) {
        self.endpoint = endpoint
        self.status = status
        self.message = message
    }
}

/// Client configuration. `appKey` plus either `region` or `wsHost` is required.
public struct BirdRealtimeOptions: Sendable {
    /// The app key from the Bird dashboard.
    public var appKey: String
    /// The app's region (`us1` / `eu1`), used to derive the WebSocket host.
    public var region: String?
    /// Explicit WebSocket host, overriding the region-derived one.
    public var wsHost: String?
    /// Endpoint POSTed to for private/presence subscribe authorization.
    public var authEndpoint: URL?
    /// Extra headers for the default authorizer's request (e.g. a session token).
    public var authHeaders: [String: String]
    /// Custom authorizer, replacing the endpoint round-trip entirely.
    public var authorizer: Authorizer?
    /// Endpoint POSTed to for signin (member identity) authorization.
    public var memberAuthEndpoint: URL?
    /// Custom signin authorizer, replacing the endpoint round-trip entirely.
    public var memberAuthorizer: MemberAuthorizer?
    /// Idle interval before the client pings, unless the server's handshake
    /// supplied its own. Seconds.
    public var activityTimeout: TimeInterval
    /// How long to wait for the pong before declaring the socket dead. Seconds.
    public var pongTimeout: TimeInterval
    /// Allow `ws://` (no TLS) for loopback hosts only — local development.
    public var allowInsecure: Bool

    public init(
        appKey: String,
        region: String? = nil,
        wsHost: String? = nil,
        authEndpoint: URL? = nil,
        authHeaders: [String: String] = [:],
        authorizer: Authorizer? = nil,
        memberAuthEndpoint: URL? = nil,
        memberAuthorizer: MemberAuthorizer? = nil,
        activityTimeout: TimeInterval = 120,
        pongTimeout: TimeInterval = 30,
        allowInsecure: Bool = false
    ) {
        self.appKey = appKey
        self.region = region
        self.wsHost = wsHost
        self.authEndpoint = authEndpoint
        self.authHeaders = authHeaders
        self.authorizer = authorizer
        self.memberAuthEndpoint = memberAuthEndpoint
        self.memberAuthorizer = memberAuthorizer
        self.activityTimeout = activityTimeout
        self.pongTimeout = pongTimeout
        self.allowInsecure = allowInsecure
    }
}
