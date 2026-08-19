import Foundation
// URLRequest/URLSession live in FoundationNetworking on non-Apple platforms.
#if canImport(FoundationNetworking)
import FoundationNetworking
#endif

/// Events addressed to the signed-in member rather than to a channel, as sent
/// by the events API. Bind here after `signin()`. Delivery starts when signin
/// succeeds and resumes after a reconnect once the connection has signed in
/// again; protocol frames never surface.
// Queue-confined, like Channel: bindings are mutated on the client queue only.
public final class MemberEvents: @unchecked Sendable {
    private var bindings: [String: [(BindingToken, @Sendable (Any?) -> Void)]] = [:]
    private var globalBindings: [(BindingToken, @Sendable (String, Any?) -> Void)] = []
    private let deliver: @Sendable (@escaping @Sendable () -> Void) -> Void
    // Public binding calls arrive on the caller's thread and emit() is reached from
    // the delivery queue, so both hop onto this one before touching the bindings.
    private let queue: DispatchQueue

    init(
        queue: DispatchQueue,
        deliver: @escaping @Sendable (@escaping @Sendable () -> Void) -> Void
    ) {
        self.queue = queue
        self.deliver = deliver
    }

    /// Bind a handler to an event addressed to this member.
    @discardableResult
    public func bind(_ event: String, _ handler: @escaping @Sendable (Any?) -> Void) -> BindingToken {
        let token = BindingToken()
        queue.sync { bindings[event, default: []].append((token, handler)) }
        return token
    }

    /// Bind a handler to every event addressed to this member.
    @discardableResult
    public func bindGlobal(_ handler: @escaping @Sendable (String, Any?) -> Void) -> BindingToken {
        let token = BindingToken()
        queue.sync { globalBindings.append((token, handler)) }
        return token
    }

    /// Remove one binding, by the token [bind] returned.
    public func unbind(_ event: String, _ token: BindingToken) {
        queue.sync { bindings[event]?.removeAll { $0.0 == token } }
    }

    /// Remove one global binding, or all of them when nil.
    public func unbindGlobal(_ token: BindingToken? = nil) {
        queue.sync {
            if let token {
                globalBindings.removeAll { $0.0 == token }
            } else {
                globalBindings.removeAll()
            }
        }
    }

    /// Remove every binding for an event, or all bindings when nil.
    public func unbindAll(_ event: String? = nil) {
        queue.sync {
            if let event { bindings[event] = nil } else { bindings.removeAll() }
            if event == nil { globalBindings.removeAll() }
        }
    }

    // Unlike Channel.emit, this one is reached from the DELIVERY queue: the member
    // bus is fed by a global binding on the reserved channel, and channel handlers
    // run on delivery. So it snapshots the handler lists under the client queue
    // before dispatching, or a bind racing an inbound member event is a dictionary
    // race. Safe to hop here precisely because delivery is not the client queue.
    func emit(_ event: String, _ data: Any?) {
        let boxed = UncheckedSendableBox(data)
        let (globals, handlers) = queue.sync {
            (globalBindings, bindings[event] ?? [])
        }
        for (_, handler) in globals { deliver { handler(event, boxed.value) } }
        for (_, handler) in handlers { deliver { handler(boxed.value) } }
    }
}

/// The Bird Realtime client: one connection, a set of channels, re-subscribed
/// whenever the connection is (re)established.
///
///     let bird = BirdRealtime(options: .init(appKey: "app-key", region: "us1"))
///     let channel = bird.subscribe("orders")
///     channel.bind("order-updated") { data in ... }
///
/// Channel handlers and connection observers run on `deliveryQueue` (main by
/// default).
// Queue-confined: `queue` serializes all client + channel state, so the
// stored observers and channel table are never touched concurrently.
public final class BirdRealtime: @unchecked Sendable {
    public static let version = "0.1.1"

    public let appKey: String

    /// Events addressed to the signed-in member. See `signin()`.
    public let member: MemberEvents

    private let connection: Connection
    /// Serializes all client + channel state.
    private let queue = DispatchQueue(label: "com.bird.realtime")
    private let deliveryQueue: DispatchQueue
    private var channels: [String: Channel] = [:]
    private let authorizer: Authorizer
    private var stateObservers: [(BindingToken, @Sendable (ConnectionState, ConnectionState) -> Void)] = []
    private var errorObservers: [(BindingToken, @Sendable (RealtimeError) -> Void)] = []
    private var signinErrorObservers: [(BindingToken, @Sendable (any BirdRealtimeError) -> Void)] = []
    private let memberAuthorizer: MemberAuthorizer
    // Signin is per connection, so `identity` is dropped whenever the connection
    // is, while `signinArmed` survives to re-sign in on the next one.
    private var signinArmed = false
    private var identity: SignedInMember?
    // The reserved channel carrying events addressed to this member.
    // Deliberately NOT in `channels`: subscribeAll() runs on every `connected`
    // and would send it before the connection has an identity, which the edge
    // rejects. It is subscribed from the signin-success path.
    private var memberChannel: Channel?
    private var signinWaiters: [CheckedContinuation<SignedInMember, Error>] = []

    /// - Parameters:
    ///   - options: Client configuration; `appKey` plus `region` or `wsHost`.
    ///   - deliveryQueue: Where handlers run. Defaults to the main queue.
    ///   - transportFactory: Test seam; leave nil for URLSessionWebSocketTask.
    public convenience init(
        options: BirdRealtimeOptions,
        deliveryQueue: DispatchQueue = .main
    ) {
        self.init(options: options, deliveryQueue: deliveryQueue, transportFactory: nil)
    }

    init(
        options: BirdRealtimeOptions,
        deliveryQueue: DispatchQueue = .main,
        transportFactory: TransportFactory?
    ) {
        self.appKey = options.appKey
        self.deliveryQueue = deliveryQueue
        self.member = MemberEvents(
            queue: queue,
            deliver: { work in deliveryQueue.async(execute: work) }
        )
        self.authorizer = options.authorizer ?? BirdRealtime.defaultAuthorizer(
            endpoint: options.authEndpoint,
            headers: options.authHeaders
        )
        self.memberAuthorizer = options.memberAuthorizer
            ?? BirdRealtime.defaultMemberAuthorizer(
                endpoint: options.memberAuthEndpoint,
                headers: options.authHeaders
            )
        let url = try! BirdRealtime.resolveURL(options: options)
        self.connection = Connection(
            url: url,
            queue: queue,
            activityTimeout: options.activityTimeout,
            pongTimeout: options.pongTimeout,
            makeTransport: transportFactory ?? { URLSessionTransport(url: $0) }
        )
        wire()
        connection.connect()
    }

    /// The current connection state.
    public var connectionState: ConnectionState {
        queue.sync { connection.state }
    }

    /// The server-assigned connection id, while connected.
    public var connectionId: String? {
        queue.sync { connection.connectionId }
    }

    /// Observe connection state changes (`previous`, `current`).
    @discardableResult
    public func onConnectionStateChange(
        _ handler: @escaping @Sendable (ConnectionState, ConnectionState) -> Void
    ) -> BindingToken {
        let token = BindingToken()
        queue.async { self.stateObservers.append((token, handler)) }
        return token
    }

    /// Identify this connection's member, so the events API can address it and
    /// the disconnect API can terminate it. Returns the member the backend
    /// signed for; the identity is re-established automatically on every
    /// reconnect, so call it once.
    ///
    ///     let me = try await bird.signin()
    ///     bird.member.bind("order-shipped") { data in ... }
    ///
    /// A pending signin throws on any connection-level error, because the wire
    /// does not attribute an error to the signin that caused it. If the signin
    /// in fact succeeded, `signedInMember` holds the identity and calling
    /// `signin()` again returns it immediately. A re-signin that fails after a
    /// reconnect has no caller to throw to, so it is reported on
    /// `onSigninError` instead.
    @discardableResult
    public func signin() async throws -> SignedInMember {
        try await withCheckedThrowingContinuation { continuation in
            queue.async {
                if let identity = self.identity {
                    self.signinArmed = true
                    continuation.resume(returning: identity)
                    return
                }
                // Waiters are only ever failed by a TRANSITION into a dead state,
                // so one appended while the connection is already dead is resumed
                // by nothing: a terminal refusal and an explicit disconnect() both
                // stay put until the caller reconnects, and this continuation
                // would leak un-resumed. Fail now, and leave signinArmed alone so
                // a later connect() does not sign in for a caller that already
                // threw.
                let state = self.connection.state
                guard state != .failed, state != .disconnected else {
                    continuation.resume(
                        throwing: RealtimeError(
                            code: nil,
                            message: """
                                signin() needs a live connection; the client is \
                                \(state). Call connect() first.
                                """
                        )
                    )
                    return
                }
                self.signinArmed = true
                self.signinWaiters.append(continuation)
                if state == .connected { self.sendSigninLocked() }
            }
        }
    }

    /// The signed-in member, or nil when this connection has no identity.
    public var signedInMember: SignedInMember? {
        queue.sync { identity }
    }

    /// Observe a re-signin failing after a reconnect (no caller to throw to).
    @discardableResult
    public func onSigninError(
        _ handler: @escaping @Sendable (any BirdRealtimeError) -> Void
    ) -> BindingToken {
        let token = BindingToken()
        queue.async { self.signinErrorObservers.append((token, handler)) }
        return token
    }

    /// Observe connection-level errors (refused closes, subscribe rejections).
    @discardableResult
    public func onError(
        _ handler: @escaping @Sendable (RealtimeError) -> Void
    ) -> BindingToken {
        let token = BindingToken()
        queue.async { self.errorObservers.append((token, handler)) }
        return token
    }

    /// Subscribe to a channel (idempotent — an already-subscribed or in-flight
    /// channel is returned as-is). The type follows the name prefix:
    /// `presence-` → `PresenceChannel`, `private-` → `PrivateChannel`.
    @discardableResult
    public func subscribe(_ name: String) -> Channel {
        queue.sync {
            if let existing = channels[name] {
                if connection.state == .connected { sendSubscribeLocked(existing) }
                return existing
            }
            let channel = makeChannel(name)
            channels[name] = channel
            if connection.state == .connected { sendSubscribeLocked(channel) }
            return channel
        }
    }

    /// Unsubscribe and forget a channel.
    public func unsubscribe(_ name: String) {
        queue.async {
            guard let channel = self.channels[name] else { return }
            self.connection.sendLocked(Frame(
                event: BirdProtocol.Outbound.unsubscribe,
                data: ["channel": name]
            ))
            channel.reset()
            self.channels[name] = nil
        }
    }

    /// The channel for `name`, if subscribed.
    public func channel(_ name: String) -> Channel? {
        queue.sync { channels[name] }
    }

    /// (Re)open the connection after a `disconnect()`.
    public func connect() {
        connection.connect()
    }

    /// Close the connection; no reconnect. Channels are retained for a later
    /// `connect()`.
    public func disconnect() {
        connection.disconnect()
    }

    // MARK: - Internals (queue-confined)

    private func wire() {
        connection.onStateChange = { [weak self] previous, current in
            guard let self else { return }
            if current == .connected {
                self.subscribeAllLocked()
                if self.signinArmed { self.sendSigninLocked() }
            }
            // A dropped connection drops every subscription with it; channels
            // stay registered and re-subscribe on the next `connected`.
            if current == .unavailable || current == .disconnected || current == .failed {
                for channel in self.channels.values { channel.reset() }
                self.identity = nil
                self.dropMemberChannelLocked()
                self.failSigninLocked(RealtimeError(
                    code: nil, message: "Connection lost before signin completed"
                ))
            }
            for (_, observer) in self.stateObservers {
                self.deliveryQueue.async { observer(previous, current) }
            }
        }
        connection.onError = { [weak self] error in
            guard let self else { return }
            // Server subscribe rejections arrive as connection-level errors
            // with no channel field; they invalidate in-flight attempts so
            // subscribe() can retry instead of being wedged.
            for channel in self.channels.values { channel.invalidateAttempt() }
            // The member channel is held outside `channels` (see its property),
            // so iterating the map alone leaves the one channel the caller cannot
            // re-subscribe by hand holding a live attempt: a stale authorize
            // could still land and subscribe against a superseded attempt.
            self.memberChannel?.invalidateAttempt()
            // A rejected signin arrives as a channel-less error too, so a
            // pending signin can only be failed on the connection's error.
            self.failSigninLocked(error)
            for (_, observer) in self.errorObservers {
                self.deliveryQueue.async { observer(error) }
            }
        }
        connection.onFrame = { [weak self] frame in
            guard let self else { return }
            if frame.event == BirdProtocol.Inbound.signinSuccess {
                self.handleSigninSuccessLocked(frame)
                return
            }
            guard let name = frame.channel else { return }
            if name == self.memberChannel?.name {
                self.memberChannel?.handleEvent(frame)
                return
            }
            self.channels[name]?.handleEvent(frame)
        }
    }

    private func makeChannel(_ name: String) -> Channel {
        let send: @Sendable (Frame) -> Bool = { [weak self] frame in
            self?.connection.sendLocked(frame) ?? false
        }
        let deliver: @Sendable (@escaping @Sendable () -> Void) -> Void = { [deliveryQueue] work in
            deliveryQueue.async(execute: work)
        }
        if name.hasPrefix("presence-") {
            return PresenceChannel(
                name: name, queue: queue, send: send, deliver: deliver,
                authorizer: authorizer
            )
        }
        if name.hasPrefix("private-") {
            return PrivateChannel(
                name: name, queue: queue, send: send, deliver: deliver,
                authorizer: authorizer
            )
        }
        return Channel(name: name, queue: queue, send: send, deliver: deliver)
    }

    private func subscribeAllLocked() {
        for channel in channels.values { sendSubscribeLocked(channel) }
    }

    private func sendSubscribeLocked(_ channel: Channel) {
        // End-to-end encrypted channels are not supported by this client yet.
        // Letting the name fall through to PrivateChannel would subscribe
        // successfully and hand the caller undecryptable ciphertext, so the
        // subscription fails loudly instead — here rather than in subscribe(),
        // so a reconnect's resubscribe sweep hits the same refusal.
        if channel.name.hasPrefix("private-encrypted-") {
            let message =
                "End-to-end encrypted channels (private-encrypted-) are not supported by this client yet"
            channel.handleSubscriptionError(["error": message])
            for (_, observer) in errorObservers {
                observer(RealtimeError(code: nil, message: message))
            }
            return
        }
        guard let connectionId = connection.connectionId else { return }
        channel.startSubscribe(connectionId: connectionId, queue: queue) {
            // Late continuations check this: same connection, not replaced.
            [weak self, weak channel] in
            guard let self, let channel else { return false }
            return self.connection.connectionId == connectionId
                && self.channels[channel.name] === channel
        }
    }

    private func sendSigninLocked() {
        guard let connectionId = connection.connectionId else { return }
        Task { [weak self] in
            guard let self else { return }
            do {
                let payload = try await self.memberAuthorizer(connectionId)
                self.queue.async {
                    // The authorization was for the connection that asked for
                    // it; a reconnect meanwhile gets its own signin.
                    guard self.connection.connectionId == connectionId else { return }
                    self.connection.sendLocked(Frame(
                        event: BirdProtocol.Outbound.signin,
                        data: ["auth": payload.auth, "member_data": payload.memberData]
                    ))
                }
            } catch {
                self.queue.async {
                    guard self.connection.connectionId == connectionId else { return }
                    let failure = (error as? any BirdRealtimeError)
                        ?? RealtimeError(code: nil, message: String(describing: error))
                    self.failSigninLocked(failure, notify: true)
                }
            }
        }
    }

    private func handleSigninSuccessLocked(_ frame: Frame) {
        // member_data is echoed as the JSON string the backend signed.
        let raw = (frame.data as? [String: Any])?["member_data"]
        var parsed: Any? = raw
        if let text = raw as? String, let bytes = text.data(using: .utf8) {
            parsed = try? JSONSerialization.jsonObject(with: bytes)
        }
        guard let memberId = (parsed as? [String: Any])?["member_id"] as? String else {
            failSigninLocked(RealtimeError(
                code: nil, message: "Signin succeeded without a member_id"
            ))
            return
        }
        let signed = SignedInMember(
            memberId: memberId,
            memberInfo: (parsed as? [String: Any])?["member_info"]
        )
        identity = signed
        // The identity is what authorizes this subscription, so it can only be
        // sent once signin has succeeded on this connection.
        subscribeMemberChannelLocked(memberId)
        let waiters = signinWaiters
        signinWaiters = []
        for waiter in waiters { waiter.resume(returning: signed) }
    }

    private func failSigninLocked(_ error: any BirdRealtimeError, notify: Bool = false) {
        let waiters = signinWaiters
        signinWaiters = []
        for waiter in waiters { waiter.resume(throwing: error) }
        // The re-signin after a reconnect has no caller to throw to, so a
        // failure there would otherwise be silent: the socket looks healthy
        // while the connection has no identity and cannot be addressed.
        if notify, waiters.isEmpty, signinArmed {
            for (_, observer) in signinErrorObservers {
                deliveryQueue.async { observer(error) }
            }
        }
    }

    // Rebuilt per signin: a channel from a previous connection cannot carry a
    // live subscription, and the member id may differ.
    private func subscribeMemberChannelLocked(_ memberId: String) {
        let name = BirdProtocol.memberChannelName(memberId)
        guard let connectionId = connection.connectionId else { return }
        // `state`, not the public `subscribed`: this runs ON the client queue and
        // that getter is a queue.sync, which deadlocks against itself rather than
        // reentering. Every other on-queue reader does the same.
        if let existing = memberChannel, existing.name == name, existing.state == .subscribed {
            return
        }
        // A plain Channel: no prefix means no authorizer call, which is right —
        // the edge authorizes this one by the signed-in identity.
        let channel = Channel(
            name: name,
            queue: queue,
            send: { [weak self] frame in self?.connection.sendLocked(frame) ?? false },
            deliver: { [deliveryQueue] work in deliveryQueue.async(execute: work) }
        )
        channel.bindGlobalLocked { [weak self] event, data in
            // Lifecycle frames belong to the channel; only application events
            // reach the member bus.
            guard !event.hasPrefix(BirdProtocol.system),
                  !BirdProtocol.isInternal(event) else { return }
            self?.member.emit(event, data)
        }
        memberChannel = channel
        channel.startSubscribe(connectionId: connectionId, queue: queue) {
            [weak self, weak channel] in
            guard let self, let channel else { return false }
            return self.connection.connectionId == connectionId
                && self.memberChannel === channel
        }
    }

    // The socket is gone, so there is nothing to unsubscribe: forgetting it is
    // what stops a stale channel from receiving a later connection's frames.
    private func dropMemberChannelLocked() {
        memberChannel?.reset()
        memberChannel?.unbindGlobalLocked()
        memberChannel = nil
    }

    // MARK: - URL + default authorizer

    static func resolveURL(options: BirdRealtimeOptions) throws -> URL {
        var host = options.wsHost
        if host == nil {
            guard let region = options.region else {
                throw RealtimeError(
                    code: nil,
                    message: "BirdRealtime: `region` (or `wsHost`) is required."
                )
            }
            host = "ws-\(region).realtime.platform.bird.com"
        }
        let host_ = host!
        // Plaintext only for loopback, and only on request — a copied config
        // can't silently downgrade real traffic.
        let hostname: String
        if host_.hasPrefix("["), let end = host_.firstIndex(of: "]") {
            hostname = String(host_[...end])
        } else {
            hostname = String(host_.split(separator: ":").first ?? Substring(host_))
        }
        let loopback: Set<String> = ["localhost", "127.0.0.1", "[::1]"]
        let scheme = options.allowInsecure && loopback.contains(hostname) ? "ws" : "wss"
        let key = options.appKey.addingPercentEncoding(
            withAllowedCharacters: .urlPathAllowed
        ) ?? options.appKey
        let query = "protocol=7&client=realtime-swift&version=\(version)"
        guard let url = URL(string: "\(scheme)://\(host_)/app/\(key)?\(query)") else {
            throw RealtimeError(code: nil, message: "BirdRealtime: invalid host \(host_)")
        }
        return url
    }

    /// Default private/presence authorizer: POST the connection id + channel
    /// name to the customer's auth endpoint as JSON and return the parsed auth
    /// payload. The endpoint is the customer's backend, which holds the app
    /// secret and computes the signature.
    static func defaultAuthorizer(
        endpoint: URL?, headers: [String: String]
    ) -> Authorizer {
        return { connectionId, channelName in
            guard let endpoint else {
                throw RealtimeError(
                    code: nil,
                    message: "Subscribing to \(channelName) needs `authEndpoint` (or a custom `authorizer`)."
                )
            }
            var request = URLRequest(url: endpoint)
            request.httpMethod = "POST"
            request.setValue("application/json", forHTTPHeaderField: "content-type")
            for (k, v) in headers { request.setValue(v, forHTTPHeaderField: k) }
            request.httpBody = try JSONSerialization.data(withJSONObject: [
                "connection_id": connectionId,
                "channel_name": channelName,
            ])
            let (body, response) = try await URLSession.shared.data(for: request)
            let status = (response as? HTTPURLResponse)?.statusCode ?? 0
            guard (200..<300).contains(status) else {
                throw RealtimeAuthError(
                    endpoint: endpoint, status: status,
                    message: "Channel authorization failed (\(status)) for \(channelName)"
                )
            }
            // The response is untrusted network input: guard the shape instead
            // of casting, or an HTML 200 would send the subscribe out unsigned.
            guard let json = try? JSONSerialization.jsonObject(with: body) as? [String: Any],
                  let auth = json["auth"] as? String, !auth.isEmpty
            else {
                throw RealtimeAuthError(
                    endpoint: endpoint, status: status,
                    message: "Channel authorization for \(channelName) returned no \"auth\" string"
                )
            }
            return ChannelAuth(
                auth: auth, memberData: json["member_data"] as? String
            )
        }
    }

    /// Default signin authorizer: POST the connection id to the customer's
    /// member-auth endpoint. Unlike channel authorization, `member_data` is
    /// required — it is the identity itself, not an extra.
    static func defaultMemberAuthorizer(
        endpoint: URL?, headers: [String: String]
    ) -> MemberAuthorizer {
        return { connectionId in
            guard let endpoint else {
                throw RealtimeError(
                    code: nil,
                    message: "signin() needs `memberAuthEndpoint` (or a custom `memberAuthorizer`)."
                )
            }
            var request = URLRequest(url: endpoint)
            request.httpMethod = "POST"
            request.setValue("application/json", forHTTPHeaderField: "content-type")
            for (k, v) in headers { request.setValue(v, forHTTPHeaderField: k) }
            request.httpBody = try JSONSerialization.data(
                withJSONObject: ["connection_id": connectionId]
            )
            let (body, response) = try await URLSession.shared.data(for: request)
            let status = (response as? HTTPURLResponse)?.statusCode ?? 0
            guard (200..<300).contains(status) else {
                throw RealtimeAuthError(
                    endpoint: endpoint, status: status,
                    message: "Signin authorization failed (\(status))"
                )
            }
            guard let json = try? JSONSerialization.jsonObject(with: body) as? [String: Any],
                  let auth = json["auth"] as? String, !auth.isEmpty,
                  let memberData = json["member_data"] as? String, !memberData.isEmpty
            else {
                throw RealtimeAuthError(
                    endpoint: endpoint, status: status,
                    message: "Signin authorization returned no \"auth\" / \"member_data\" strings"
                )
            }
            return MemberAuth(auth: auth, memberData: memberData)
        }
    }
}
