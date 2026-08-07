import Foundation
// URLRequest/URLSession live in FoundationNetworking on non-Apple platforms.
#if canImport(FoundationNetworking)
import FoundationNetworking
#endif

/// A handler bound to an event on a channel. Fired on the client's delivery
/// queue (main by default). The token unbinds it.
public final class BindingToken: Hashable, Sendable {
    public static func == (lhs: BindingToken, rhs: BindingToken) -> Bool {
        lhs === rhs
    }
    public func hash(into hasher: inout Hasher) {
        hasher.combine(ObjectIdentifier(self))
    }
}

/// The subscribe lifecycle. Every transition happens inside the channel, and
/// an async continuation (the authorize round-trip) is validated against a
/// monotonic attempt id — state that moved underneath it invalidates it.
enum SubscriptionState {
    case idle, authorizing, pending, subscribed, failed
}

/// A channel: bind handlers to events, trigger client events, observe the
/// subscribe lifecycle via `BirdProtocol.Event` bindings. Public channels need
/// no authorization; `private-` and `presence-` names authorize through the
/// client's authorizer.
///
/// All mutable state is confined to the client's internal queue. Handlers run
/// on the delivery queue.
// Queue-confined: every mutable member is read and written only on the
// client's serial queue, which the compiler cannot see through the
// DispatchQueue.async hops the subscribe lifecycle is built from.
public class Channel: @unchecked Sendable {
    public let name: String

    var state: SubscriptionState = .idle
    private var attempt = 0
    private var bindings: [String: [(BindingToken, @Sendable (Any?) -> Void)]] = [:]
    private var globalBindings: [(BindingToken, @Sendable (String, Any?) -> Void)] = []

    let send: @Sendable (Frame) -> Bool
    private let deliver: @Sendable (@escaping @Sendable () -> Void) -> Void
    // The client's serial queue. Public calls arrive on the caller's thread while
    // emit() and the subscribe lifecycle run on the queue, so anything reading or
    // writing `bindings` or `state` from outside hops onto it first.
    let queue: DispatchQueue

    init(
        name: String,
        queue: DispatchQueue,
        send: @escaping @Sendable (Frame) -> Bool,
        deliver: @escaping @Sendable (@escaping @Sendable () -> Void) -> Void
    ) {
        self.name = name
        self.queue = queue
        self.send = send
        self.deliver = deliver
    }

    /// True while the server has this channel subscribed.
    ///
    /// For callers OFF the client queue. Internal code already on it reads
    /// `state`: this hop is a `queue.sync`, which deadlocks against its own queue
    /// instead of reentering the way the Kotlin client's confinement does.
    public var subscribed: Bool { queue.sync { state == .subscribed } }

    /// Bind a handler to an event (an application event or one of
    /// `BirdProtocol.Event`). Returns a token for `unbind`.
    @discardableResult
    public func bind(_ event: String, _ handler: @escaping @Sendable (Any?) -> Void) -> BindingToken {
        let token = BindingToken()
        queue.sync { bindings[event, default: []].append((token, handler)) }
        return token
    }

    /// Remove one binding by its token.
    public func unbind(_ event: String, _ token: BindingToken) {
        queue.sync { bindings[event]?.removeAll { $0.0 == token } }
    }

    /// Remove every binding for an event, or all bindings when nil.
    public func unbindAll(_ event: String? = nil) {
        queue.sync {
            if let event { bindings[event] = nil } else { bindings.removeAll() }
        }
    }

    /// Bind a handler to *every* event on this channel, receiving the event
    /// name alongside the payload. Lifecycle events (`bird:*`) are delivered
    /// too — filter on the name if you only want application events.
    @discardableResult
    public func bindGlobal(
        _ handler: @escaping @Sendable (String, Any?) -> Void
    ) -> BindingToken {
        let token = BindingToken()
        queue.sync { globalBindings.append((token, handler)) }
        return token
    }

    /// Register a global binding from code already running on the client queue.
    /// The public entry point hops onto that queue, which would deadlock here.
    @discardableResult
    func bindGlobalLocked(
        _ handler: @escaping @Sendable (String, Any?) -> Void
    ) -> BindingToken {
        let token = BindingToken()
        globalBindings.append((token, handler))
        return token
    }

    /// Drop every global binding, for callers already on the client queue.
    func unbindGlobalLocked() {
        globalBindings.removeAll()
    }

    /// Remove one global binding, or all of them when no token is given.
    public func unbindGlobal(_ token: BindingToken? = nil) {
        queue.sync {
            if let token {
                globalBindings.removeAll { $0.0 == token }
            } else {
                globalBindings.removeAll()
            }
        }
    }

    /// Trigger a client event (`client-*`). Requires an active subscription;
    /// returns false when not subscribed. Throws on a bad event name.
    @discardableResult
    public func trigger(_ event: String, data: Any? = nil) throws -> Bool {
        guard event.hasPrefix(BirdProtocol.clientEventPrefix) else {
            throw RealtimeError(
                code: nil,
                message: "Client events must be prefixed with \"\(BirdProtocol.clientEventPrefix)\""
            )
        }
        let boxed = UncheckedSendableBox(data)
        return queue.sync {
            guard state == .subscribed else { return false }
            return send(Frame(event: event, channel: name, data: boxed.value))
        }
    }

    func emit(_ event: String, _ data: Any?) {
        let boxed = UncheckedSendableBox(data)
        for (_, handler) in globalBindings {
            deliver { handler(event, boxed.value) }
        }
        guard let handlers = bindings[event], !handlers.isEmpty else { return }
        for (_, handler) in handlers {
            deliver { handler(boxed.value) }
        }
    }

    // MARK: - Subscribe lifecycle (queue-confined)

    /// Public channels need no auth. Subclasses return an auth payload.
    func authorize(connectionId: String) async throws -> ChannelAuth? { nil }

    /// Hook for the accepted (attempt-validated) auth payload — a stale
    /// authorize resolution never reaches it.
    func acceptAuth(_ auth: ChannelAuth?) {}

    /// Drive one subscribe attempt: authorize, then send — unless the state
    /// moved while authorize was in flight (reconnect, unsubscribe, a server
    /// error), in which case the late continuation is dropped. `stillCurrent`
    /// is the client's view (same connection id, channel still registered)
    /// and, like the continuation, runs on the client queue.
    func startSubscribe(
        connectionId: String,
        queue: DispatchQueue,
        stillCurrent: @escaping @Sendable () -> Bool
    ) {
        guard state == .idle || state == .failed else { return }
        attempt += 1
        let thisAttempt = attempt
        state = .authorizing
        Task { [weak self] in
            guard let self else { return }
            do {
                let auth = try await self.authorize(connectionId: connectionId)
                queue.async {
                    guard thisAttempt == self.attempt, stillCurrent() else { return }
                    // Only the winning attempt gets to act on its auth payload.
                    self.acceptAuth(auth)
                    var data: [String: Any] = ["channel": self.name]
                    if let auth {
                        data["auth"] = auth.auth
                        if let memberData = auth.memberData {
                            data["member_data"] = memberData
                        }
                    }
                    self.state = .pending
                    _ = self.send(Frame(
                        event: BirdProtocol.Outbound.subscribe, data: data
                    ))
                }
            } catch {
                let payload = UncheckedSendableBox(Self.subscriptionErrorPayload(error))
                queue.async {
                    guard thisAttempt == self.attempt else { return }
                    self.handleSubscriptionError(payload.value)
                }
            }
        }
    }

    /// The `bird:subscription_error` payload for a local authorization failure.
    /// Wire-shaped, because a binding cannot tell this apart from the server's own
    /// rejection frame, and the authorizer's endpoint and status are carried as
    /// fields rather than lost to a stringified error: a customer debugging their
    /// own auth endpoint reads them here.
    static func subscriptionErrorPayload(_ error: Error) -> [String: Any] {
        var payload: [String: Any] = [
            "error": (error as? any BirdRealtimeError)?.message ?? String(describing: error)
        ]
        if let authError = error as? RealtimeAuthError {
            payload["endpoint"] = authError.endpoint.absoluteString
            payload["status"] = authError.status
        }
        return payload
    }

    /// Invalidate any in-flight attempt without emitting: the connection
    /// dropped (a reconnect re-subscribes) or a connection-level error arrived.
    func invalidateAttempt() {
        attempt += 1
        if state != .subscribed { state = .idle }
    }

    /// Route a frame addressed to this channel. Internal frames are consumed.
    func handleEvent(_ frame: Frame) {
        switch frame.event {
        case BirdProtocol.Inbound.subscriptionSucceeded:
            state = .subscribed
            emit(BirdProtocol.Event.subscriptionSucceeded, nil)
        case BirdProtocol.Inbound.subscriptionError:
            handleSubscriptionError(frame.data)
        case BirdProtocol.Inbound.connectionCount:
            emit(BirdProtocol.Event.connectionCount, frame.data)
        default:
            if BirdProtocol.isInternal(frame.event) { return }
            emit(frame.event, frame.data)
        }
    }

    /// A failed subscription (authorizer error or server rejection).
    func handleSubscriptionError(_ data: Any?) {
        attempt += 1
        state = .failed
        emit(BirdProtocol.Event.subscriptionError, data)
    }

    func reset() {
        attempt += 1
        state = .idle
    }
}

/// A private channel: subscription is authorized via the configured authorizer.
public class PrivateChannel: Channel, @unchecked Sendable {
    private let authorizer: Authorizer

    init(
        name: String,
        queue: DispatchQueue,
        send: @escaping @Sendable (Frame) -> Bool,
        deliver: @escaping @Sendable (@escaping @Sendable () -> Void) -> Void,
        authorizer: @escaping Authorizer
    ) {
        self.authorizer = authorizer
        super.init(name: name, queue: queue, send: send, deliver: deliver)
    }

    override func authorize(connectionId: String) async throws -> ChannelAuth? {
        try await authorizer(connectionId, name)
    }
}

/// A presence channel: authorized like a private channel, plus member tracking.
public final class PresenceChannel: PrivateChannel, @unchecked Sendable {
    private var membersStorage: [String: Member] = [:]
    /// Current members by id, valid while subscribed.
    public var members: [String: Member] { queue.sync { membersStorage } }
    private var myIdStorage: String?
    /// The local member's id, known once the subscription succeeds.
    public var myId: String? { queue.sync { myIdStorage } }
    private var pendingMyId: String?

    override func acceptAuth(_ auth: ChannelAuth?) {
        // member_data is the customer-signed identity blob; its member_id is
        // us. Held as pending until the server confirms the subscription.
        pendingMyId = nil
        guard let blob = auth?.memberData, let bytes = blob.data(using: .utf8),
              let d = try? JSONSerialization.jsonObject(with: bytes) as? [String: Any]
        else { return }
        if let id = d["member_id"] {
            pendingMyId = (id as? String) ?? (id as? NSNumber)?.stringValue
        }
    }

    override func handleEvent(_ frame: Frame) {
        switch frame.event {
        case BirdProtocol.Inbound.subscriptionSucceeded:
            state = .subscribed
            myIdStorage = pendingMyId
            loadMembers(frame.data)
            emit(BirdProtocol.Event.subscriptionSucceeded, membersStorage)
        case BirdProtocol.Inbound.memberAdded:
            if let m = Self.member(from: frame.data) {
                membersStorage[m.memberId] = m
                emit(BirdProtocol.Event.memberAdded, m)
            }
        case BirdProtocol.Inbound.memberRemoved:
            if let m = Self.member(from: frame.data),
               membersStorage.removeValue(forKey: m.memberId) != nil {
                emit(BirdProtocol.Event.memberRemoved, m)
            }
        default:
            super.handleEvent(frame)
        }
    }

    override func handleSubscriptionError(_ data: Any?) {
        membersStorage.removeAll()
        myIdStorage = nil
        pendingMyId = nil
        super.handleSubscriptionError(data)
    }

    override func reset() {
        super.reset()
        membersStorage.removeAll()
        myIdStorage = nil
        pendingMyId = nil
    }

    private func loadMembers(_ data: Any?) {
        membersStorage.removeAll()
        guard let presence = (data as? [String: Any])?["presence"] as? [String: Any],
              let ids = presence["ids"] as? [String]
        else { return }
        let hash = presence["hash"] as? [String: Any]
        for id in ids {
            membersStorage[id] = Member(memberId: id, memberInfo: hash?[id])
        }
    }

    private static func member(from data: Any?) -> Member? {
        guard let d = data as? [String: Any], let raw = d["member_id"] else { return nil }
        let id = (raw as? String) ?? (raw as? NSNumber)?.stringValue
        guard let id else { return nil }
        return Member(memberId: id, memberInfo: d["member_info"])
    }
}
