import Foundation
// URLRequest/URLSession live in FoundationNetworking on non-Apple platforms.
#if canImport(FoundationNetworking)
import FoundationNetworking
#endif

/// Connection owns a single socket and the reconnection state machine. All of
/// its mutable state is confined to `queue`; every public entry point hops
/// onto it. Callbacks (`onStateChange`, `onError`, `onFrame`) fire on `queue`
/// — the client re-dispatches to the delivery queue before touching app code.
// Queue-confined: driven entirely from the client's serial queue.
final class Connection: @unchecked Sendable {
    private(set) var state: ConnectionState = .initialized
    private(set) var connectionId: String?

    var onStateChange: ((ConnectionState, ConnectionState) -> Void)?
    var onError: ((RealtimeError) -> Void)?
    var onFrame: ((Frame) -> Void)?

    private let url: URL
    private let queue: DispatchQueue
    private let makeTransport: TransportFactory
    private let activityTimeout: TimeInterval
    private let pongTimeout: TimeInterval
    private var serverActivityTimeout: TimeInterval?

    private var transport: Transport?
    private var attempt = 0
    private var intentional = false
    // Set when this side has already explained a refusal it is about to
    // trigger, so the close it causes doesn't report the same failure twice.
    private var refusalReported = false
    private var reconnectTimer: DispatchSourceTimer?
    private var activityTimer: DispatchSourceTimer?
    private var pongTimer: DispatchSourceTimer?

    // Reconnect backoff: exponential from 1s, capped at 30s, with full jitter
    // so a fleet of clients dropped together doesn't reconnect in lockstep.
    private static let backoffBase: TimeInterval = 1
    private static let backoffCap: TimeInterval = 30

    init(
        url: URL,
        queue: DispatchQueue,
        activityTimeout: TimeInterval,
        pongTimeout: TimeInterval,
        makeTransport: @escaping TransportFactory
    ) {
        self.url = url
        self.queue = queue
        self.activityTimeout = activityTimeout
        self.pongTimeout = pongTimeout
        self.makeTransport = makeTransport
    }

    // MARK: - Public entry points (hop to the queue)

    func connect() {
        queue.async { self.connectLocked() }
    }

    /// `disconnect()` is the only other path that closes the socket, and a caller
    /// that simply drops the client never takes it. That leaks more than memory:
    /// `URLSession` holds its delegate until invalidated, so the transport, the
    /// session and the OPEN SOCKET outlive this object with the server none the
    /// wiser, and the timers keep waking the queue.
    ///
    /// Touching the confined fields directly is correct here and hopping is not:
    /// deinit runs only once nothing else can reach us, and `queue.async` from
    /// deinit would capture a self that is already being destroyed.
    deinit {
        clearTimers()
        reconnectTimer?.cancel()
        transport?.close(code: 1000, reason: "client released")
    }

    func disconnect() {
        queue.async {
            self.intentional = true
            self.clearTimers()
            self.reconnectTimer?.cancel()
            self.reconnectTimer = nil
            self.transport?.close(code: 1000, reason: "client disconnect")
            self.transport = nil
            self.setState(.disconnected)
        }
    }

    /// Send a frame if connected. Queue-confined; callers already on `queue`.
    @discardableResult
    func sendLocked(_ frame: Frame) -> Bool {
        guard state == .connected, let transport, let text = frame.encoded()
        else { return false }
        transport.send(text)
        return true
    }

    // MARK: - Queue-confined machinery

    private func connectLocked() {
        if state == .connected || state == .connecting { return }
        intentional = false
        // A reconnect may already be scheduled (connect() during
        // `unavailable`); cancel it so exactly one open is in flight.
        reconnectTimer?.cancel()
        reconnectTimer = nil
        open()
    }

    private func open() {
        // Orphan any previous socket: detach its handlers first so its close
        // can't schedule a competing reconnect, then close it.
        if let old = transport {
            old.onMessage = nil
            old.onClose = nil
            old.close(code: 1000, reason: "superseded")
        }
        setState(.connecting)
        refusalReported = false
        let transport = makeTransport(url)
        self.transport = transport
        transport.onMessage = { [weak self, weak transport] text in
            guard let self, let transport else { return }
            self.queue.async {
                guard self.transport === transport else { return }
                self.onMessage(text)
            }
        }
        transport.onClose = { [weak self, weak transport] code, reason in
            guard let self, let transport else { return }
            self.queue.async {
                guard self.transport === transport else { return }
                self.onClose(code: code, reason: reason)
            }
        }
        transport.connect()
        // The connection isn't usable until the server's handshake frame
        // arrives; stay in `connecting` until bird:connection_established,
        // but arm the activity window so a silent socket doesn't hang forever.
        resetActivityTimer()
    }

    private func onMessage(_ raw: String) {
        resetActivityTimer()
        guard let frame = Frame.decode(raw) else { return }

        switch frame.event {
        case BirdProtocol.Inbound.connectionEstablished:
            let d = frame.data as? [String: Any]
            guard let id = d?["connection_id"] as? String else {
                // No connection id means this is not a Bird Realtime app.
                // Fail loudly instead of sitting "connected" but unable to
                // subscribe; 4001 is in the no-retry band.
                refusalReported = true
                onError?(RealtimeError(
                    code: nil,
                    message: "Handshake missing connection_id — not a Bird Realtime app?"
                ))
                transport?.close(code: 4001, reason: "handshake missing connection_id")
                return
            }
            connectionId = id
            if let t = d?["activity_timeout"] as? NSNumber {
                serverActivityTimeout = t.doubleValue
            }
            attempt = 0
            setState(.connected)
        case BirdProtocol.Inbound.ping:
            sendLocked(Frame(event: BirdProtocol.Outbound.pong))
        case BirdProtocol.Inbound.pong:
            pongTimer?.cancel()
            pongTimer = nil
        case BirdProtocol.Inbound.error:
            // Channel-less server errors (incl. subscribe rejections, which
            // the wire does not attribute to a channel) surface here.
            let d = frame.data as? [String: Any]
            onError?(RealtimeError(
                code: (d?["code"] as? NSNumber)?.intValue,
                message: d?["message"] as? String ?? "Realtime connection error"
            ))
        default:
            // Application events and subscription internals go to the client,
            // which routes them to the addressed channel.
            onFrame?(frame)
        }
    }

    private func onClose(code: Int, reason: String?) {
        clearTimers()
        transport = nil
        if intentional {
            setState(.disconnected)
            return
        }
        // Close-code policy (the protocol the Bird server speaks):
        //   4000–4099 refused, do not retry  → failed
        //   4200–4299 retry immediately
        //   everything else (incl. network)  → retry with backoff
        if (4000...4099).contains(code) {
            // A refused close is terminal, so the code is the only explanation
            // the app ever gets. A refusal this side already reported (and
            // then closed on) is not repeated.
            if !refusalReported {
                onError?(RealtimeError(
                    code: code, message: reason ?? "Connection refused"
                ))
            }
            refusalReported = false
            setState(.failed)
            return
        }
        setState(.unavailable)
        scheduleReconnect(delay: (4200...4299).contains(code) ? 0 : backoff())
    }

    private func scheduleReconnect(delay: TimeInterval) {
        reconnectTimer?.cancel()
        let timer = DispatchSource.makeTimerSource(queue: queue)
        timer.schedule(deadline: .now() + delay)
        timer.setEventHandler { [weak self] in
            guard let self else { return }
            self.reconnectTimer = nil
            self.attempt += 1
            self.open()
        }
        reconnectTimer = timer
        timer.resume()
    }

    private func backoff() -> TimeInterval {
        let ceiling = min(
            Self.backoffCap,
            Self.backoffBase * pow(2, Double(attempt))
        )
        return TimeInterval.random(in: 0...ceiling) // full jitter
    }

    private func resetActivityTimer() {
        clearTimers()
        let timeout = serverActivityTimeout ?? activityTimeout
        let timer = DispatchSource.makeTimerSource(queue: queue)
        timer.schedule(deadline: .now() + timeout)
        timer.setEventHandler { [weak self] in
            guard let self else { return }
            guard self.sendLocked(Frame(event: BirdProtocol.Outbound.ping)) else {
                // Still not connected after a full activity window: the socket
                // is dead; close it directly rather than arming a pong wait
                // for a ping that was never sent.
                self.transport?.close(code: 4200, reason: "activity timeout")
                return
            }
            let pong = DispatchSource.makeTimerSource(queue: self.queue)
            pong.schedule(deadline: .now() + self.pongTimeout)
            pong.setEventHandler { [weak self] in
                // No pong: the connection is stale. Force a close to reconnect.
                self?.transport?.close(code: 4200, reason: "activity timeout")
            }
            self.pongTimer = pong
            pong.resume()
        }
        activityTimer = timer
        timer.resume()
    }

    private func clearTimers() {
        activityTimer?.cancel()
        pongTimer?.cancel()
        activityTimer = nil
        pongTimer = nil
    }

    private func setState(_ next: ConnectionState) {
        guard state != next else { return }
        let previous = state
        state = next
        // `connecting` keeps the previous connection id (an in-flight
        // authorize may still compare against it); every other non-connected
        // state drops it.
        if next != .connected && next != .connecting {
            connectionId = nil
        }
        onStateChange?(previous, next)
    }
}
