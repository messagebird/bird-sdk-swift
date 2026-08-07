import Foundation
// URLRequest/URLSession live in FoundationNetworking on non-Apple platforms.
#if canImport(FoundationNetworking)
import FoundationNetworking
#endif

/// The socket seam: `Connection` drives this instead of URLSession directly so
/// tests can inject a scripted transport.
protocol Transport: AnyObject, Sendable {
    /// Called with each received text message.
    var onMessage: ((String) -> Void)? { get set }
    /// Called exactly once when the socket closes, with the close code and reason.
    var onClose: ((Int, String?) -> Void)? { get set }
    func connect()
    func send(_ text: String)
    func close(code: Int, reason: String?)
}

typealias TransportFactory = @Sendable (URL) -> Transport

/// URLSessionWebSocketTask-backed transport. All callbacks are funneled to the
/// connection's queue by the caller; this class only guarantees each callback
/// fires at most the documented number of times.
// The callbacks are set once at construction, before connect(); URLSession
// then drives them from its own delegate queue.
final class URLSessionTransport: NSObject, Transport, URLSessionWebSocketDelegate, @unchecked Sendable {
    var onMessage: ((String) -> Void)?
    var onClose: ((Int, String?) -> Void)?

    private let url: URL
    private var session: URLSession?
    private var task: URLSessionWebSocketTask?
    private var closed = false

    init(url: URL) {
        self.url = url
    }

    func connect() {
        let session = URLSession(
            configuration: .default, delegate: self, delegateQueue: nil
        )
        self.session = session
        let task = session.webSocketTask(with: url)
        self.task = task
        receiveLoop(task)
        task.resume()
    }

    func send(_ text: String) {
        task?.send(.string(text)) { _ in
            // A send failure surfaces as the socket closing; nothing to do here.
        }
    }

    func close(code: Int, reason: String?) {
        let closeCode = URLSessionWebSocketTask.CloseCode(rawValue: code) ?? .normalClosure
        task?.cancel(with: closeCode, reason: reason?.data(using: .utf8))
        finish(code: code, reason: reason)
    }

    private func receiveLoop(_ task: URLSessionWebSocketTask) {
        task.receive { [weak self, weak task] result in
            guard let self, let task else { return }
            switch result {
            case .success(.string(let text)):
                self.onMessage?(text)
                self.receiveLoop(task)
            case .success(.data(let data)):
                self.onMessage?(String(decoding: data, as: UTF8.self))
                self.receiveLoop(task)
            case .success:
                self.receiveLoop(task)
            case .failure:
                self.finish(from: task)
            }
        }
    }

    // corelibs-foundation declares this protocol in pure Swift, so it has no
    // optional members and Linux requires the open callback even though the
    // receive loop is what actually starts the flow.
    func urlSession(
        _ session: URLSession,
        webSocketTask: URLSessionWebSocketTask,
        didOpenWithProtocol protocol: String?
    ) {}

    func urlSession(
        _ session: URLSession,
        webSocketTask: URLSessionWebSocketTask,
        didCloseWith closeCode: URLSessionWebSocketTask.CloseCode,
        reason: Data?
    ) {
        finish(
            code: closeCode.rawValue,
            reason: reason.map { String(decoding: $0, as: UTF8.self) }
        )
    }

    func urlSession(
        _ session: URLSession, task: URLSessionTask,
        didCompleteWithError error: Error?
    ) {
        guard error != nil else { return }
        guard let task = task as? URLSessionWebSocketTask else {
            finish(code: 1006, reason: "transport failure")
            return
        }
        finish(from: task)
    }

    /// End the socket with the code the task actually carries.
    ///
    /// Both failure paths race — a server close surfaces as a receive error AND
    /// as a delegate callback, and `finish` delivers once — so both must read the
    /// code the same way. Assuming 1006 reports a REFUSED close (4000–4099, e.g.
    /// 4009 "not authorized within timeout") as a transport drop, which
    /// `Connection` retries, and a terminal refusal then reconnect-loops forever.
    ///
    /// Nothing here is covered: `Tests/` drives `FakeTransport` end to end and
    /// never constructs a `URLSessionTransport`, and reaching this needs a live
    /// socket sending a real close frame. Changing it, the callers, or the codes
    /// `Connection` treats as terminal is unguarded by the suite.
    private func finish(from task: URLSessionWebSocketTask) {
        let code = task.closeCode.rawValue
        guard code != 0 else {
            finish(code: 1006, reason: "transport failure")
            return
        }
        finish(
            code: code,
            reason: task.closeReason.map { String(decoding: $0, as: UTF8.self) }
        )
    }

    /// Deliver onClose exactly once, then break the session retain cycle.
    private func finish(code: Int, reason: String?) {
        guard !closed else { return }
        closed = true
        onClose?(code, reason)
        session?.finishTasksAndInvalidate()
        session = nil
        task = nil
    }
}
