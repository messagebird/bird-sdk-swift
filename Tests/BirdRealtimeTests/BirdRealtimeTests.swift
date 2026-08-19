import Foundation
// URLRequest/URLSession live in FoundationNetworking on non-Apple platforms.
#if canImport(FoundationNetworking)
import FoundationNetworking
#endif
import Testing
@testable import BirdRealtime

// MARK: - Protocol

@Suite struct ProtocolTests {
    @Test func encodeOmitsAbsentFields() async {
        #expect(Frame(event: "bird:ping").encoded() == #"{"event":"bird:ping"}"#)
    }

    @Test func encodeSendsDataAsValueNotString() async throws {
        let text = Frame(event: "bird:subscribe", data: ["channel": "orders"]).encoded()!
        let parsed = try JSONSerialization.jsonObject(
            with: text.data(using: .utf8)!
        ) as! [String: Any]
        // Outbound data must be a JSON value — a stringified hash is rejected
        // by the edge ("Expected parameter data to be a hash").
        #expect((parsed["data"] as? [String: Any])?["channel"] as? String == "orders")
    }

    @Test func decodeNormalizesDoubleEncodedData() async {
        let frame = Frame.decode(
            #"{"event":"order-updated","channel":"orders","data":"{\"status\":\"shipped\"}"}"#
        )!
        #expect(frame.event == "order-updated")
        #expect(frame.channel == "orders")
        #expect((frame.data as? [String: Any])?["status"] as? String == "shipped")
    }

    @Test func decodeLeavesNonJSONStringData() async {
        let frame = Frame.decode(#"{"event":"e","data":"plain text"}"#)!
        #expect(frame.data as? String == "plain text")
    }

    @Test func decodeRejectsFramesWithoutEvent() async {
        #expect(Frame.decode(#"{"channel":"orders"}"#) == nil)
        #expect(Frame.decode("not json") == nil)
    }
}

// MARK: - URL resolution

@Suite struct URLTests {
    @Test func regionDerivesHost() async throws {
        let url = try BirdRealtime.resolveURL(options: .init(appKey: "k e y", region: "us1"))
        #expect(url.scheme == "wss")
        #expect(url.host == "ws-us1.realtime.platform.bird.com")
        #expect(url.path.contains("/app/k e y") || url.absoluteString.contains("/app/k%20e%20y"))
        #expect(url.query!.contains("protocol=7"))
    }

    @Test func insecureOnlyForLoopback() async throws {
        let loopback = try BirdRealtime.resolveURL(
            options: .init(appKey: "k", wsHost: "localhost:8080", allowInsecure: true)
        )
        #expect(loopback.scheme == "ws")
        let real = try BirdRealtime.resolveURL(
            options: .init(appKey: "k", wsHost: "example.com", allowInsecure: true)
        )
        #expect(real.scheme == "wss")
    }

    @Test func missingRegionAndHostThrows() async {
        #expect(throws: (any Error).self) {
            try BirdRealtime.resolveURL(options: .init(appKey: "k"))
        }
    }
}

// MARK: - Client behaviour against a scripted transport

/// A scripted transport: the test is the server.
final class FakeTransport: Transport, @unchecked Sendable {
    var onMessage: ((String) -> Void)?
    var onClose: ((Int, String?) -> Void)?
    private var sent: [String] = []
    private(set) var connected = false
    private(set) var closedWith: Int?
    private let lock = NSLock()

    func connect() { connected = true }

    func send(_ text: String) {
        lock.lock()
        sent.append(text)
        lock.unlock()
    }

    func close(code: Int, reason: String?) {
        lock.lock()
        closedWith = code
        lock.unlock()
        onClose?(code, reason)
    }

    func serverSends(_ frame: [String: Any]) {
        let bytes = try! JSONSerialization.data(withJSONObject: frame)
        onMessage?(String(decoding: bytes, as: UTF8.self))
    }

    func sentFrames() -> [[String: Any]] {
        lock.lock()
        defer { lock.unlock() }
        return sent.map {
            try! JSONSerialization.jsonObject(with: $0.data(using: .utf8)!) as! [String: Any]
        }
    }

    func sentEvent(_ event: String) -> [String: Any]? {
        sentFrames().first { $0["event"] as? String == event }
    }
}

/// Collects transports across reconnects; thread-safe.
final class TransportHolder: @unchecked Sendable {
    private var transports: [FakeTransport] = []
    private let lock = NSLock()
    var latest: FakeTransport? {
        lock.lock()
        defer { lock.unlock() }
        return transports.last
    }
    func append(_ t: FakeTransport) {
        lock.lock()
        transports.append(t)
        lock.unlock()
    }
}

/// Thread-safe capture box for values observed from delivery-queue callbacks.
final class Box<T>: @unchecked Sendable {
    private var value: [T] = []
    private let lock = NSLock()
    func push(_ v: T) {
        lock.lock()
        value.append(v)
        lock.unlock()
    }
    var all: [T] {
        lock.lock()
        defer { lock.unlock() }
        return value
    }
}

func makeClient(
    authorizer: Authorizer? = nil,
    memberAuthorizer: MemberAuthorizer? = nil
) -> (BirdRealtime, @Sendable () async -> FakeTransport) {
    let holder = TransportHolder()
    let client = BirdRealtime(
        options: .init(
            appKey: "key", region: "us1",
            authorizer: authorizer, memberAuthorizer: memberAuthorizer
        ),
        deliveryQueue: DispatchQueue(label: "test-delivery"),
        transportFactory: { _ in
            let t = FakeTransport()
            holder.append(t)
            return t
        }
    )
    // The connection opens asynchronously on the client's queue, so the first
    // transport may not exist yet when a test asks for it.
    return (client, {
        await waitFor("transport never created") { holder.latest != nil }
        return holder.latest!
    })
}

func handshake(_ transport: FakeTransport, id: String = "conn-1") {
    transport.serverSends([
        "event": "bird:connection_established",
        "data": ["connection_id": id, "activity_timeout": 120],
    ])
}

/// Wait until `condition` is true. The client's internal queue is
/// asynchronous, so tests poll rather than block it. Suites that use this are
/// `.serialized`: the sleep below blocks a thread, and parallel suites would
/// starve one another into false timeouts.
// Polls, because the client's work lands on its own serial queue with no
// completion to await. It yields with Task.sleep rather than blocking in
// Thread.sleep: swift-testing runs suites concurrently, and a blocking wait holds
// a worker thread hostage, so the queue hop the condition is waiting FOR can be
// left with no thread to run on. That deadlocked the first test of each parallel
// suite on a cold 4-core runner while passing everywhere with a wider pool.
// The timeout is a failure deadline, not a delay: the loop exits the moment the
// condition holds, so a generous budget costs nothing on the happy path.
func waitFor(
    _ message: String = "condition not met",
    timeout: TimeInterval = 30,
    _ condition: () async -> Bool
) async {
    let deadline = Date().addingTimeInterval(timeout)
    while await !condition() {
        if Date() > deadline {
            Issue.record("timed out: \(message)")
            return
        }
        try? await Task.sleep(nanoseconds: 10_000_000)
    }
}

@Suite(.serialized) struct ClientTests {
    @Test func refusesEncryptedChannelSubscriptions() async {
        let (client, transport) = makeClient()
        await waitFor("transport never connected") { await transport().connected }

        let channel = client.subscribe("private-encrypted-orders")
        let failures = Box<Any?>()
        channel.bind(BirdProtocol.Event.subscriptionError) { failures.push($0) }
        let clientErrors = Box<String?>()
        client.onError { clientErrors.push($0.message) }

        handshake(await transport())
        await waitFor("subscription never failed") { !failures.all.isEmpty }
        #expect(await transport().sentEvent("bird:subscribe") == nil)
        await waitFor("client error never emitted") { !clientErrors.all.isEmpty }
        #expect(clientErrors.all.first??.contains("private-encrypted-") == true)
    }

    @Test func handshakeThenSubscribeFlow() async {
        let (client, transport) = makeClient()
        await waitFor("transport never connected") { await transport().connected }

        let channel = client.subscribe("orders")
        let events = Box<Any?>()
        channel.bind("order-updated") { events.push($0) }

        handshake(await transport())
        await waitFor("subscribe never sent") { await transport().sentEvent("bird:subscribe") != nil }
        let sub = await transport().sentEvent("bird:subscribe")!
        #expect((sub["data"] as? [String: Any])?["channel"] as? String == "orders")

        await transport().serverSends([
            "event": "bird_internal:subscription_succeeded", "channel": "orders",
        ])
        await waitFor("channel never subscribed") { channel.subscribed }

        await transport().serverSends([
            "event": "order-updated", "channel": "orders",
            "data": "{\"status\":\"shipped\"}",
        ])
        await waitFor("event never delivered") { !events.all.isEmpty }
        #expect((events.all.first as? [String: Any])?["status"] as? String == "shipped")
    }

    @Test func privateChannelSendsAuthPayload() async {
        let (client, transport) = makeClient(authorizer: { connectionId, channel in
            ChannelAuth(auth: "key:sig-for-\(connectionId)-\(channel)")
        })
        await waitFor("transport never connected") { await transport().connected }
        client.subscribe("private-room")
        handshake(await transport())
        await waitFor("authorized subscribe never sent") {
            await transport().sentFrames().contains {
                ($0["data"] as? [String: Any])?["auth"] != nil
            }
        }
        let sub = await transport().sentEvent("bird:subscribe")!
        #expect(
            (sub["data"] as? [String: Any])?["auth"] as? String
                == "key:sig-for-conn-1-private-room"
        )
    }

    @Test func channelAuthFailureCarriesEndpointAndStatus() async {
        let endpoint = URL(string: "https://backend.example.com/bird/auth")!
        let (client, transport) = makeClient(authorizer: { _, channel in
            throw RealtimeAuthError(
                endpoint: endpoint, status: 403,
                message: "Channel authorization failed (403) for \(channel)"
            )
        })
        await waitFor("transport never connected") { await transport().connected }
        let errors = Box<[String: Any]>()
        let channel = client.subscribe("private-room")
        channel.bind(BirdProtocol.Event.subscriptionError) { data in
            if let payload = data as? [String: Any] { errors.push(payload) }
        }
        handshake(await transport())

        await waitFor("subscription_error never emitted") { !errors.all.isEmpty }
        let payload = errors.all.first
        // The endpoint and status are the part a customer debugging their own auth
        // endpoint needs, and a stringified error loses them.
        #expect(payload?["endpoint"] as? String == endpoint.absoluteString)
        #expect(payload?["status"] as? Int == 403)
        #expect(payload?["error"] as? String == "Channel authorization failed (403) for private-room")
    }

    @Test func presenceMembersLifecycle() async {
        let (client, transport) = makeClient(authorizer: { _, _ in
            ChannelAuth(auth: "key:sig", memberData: "{\"member_id\":\"me\"}")
        })
        await waitFor("transport never connected") { await transport().connected }
        let channel = client.subscribe("presence-room") as! PresenceChannel
        handshake(await transport())
        await waitFor("subscribe never sent") { await transport().sentEvent("bird:subscribe") != nil }
        await transport().serverSends([
            "event": "bird_internal:subscription_succeeded",
            "channel": "presence-room",
            "data": [
                "presence": [
                    "ids": ["me", "them"],
                    "hash": ["me": ["name": "Me"], "them": ["name": "Them"]],
                    "count": 2,
                ]
            ],
        ])
        await waitFor("presence never subscribed") { channel.subscribed }
        #expect(channel.members.count == 2)
        #expect(channel.myId == "me")

        await transport().serverSends([
            "event": "bird_internal:member_removed",
            "channel": "presence-room",
            "data": ["member_id": "them"],
        ])
        await waitFor("member never removed") { channel.members.count == 1 }
    }

    @Test func refusedCloseIsTerminal() async {
        let (client, transport) = makeClient()
        await waitFor("transport never connected") { await transport().connected }
        handshake(await transport())
        await waitFor("never connected") { client.connectionState == .connected }

        let errors = Box<RealtimeError>()
        client.onError { errors.push($0) }
        await transport().onClose?(4004, "over quota")
        await waitFor("error never observed") { !errors.all.isEmpty }
        #expect(errors.all.first?.code == 4004)
        await waitFor("state never failed") { client.connectionState == .failed }
    }

    @Test func reconnectResubscribes() async {
        let (client, transport) = makeClient()
        await waitFor("transport never connected") { await transport().connected }
        let channel = client.subscribe("orders")
        handshake(await transport())
        await waitFor("subscribe never sent") { await transport().sentEvent("bird:subscribe") != nil }
        await transport().serverSends([
            "event": "bird_internal:subscription_succeeded", "channel": "orders",
        ])
        await waitFor("never subscribed") { channel.subscribed }

        // Retry-immediately band: a new transport appears without backoff.
        let first = await transport()
        first.onClose?(4200, "activity timeout")
        await waitFor("no reconnect transport") { await transport() !== first }
        await waitFor("reconnect never connected") { await transport().connected }
        handshake(await transport(), id: "conn-2")
        await waitFor("resubscribe never sent") { await transport().sentEvent("bird:subscribe") != nil }
    }

    @Test func clientEventRequiresSubscription() async throws {
        let (client, transport) = makeClient()
        await waitFor("transport never connected") { await transport().connected }
        let channel = client.subscribe("orders")
        #expect(throws: (any Error).self) { try channel.trigger("not-client-prefixed") }
        #expect(try channel.trigger("client-typing") == false)
        handshake(await transport())
        await waitFor("subscribe never sent") { await transport().sentEvent("bird:subscribe") != nil }
        await transport().serverSends([
            "event": "bird_internal:subscription_succeeded", "channel": "orders",
        ])
        await waitFor("never subscribed") { channel.subscribed }
        #expect(try channel.trigger("client-typing", data: ["on": true]) == true)
    }
}


@Suite(.serialized) struct SigninTests {
    private static let identity = #"{"member_id":"me","member_info":{"name":"Me"}}"#

    @Test func signinSendsAuthAndResolvesWithTheMember() async throws {
        let (client, transport) = makeClient(memberAuthorizer: { connectionId in
            MemberAuth(auth: "key:sig-for-\(connectionId)", memberData: Self.identity)
        })
        await waitFor("transport never connected") { await transport().connected }
        handshake(await transport())
        await waitFor("never connected") { client.connectionState == .connected }

        // Resolve the signin once the server confirms it.
        Task {
            await waitFor("signin frame never sent") { await transport().sentEvent("bird:signin") != nil }
            await transport().serverSends([
                "event": "bird:signin_success",
                "data": ["member_data": Self.identity],
            ])
        }
        let member = try await client.signin()
        #expect(member.memberId == "me")
        #expect((member.memberInfo as? [String: Any])?["name"] as? String == "Me")
        #expect(client.signedInMember?.memberId == "me")

        let frame = await transport().sentEvent("bird:signin")!
        let data = frame["data"] as? [String: Any]
        #expect(data?["auth"] as? String == "key:sig-for-conn-1")
        #expect(data?["member_data"] as? String == Self.identity)

        // Signin subscribes the reserved member channel — with no auth payload,
        // because the edge authorizes it by the signed-in identity.
        await waitFor("member channel never subscribed") {
            await transport().sentFrames().contains {
                ($0["data"] as? [String: Any])?["channel"] as? String == "#server-to-user-me"
            }
        }
        let sub = await transport().sentFrames().first {
            ($0["data"] as? [String: Any])?["channel"] as? String == "#server-to-user-me"
        }!
        #expect((sub["data"] as? [String: Any])?["auth"] == nil)
    }

    @Test func memberEventsReachTheMemberBus() async throws {
        let (client, transport) = makeClient(memberAuthorizer: { _ in
            MemberAuth(auth: "key:sig", memberData: Self.identity)
        })
        await waitFor("transport never connected") { await transport().connected }
        handshake(await transport())
        Task {
            await waitFor("signin frame never sent") { await transport().sentEvent("bird:signin") != nil }
            await transport().serverSends([
                "event": "bird:signin_success",
                "data": ["member_data": Self.identity],
            ])
        }
        _ = try await client.signin()

        let events = Box<String>()
        client.member.bindGlobal { event, _ in events.push(event) }
        await transport().serverSends([
            "event": "bird_internal:subscription_succeeded",
            "channel": "#server-to-user-me",
        ])
        await transport().serverSends([
            "event": "order-shipped",
            "channel": "#server-to-user-me",
            "data": ["id": "ord_1"],
        ])
        await waitFor("member event never delivered") { events.all.contains("order-shipped") }
        // Protocol frames stay off the member bus.
        #expect(!events.all.contains { $0.hasPrefix("bird") })

        // The token `bind` hands back has to actually unbind, the way Channel's
        // does — it is the only way to drop ONE handler, and returning an inert
        // one leaves a caller with no way to stop a member subscription short of
        // unbindAll.
        let scoped = Box<String>()
        let token = client.member.bind("order-shipped") { _ in scoped.push("kept") }
        client.member.unbind("order-shipped", token)
        await transport().serverSends([
            "event": "order-shipped",
            "channel": "#server-to-user-me",
            "data": ["id": "ord_2"],
        ])
        await waitFor("second event never delivered") { events.all.filter { $0 == "order-shipped" }.count == 2 }
        #expect(scoped.all.isEmpty)
    }

    @Test func identityIsDroppedWhenTheConnectionIs() async throws {
        let (client, transport) = makeClient(memberAuthorizer: { _ in
            MemberAuth(auth: "key:sig", memberData: Self.identity)
        })
        await waitFor("transport never connected") { await transport().connected }
        handshake(await transport())
        Task {
            await waitFor("signin frame never sent") { await transport().sentEvent("bird:signin") != nil }
            await transport().serverSends([
                "event": "bird:signin_success",
                "data": ["member_data": Self.identity],
            ])
        }
        _ = try await client.signin()
        #expect(client.signedInMember != nil)

        // Hold the current transport: right after the close it is still the
        // latest, and handshaking it would feed a socket the client has already
        // detached — the reconnect must be waited for by identity, not by state.
        let first = await transport()
        first.onClose?(4200, "activity timeout")
        await waitFor("identity never dropped") { client.signedInMember == nil }
        await waitFor("no reconnect transport") { await transport() !== first }
        await waitFor("reconnect never connected") { await transport().connected }
        handshake(await transport(), id: "conn-2")
        // Armed signin re-runs itself on the new connection.
        await waitFor("re-signin never sent") { await transport().sentEvent("bird:signin") != nil }
    }

    @Test func signinOnATerminatedConnectionFailsInsteadOfHanging() async throws {
        let (client, transport) = makeClient()
        await waitFor("transport never connected") { await transport().connected }
        handshake(await transport())
        await waitFor("never connected") { client.connectionState == .connected }

        // A refusal is terminal: nothing reconnects, so nothing will ever move
        // this connection back to .connected on its own.
        await transport().onClose?(4009, "not authorized within timeout")
        await waitFor("state never failed") { client.connectionState == .failed }

        // Driven from an UNSTRUCTURED Task and polled, rather than awaited, because
        // the bug's symptom is a suspension that never ends: a waiter appended to a
        // dead connection is resumed by nobody, `withCheckedThrowingContinuation`
        // ignores cancellation, and a child of a task group is awaited at scope exit.
        // Every structured form therefore hangs the whole suite on a regression
        // instead of failing it; nothing awaits this one, so `waitFor` can time out
        // and report.
        let outcome = Box<String>()
        let probe = Task {
            do {
                _ = try await client.signin()
                outcome.push("returned a member")
            } catch is RealtimeError {
                outcome.push("threw")
            } catch {
                outcome.push("threw \(type(of: error))")
            }
        }
        await waitFor("signin neither returned nor threw", timeout: 5) { !outcome.all.isEmpty }
        probe.cancel()
        #expect(outcome.all.first == "threw")
    }

    @Test func aSecondSigninSuccessDoesNotDeadlockTheClient() async throws {
        let (client, transport) = makeClient(memberAuthorizer: { _ in
            MemberAuth(auth: "key:sig", memberData: Self.identity)
        })
        await waitFor("transport never connected") { await transport().connected }
        handshake(await transport())
        Task {
            await waitFor("signin frame never sent") { await transport().sentEvent("bird:signin") != nil }
            await transport().serverSends([
                "event": "bird:signin_success",
                "data": ["member_data": Self.identity],
            ])
        }
        _ = try await client.signin()
        await transport().serverSends([
            "event": "bird_internal:subscription_succeeded",
            "channel": "#server-to-user-me",
        ])

        // The second signin_success takes the already-subscribed path, which read
        // the channel's PUBLIC `subscribed` while already on the client queue and
        // deadlocked it. Probed from an unstructured Task: a wedged serial queue
        // freezes every later call, so an assertion made inline would hang the
        // suite rather than fail it.
        await transport().serverSends([
            "event": "bird:signin_success",
            "data": ["member_data": Self.identity],
        ])
        // Probed rather than asserted inline, and from a REAL thread: a wedged
        // serial queue takes down whatever touches it, and blocking one of Swift
        // concurrency's few cooperative threads would starve the poller below.
        // On Linux the regression trips libdispatch's same-queue-sync trap and
        // kills the process outright; on Apple platforms it just hangs, which is
        // the case this probe and its deadline turn into a failure.
        let alive = Box<String>()
        Thread.detachNewThread {
            _ = client.signedInMember          // any queue.sync hop off the queue
            alive.push("responsive")
        }
        await waitFor("client queue wedged after a second signin_success", timeout: 5) {
            !alive.all.isEmpty
        }
        #expect(alive.all.first == "responsive")
    }

    @Test func droppingTheClientClosesTheSocket() async {
        let holder = TransportHolder()
        var client: BirdRealtime? = BirdRealtime(
            options: .init(appKey: "key", region: "us1"),
            deliveryQueue: DispatchQueue(label: "test-delivery"),
            transportFactory: { _ in
                let t = FakeTransport()
                holder.append(t)
                return t
            }
        )
        await waitFor("transport never connected") { holder.latest?.connected == true }
        #expect(holder.latest?.closedWith == nil)

        // Releasing without disconnect() is the case that leaked: URLSession holds
        // its delegate until invalidated, so the real transport kept an open socket
        // alive with nothing referencing the client.
        client = nil
        await waitFor("socket never closed when the client was released", timeout: 5) {
            holder.latest?.closedWith != nil
        }
        #expect(holder.latest?.closedWith == 1000)
    }

    @Test func authFailureKeepsItsEndpointAndStatus() async throws {
        let endpoint = URL(string: "https://backend.example.com/bird/auth/member")!
        let (client, transport) = makeClient(memberAuthorizer: { _ in
            throw RealtimeAuthError(
                endpoint: endpoint, status: 403, message: "Signin authorization failed (403)"
            )
        })
        await waitFor("transport never connected") { await transport().connected }
        handshake(await transport())

        do {
            _ = try await client.signin()
            Issue.record("signin should have thrown")
        } catch let error as any BirdRealtimeError {
            // The endpoint and status are what say whose endpoint to go fix, so
            // the authorizer's error has to survive the trip rather than being
            // flattened into a message.
            let authError = try #require(error as? RealtimeAuthError)
            #expect(authError.endpoint == endpoint)
            #expect(authError.status == 403)
        }
    }
}
