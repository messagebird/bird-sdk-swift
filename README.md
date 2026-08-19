# BirdRealtime

The official Bird Realtime client for Swift: subscribe to channels and receive events in real time over a WebSocket.

> Looking for the server side (sending messages, managing resources, verifying webhooks, publishing Realtime events)? Those live in the Bird API; see the [API reference](https://bird.com/docs/api).

Zero dependencies: `URLSessionWebSocketTask` for transport, `JSONSerialization` for frames. iOS 15+ / macOS 12+ / tvOS 15+ / watchOS 8+, and Linux.

- Guides: [Realtime overview](https://bird.com/docs/guides/realtime/overview)
- SDK docs: [Swift](https://bird.com/docs/sdks/swift)

## Install

Add the package in Xcode (File › Add Package Dependencies) with the repository URL, or declare it in `Package.swift`:

```swift
dependencies: [
    .package(url: "https://github.com/messagebird/bird-sdk-swift.git", from: "0.1.0")
]
```

Then add the product to your target:

```swift
.target(
    name: "YourApp",
    dependencies: [.product(name: "BirdRealtime", package: "bird-sdk-swift")]
)
```

## Quickstart

Browsable example: [`examples/quickstart-realtime.swift`](./examples/quickstart-realtime.swift)

```swift
import BirdRealtime

let bird = BirdRealtime(options: .init(
    appKey: "your-app-key",
    region: "us1"          // us1 | eu1, picks the edge automatically
))

let orders = bird.subscribe("orders")
orders.bind("order-updated") { data in
    print("order changed:", data ?? "")
}

bird.onConnectionStateChange { previous, current in
    print("connection: \(previous) → \(current)")
}
bird.onError { error in
    print("realtime error:", error.message)
}
```

### Private and presence channels

Subscriptions to `private-` / `presence-` channels are signed by **your backend**, which holds the app secret. Point the client at your auth endpoint: it POSTs `{"connection_id", "channel_name"}` and expects `{"auth", "member_data"?}` back.

```swift
let bird = BirdRealtime(options: .init(
    appKey: "your-app-key",
    region: "us1",
    authEndpoint: URL(string: "https://your-backend.example.com/bird/auth")!,
    authHeaders: ["authorization": "Bearer <session token>"]
))

guard let room = bird.subscribe("presence-room-42") as? PresenceChannel else { return }
room.bind(BirdProtocol.Event.subscriptionSucceeded) { _ in
    print("me:", room.myId ?? "?", "members:", room.members.keys)
}
room.bind(BirdProtocol.Event.memberAdded) { member in
    print("joined:", member ?? "")
}
```

Supply a custom `authorizer` closure instead to sign through your own networking stack.

Server-side subscription rejections (bad signature, capacity) arrive on the connection rather than the channel, because the wire carries no channel attribution, so observe them with `onError`. An authorizer failure does emit `bird:subscription_error` on the channel.

### Signing in a member

`signin()` tells the edge who this connection belongs to, which is what lets the events API address a member and the disconnect API terminate them. It is also what satisfies an app configured to **require authorized connections**.

```swift
let bird = BirdRealtime(options: .init(
    appKey: "your-app-key",
    region: "us1",
    memberAuthEndpoint: URL(string: "https://your-backend.example.com/bird/auth/member")!
))

let me = try await bird.signin()   // call once; survives reconnects
print("signed in as", me.memberId)

bird.onSigninError { error in
    print("re-signin failed:", error.message)   // still connected, no identity
}
```

Your endpoint receives `{"connection_id"}` and returns `{"auth", "member_data"}`, where `member_data` is the JSON string (carrying `member_id`) that your backend signed.

The identity lives on the connection, so it is dropped when the connection drops and re-established on the next one. The re-signin has no promise to reject, which is what `onSigninError` is for; it is separate from `onError` so a failing member endpoint cannot disturb channel subscriptions.

### Events addressed to a member

Your server can send an event to a member rather than to a channel, reaching every connection that member holds. Once `signin()` succeeds the client subscribes the member's reserved channel automatically; bind on `bird.member`:

```swift
_ = try await bird.signin()

bird.member.bind("order.shipped") { data in
    print("your order moved:", data ?? "")
}
```

Delivery is tied to the identity, not to the process: after a reconnect the client signs in again and resubscribes, and while a connection has no identity nothing arrives.

### Client events

On a subscribed private/presence channel, with the app's client-events setting enabled:

```swift
try room.trigger("client-typing", data: ["on": true])
```

### Connection counting

With the app's connection counting **and** connection count events settings enabled:

```swift
orders.bind(BirdProtocol.Event.connectionCount) { data in
    print("connections:", data ?? "")
}
```

## Behaviour

- **Reconnection** is automatic, with full-jitter exponential backoff (1s base, 30s cap). Close codes 4000–4099 are refusals and terminal (no retry; the code is surfaced through `onError`); 4200–4299 retry immediately; everything else backs off.
- **Liveness**: the client pings after the activity timeout (server-supplied, default 120s) and reconnects if no pong arrives within 30s.
- Channels re-subscribe automatically on every reconnect, with fresh authorization; handlers survive the round-trip.
- **TLS always** for non-loopback hosts. `allowInsecure` is honored only for `localhost`, `127.0.0.1` and `[::1]`, so a copied config cannot silently downgrade real traffic.
- Handlers run on the main queue by default; pass `deliveryQueue` to change that.
- **Errors** all conform to `BirdRealtimeError`, so one `catch` covers the SDK. `RealtimeError` carries the server's close code where there is one; a failing authorization endpoint throws `RealtimeAuthError` with the endpoint and HTTP status instead, so an HTTP 403 never arrives in the field you check for close code 4009. A channel authorization failure reaches `bind("bird:subscription_error")` rather than being thrown, and carries the same `endpoint` and `status` as payload fields.

## Channels and events

| Name prefix | Type     | Authorized |
| ----------- | -------- | ---------- |
| _(none)_    | public   | no         |
| `private-`  | private  | yes        |
| `presence-` | presence | yes        |

`signin()` adds a fourth authorized surface: it signs `<connection_id>::member::<member_data>` rather than a channel name, so a presence auth response can never be replayed as a signin.

Lifecycle events available to `bind`: `bird:subscription_succeeded`, `bird:subscription_error`, `bird:connection_count`, and on presence channels `bird:member_added` / `bird:member_removed`.

## Not yet implemented

- End-to-end encrypted channels (missing from every Bird Realtime client, not just this one)
- `connecting_in` (the countdown to the next reconnect attempt)
- Mobile lifecycle helpers: background/foreground socket handling, `NWPathMonitor` reachability

## Development

```sh
swift build
swift test
```

Tests drive the client against a scripted in-memory transport, so they need no network.

## License

MIT
