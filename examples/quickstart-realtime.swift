// Quickstart: subscribe to a channel and receive events.
//
// Prerequisites: a Realtime app (dashboard → Realtime → Apps) and its app key.
// Private/presence channels also need an auth endpoint on your backend: sign
// the client's `{ connection_id, channel_name }` with the app secret and
// return `{ auth, member_data? }`.

import BirdRealtime
import Foundation

let bird = BirdRealtime(options: .init(
    appKey: "your-app-key",
    region: "us1",  // us1 | eu1, picks the edge automatically
    // Only needed for private-/presence- channels.
    authEndpoint: URL(string: "https://your-backend.example.com/bird/auth")
))

// Public channel: no authorization required.
let orders = bird.subscribe("orders")
orders.bind("order-updated") { data in
    print("order changed:", data ?? "")
}

// Presence channel: authorized by your backend; tracks who is present.
if let room = bird.subscribe("presence-room-1") as? PresenceChannel {
    room.bind(BirdProtocol.Event.subscriptionSucceeded) { _ in
        print("me:", room.myId ?? "?", "members:", Array(room.members.keys))
    }
    room.bind(BirdProtocol.Event.memberAdded) { member in
        print("joined:", member ?? "")
    }
    room.bind(BirdProtocol.Event.memberRemoved) { member in
        print("left:", member ?? "")
    }

    // Client events on a subscribed private/presence channel.
    try? room.trigger("client-typing", data: ["member_id": "42"])
}

// Connection lifecycle.
bird.onConnectionStateChange { previous, current in
    print("connection: \(previous) → \(current)")
}
