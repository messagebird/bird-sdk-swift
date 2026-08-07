// Live smoke runner: connects to a real Bird Realtime app from the terminal
// (the library supports macOS) and prints everything it sees.
//
//   BIRD_APP_KEY=xxx BIRD_REGION=us1 BIRD_CHANNEL=orders swift run BirdRealtimeDemo
//
// Then publish to the channel (dashboard onboarding "Send test event", or the
// publish API) and watch the events arrive.
import BirdRealtime
import Foundation

// Stream events as they happen: stdout is block-buffered when piped, which
// would hold a live log hostage until the buffer fills. Reached through
// fdopen rather than the `stdout` global, which Glibc exposes as a mutable
// var that Swift 6 refuses to read from concurrent code.
setvbuf(fdopen(1, "w"), nil, _IONBF, 0)

let env = ProcessInfo.processInfo.environment
guard let appKey = env["BIRD_APP_KEY"] else {
    print("usage: BIRD_APP_KEY=... [BIRD_REGION=us1] [BIRD_CHANNEL=demo] swift run BirdRealtimeDemo")
    exit(2)
}
let region = env["BIRD_REGION"] ?? "us1"
let channelName = env["BIRD_CHANNEL"] ?? "demo"

let bird = BirdRealtime(options: .init(appKey: appKey, region: region))

bird.onConnectionStateChange { previous, current in
    print("connection: \(previous.rawValue) → \(current.rawValue)")
}
bird.onError { error in
    print("error: code=\(error.code.map(String.init) ?? "-") \(error.message)")
}

let channel = bird.subscribe(channelName)
channel.bind(BirdProtocol.Event.subscriptionSucceeded) { _ in
    print("subscribed to \(channelName) — publish something to it now")
}
channel.bind(BirdProtocol.Event.subscriptionError) { data in
    print("subscription error: \(data ?? "?")")
}
channel.bind(BirdProtocol.Event.connectionCount) { data in
    print("connection count: \(data ?? "?")")
}
// Application events: the demo binds a few likely names; add your own.
for event in ["demo", "test", "order-updated", "message"] {
    channel.bind(event) { data in
        print("event \(event): \(data ?? "(no data)")")
    }
}

print("connecting to \(region) as \(appKey.prefix(6))…, channel \(channelName). Ctrl-C to quit.")
RunLoop.main.run()
