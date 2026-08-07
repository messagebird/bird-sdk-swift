#!/usr/bin/env bash
# Post-publish smoke test for github.com/messagebird/bird-sdk-swift.
#
# From a throwaway package, depend on the just-tagged version and build a file
# that references the public client type. This proves the tag is resolvable by
# SwiftPM and that the exported package compiles with no monorepo context: the
# standalone equivalent of "did the release actually produce something
# installable". Import-only by design: it validates packaging, not API calls (a
# real connection would need an app key and a live edge).
#
# Usage: smoke.sh <version-without-leading-v>
# Called by: the mirror release workflow after the tag is pushed.
set -euo pipefail
ver="${1:?usage: smoke.sh <version-without-leading-v>}"

tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT
cd "$tmp"

mkdir -p Sources/Smoke

cat > Package.swift <<SWIFT
// swift-tools-version: 6.0
import PackageDescription

let package = Package(
    name: "smoke",
    dependencies: [
        .package(
            url: "https://github.com/messagebird/bird-sdk-swift.git",
            exact: "${ver}"
        )
    ],
    targets: [
        .executableTarget(
            name: "Smoke",
            dependencies: [
                .product(name: "BirdRealtime", package: "bird-sdk-swift")
            ]
        )
    ]
)
SWIFT

# Reference the public surface rather than only importing: an import alone still
# links a package whose entry points moved, which is the break this catches.
cat > Sources/Smoke/main.swift <<'SWIFT'
import BirdRealtime

let options = BirdRealtimeOptions(appKey: "smoke", region: "eu1")
let client = BirdRealtime(options: options)
precondition(!BirdRealtime.version.isEmpty, "version is empty")
_ = client
SWIFT

# A just-pushed tag can lag in GitHub's git-protocol caches, so retry for ~5
# minutes. Print SwiftPM's own output on the last attempt: swallowing it makes
# propagation lag look identical to a packaging break.
attempts=10
for attempt in $(seq 1 "$attempts"); do
	if out=$(swift build 2>&1); then
		echo "smoke: bird-sdk-swift ${ver} resolved and built"
		exit 0
	fi
	if [ "$attempt" -eq "$attempts" ]; then
		echo "smoke: bird-sdk-swift ${ver} not resolvable after ${attempts} attempts (~5m); last error:" >&2
		printf '%s\n' "$out" >&2
		exit 1
	fi
	sleep 30
	rm -rf .build Package.resolved
done
