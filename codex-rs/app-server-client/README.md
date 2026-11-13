# codex-app-server-client

Shared in-process app-server client used by conversational CLI surfaces:

- `codex-exec`
- `codex-tui`

## Purpose

This crate centralizes startup and lifecycle management for an in-process
`codex-app-server` runtime, so CLI clients do not need to duplicate:

- app-server bootstrap and initialize handshake
- in-memory request/event transport wiring
- lifecycle orchestration around caller-provided startup identity
- graceful shutdown behavior

## Startup identity

Callers pass both the app-server `SessionSource` and the initialize
`client_info.name` explicitly when starting the facade.

That keeps thread metadata (for example in `thread/list` and `thread/read`)
aligned with the originating runtime without baking TUI/exec-specific policy
into the shared client layer.

## Transport model

The in-process path uses typed channels:

- client -> server: `ClientRequest` / `ClientNotification`
- server -> client: `InProcessServerEvent`
  - `ServerRequest`
  - `ServerNotification`
  - `LegacyNotification`

JSON serialization is still used at external transport boundaries
(stdio/websocket), but the in-process hot path is typed.

Typed requests still receive app-server responses through the JSON-RPC
result envelope internally. That is intentional: the in-process path is
meant to preserve app-server semantics while removing the process
boundary, not to introduce a second response contract.

## Bootstrap behavior

The client facade starts an already-initialized in-process runtime, but
thread bootstrap still follows normal app-server flow:

- caller sends `thread/start` or `thread/resume`
- app-server returns the immediate typed response
- richer session metadata may arrive later as a `SessionConfigured`
  legacy event

Surfaces such as TUI and exec may therefore need a short bootstrap
phase where they reconcile startup response data with later events.

## Backpressure and shutdown

- Command queues and the embedded runtime remain bounded, using
  `DEFAULT_IN_PROCESS_CHANNEL_CAPACITY` by default.
- The facade's local consumer event queue is unbounded and preserves notification
  order. This keeps the worker draining the bounded runtime while a caller waits
  for a request, preventing unread notifications from blocking its response.
- `shutdown()` performs a bounded graceful shutdown and then aborts if timeout
  is exceeded.

## Sovereign read-head callback owner

`SovereignCallbackClient` is an opt-in Linux callback adapter. Construction
requires a request handle, an operator-verified server generation, an absolute
primary Git repository directory, and an explicit set of allowed thread IDs.
It accepts only `witnessops/read_head` with empty arguments. It runs the fixed
local Git read with an empty environment and bounded output, pins the root and
`.git` directory descriptors, and retains them until the child is reaped.
Linked-worktree `.git` files are not admitted.

The embedding surface must retain the adapter and perform these steps:

1. Pass each `ServerRequest` to `dispatch` with the same verified generation.
2. Feed server notifications to `observe_notification` and call `poll` while
   work or cleanup delivery remains pending. Polling only retries status and
   acknowledgement delivery; it never executes a callback again.
3. On transport loss call `disconnected`, retain the owner while its child is
   cancelled and reaped, and continue polling to collect its cleanup result.
4. After reconnecting to the same server instance, call `reconnect` with its new
   request handle and continue polling until `Acknowledged`.
5. Treat `Hold` as unresolved. A dropped object, timeout, missing server state,
   or generation mismatch is not evidence of cleanup or acknowledgement.

The server issues a private per-callback capability to exactly one connection.
The typed `item/tool/cleanup/read` and `item/tool/cleanup/ack` RPCs bind that
capability to the original thread, turn, call and request ID. Acknowledgement
requires a valid response with `cleanupComplete: true`; the server delivers it
to the pinned original core instance before recording acceptance. Invalid replies
leave the obligation pending. Cleanup requests bypass the turn queue so an
interrupt waiting on cleanup cannot block its own acknowledgement.

Completed identities and accepted receipts are retained within a bounded server
lifetime. The adapter admits at most 64 active workers; excess already-issued
requests receive a retained no-execution cleanup result. It shares the server's
16,384-record retention bound. Capabilities must not be logged or published.

Recovery is in memory and requires the original server and retained client
owner. Process-crash recovery and durable outboxes are not implemented. The
adapter is exported for explicit embedding and is wired into CLI exec and TUI
startup only when `--sovereign-read-head` is explicitly selected. Ordinary
sessions keep the normal in-process client. The owner mode is foreground-only;
shared daemon/background routes are refused. Building this crate does not
install or select a runtime. See [the recovery provenance](../../docs/witnessops-recovery.md).
